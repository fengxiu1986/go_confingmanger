package configmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// EtcdSourceName is the source name of etcd
	EtcdSourceName = "etcd"
)

// EtcdSource handles configs from etcd
type EtcdSource struct {
	m sync.RWMutex

	client  *clientv3.Client
	prefixs []string
	// currentConfig map[string]interface{}
	currentConfig map[string]*etcdConfigInfo
	priority      int

	RefreshInterval time.Duration

	eh EventHandler
}

type etcdConfigInfo struct {
	key      string
	prefix   string
	priority int
	value    interface{}
}

var _ ConfigSource = &EtcdSource{}

type label struct {
	Key   string
	Value string
}

type labels []label

func (l labels) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l labels) Len() int           { return len(l) }
func (l labels) Less(i, j int) bool { return strings.Compare(l[i].Value, l[j].Value) < 0 }

// NewEtcdSource initializes all components of etcd
func NewEtcdSource(ci *SourceInfo) (ConfigSource, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{ci.Host},
		DialTimeout: time.Second * 3,
	})
	if err != nil {
		return nil, err
	}
	// 增加etcd连接超时失效，报错返回
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = client.Status(timeoutCtx, ci.Host)
	if err != nil {
		return nil, fmt.Errorf("etcd client status error:%s", err.Error())
	}

	source := &EtcdSource{
		client:        client,
		currentConfig: make(map[string]*etcdConfigInfo),
	}
	if ci.RefreshInterval == 0 {
		source.RefreshInterval = DefaultInterval
	} else {
		source.RefreshInterval = time.Second * time.Duration(ci.RefreshInterval)
	}
	// 按照value排序
	source.prefixs = source.getPrefixs(ci)
	return source, nil
}

func (s *EtcdSource) getPrefixs(ci *SourceInfo) []string {
	if ci == nil {
		return []string{}
	}
	labelList := make(labels, 0)
	for key, value := range ci.Labels {
		labelList = append(labelList, label{Key: key, Value: value})
	}
	sort.Sort(labelList)
	prefixs := make([]string, 0)
	for _, label := range labelList {
		prefixs = append(prefixs, label.Key)
	}
	return prefixs
}

// Set set key value
func (s *EtcdSource) Set(key string, value interface{}, opts ...SetOption) error {
	options := &SetOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if !strings.HasPrefix(key, "/") {
		key = "/" + strings.TrimSpace(strings.ReplaceAll(key, ".", "/"))
	}
	if options.expiresIn != 0 {
		return s.setWithExpire(key, value, options.expiresIn)
	}
	return s.set(key, value)
}

func (s *EtcdSource) setWithExpire(key string, value interface{}, expiresIn int64) error {
	lease := clientv3.NewLease(s.client)
	grant, err := lease.Grant(context.Background(), expiresIn)
	if err != nil {
		return err
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := s.client.Put(context.Background(), key, string(b), clientv3.WithLease(grant.ID)); err != nil {
		return err
	}
	return nil
}

func (s *EtcdSource) set(key string, value interface{}) error {
	if _, ok := value.(string); ok {
		if _, err := s.client.Put(context.Background(), key, value.(string)); err != nil {
			return err
		}
		return nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := s.client.Put(context.Background(), key, string(b)); err != nil {
		return err
	}
	return nil
}

// Delete delete key
func (s *EtcdSource) Delete(key string) error {
	if _, err := s.client.Delete(context.Background(), key); err != nil {
		return err
	}
	return nil
}

// GetConfigurations get all configs
func (s *EtcdSource) GetConfigurations() (map[string]interface{}, error) {
	if len(s.currentConfig) == 0 {
		s.refreshConfigurations()
	}
	// go s.refreshConfigurationsPeriodically()

	configMap := make(map[string]interface{})
	s.m.RLock()
	for key, value := range s.currentConfig {
		configMap[key] = value.value
	}
	s.m.RUnlock()
	return configMap, nil
}

// func (s *EtcdSource) refreshConfigurationsPeriodically() {
// 	ticker := time.Tick(s.RefreshInterval)
// 	for range ticker {
// 		s.refreshConfigurations()
// 	}
// }

func (s *EtcdSource) refreshConfigurations() {
	configs, err := s.pullConfigs()
	if err != nil {
		return
	}
	s.updateConfigAndFireEvent(configs)
}

func (s *EtcdSource) updateConfigAndFireEvent(updateConfig map[string]*etcdConfigInfo) error {
	s.m.Lock()
	defer s.m.Unlock()
	events, err := PopulateEvents(EtcdSourceName, s.currentConfig, updateConfig)
	if err != nil {
		return err
	}
	s.currentConfig = updateConfig
	if s.eh != nil {
		for _, et := range events {
			s.eh.OnEvent(et)
		}
	}
	return nil
}

// pull configs from etcd
func (s *EtcdSource) pullConfigs() (map[string]*etcdConfigInfo, error) {
	updateConfig := make(map[string]*etcdConfigInfo)
	var kvs []*etcdConfigInfo
	for index, prefix := range s.prefixs {
		if resp, err := s.client.Get(context.Background(), prefix, clientv3.WithPrefix()); err == nil {
			// 删除prefix
			for _, kv := range resp.Kvs {
				// kv.Key = kv.Key[len(prefix):]
				// kvs = append(kvs, kv)
				kvs = append(kvs, &etcdConfigInfo{
					prefix:   prefix,
					priority: index,
					key:      s.keyWithoutPrefix(prefix, kv.Key),
					value:    kv.Value,
				})
			}
		}
		go s.watch(prefix, index)
	}
	for _, kv := range kvs {
		// updateConfig[kv.key] = kv
		updateConfig[kv.key] = kv
	}
	return updateConfig, nil
}

func (s *EtcdSource) watch(prefix string, priority int) {
	for {
		rch := s.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.PUT {
					s.update(prefix, priority, ev.Kv)
				}
				if ev.Type == mvccpb.DELETE {
					s.delete(prefix, priority, ev.Kv)
				}
			}
		}
	}
}

func (s *EtcdSource) getConfig(key string) (*etcdConfigInfo, bool) {
	s.m.RLock()
	defer s.m.RUnlock()
	configInfo, ok := s.currentConfig[key]
	return configInfo, ok
}

// 如果优先级高于当前存的值则更新
func (s *EtcdSource) update(prefix string, priority int, kv *mvccpb.KeyValue) {
	key := s.keyWithoutPrefix(prefix, kv.Key)
	configInfo, ok := s.getConfig(key)
	if !ok || priority >= configInfo.priority {
		s.setConfig(prefix, key, priority, kv.Value)
	}
}

func (s *EtcdSource) delete(prefix string, priority int, kv *mvccpb.KeyValue) {
	key := s.keyWithoutPrefix(prefix, kv.Key)
	configInfo, ok := s.getConfig(key)
	if !ok {
		return
	}
	if configInfo.priority == priority && configInfo.prefix == prefix {
		s.deleteConfig(key)
	}
}

func (s *EtcdSource) setConfig(prefix, key string, priority int, value interface{}) {
	s.m.Lock()
	s.currentConfig[key] = &etcdConfigInfo{
		prefix:   prefix,
		key:      key,
		priority: priority,
		value:    value,
	}
	s.m.Unlock()
	s.callback(key, EventTypeUpdate, value)
}

func (s *EtcdSource) deleteConfig(key string) {
	s.m.Lock()
	delete(s.currentConfig, key)
	s.m.Unlock()
	s.callback(key, EventTypeDelete, nil)
}

func (s *EtcdSource) keyWithoutPrefix(prefix string, key []byte) string {
	return strings.Trim(strings.Replace(string(key[len(prefix):]), "/", ".", -1), ".")
}

func (s *EtcdSource) callback(key string, eventType EventType, value interface{}) {
	if s.eh != nil {
		s.eh.OnEvent(&Event{
			EventSource: s.GetSourceName(),
			EventType:   eventType,
			Key:         key,
			Value:       value,
		})
	}
}

// GetConfigurationByKey get config by key
func (s *EtcdSource) GetConfigurationByKey(key string) (interface{}, error) {
	if s.currentConfig == nil {
		return nil, errors.New("etcd source currentconfig is nil")
	}
	s.m.RLock()
	defer s.m.RUnlock()
	val, ok := s.currentConfig[key]
	if ok {
		return val.value, nil
	}
	return nil, fmt.Errorf("etcd source key '%s' not exist", key)
}

// Watch watch
func (s *EtcdSource) Watch(callback EventHandler) error {
	s.eh = callback
	return nil
}

// GetPriority .
func (s *EtcdSource) GetPriority() int {
	return s.priority
}

// SetPriority .
func (s *EtcdSource) SetPriority(priority int) {
	s.priority = priority
}

// Cleanup clean all config
func (s *EtcdSource) Cleanup() error {
	s.m.Lock()
	defer s.m.Unlock()
	s.currentConfig = nil
	return nil
}

// GetSourceName get source name
func (s *EtcdSource) GetSourceName() string {
	return EtcdSourceName
}

// AddDimensionInfo add labels
func (s *EtcdSource) AddDimensionInfo(labels map[string]string) error {
	for label := range labels {
		exist := false
		for _, prefix := range s.prefixs {
			if label == prefix {
				exist = true
				break
			}
		}
		if !exist {
			s.prefixs = append(s.prefixs, label)
		}
	}
	return nil
}

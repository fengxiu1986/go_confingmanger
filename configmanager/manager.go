package configmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"git.yujing.live/Golang/source/pkg/utils"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

// const
const (
	DefaultInterval = time.Second * 5
)

// errors
var (
	ErrKeyNotExist   = errors.New("key does not exist")
	ErrIgnoreChange  = errors.New("ignore key changed")
	ErrWriterInvalid = errors.New("writer is invalid")
)

// Manager 配置管理
type Manager struct {
	m               sync.RWMutex
	sources         map[string]ConfigSource
	configSourceMap sync.Map
	dispatcher      *Dispatcher
	// 持久化
	saveFunc func(event *Event)
	saveOnce sync.Once
	cache    sync.Map
}

var manager *Manager
var _ EventHandler = &Manager{}

func init() {
	manager = &Manager{
		sources:    make(map[string]ConfigSource),
		dispatcher: NewDispatcher(),
	}
}

// AddDefaultSource 添加默认数据源
func AddDefaultSource() {
	// 添加文件监听
	fileSource, err := NewFileSource(utils.GetConfDir())
	if err == nil {
		AddSource(fileSource)
		// 添加环境变量监听
		envSource := NewEnvSource(fmt.Sprintf("%s_", GetString("app", "app")))
		AddSource(envSource)
	}
}

// AddSource .
func AddSource(sources ...ConfigSource) {
	for _, source := range sources {
		manager.m.RLock()
		_, ok := manager.sources[source.GetSourceName()]
		manager.m.RUnlock()
		if !ok {
			source.Watch(manager)
			manager.m.Lock()
			manager.sources[source.GetSourceName()] = source
			manager.m.Unlock()
			configs, err := source.GetConfigurations()
			if err == nil {
				manager.updateConfigurationMap(source, configs)
			}
		}
	}
}

type emptyValueC struct{}

// 警告: 该方法未命中也会缓存，注意不要出现不可控的key，防止内存溢出
func ValueC[T any](key string, convertTo func(val any) (T, error)) (T, error) {
	replace := true
	obj, ok := manager.cache.Load(key)
	if ok {
		// 判断是否为空
		if _, ok = obj.(emptyValueC); ok {
			var t T
			return t, errors.New("not found")
		}
		// 以防类型不相同
		v, ok := obj.(T)
		if ok {
			return v, nil
		}
		// 不应该出现这种情况
		replace = false
	}
	val := manager.getConfig(key)
	if val == nil {
		var t T
		manager.cache.Store(key, emptyValueC{})
		return t, errors.New("not found")
	}
	if convertTo == nil {
		var t T
		return t, errors.New("not supported")
	}
	val, err := convertTo(val)
	if err != nil {
		return val.(T), err
	}
	if replace {
		manager.cache.Store(key, val)
	}
	return val.(T), nil
}

func GetStringC(key string, defaultValue string) string {
	val, err := ValueC(key, cast.ToStringE)
	if err != nil {
		return defaultValue
	}
	return val
}

func GetBoolC(key string, defaultValue bool) bool {
	val, err := ValueC(key, func(val any) (bool, error) {
		strVal, err := cast.ToStringE(val)
		if err != nil {
			return false, err
		}
		return cast.ToBoolE(strVal)
	})
	if err != nil {
		return defaultValue
	}
	return val
}

func GetFloat64C(key string, defaultValue float64) float64 {
	val, err := ValueC(key, func(val any) (float64, error) {
		strVal, err := cast.ToStringE(val)
		if err != nil {
			return 0, err
		}
		return cast.ToFloat64E(strVal)
	})
	if err != nil {
		return defaultValue
	}
	return val
}

func GetFloat32C(key string, defaultValue float32) float32 {
	val, err := ValueC(key, func(val any) (float32, error) {
		strVal, err := cast.ToStringE(val)
		if err != nil {
			return 0, err
		}
		return cast.ToFloat32E(strVal)
	})
	if err != nil {
		return defaultValue
	}
	return val
}

func GetIntC(key string, defaultValue int) int {
	val, err := ValueC(key, func(val any) (int, error) {
		strVal, err := cast.ToStringE(val)
		if err != nil {
			return 0, err
		}
		return cast.ToIntE(strVal)
	})
	if err != nil {
		return defaultValue
	}
	return val
}

func GetInt32C(key string, defaultValue int32) int32 {
	val, err := ValueC(key, func(val any) (int32, error) {
		strVal, err := cast.ToStringE(val)
		if err != nil {
			return 0, err
		}
		return cast.ToInt32E(strVal)
	})
	if err != nil {
		return defaultValue
	}
	return val
}

func GetInt64C(key string, defaultValue int64) int64 {
	val, err := ValueC(key, func(val any) (int64, error) {
		strVal, err := cast.ToStringE(val)
		if err != nil {
			return 0, err
		}
		return cast.ToInt64E(strVal)
	})
	if err != nil {
		return defaultValue
	}
	return val
}

func GetBytesC[T any](key string, defaultValue []byte) []byte {
	val, err := ValueC(key, func(val any) ([]byte, error) {
		strVal, err := cast.ToStringE(val)
		if err != nil {
			return nil, err
		}
		return []byte(strVal), nil
	})
	if err != nil {
		return defaultValue
	}
	return val
}

func GetObjectJsonC[T any](key string) (*T, error) {
	val, err := ValueC(key, func(val any) (*T, error) {
		strVal, err := cast.ToStringE(val)
		if err != nil {
			return nil, err
		}
		var t T
		err = json.Unmarshal([]byte(strVal), &t)
		if err != nil {
			return nil, err
		}
		return &t, nil
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

func GetObjectYamlC[T any](key string) (*T, error) {
	val, err := ValueC(key, func(val any) (*T, error) {
		strVal, err := cast.ToStringE(val)
		if err != nil {
			return nil, err
		}
		var t T
		err = yaml.Unmarshal([]byte(strVal), &t)
		if err != nil {
			return nil, err
		}
		return &t, nil
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Value return value
func Value(key string) interface{} {
	return manager.getConfig(key)
}

// GetBool get key value to bool type
func GetBool(key string, defaultValue bool) bool {
	if val, err := cast.ToBoolE(GetString(key, strconv.FormatBool(defaultValue))); err == nil {
		return val
	}
	return defaultValue
}

// GetString get key value to string type
func GetString(key, defaultValue string) string {
	val := Value(key)
	if val == nil {
		return defaultValue
	}
	if val, err := cast.ToStringE(val); err == nil {
		return val
	}
	return defaultValue
}

// GetFloat64 get key value to float64 type
func GetFloat64(key string, defaultValue float64) float64 {
	if val, err := cast.ToFloat64E(GetString(key, fmt.Sprintf("%f", defaultValue))); err == nil {
		return val
	}
	return defaultValue
}

// GetFloat32 get key value to float32 type
func GetFloat32(key string, defaultValue float32) float32 {
	if val, err := cast.ToFloat32E(GetString(key, fmt.Sprintf("%f", defaultValue))); err == nil {
		return val
	}
	return defaultValue
}

// GetInt get key value to int type
func GetInt(key string, defaultValue int) int {
	if val, err := cast.ToIntE(GetString(key, fmt.Sprintf("%d", defaultValue))); err == nil {
		return val
	}
	return defaultValue
}

// GetInt32 get key value to int type
func GetInt32(key string, defaultValue int32) int32 {
	if val, err := cast.ToInt32E(GetString(key, fmt.Sprintf("%d", defaultValue))); err == nil {
		return val
	}
	return defaultValue
}

// GetInt64 get key value to int type
func GetInt64(key string, defaultValue int64) int64 {
	if val, err := cast.ToInt64E(GetString(key, fmt.Sprintf("%d", defaultValue))); err == nil {
		return val
	}
	return defaultValue
}

// GetBytes get key value to bytes type
func GetBytes(key string, defaultValue []byte) []byte {
	val := Value(key)
	if val == nil {
		return defaultValue
	}
	if val, err := cast.ToStringE(val); err == nil {
		return []byte(val)
	}
	return defaultValue
}

// GetJsonUnmarshal get key value and unmarshal with json
func GetJsonUnmarshal(key string, out interface{}) error {
	return json.Unmarshal(GetBytes(key, []byte("{}")), out)
}

// GetYamlUnmarshal get key value and unmarshal with yaml
func GetYamlUnmarshal(key string, out interface{}) error {
	return yaml.Unmarshal(GetBytes(key, []byte{}), out)
}

// GetJsonUnmarshalAndWatch get key value and watch
func GetJsonUnmarshalAndWatch(key string, out interface{}, fn func(event *Event)) error {
	if err := GetJsonUnmarshal(key, out); err != nil {
		return err
	}
	return Watch(key, fn)
}

// GetYamlUnmarshalAndWatch get key value and watch
func GetYamlUnmarshalAndWatch(key string, out interface{}, fn func(event *Event)) error {
	if err := GetYamlUnmarshal(key, out); err != nil {
		return err
	}
	return Watch(key, fn)
}

// Set add config
func Set(key string, value interface{}, opts ...SetOption) error {
	return manager.set(key, value, opts...)
}

// Save 持久化
func Save(saveFunc func(event *Event)) {
	manager.save(saveFunc)
}

// Delete delete config
func Delete(key string) error {
	return manager.delete(key)
}

// RegisterListener to Register all listener for different key changes, each key could be a regular expression
func RegisterListener(listener Listener, key ...string) error {
	return manager.dispatcher.RegisterListener(listener, key...)
}

// Watch 监听key
func Watch(key string, fn func(event *Event)) error {
	return manager.dispatcher.RegisterListener(&listener{fn: fn}, key)
}

// UnRegisterListener remove listener
func UnRegisterListener(listener Listener, key ...string) error {
	return manager.dispatcher.UnRegisterListener(listener, key...)
}

func (m *Manager) save(saveFunc func(event *Event)) {
	m.saveOnce.Do(func() {
		m.saveFunc = saveFunc
	})
}

// GetConfig get config
func (m *Manager) getConfig(key string) interface{} {
	sourceName, ok := m.configSourceMap.Load(key)
	if !ok {
		return nil
	}
	m.m.RLock()
	source, ok := m.sources[sourceName.(string)]
	m.m.RUnlock()
	if !ok {
		return nil
	}
	val, err := source.GetConfigurationByKey(key)
	if err != nil {
		nbSource := m.findNextBestSource(key, sourceName.(string))
		if nbSource != nil {
			val, _ = nbSource.GetConfigurationByKey(key)
			return val
		}
		return nil
	}
	return val
}

// OnEvent on event handler
func (m *Manager) OnEvent(event *Event) {
	m.cache.Delete(event.Key)
	m.onEvent(event)
	m.dispatcher.DispatchEvent(event)
}

func (m *Manager) onEvent(e *Event) error {
	// refresh all configuration one by one
	if e == nil || e.EventSource == "" || e.Key == "" {
		return errors.New("nil or invalid event supplied")
	}
	if e.HasUpdated {
		return nil
	}
	switch e.EventType {
	case EventTypeCreate, EventTypeUpdate:
		log.Println("on event: ", e.Key, "type: ", e.EventType)
		sourceName, ok := m.configSourceMap.Load(e.Key)
		if !ok {
			m.configSourceMap.Store(e.Key, e.EventSource)
			e.EventType = EventTypeCreate
			if m.saveFunc != nil {
				m.saveFunc(e)
			}
		} else if sourceName == e.EventSource {
			e.EventType = EventTypeUpdate
			if m.saveFunc != nil {
				m.saveFunc(e)
			}
		} else if sourceName != e.EventSource {
			e.EventType = EventTypeUpdate
			prioritySrc := m.getHighPrioritySource(sourceName.(string), e.EventSource)
			if prioritySrc != nil && prioritySrc.GetSourceName() == sourceName {
				// if event generated from less priority source then ignore
				return ErrIgnoreChange
			}
			m.configSourceMap.Store(e.Key, e.EventSource)
			if m.saveFunc != nil {
				m.saveFunc(e)
			}
		}

	case EventTypeDelete:
		sourceName, ok := m.configSourceMap.Load(e.Key)
		if !ok || sourceName != e.EventSource {
			// if delete event generated from source not maintained ignore it
			return ErrIgnoreChange
		} else if sourceName == e.EventSource {
			// find less priority source or delete key
			source := m.findNextBestSource(e.Key, sourceName.(string))
			if source == nil {
				m.configSourceMap.Delete(e.Key)
				if m.saveFunc != nil {
					m.saveFunc(e)
				}
			} else {
				m.configSourceMap.Store(e.Key, source.GetSourceName())
				e.EventType = EventTypeUpdate
				e.Value = m.getConfig(e.Key)
				if m.saveFunc != nil {
					m.saveFunc(e)
				}
			}
		}
	}
	e.HasUpdated = true
	return nil
}

// OnModuleEvent Triggers actions when events are generated
func (m *Manager) OnModuleEvent(event []*Event) {
	m.updateModuleEvent(event)
}

func (m *Manager) updateModuleEvent(es []*Event) error {
	if len(es) == 0 {
		return errors.New("nil or invalid events supplied")
	}

	var validEvents []*Event
	for i := 0; i < len(es); i++ {
		err := m.onEvent(es[i])
		if err != nil {
			continue
		}
		validEvents = append(validEvents, es[i])
	}

	if len(validEvents) == 0 {
		return nil
	}

	return m.dispatcher.DispatchModuleEvent(validEvents)
}

func (m *Manager) getHighPrioritySource(srcNameA, srcNameB string) ConfigSource {
	m.m.RLock()
	sourceA, okA := m.sources[srcNameA]
	sourceB, okB := m.sources[srcNameB]
	m.m.RUnlock()

	if !okA && !okB {
		return nil
	} else if !okA {
		return sourceB
	} else if !okB {
		return sourceA
	}

	if sourceA.GetPriority() < sourceB.GetPriority() { //less value has high priority
		return sourceA
	}

	return sourceB
}

func (m *Manager) findNextBestSource(key string, sourceName string) ConfigSource {
	var rSource ConfigSource
	m.m.RLock()
	for _, source := range m.sources {
		if source.GetSourceName() == sourceName {
			continue
		}
		value, err := source.GetConfigurationByKey(key)
		if err != nil || value == nil {
			continue
		}
		if rSource == nil {
			rSource = source
			continue
		}
		if source.GetPriority() < rSource.GetPriority() { // less value has high priority
			rSource = source
		}
	}
	m.m.RUnlock()

	return rSource
}

func (m *Manager) updateConfigurationMap(source ConfigSource, configs map[string]interface{}) error {
	for key, value := range configs {
		sourceName, ok := m.configSourceMap.Load(key)
		if !ok { // if key do not exist then add source
			m.configSourceMap.Store(key, source.GetSourceName())
			log.Println(key, "not found", m.saveFunc != nil)
			if m.saveFunc != nil {
				go m.saveFunc(&Event{
					EventType:   EventTypeCreate,
					Key:         key,
					Value:       value,
					EventSource: source.GetSourceName(),
				})
			}
			continue
		}
		m.m.RLock()
		currentSource, ok := m.sources[sourceName.(string)]
		m.m.RUnlock()
		log.Println(key, "exist", m.saveFunc != nil, ok, currentSource.GetSourceName())
		if !ok {
			log.Println(currentSource.GetSourceName(), "not exist")
			m.configSourceMap.Store(key, source.GetSourceName())
			if m.saveFunc != nil {
				go m.saveFunc(&Event{
					EventType:   EventTypeCreate,
					Key:         key,
					Value:       value,
					EventSource: source.GetSourceName(),
				})
			}
			continue
		}
		log.Println("currency source: ", currentSource.GetSourceName(), "priority: ", currentSource.GetPriority(),
			"source: ", source.GetSourceName(), "priority: ", source.GetPriority())
		if currentSource.GetPriority() >= source.GetPriority() { // lesser value has high priority
			log.Println(key, "has hight priotiry", m.saveFunc != nil)
			m.configSourceMap.Store(key, source.GetSourceName())
			if m.saveFunc != nil {
				go m.saveFunc(&Event{
					EventType:   EventTypeUpdate,
					Key:         key,
					Value:       value,
					EventSource: source.GetSourceName(),
				})
			}
		}
	}

	return nil
}

// Set set key value of all sources
func (m *Manager) set(key string, value interface{}, opts ...SetOption) error {
	m.m.RLock()
	defer m.m.RUnlock()
	for _, source := range m.sources {
		if err := source.Set(key, value, opts...); err != nil {
			return err
		}
	}
	return nil
}

// Delete delete key of all sources
func (m *Manager) delete(key string) error {
	m.m.RLock()
	defer m.m.RUnlock()
	for _, source := range m.sources {
		if err := source.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

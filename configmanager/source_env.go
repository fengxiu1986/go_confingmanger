/*
 * Copyright 2017 Huawei Technologies Co., Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//Package configmanager created on 2017/6/22.
package configmanager

import (
	"os"
	"strings"
	"sync"
)

const (
	envSourceConst            = "EnvironmentSource"
	envVariableSourcePriority = 3
)

//EnvSource is a struct
type EnvSource struct {
	Configs  sync.Map
	priority int
	prefixs  []string
	eh       EventHandler
}

var _ ConfigSource = &EtcdSource{}

//NewEnvSource configures a new environment configuration
func NewEnvSource(prefixs ...string) ConfigSource {
	envSource := &EnvSource{
		priority: envVariableSourcePriority,
		prefixs:  prefixs,
	}
	envSource.pullConfigurations()
	return envSource
}

func (es *EnvSource) pullConfigurations() {
	es.Configs = sync.Map{}
	for _, value := range os.Environ() {
		rs := []rune(value)
		in := strings.Index(value, "=")
		key := string(rs[0:in])
		for _, prefix := range es.prefixs {
			if strings.HasPrefix(key, prefix) {
				value := string(rs[in+1:])
				key = strings.TrimPrefix(key, prefix)
				envKey := strings.Replace(key, "_", ".", -1)
				es.Configs.Store(key, value)
				es.Configs.Store(envKey, value)
			}
		}

	}
}

//GetConfigurations gets all configuration
func (es *EnvSource) GetConfigurations() (map[string]interface{}, error) {
	configMap := make(map[string]interface{})
	es.Configs.Range(func(k, v interface{}) bool {
		configMap[k.(string)] = v
		return true
	})

	return configMap, nil
}

//GetConfigurationByKey gets required configuration for a particular key
func (es *EnvSource) GetConfigurationByKey(key string) (interface{}, error) {
	value, ok := es.Configs.Load(key)
	if !ok {
		return nil, ErrKeyNotExist
	}
	return value, nil
}

//GetPriority returns priority of environment configuration
func (es *EnvSource) GetPriority() int {
	return es.priority
}

//SetPriority custom priority
func (es *EnvSource) SetPriority(priority int) {
	es.priority = priority
}

//GetSourceName returns the name of environment source
func (*EnvSource) GetSourceName() string {
	return envSourceConst
}

//Watch dynamically handles a environment configuration
func (es *EnvSource) Watch(callback EventHandler) error {
	es.eh = callback
	return nil
}

//Cleanup cleans a particular environment configuration up
func (es *EnvSource) Cleanup() error {
	es.Configs = sync.Map{}
	return nil
}

//AddDimensionInfo no use
func (es *EnvSource) AddDimensionInfo(labels map[string]string) error {
	return nil
}

//Set no use
func (es *EnvSource) Set(key string, value interface{}, opts ...SetOption) error {
	return nil
}

//Delete no use
func (es *EnvSource) Delete(key string) error {
	return nil
}

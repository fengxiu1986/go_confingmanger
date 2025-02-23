package configmanager

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"gopkg.in/yaml.v2"
)

//FileHandler decide how to convert a file content into key values
//archaius will manage file content as those key values
type FileHandler func(filePath string, content []byte) (map[string]interface{}, error)

//Convert2JavaProps is a FileHandler
//it convert the yaml content into java props
func Convert2JavaProps(p string, content []byte) (map[string]interface{}, error) {
	configMap := make(map[string]interface{})

	ss := yaml.MapSlice{}
	err := yaml.Unmarshal([]byte(content), &ss)
	if err != nil {
		return nil, fmt.Errorf("yaml unmarshal [%s] failed, %s", content, err)
	}
	configMap = retrieveItems("", ss)

	return configMap, nil
}
func retrieveItems(prefix string, subItems yaml.MapSlice) map[string]interface{} {
	if prefix != "" {
		prefix += "."
	}

	result := map[string]interface{}{}

	for _, item := range subItems {
		//check the item key first
		k, ok := checkKey(item.Key)
		if !ok {
			continue
		}
		//If there are sub-items existing
		switch item.Value.(type) {
		//sub items in a map
		case yaml.MapSlice:
			subResult := retrieveItems(prefix+item.Key.(string), item.Value.(yaml.MapSlice))
			for k, v := range subResult {
				result[k] = v
			}

		// sub items in an array
		case []interface{}:
			keyVal := item.Value.([]interface{})
			result[prefix+k] = retrieveItemInSlice(keyVal)

		// sub item is a string
		case string:
			result[prefix+k] = ExpandValueEnv(item.Value.(string))

		// sub item in other type
		default:
			result[prefix+k] = item.Value

		}

	}

	return result
}

func checkKey(key interface{}) (string, bool) {
	k, ok := key.(string)
	if !ok {
		return "", false
	}
	return k, true
}

func retrieveItemInSlice(value []interface{}) []interface{} {
	for i, v := range value {
		switch v.(type) {
		case yaml.MapSlice:
			value[i] = retrieveItems("", v.(yaml.MapSlice))
		case string:
			value[i] = ExpandValueEnv(v.(string))
		default:
			//do nothing
		}
	}
	return value
}

//UseFileNameAsKeyContentAsValue is a FileHandler, it sets the yaml file name as key and the content as value
func UseFileNameAsKeyContentAsValue(p string, content []byte) (map[string]interface{}, error) {
	_, filename := filepath.Split(p)
	configMap := make(map[string]interface{})
	configMap[filename] = content
	return configMap, nil
}

//Convert2configMap is legacy API
func Convert2configMap(p string, content []byte) (map[string]interface{}, error) {
	return UseFileNameAsKeyContentAsValue(p, content)
}

// The name of a variable can contain only letters (a to z or A to Z), numbers ( 0 to 9) or
// the underscore character ( _), and can't begin with number.
const envVariable = `\${([a-zA-Z_]{1}[\w]+)((?:\,\,|\^\^)?)[\|]{2}(.*?)}`

// reg exp
var variableReg *regexp.Regexp

func init() {
	variableReg = regexp.MustCompile(envVariable)
}

// ExpandValueEnv if string like ${NAME||archaius}
// will query environment variable for ${NAME}
// if environment variable is "" return default string `archaius`
// support multi variable, eg:
//    value string => addr:${IP||127.0.0.1}:${PORT||8080}
//    if environment variable =>  IP=0.0.0.0 PORT=443 , result => addr:0.0.0.0:443
//    if no exist environment variable                , result => addr:127.0.0.1:8080
func ExpandValueEnv(value string) (realValue string) {
	value = strings.TrimSpace(value)
	submatch := variableReg.FindAllStringSubmatch(value, -1)
	if len(submatch) == 0 {
		return value
	}

	realValue = value
	for _, sub := range submatch {
		if len(sub) != 4 { //rule matching behaves in an unexpected way
			continue
		}
		item := os.Getenv(sub[1])
		if item == "" {
			item = sub[3]
		} else {
			if sub[2] == "^^" {
				item = strings.ToUpper(item)
			} else if sub[2] == ",," {
				item = strings.ToLower(item)
			}
		}
		realValue = strings.ReplaceAll(realValue, sub[0], item)
	}

	return
}

//PopulateEvents compare old and new configurations to generate events that need to be updated
func PopulateEvents(sourceName string, currentConfig, updatedConfig map[string]*etcdConfigInfo) ([]*Event, error) {
	events := make([]*Event, 0)

	// generate create and update event
	for key, value := range updatedConfig {
		if currentConfig != nil {
			currentValue, ok := currentConfig[key]
			if !ok { // if new configuration introduced
				events = append(events, constructEvent(sourceName, EventTypeCreate, key, value.value))
			} else if !reflect.DeepEqual(currentValue.value, value.value) {
				events = append(events, constructEvent(sourceName, EventTypeUpdate, key, value.value))
			}
		} else {
			events = append(events, constructEvent(sourceName, EventTypeCreate, key, value.value))
		}
	}

	// generate delete event
	for key, value := range currentConfig {
		_, ok := updatedConfig[key]
		if !ok { // when old config not present in new config
			events = append(events, constructEvent(sourceName, EventTypeDelete, key, value.value))
		}
	}
	return events, nil
}

func constructEvent(sourceName string, eventType EventType, key string, value interface{}) *Event {
	newEvent := new(Event)
	newEvent.EventSource = sourceName
	newEvent.EventType = eventType
	newEvent.Key = key
	newEvent.Value = value

	return newEvent
}

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

// Package configmanager created on 2017/6/22.
package configmanager

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
    // FileConfigSourceConst is a variable of type string
    FileConfigSourceConst = "FileSource"
    fileSourcePriority    = 3
)

// FileSourceTypes is a string
type FileSourceTypes string

const (
    // RegularFile is a variable of type string
    RegularFile FileSourceTypes = "RegularFile"
    // Directory is a variable of type string
    Directory FileSourceTypes = "Directory"
    // InvalidFileType is a variable of type string
    InvalidFileType FileSourceTypes = "InvalidType"
)

// ConfigInfo is s struct
type ConfigInfo struct {
    FilePath string
    Value    interface{}
}

// FileSource is file source
type FileSource struct {
    Configurations map[string]*ConfigInfo
    files          []file
    fileHandlers   map[string]FileHandler
    watchPool      *watch
    filelock       sync.Mutex
    priority       int
    sync.RWMutex
}

type file struct {
    filePath string
    priority uint32
}

type watch struct {
    // files   []string
    watcher    *fsnotify.Watcher
    callback   EventHandler
    fileSource *FileSource
    sync.RWMutex
}

/*
	accepts files and directories as file-source
  		1. Directory: all files considered as file source
  		2. File: specified file considered as file source

  	TODO: Currently file sources priority not considered. if key conflicts then latest key will get considered
*/

// FileSourcer is a interface
type FileSourcer interface {
    ConfigSource
    AddFile(filePath string, priority uint32, handler FileHandler) error
}

var _ FileSourcer = &FileSource{}

// NewFileSource creates a source which can handler local files
func NewFileSource(files ...string) (FileSourcer, error) {
    fileSource := &FileSource{
        priority:     fileSourcePriority,
        files:        make([]file, 0),
        fileHandlers: make(map[string]FileHandler),
    }
    for _, file := range files {
        if err := fileSource.AddFile(file, fileSourcePriority, Convert2JavaProps); err != nil {
            return nil, err
        }
    }
    return fileSource, nil
}

// AddFile add file and manage configs
func (fSource *FileSource) AddFile(p string, priority uint32, handle FileHandler) error {
    path, err := filepath.Abs(p)
    if err != nil {
        return err
    }
    // check existence of file
    fs, err := os.Open(path)
    if os.IsNotExist(err) {
        return fmt.Errorf("[%s] file not exist", path)
    }
    defer fs.Close()

    // prevent duplicate file source
    if fSource.isFileSrcExist(path) {
        return nil
    }
    fSource.fileHandlers[path] = handle
    fileType := fileType(fs)
    switch fileType {
    case Directory:
        // handle Directory input. Include all files as file
        err := fSource.handleDirectory(fs, priority, handle)
        if err != nil {
            return err
        }
    case RegularFile:
        // handle file and include as file
        err := fSource.handleFile(fs, priority, handle)
        if err != nil {
            return err
        }
    case InvalidFileType:
        return fmt.Errorf("file type of [%s] not supported", path)
    }

    if fSource.watchPool != nil {
        fSource.watchPool.AddWatchFile(path)
    }

    return nil
}

func (fSource *FileSource) isFileSrcExist(filePath string) bool {
    var exist bool
    for _, file := range fSource.files {
        if filePath == file.filePath {
            return true
        }
    }
    return exist
}

func fileType(fs *os.File) FileSourceTypes {
    fileInfo, err := fs.Stat()
    if err != nil {
        return InvalidFileType
    }

    fileMode := fileInfo.Mode()

    if fileMode.IsDir() {
        return Directory
    } else if fileMode.IsRegular() {
        return RegularFile
    }

    return InvalidFileType
}

func (fSource *FileSource) handleDirectory(dir *os.File, priority uint32, handle FileHandler) error {

    filesInfo, err := dir.Readdir(-1)
    if err != nil {
        return errors.New("failed to read Directory contents")
    }

    for _, fileInfo := range filesInfo {
        filePath := filepath.Join(dir.Name(), fileInfo.Name())

        fs, err := os.Open(filePath)
        if err != nil {
            continue
        }

        fSource.handleFile(fs, priority, handle)
        fs.Close()

    }

    return nil
}

func (fSource *FileSource) handleFile(file *os.File, priority uint32, handle FileHandler) error {
    Content, err := ioutil.ReadFile(file.Name())
    if err != nil {
        return err
    }
    var config map[string]interface{}
    if handle != nil {
        config, err = handle(file.Name(), Content)
    } else {
        config, err = Convert2JavaProps(file.Name(), Content)
    }
    if err != nil {
        return fmt.Errorf("failed to pull configurations from [%s] file, %s", file.Name(), err)
    }

    err = fSource.handlePriority(file.Name(), priority)
    if err != nil {
        return fmt.Errorf("failed to handle priority of [%s], %s", file.Name(), err)
    }

    events := fSource.compareUpdate(config, file.Name())
    if fSource.watchPool != nil && fSource.watchPool.callback != nil { // if file source already added and try to add
        for _, e := range events {
            fSource.watchPool.callback.OnEvent(e)
        }
        fSource.watchPool.callback.OnModuleEvent(events)
    }

    return nil
}

func (fSource *FileSource) handlePriority(filePath string, priority uint32) error {
    fSource.Lock()
    newFilePriority := make([]file, 0)
    var prioritySet bool
    for _, f := range fSource.files {

        if f.filePath == filePath && f.priority == priority {
            prioritySet = true
            newFilePriority = append(newFilePriority, file{
                filePath: filePath,
                priority: priority,
            })
        }
        newFilePriority = append(newFilePriority, f)
    }

    if !prioritySet {
        newFilePriority = append(newFilePriority, file{
            filePath: filePath,
            priority: priority,
        })
    }

    fSource.files = newFilePriority
    fSource.Unlock()

    return nil
}

// GetConfigurations get all configs
func (fSource *FileSource) GetConfigurations() (map[string]interface{}, error) {
    configMap := make(map[string]interface{})

    fSource.Lock()
    defer fSource.Unlock()
    for key, confInfo := range fSource.Configurations {
        if confInfo == nil {
            configMap[key] = nil
            continue
        }

        configMap[key] = confInfo.Value
    }

    return configMap, nil
}

// GetConfigurationByKey get one key value
func (fSource *FileSource) GetConfigurationByKey(key string) (interface{}, error) {
    fSource.RLock()
    defer fSource.RUnlock()

    confInfo := fSource.Configurations[key]
    if confInfo != nil{
        return confInfo.Value, nil
    }

    return nil, ErrKeyNotExist
}

// GetSourceName get name of source
func (*FileSource) GetSourceName() string {
    return FileConfigSourceConst
}

// GetPriority get precedence
func (fSource *FileSource) GetPriority() int {
    return fSource.priority
}

// SetPriority custom priority
func (fSource *FileSource) SetPriority(priority int) {
    fSource.priority = priority
}

// Watch watch change event
func (fSource *FileSource) Watch(callback EventHandler) error {
    if callback == nil {
        return errors.New("call back can not be nil")
    }

    watchPool, err := newWatchPool(callback, fSource)
    if err != nil {
        return err
    }

    fSource.watchPool = watchPool

    go fSource.watchPool.startWatchPool()

    return nil
}

func newWatchPool(callback EventHandler, cfgSrc *FileSource) (*watch, error) {
    watcher, err := fsnotify.NewWatcher()
    if err != nil {
        log.Printf("FileSourcer newWatchPool fsnotify.NewWatcher err:%v \n", err)
        return nil, err
    }

    watch := new(watch)
    watch.callback = callback
    watch.fileSource = cfgSrc
    watch.watcher = watcher
    return watch, nil
}

func (wth *watch) startWatchPool() {
    go wth.watchFile()
    for _, file := range wth.fileSource.files {
        f, err := filepath.Abs(file.filePath)
        if err != nil {
            return
        }

        err = wth.watcher.Add(f)
        log.Printf("FileSourcer startWatchPool add file:%s,err:%v \n", f, err)
        if err != nil {
            return
        }
    }
}

func (wth *watch) AddWatchFile(filePath string) {
    err := wth.watcher.Add(filePath)
    if err != nil {
        return
    }
}

func (wth *watch) watchFile() {
    for {
        select {
        case et, ok := <-wth.watcher.Events:
            if !ok {
                return
            }

            if strings.HasSuffix(et.Name, ".swx") || strings.HasSuffix(et.Name, ".swp") || strings.HasSuffix(et.Name, "~") {
                // ignore
                continue
            }

            if et.Op == fsnotify.Remove {
                continue
            }

            if et.Op == fsnotify.Rename {
                wth.watcher.Remove(et.Name)
                // check existence of file
                _, err := os.Open(et.Name)
                if !os.IsNotExist(err) {
                    wth.AddWatchFile(et.Name)
                }

                continue
            }
            log.Printf("FileSourcer watchFile change:%+v \n", et)
            if et.Op == fsnotify.Create {
                time.Sleep(time.Millisecond)
            }
            handle := wth.fileSource.fileHandlers[et.Name]
            if handle == nil {
                handle = Convert2JavaProps
            }
            content, err := ioutil.ReadFile(et.Name)
            if err != nil {
                continue
            }

            newConf, err := handle(et.Name, content)
            if err != nil {
                continue
            }
            events := wth.fileSource.compareUpdate(newConf, et.Name)
            if len(events) > 0 { // avoid OnModuleEvent empty events error
                for _, e := range events {
                    wth.callback.OnEvent(e)
                }
                wth.callback.OnModuleEvent(events)
            }
        case errMsg := <-wth.watcher.Errors:
            log.Printf("FileSourcer watchFile err:%v, watch_file_list:%v \n", errMsg, wth.watcher.WatchList())
            return
        }
    }

}

func (fSource *FileSource) compareUpdate(configs map[string]interface{}, filePath string) []*Event {
    events := make([]*Event, 0)
    fileConfs := make(map[string]*ConfigInfo)
    if fSource == nil {
        return nil
    }

    fSource.Lock()
    defer fSource.Unlock()

    var filePathPriority uint32 = math.MaxUint32
    for _, file := range fSource.files {
        if file.filePath == filePath {
            filePathPriority = file.priority
        }
    }

    if filePathPriority == math.MaxUint32 {
        return nil
    }

    // update and delete with latest configs

    for key, confInfo := range fSource.Configurations {
        if confInfo == nil {
            continue
        }

        switch confInfo.FilePath {
        case filePath:
            newConfValue, ok := configs[key]
            if !ok {
                events = append(events, &Event{EventSource: FileConfigSourceConst, Key: key,
                    EventType: EventTypeDelete, Value: confInfo.Value})
                continue
            } else if reflect.DeepEqual(confInfo.Value, newConfValue) {
                fileConfs[key] = confInfo
                continue
            }

            confInfo.Value = newConfValue
            fileConfs[key] = confInfo

            events = append(events, &Event{EventSource: FileConfigSourceConst, Key: key,
                EventType: EventTypeUpdate, Value: newConfValue})

        default: // configuration file not same
            // no need to handle delete scenario
            // only handle if configuration conflicts between two sources
            newConfValue, ok := configs[key]
            if ok {
                var priority uint32 = math.MaxUint32
                for _, file := range fSource.files {
                    if file.filePath == confInfo.FilePath {
                        priority = file.priority
                    }
                }

                if priority == filePathPriority {
                    fileConfs[key] = confInfo

                } else if filePathPriority < priority { // lower the vale higher is the priority
                    confInfo.Value = newConfValue
                    fileConfs[key] = confInfo
                    events = append(events, &Event{EventSource: FileConfigSourceConst,
                        Key: key, EventType: EventTypeUpdate, Value: newConfValue})

                } else {
                    fileConfs[key] = confInfo
                }
            } else {
                fileConfs[key] = confInfo
            }
        }
    }

    // create add/create new config
    fileConfs, events = fSource.addOrCreateConf(fileConfs, configs, events, filePath)

    fSource.Configurations = fileConfs

    return events
}

func (fSource *FileSource) addOrCreateConf(fileConfs map[string]*ConfigInfo, newconf map[string]interface{},
    events []*Event, filePath string) (map[string]*ConfigInfo, []*Event) {
    for key, value := range newconf {
        handled := false

        _, ok := fileConfs[key]
        if ok {
            handled = true
        }

        if !handled {
            events = append(events, &Event{EventSource: FileConfigSourceConst, Key: key,
                EventType: EventTypeCreate, Value: value})
            fileConfs[key] = &ConfigInfo{
                FilePath: filePath,
                Value:    value,
            }
        }
    }

    return fileConfs, events
}

// Cleanup clear all configs
func (fSource *FileSource) Cleanup() error {
    fSource.filelock.Lock()
    defer fSource.filelock.Unlock()

    if fSource.watchPool != nil && fSource.watchPool.watcher != nil {
        fSource.watchPool.watcher.Close()
    }

    fSource.files = make([]file, 0)
    fSource.Configurations = make(map[string]*ConfigInfo, 0)
    return nil
}

// AddDimensionInfo  is none function
func (fSource *FileSource) AddDimensionInfo(labels map[string]string) error {
    return nil
}

// Set no use
func (fSource *FileSource) Set(key string, value interface{}, opts ...SetOption) error {
    return nil
}

// Delete no use
func (fSource *FileSource) Delete(key string) error {
    return nil
}

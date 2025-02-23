package configmanager

import (
	"fmt"
	"testing"
	"time"

	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

func TestBool(t *testing.T) {
	etcdSource, err := NewEtcdSource(&SourceInfo{
		Host: "172.16.130.27:2379",
		Labels: map[string]string{
			"/platform/config/global/":           "1",
			"/platform/config/srv_admin/":        "2",
			"/platform/config/srv_admin/tenant/": "3",
		},
		RefreshInterval: 3,
	})
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	AddSource(etcdSource)
	t.Log(GetBool("cache.enabled", true))
	t.Log(GetString("cache.enabled", "not found"))
}

func TestFloat64(t *testing.T) {
	etcdSource, err := NewEtcdSource(&SourceInfo{
		Host: "172.16.130.23:2379",
		Labels: map[string]string{
			"/platform/config/global/":           "1",
			"/platform/config/srv_admin/":        "2",
			"/platform/config/srv_admin/tenant/": "3",
		},
		RefreshInterval: 3,
	})
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	AddSource(etcdSource)
	t.Log(GetFloat64("rate_limit.limit", 0))
	t.Log(GetString("rate_limit.limit", "0"))
	t.Log(GetInt("rate_limit.limit", 0))
}

func TestEtcdConfig(t *testing.T) {
	etcdSource, err := NewEtcdSource(&SourceInfo{
		Host: "172.16.130.23:2379",
		Labels: map[string]string{
			"/platform/config/global/":           "1",
			"/platform/config/srv_admin/":        "2",
			"/platform/config/srv_admin/tenant/": "3",
		},
		RefreshInterval: 3,
	})
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	AddSource(etcdSource)
	data := []struct {
		key    string
		expect string
	}{
		// 只写在global中
		{key: "config_global", expect: "global"},
		// 写在global和srv中， srv会替换global
		{key: "config_srv", expect: "srv"},
		// 写在global、srv、tenant中，tenant替换srv，srv替换global
		{key: "config_tenant", expect: "tenant123"},
	}
	for _, d := range data {
		if out := GetString(d.key, "not found"); out != d.expect {
			t.Errorf("get key '%s' value '%s' expect '%s'", d.key, out, d.expect)
		}
	}

}

func TestFileConfig(t *testing.T) {
	fileSource, err := NewFileSource("/home/p8s8wangkai/workspace/src/git.yujing.live/Golang/source/configmanager/conf")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	AddSource(fileSource)
	for i := 0; i < 100; i++ {
		t.Log("a.b output: ", GetString("a.b", "not found"))
		t.Log("a.float64 output: ", GetFloat64("a.float64", 0))
		time.Sleep(1 * time.Second)
	}
	t.Log("service.name:", GetString("service.name", ""))
	envSource := NewEnvSource(fmt.Sprintf("%s_", GetString("app", "app")))
	AddSource(envSource)
}

func TestEnvConfig(t *testing.T) {
	envSource := NewEnvSource("_")
	AddSource(envSource)
	for i := 0; i < 100; i++ {
		t.Log("output: ", GetString("test_env", "not found"))
		t.Log("output: ", GetString("test.env", "not found"))
		time.Sleep(1 * time.Second)
	}
}

func TestConfig(t *testing.T) {
	fileSource, err := NewFileSource("./test.yaml")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	etcdSource, err := NewEtcdSource(&SourceInfo{
		Host: "172.16.130.23:2379",
		Labels: map[string]string{
			"/game/": "1",
		},
		RefreshInterval: 3,
	})
	envSource := NewEnvSource("game_")
	AddSource(fileSource, etcdSource, envSource)
	for i := 0; i < 100; i++ {
		t.Log("output: ", GetInt("game.test", 123))
		time.Sleep(1 * time.Second)
	}
}

type testListen struct{}

func (t *testListen) Event(event *Event) {
	fmt.Printf("----event: %+v\n", event)
}

func TestListener(t *testing.T) {
	etcdSource, err := NewEtcdSource(&SourceInfo{
		Host: "172.16.130.23:2379",
		Labels: map[string]string{
			"/game/": "1",
		},
		RefreshInterval: 3,
	})
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	AddSource(etcdSource)
	RegisterListener(&testListen{}, "test123")
	time.Sleep(60 * time.Second)
}

func TestWatch(t *testing.T) {
	etcdSource, err := NewEtcdSource(&SourceInfo{
		Host: "172.16.130.23:2379",
		Labels: map[string]string{
			"/platform/config/global/": "1",
		},
		RefreshInterval: 3,
	})
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	AddSource(etcdSource)
	Watch("test_watch", func(event *Event) {
		t.Logf("event: %s, key: %s, value:%s", event.EventType, event.Key, cast.ToString(event.Value))
	})
	time.Sleep(60 * time.Second)
}

func TestYamlUnmarshalAndWatch(t *testing.T) {
	etcdSource, err := NewEtcdSource(&SourceInfo{
		Host: "172.16.130.23:2379",
		Labels: map[string]string{
			"/platform/config/global/": "1",
		},
		RefreshInterval: 3,
	})
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	AddSource(etcdSource)
	testYaml := make(map[int64]int)
	GetYamlUnmarshalAndWatch("test_yaml_watch", &testYaml, func(event *Event) {
		if event.EventType == EventTypeDelete {
			testYaml = nil
		}
		if event.EventType == EventTypeCreate || event.EventType == EventTypeUpdate {
			t.Logf("key: %s changed", event.Key)
			if err := yaml.Unmarshal([]byte(cast.ToString(event.Value)), &testYaml); err != nil {
				t.Error(err.Error())
			}
		}
		t.Logf("watch: %+v", testYaml)
	})

	GetYamlUnmarshalAndWatch("yaml_watch", &testYaml, func(event *Event) {
		if event.EventType == EventTypeDelete {
			testYaml = nil
		}
		if event.EventType == EventTypeCreate || event.EventType == EventTypeUpdate {
			t.Logf("key: %s changed", event.Key)
			if err := yaml.Unmarshal([]byte(cast.ToString(event.Value)), &testYaml); err != nil {
				t.Error(err.Error())
			}
		}
		t.Logf("watch: %+v", testYaml)
	})
	t.Logf("%+v", testYaml)
	time.Sleep(60 * time.Second)
}

func TestJsonUnmarshal(t *testing.T) {
	etcdSource, err := NewEtcdSource(&SourceInfo{
		Host: "172.16.130.23:2379",
		Labels: map[string]string{
			"/platform/config/global/": "1",
		},
		RefreshInterval: 3,
	})
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	AddSource(etcdSource)
	testJson := make(map[int64]int)
	if err := GetJsonUnmarshal("test_json", &testJson); err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	t.Logf("%+v", testJson)
}

func TestYamlUnmarshal(t *testing.T) {
	etcdSource, err := NewEtcdSource(&SourceInfo{
		Host: "172.16.130.23:2379",
		Labels: map[string]string{
			"/platform/config/global/": "1",
		},
		RefreshInterval: 3,
	})
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	AddSource(etcdSource)
	testYaml := make(map[int64]int)
	if err := GetYamlUnmarshal("test_yaml", &testYaml); err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	t.Logf("%+v", testYaml)
}

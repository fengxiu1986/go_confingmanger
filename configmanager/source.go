package configmanager

// ConfigSource get key values from a system
type ConfigSource interface {
	Set(key string, value interface{}, opts ...SetOption) error
	Delete(key string) error
	GetConfigurations() (map[string]interface{}, error)
	GetConfigurationByKey(string) (interface{}, error)
	Watch(EventHandler) error
	GetPriority() int
	SetPriority(priority int)
	Cleanup() error
	GetSourceName() string

	AddDimensionInfo(labels map[string]string) error
}

// SourceInfo .
type SourceInfo struct {
	Host            string
	RefreshInterval int
	Labels          map[string]string
}

type SetOptions struct {
	// 失效时间，单位秒
	expiresIn int64
}

type SetOption func(opts *SetOptions)

func WithExpiresIn(expiresIn int64) func(opts *SetOptions) {
	return func(opts *SetOptions) {
		opts.expiresIn = expiresIn
	}
}

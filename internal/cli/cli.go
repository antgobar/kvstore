package cli

type Client interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

type Cli struct {
	Client
}

package conf

import (
	"github.com/BurntSushi/toml"
	"sync"
)

var TomlConf TomlConfig
type TomlConfig struct {
	Nodes int64
	Replicate int64
	NodesHTTP []string
	MiddleWareRPC string
	MiddleWareHTTP string
}

var once sync.Once

// 生成multi-raft配置
func ConfigInit(){
	once.Do(func() {
		toml.DecodeFile("conf.toml", &TomlConf)
	})


}

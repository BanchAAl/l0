package config

type DBConfig struct {
	UserName string `json:"user_name"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	Database string `json:"database"`
}

const (
	DefaultDBDatabase    = "wbdb"
	DefaultNatsSubject   = "foo"
	DefaultDBPassword    = "12345678"
	DefaultDBHost        = "localhost"
	DefaultDBPort        = "5433"
	DefaultDBUserName    = "wbstudy"
	DefaultNatsClasterID = "test-cluster"
	DefaultNatsClientID  = "stan-sub"
	DefaultNatsQGroup    = ""
	MaxAttempts          = 5
)

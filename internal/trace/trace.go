package trace

type Logger interface {
	Println(v ...interface{})
	Printf(format string, v ...interface{})
}

type NOOPLogger struct{}

func (NOOPLogger) Println(v ...interface{})               {}
func (NOOPLogger) Printf(format string, v ...interface{}) {}

var (
	ERR Logger = NOOPLogger{}
	INF Logger = NOOPLogger{}
	WRN Logger = NOOPLogger{}
	DBG Logger = NOOPLogger{}
)

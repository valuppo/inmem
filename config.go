package inmem

type InmemConfig struct {
	IsEnabled         bool
	NSQD              string
	Lookupd           string
	ConnectionNumber  int
	MaximumInFlight   int
	ConcurrentHandler int
	MaxAttempt        int
}

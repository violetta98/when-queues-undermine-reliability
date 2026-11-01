module qlab

// franz-go/pkg/kadm v1.19.0 declares go >= 1.26.0, so this floor is set by
// the dependency rather than by anything in this module.
go 1.26.0

require (
	github.com/rabbitmq/amqp091-go v1.16.0
	github.com/twmb/franz-go v1.22.2
	github.com/twmb/franz-go/pkg/kadm v1.19.0
)

require (
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.0 // indirect
)

package subscriber

import (
	"os"
	"os/signal"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"
	"github.com/rs/zerolog/log"

	"l0/pkg/config"
)

// ReadNats висим и читаем всё, что приходит в стрим
func ReadNats(exit chan bool, msgChannel chan *stan.Msg) {
	var (
		unsubscribe bool
		durable     string
	)

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Can't connect to NATS")
	}
	defer nc.Close()

	sc, err := stan.Connect(config.DefaultNatsClasterID, config.DefaultNatsClientID, stan.NatsConn(nc),
		stan.SetConnectionLostHandler(func(_ stan.Conn, err error) { log.Fatal().Err(err).Msg("Connection to NATS lost") }))
	if err != nil {
		log.Fatal().Err(err).Str("NATS URL", nats.DefaultURL).Msg("Can't connect to NATS")
	}
	defer sc.Close()

	log.Debug().Str("NATS URL", nats.DefaultURL).Str("NATS claster ID", config.DefaultNatsClasterID).Str("NATS client ID", config.DefaultNatsClientID).Msg("Connected to NATS ok")

	startOpt := stan.StartAt(pb.StartPosition_NewOnly)
	mcb := func(msg *stan.Msg) {
		msgChannel <- msg
	}

	sub, errSub := sc.QueueSubscribe(config.DefaultNatsSubject, config.DefaultNatsQGroup, mcb, startOpt)
	if errSub != nil {
		log.Fatal().Err(errSub).Str("NATS subject", config.DefaultNatsSubject).Msg("Can't subscribe to subject")
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	for range signalChan {
		log.Debug().Str("Package", "Subscriber").Msg("Received an interrupt, unsubscribing and closing connection...")
		if durable == "" || unsubscribe {
			sub.Unsubscribe()
		}
		exit <- true
	}

}

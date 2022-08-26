package main

import (
	"context"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/nats-io/stan.go"
	"github.com/rs/zerolog/log"

	"l0/pkg/cache"
	"l0/pkg/config"
	"l0/pkg/dbclient"
	"l0/pkg/models"
	subscriber "l0/pkg/natsstrm"
)

func main() {

	configDB := config.DBConfig{config.DefaultDBUserName, config.DefaultDBPassword, config.DefaultDBHost, config.DefaultDBPort, config.DefaultDBDatabase}
	db, err := dbclient.NewClient(context.Background(), configDB, config.MaxAttempts)
	if err != nil {
		log.Fatal().Err(err).Msg("Connect to DB Failed")
	}

	cacheStorage := cache.New(0, 0)

	err = cacheStorage.Init(context.Background(), db)
	if err != nil {
		log.Fatal().Err(err).Msg("Init cache fail")
	}

	log.Debug().Int("Cache size", cacheStorage.Size()).Msg("Cache init OK")

	exit := make(chan bool)
	msgChannel := make(chan *stan.Msg)
	orderChannel := make(chan models.Order)

	go subscriber.ReadNats(exit, msgChannel)
	go dbclient.Start(context.Background(), db, msgChannel, orderChannel)
	go cacheStorage.Start(orderChannel)

	app := fiber.New()
	app.Use(cors.New())

	app.Get("/getallids", func(ctx *fiber.Ctx) error {
		return ctx.JSON(cacheStorage.AllIDs())
	})

	app.Get("/getorder/:id", func(ctx *fiber.Ctx) error {

		id := ctx.Params("id")
		log.Debug().Str("ID", id).Msg("getorder")
		cacheData, ok := cacheStorage.Get(id)
		if !ok {
			return ctx.SendStatus(500)
		}

		return ctx.JSON(cacheData)
	})

	go func() {
		if errF := app.Listen(":9000"); errF != nil {
			log.Fatal().Err(errF).Msg("http server not started")
		}
	}()

	<-exit
	log.Debug().Msg("Gracefully shutting down...")
	_ = app.Shutdown()
}

package dbclient

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/stan.go"
	"l0/pkg/models"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog/log"

	"l0/pkg/config"
)

type Client interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error
}

func Start(ctx context.Context, pool *pgxpool.Pool, msgChannel chan *stan.Msg, cacheChannel chan models.Order) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	for {
		select {
		case msg := <-msgChannel:
			var order models.Order
			err := json.Unmarshal(msg.Data, &order)
			if err != nil {
				log.Error().Err(err).Msg("Unmarshal order errors. Unknown json struct.")
				continue
			}
			log.Debug().Str("User ID", order.OrderUid).Msg("Unmarshal order OK")

			_, err = pool.Query(ctx, "insert into study.orders(id, data) values($1, $2)", order.OrderUid, msg.Data)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {

					log.Fatal().Err(pgErr).Str("Message", pgErr.Message).Str("Details", pgErr.Detail).Str("Code", pgErr.Code).
						Str("ColumnName", pgErr.ColumnName).Str("ConstraintName", pgErr.ConstraintName).Str("DataTypeName", pgErr.DataTypeName).
						Str("InternalQuery", pgErr.InternalQuery).Str("SchemaName", pgErr.SchemaName).Str("TableName", pgErr.TableName).
						Str("Where", pgErr.Where).Int("Line", int(pgErr.Line)).Int("Position", int(pgErr.Position)).Str("SQLState", pgErr.SQLState()).
						Msg("Error write message to DB")
				}
				continue
			}
			cacheChannel <- order
		case <-signalChan:
			log.Debug().Str("Package", "dbclient").Msg("Received an interrupt")
			return
		}
	}
}

func NewClient(ctx context.Context, dbc config.DBConfig, maxAttemps int) (pool *pgxpool.Pool, err error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", dbc.UserName, dbc.Password, dbc.Host, dbc.Port, dbc.Database)

	for i := 0; i < maxAttemps; i++ {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		pool, err = pgxpool.Connect(ctx, dsn)
		if err != nil {
			log.Error().Err(err).Msg("failed connected to DB. Reconnected...")
			time.Sleep(1 * time.Second)
			continue
		}
		log.Debug().Str("connected string", dsn).Msg("DB connected OK")
		return
	}

	return
}

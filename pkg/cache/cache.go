package cache

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog/log"
	"l0/pkg/models"
	"os"
	"os/signal"
	"sync"
	"time"
)

// Cache описание структуры кэша
type Cache struct {
	sync.RWMutex
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
	items             map[string]Item
}

// Item описание структуры значения в кэше
type Item struct {
	Value      interface{}
	Created    time.Time
	Expiration int64
}

// New инициализация хранилища
// defaultExpiration — время жизни кеша по-умолчанию, если установлено значение меньше или равно 0 — время жизни кеша бессрочно.
// cleanupInterval — интервал между удалением просроченного кеша. При установленном значении меньше или равно 0 — очистка
// и удаление просроченного кеша не происходит.
func New(defaultExpiration, cleanupInterval time.Duration) *Cache {
	items := make(map[string]Item)

	cache := Cache{
		items:             items,
		defaultExpiration: defaultExpiration,
		cleanupInterval:   cleanupInterval,
	}

	// запускаем регулярную очистку старых данных
	if cleanupInterval > 0 {
		cache.StartGC()
	}

	return &cache
}

func (c *Cache) Init(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, "SELECT * FROM study.orders")
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			log.Fatal().Err(pgErr).Str("Message error", pgErr.Message).Str("Details error", pgErr.Detail).Msg("Error read data for init")
		}
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id    string
			order models.Order
		)
		err = rows.Scan(&id, &order)
		if err != nil {
			fmt.Printf("scan err %s", err.Error())
			log.Fatal().Err(err).Msg("Error scan data for init")
			return err
		}
		if id == order.OrderUid {
			c.Set(order.OrderUid, order, 0)
		}
	}

	return err
}

func (c *Cache) Start(orderChannel chan models.Order) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	for {
		select {
		case order := <-orderChannel:
			log.Debug().Int("Cache size", c.Size()).Msg("Old cache size")
			c.Set(order.OrderUid, order, 0)
			log.Debug().Int("Cache size", c.Size()).Msg("Order added")
		case <-signalChan:
			log.Debug().Str("Package", "Cache").Msg("Received an interrupt")
			return
		}
	}
}

// Size сделал её только для дебага
func (c *Cache) Size() int {
	return len(c.items)
}

// StartGC запуск очистки устаревших данных
func (c *Cache) StartGC() {
	go c.GC()
}

// GC очистка устаревших данных
func (c *Cache) GC() {

	for {
		<-time.After(c.cleanupInterval)

		if c.items == nil {
			return
		}

		if keys := c.expiredKeys(); len(keys) != 0 {
			c.clearItems(keys)

		}

	}

}

// Set запись данных в кэш
func (c *Cache) Set(key string, value interface{}, duration time.Duration) {

	var expiration int64

	if duration == 0 {
		duration = c.defaultExpiration
	}

	if duration > 0 {
		expiration = time.Now().Add(duration).UnixNano()
	}

	c.Lock()

	defer c.Unlock()

	c.items[key] = Item{
		Value:      value,
		Expiration: expiration,
		Created:    time.Now(),
	}
}

// Get получение значения из кэша
func (c *Cache) Get(key string) (interface{}, bool) {

	c.RLock()

	defer c.RUnlock()

	item, found := c.items[key]

	if !found {
		return nil, false
	}

	if item.Expiration > 0 {
		if time.Now().UnixNano() > item.Expiration {
			return nil, false
		}
	}

	log.Debug().Str("user ID", key).Msg("Cache get message")
	return item.Value, true
}

// Delete удаляет элемент по ключу, если ключа не существует, возвращает ошибку
func (c *Cache) Delete(key string) error {

	c.Lock()

	defer c.Unlock()

	if _, found := c.items[key]; !found {
		return errors.New("key not found")
	}

	delete(c.items, key)

	return nil
}

func (c *Cache) AllIDs() []string {
	keys := make([]string, len(c.items))

	i := 0
	for k := range c.items {
		keys[i] = k
		i++
	}
	return keys
}

// expiredKeys возвращает список просроченных ключей
func (c *Cache) expiredKeys() (keys []string) {

	c.RLock()

	defer c.RUnlock()

	for k, i := range c.items {
		if time.Now().UnixNano() > i.Expiration && i.Expiration > 0 {
			keys = append(keys, k)
		}
	}

	return
}

// clearItems удаляет данные по переданным ключам
func (c *Cache) clearItems(keys []string) {

	c.Lock()

	defer c.Unlock()

	for _, k := range keys {
		delete(c.items, k)
	}
}

package inmem

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"

	"fmt"
	"time"

	nsq "github.com/nsqio/go-nsq"
	"github.com/robfig/cron"
)

const dbTopic = "topchannel_inmem"

type dbOperation int

const (
	dbOperationSet dbOperation = iota
	dbOperationHset
	dbOperationSadd
	dbOperationSpop
	dbOperationIncr
	dbOperationDecr
	dbOperationDel
	dbOperationExpire
)

const dbDefaultExpire int64 = 3600

type dbMessage struct {
	Operation dbOperation
	Key       string
	Field     string
	Data      interface{}
}

type DB struct {
	conf           InmemConfig
	producer       *nsq.Producer
	scheduler      *cron.Cron
	dataBasic      map[string]interface{}
	lockDataBasic  sync.RWMutex
	dataHash       map[string]map[string]interface{}
	lockDataHash   sync.RWMutex
	dataSet        map[string]map[interface{}]interface{}
	lockDataSet    sync.RWMutex
	dataSetKey     map[string][]interface{}
	lockDataSetKey sync.RWMutex
	dataNum        map[string]int
	lockDataNum    sync.RWMutex
	dataTTL        map[string]time.Time
	lockDataTTL    sync.RWMutex
}

func New(conf InmemConfig) (*DB, error) {
	cfg := nsq.NewConfig()

	producer, err := nsq.NewProducer(conf.NSQD, cfg)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	cr := cron.New()

	db := &DB{
		conf:           conf,
		producer:       producer,
		scheduler:      cr,
		dataBasic:      make(map[string]interface{}),
		lockDataBasic:  sync.RWMutex{},
		dataHash:       make(map[string]map[string]interface{}),
		lockDataHash:   sync.RWMutex{},
		dataSet:        make(map[string]map[interface{}]interface{}),
		lockDataSet:    sync.RWMutex{},
		dataSetKey:     make(map[string][]interface{}),
		lockDataSetKey: sync.RWMutex{},
		dataNum:        make(map[string]int),
		lockDataNum:    sync.RWMutex{},
		dataTTL:        make(map[string]time.Time),
		lockDataTTL:    sync.RWMutex{},
	}

	db.registerUsedTypes()
	db.scheduler.Start()

	if err := db.startConsumer(); err != nil {
		log.Println(err)
		return nil, err
	}

	return db, nil
}

func (db *DB) SET(key string, data interface{}) error {
	dbMsg, err := db.composeMessage(dbOperationSet, key, "", data)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := db.producer.Publish(dbTopic, dbMsg); err != nil {
		log.Println(err)
		return err
	}

	if err := db.EXPIRE(key, dbDefaultExpire); err != nil {
		log.Println(err)
	}

	return nil
}

func (db *DB) GET(key string) (interface{}, error) {
	db.lockDataBasic.RLock()
	data, ok := db.dataBasic[key]
	db.lockDataBasic.RUnlock()

	if !ok {
		return nil, ErrKeyNotExist
	}

	return data, nil
}

func (db *DB) HSET(key string, field string, data interface{}) error {
	dbMsg, err := db.composeMessage(dbOperationHset, key, field, data)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := db.producer.Publish(dbTopic, dbMsg); err != nil {
		log.Println(err)
		return err
	}

	if err := db.EXPIRE(key, dbDefaultExpire); err != nil {
		log.Println(err)
	}

	return nil
}

func (db *DB) HMSET(key string, fields []string, datas []interface{}) error {
	for i := 0; i < len(fields) && i < len(datas); i++ {
		if err := db.HSET(key, fields[i], datas[i]); err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}

func (db *DB) HGET(key string, field string) (interface{}, error) {
	db.lockDataHash.RLock()
	data, ok := db.dataHash[key][field]
	db.lockDataHash.RUnlock()

	if !ok {
		return nil, ErrKeyNotExist
	}

	return data, nil
}

func (db *DB) HGETALL(key string) (map[string]interface{}, error) {
	db.lockDataHash.RLock()
	dataMap, ok := db.dataHash[key]
	db.lockDataHash.RUnlock()

	if !ok {
		return nil, ErrKeyNotExist
	}
	return dataMap, nil
}

func (db *DB) SADD(key string, datas ...interface{}) error {
	dbMsg, err := db.composeMessage(dbOperationSadd, key, "", datas)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := db.producer.Publish(dbTopic, dbMsg); err != nil {
		log.Println(err)
		return err
	}

	if err := db.EXPIRE(key, dbDefaultExpire); err != nil {
		log.Println(err)
	}

	return nil
}

func (db *DB) SPOP(key string) (interface{}, error) {
	db.lockDataSetKey.RLock()
	_, ok := db.dataSetKey[key]
	db.lockDataSetKey.RUnlock()

	if !ok {
		return nil, ErrKeyNotExist
	}

	if len(db.dataSetKey[key]) <= 0 {
		return nil, ErrSetEmpty
	}

	dbMsg, err := db.composeMessage(dbOperationSpop, key, "", nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if err := db.producer.Publish(dbTopic, dbMsg); err != nil {
		log.Println(err)
		return nil, err
	}

	db.lockDataSetKey.RLock()
	lastKey := db.dataSetKey[key][len(db.dataSetKey[key])-1]
	db.lockDataSetKey.RUnlock()

	db.lockDataSet.RLock()
	data := db.dataSet[key][lastKey]
	db.lockDataSet.RUnlock()

	return data, nil
}

func (db *DB) SMEMBERS(key string) ([]interface{}, error) {
	db.lockDataSetKey.RLock()
	dataKeys, ok := db.dataSetKey[key]
	db.lockDataSetKey.RUnlock()

	if !ok {
		return nil, ErrKeyNotExist
	}

	datas := make([]interface{}, len(dataKeys))
	for i, dataKey := range dataKeys {
		db.lockDataSet.RLock()
		datas[i] = db.dataSet[key][dataKey]
		db.lockDataSet.RUnlock()
	}

	return datas, nil
}

func (db *DB) SISMEMBER(key string, member interface{}) (bool, error) {
	db.lockDataSet.RLock()
	_, ok := db.dataSet[key]
	db.lockDataSet.RUnlock()

	if !ok {
		return false, ErrKeyNotExist
	}

	db.lockDataSetKey.RLock()
	_, ok = db.dataSet[key][member]
	db.lockDataSetKey.RUnlock()

	if ok {
		return true, nil
	}
	return false, nil
}

func (db *DB) SCARD(key string) (int, error) {
	db.lockDataSet.RLock()
	datas, ok := db.dataSet[key]
	db.lockDataSet.RUnlock()

	if !ok {
		return -1, ErrKeyNotExist
	}
	return len(datas), nil
}

func (db *DB) DEL(keys ...string) error {
	for _, key := range keys {
		dbMsg, err := db.composeMessage(dbOperationDel, key, "", nil)
		if err != nil {
			log.Println(err)
			return err
		}

		if err := db.producer.Publish(dbTopic, dbMsg); err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}

func (db *DB) INCR(key string) error {
	dbMsg, err := db.composeMessage(dbOperationIncr, key, "", nil)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := db.producer.Publish(dbTopic, dbMsg); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (db *DB) DECR(key string) error {
	dbMsg, err := db.composeMessage(dbOperationDecr, key, "", nil)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := db.producer.Publish(dbTopic, dbMsg); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (db *DB) GETNUM(key string) int {
	db.lockDataNum.RLock()
	num := db.dataNum[key]
	db.lockDataNum.RUnlock()

	return num
}

//Expire time in seconds
func (db *DB) EXPIRE(key string, expire int64) error {
	dbMsg, err := db.composeMessage(dbOperationExpire, key, "", expire)
	if err != nil {
		log.Println(err)
		return err
	}

	if err := db.producer.Publish(dbTopic, dbMsg); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (db *DB) TTL(key string) (time.Time, error) {
	db.lockDataTTL.RLock()
	data, ok := db.dataTTL[key]
	db.lockDataTTL.RUnlock()

	if !ok {
		return time.Time{}, ErrKeyNotExist
	}
	return data, nil
}

func (db *DB) delete(key string) {
	db.lockDataBasic.Lock()
	delete(db.dataBasic, key)
	db.lockDataBasic.Unlock()

	db.lockDataHash.Lock()
	delete(db.dataHash, key)
	db.lockDataHash.Unlock()

	db.lockDataSet.Lock()
	delete(db.dataSet, key)
	db.lockDataSet.Unlock()

	db.lockDataSetKey.Lock()
	delete(db.dataSetKey, key)
	db.lockDataSetKey.Unlock()

	db.lockDataNum.Lock()
	delete(db.dataNum, key)
	db.lockDataNum.Unlock()

	db.lockDataTTL.Lock()
	delete(db.dataTTL, key)
	db.lockDataTTL.Unlock()
}

func (db *DB) startConsumer() error {
	for i := 0; i < db.conf.ConnectionNumber; i++ {
		config := nsq.NewConfig()
		config.Set("max_attempts", 200)
		consumer, err := nsq.NewConsumer(dbTopic, getLocalIP(), config)
		if err != nil {
			log.Println(err)
			return err
		}

		consumer.ChangeMaxInFlight(db.conf.MaximumInFlight)
		consumer.AddConcurrentHandlers(nsq.HandlerFunc(db.parseMessage), db.conf.ConcurrentHandler)

		if err := consumer.ConnectToNSQLookupd(db.conf.Lookupd); err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}

func (db *DB) composeMessage(operation dbOperation, key string, field string, data interface{}) ([]byte, error) {
	dbMsg := dbMessage{
		Operation: operation,
		Key:       key,
		Field:     field,
		Data:      data,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(dbMsg); err != nil {
		log.Println(err)
		return nil, err
	}

	return buf.Bytes(), nil
}

func (db *DB) parseMessage(m *nsq.Message) error {
	dbMsg := dbMessage{}

	buf := bytes.NewBuffer(m.Body)
	if err := gob.NewDecoder(buf).Decode(&dbMsg); err != nil {
		log.Println(err)
		return err
	}

	op := dbOperation(dbMsg.Operation)
	key := dbMsg.Key
	field := dbMsg.Field
	data := dbMsg.Data

	switch op {
	case dbOperationSet:
		db.lockDataBasic.Lock()
		db.dataBasic[key] = data
		db.lockDataBasic.Unlock()
	case dbOperationHset:
		if db.dataHash[key] == nil {
			db.lockDataHash.Lock()
			db.dataHash[key] = make(map[string]interface{})
			db.lockDataHash.Unlock()
		}
		db.lockDataHash.Lock()
		db.dataHash[key][field] = data
		db.lockDataHash.Unlock()
	case dbOperationSadd:
		if db.dataSet[key] == nil {
			db.lockDataSet.Lock()
			db.dataSet[key] = make(map[interface{}]interface{})
			db.lockDataSet.Unlock()
		}
		datas, _ := data.([]interface{})
		for _, data := range datas {
			db.lockDataSet.RLock()
			_, ok := db.dataSet[key][data]
			db.lockDataSet.RUnlock()
			if !ok {
				db.lockDataSet.Lock()
				db.dataSet[key][data] = data
				db.lockDataSet.Unlock()

				db.lockDataSetKey.Lock()
				db.dataSetKey[key] = append(db.dataSetKey[key], data)
				db.lockDataSetKey.Unlock()
			}
		}
	case dbOperationSpop:
		if len(db.dataSetKey[key]) > 0 {
			db.lockDataSetKey.RLock()
			lastKey := db.dataSetKey[key][len(db.dataSetKey[key])-1]
			db.lockDataSet.RUnlock()

			delete(db.dataSet[key], lastKey)

			db.lockDataSetKey.Lock()
			_, db.dataSetKey[key] = db.dataSetKey[key][len(db.dataSetKey[key])-1], db.dataSetKey[key][:len(db.dataSetKey[key])-1]
			db.lockDataSetKey.Unlock()

			if len(db.dataSet[key]) <= 0 || len(db.dataSetKey[key]) <= 0 {
				delete(db.dataSet, key)
				delete(db.dataSetKey, key)
			}
		}
	case dbOperationIncr:
		db.lockDataNum.Lock()
		db.dataNum[key]++
		db.lockDataNum.Unlock()
	case dbOperationDecr:
		db.lockDataNum.Lock()
		db.dataNum[key]--
		db.lockDataNum.Unlock()
	case dbOperationDel:
		db.delete(key)
	case dbOperationExpire:
		exp := data.(int64)
		expTime := time.Now().Add(time.Duration(exp) * time.Second)

		db.lockDataTTL.Lock()
		db.dataTTL[key] = expTime
		db.lockDataTTL.Unlock()

		db.schedule(key, expTime, func() error {
			db.delete(key)
			return nil
		})
	}
	return nil
}

func (db *DB) schedule(key string, schTime time.Time, callback func() error) {
	spec := fmt.Sprintf("%d %d %d %d %d *", schTime.Second(), schTime.Minute(), schTime.Hour(), schTime.Day(), int(schTime.Month()))
	db.scheduler.AddFunc(spec, func() {
		if err := callback(); err != nil {
			log.Println(err)
		}
	})
}

func (db *DB) registerUsedTypes() {
	//Common
	interfaces := make([]interface{}, 0)
	gob.Register(interfaces)
}

package badger_db_wrapper

import (
	"encoding/json"
	"sync"

	"github.com/dgraph-io/badger/v4"
	
)

type BadgerDB struct {
	db   *badger.DB
	lock sync.RWMutex
}

// NewBadgerDB initializes a new BadgerDB instance
func NewBadgerDB(path string) (*BadgerDB, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &BadgerDB{db: db}, nil
}

// InsertMap stores a map under a given key
func (b *BadgerDB) InsertMap(key string, data map[string]interface{}) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), jsonData)
	})
}

// RetrieveMap fetches a map by key
func (b *BadgerDB) RetrieveMap(key string) (map[string]interface{}, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	var result map[string]interface{}

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &result)
		})
	})

	return result, err
}

// DeleteMap removes a map by key
func (b *BadgerDB) DeleteMap(key string) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// InsertString stores a string under a given key
func (b *BadgerDB) InsertString(key, value string) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(value))
	})
}

// RetrieveString fetches a string by key
func (b *BadgerDB) RetrieveString(key string) (string, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	var result string

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			result = string(val)
			return nil
		})
	})

	return result, err
}

// DeleteString removes a string by key
func (b *BadgerDB) DeleteString(key string) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// Close closes the database connection
func (b *BadgerDB) Close() error {
	return b.db.Close()
}

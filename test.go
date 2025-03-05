package badger_db_wrapper

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBadgerDB(t *testing.T) {
	path := "./test_badger_db"
	os.RemoveAll(path)
	defer os.RemoveAll(path)

	db, err := NewBadgerDB(path)
	assert.NoError(t, err)
	defer db.Close()

	// Test InsertMap and RetrieveMap
	mapData := map[string]interface{}{"name": "Alice", "age": 30}
	err = db.InsertMap("user:1", mapData)
	assert.NoError(t, err)

	retrievedMap, err := db.RetrieveMap("user:1")
	assert.NoError(t, err)
	assert.Equal(t, mapData, retrievedMap)

	// Test DeleteMap
	err = db.DeleteMap("user:1")
	assert.NoError(t, err)

	retrievedMap, err = db.RetrieveMap("user:1")
	assert.Error(t, err)
	assert.Nil(t, retrievedMap)

	// Test InsertString and RetrieveString
	err = db.InsertString("greeting", "Hello, World!")
	assert.NoError(t, err)

	retrievedString, err := db.RetrieveString("greeting")
	assert.NoError(t, err)
	assert.Equal(t, "Hello, World!", retrievedString)

	// Test DeleteString
	err = db.DeleteString("greeting")
	assert.NoError(t, err)

	retrievedString, err = db.RetrieveString("greeting")
	assert.Error(t, err)
	assert.Empty(t, retrievedString)
}

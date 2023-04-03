package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.False(t, iter.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.RandomValue(10))
	assert.Nil(t, err)

	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.True(t, iter.Valid())
	assert.NotNil(t, iter.Key())
	value, err := iter.Value()
	assert.Nil(t, err)
	assert.NotNil(t, value)

	iter.Next()
	assert.False(t, iter.Valid())
}

func TestDB_Iterator_Multi_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("alice"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bob"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("houston"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("kafka"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("zero"), utils.RandomValue(10))
	assert.Nil(t, err)

	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Seek([]byte("c"))
	assert.Equal(t, []byte("houston"), iter1.Key())

	// 反向遍历
	iter2 := db.NewIterator(IteratorOptions{Reverse: true})
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Seek([]byte("m"))
	assert.Equal(t, []byte("kafka"), iter2.Key())

	// 指定 prefix
	iter3 := db.NewIterator(IteratorOptions{prefix: []byte("z")})
	iter3.Rewind()
	assert.Equal(t, []byte("zero"), iter3.Key())
}

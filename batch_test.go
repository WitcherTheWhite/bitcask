package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 事务未提交
	wb1 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb1.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb1.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 提交事务
	err = wb1.Commit()
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 删除数据
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val2)

	err = wb2.Commit()
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val3)
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 测试事务未提交前重启数据库
	wb1 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb1.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Close()
	assert.Nil(t, err)

	db, err = Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	val, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, val)
	assert.Equal(t, ErrKeyNotFound, err)
}

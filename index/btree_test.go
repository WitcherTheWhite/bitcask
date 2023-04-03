package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    10,
		Offset: 100,
	})
	assert.True(t, res1)

	res2 := bt.Put([]byte("hsy"), &data.LogRecordPos{
		Fid:    5,
		Offset: -100,
	})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    10,
		Offset: 100,
	})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(10), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("hsy"), &data.LogRecordPos{
		Fid:    5,
		Offset: -100,
	})
	assert.True(t, res2)

	pos2 := bt.Get([]byte("hsy"))
	assert.Equal(t, uint32(5), pos2.Fid)
	assert.Equal(t, int64(-100), pos2.Offset)

	res3 := bt.Put([]byte("hsy"), &data.LogRecordPos{
		Fid:    8,
		Offset: 666,
	})
	assert.True(t, res3)

	pos3 := bt.Get([]byte("hsy"))
	assert.Equal(t, uint32(8), pos3.Fid)
	assert.Equal(t, int64(666), pos3.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    10,
		Offset: 100,
	})
	assert.True(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Delete([]byte("hsy"))
	assert.False(t, res3)

	res4 := bt.Put([]byte("hsy"), &data.LogRecordPos{
		Fid:    5,
		Offset: -100,
	})
	assert.True(t, res4)
	res5 := bt.Delete([]byte("hsy"))
	assert.True(t, res5)
}

func TestBTree_Iterator(t *testing.T) {
	bt := NewBTree()

	// 1.b树为空的情况
	iter1 := bt.Iterator(false)
	assert.False(t, iter1.Valid())

	// 2.b树有数据的情况
	bt.Put([]byte("hsy"), &data.LogRecordPos{
		Fid:    10,
		Offset: 100,
	})
	iter2 := bt.Iterator(false)
	assert.True(t, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.False(t, iter2.Valid())

	// 3.有多条数据
	bt.Put([]byte("witcher"), &data.LogRecordPos{
		Fid:    10,
		Offset: 100,
	})
	bt.Put([]byte("white"), &data.LogRecordPos{
		Fid:    10,
		Offset: 100,
	})
	iter3 := bt.Iterator(false)
	for iter3.Valid() {
		assert.NotNil(t, iter3.Key())
		iter3.Next()
	}

	iter4 := bt.Iterator(true)
	for iter4.Valid() {
		assert.NotNil(t, iter4.Key())
		iter4.Next()
	}

	// 4.测试seek()
	iter5 := bt.Iterator(false)
	for iter5.Seek([]byte("wh")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	iter6 := bt.Iterator(true)
	for iter6.Seek([]byte("wh")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}

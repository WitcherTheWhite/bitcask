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

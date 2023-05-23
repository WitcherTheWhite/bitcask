package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

// Iterator 迭代器

type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB
	options   IteratorOptions
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	return &Iterator{
		indexIter: db.index.Iterator(opts.Reverse),
		db:        db,
		options:   opts,
	}
}

// Rewind 重新回到迭代器的起点，即第一条数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 查找第一个大于（或小于）等于指定 key 的值，从该值开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 下一个 key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid false 表示遍历结束，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前遍历的 Key 数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历的 value 数据
func (it *Iterator) Value() ([]byte, error) {
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.Get(it.Key())
}

// Close 关闭迭代器，释放资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}

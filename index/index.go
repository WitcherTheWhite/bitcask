package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 抽象索引接口，后续要接入其他数据结构，直接实现这个接口即可
type Indexer interface {

	// Put 向索引存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 key 得到索引中数据位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 删除索引中 key 的数据位置信息
	Delete(key []byte) bool

	// Size 索引中数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数索引树
	ART
)

// NewIndexer 根据类型初始化索引
func NewIndexer(indexType IndexType) Indexer {
	switch indexType {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一条数据
	Rewind()

	// Seek 查找第一个大于（或小于）等于指定 key 的值，从该值开始遍历
	Seek(key []byte)

	// Next 下一个 key
	Next()

	// Valid false 表示遍历结束，用于退出遍历
	Valid() bool

	// Key 当前遍历的 Key 数据
	Key() []byte

	// Value 当前遍历的 value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放资源
	Close()
}

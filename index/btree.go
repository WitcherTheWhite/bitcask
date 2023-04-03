package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTree 用b树作为索引，封装了 google 的b树实现
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree 初始化索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return NewBTreeIterator(bt.tree, reverse)
}

// BTreeIterator b树迭代器
type BTreeIterator struct {
	currIndex int     // 当前遍历下标
	reverse   bool    // 是否逆序遍历
	values    []*Item // key + 位置索引信息
}

func NewBTreeIterator(bt *btree.BTree, reverse bool) *BTreeIterator {
	var index int
	values := make([]*Item, bt.Len())

	// 将所有数据放到数组中
	saveValues := func(it btree.Item) bool {
		values[index] = it.(*Item)
		index++
		return true
	}
	if reverse {
		bt.Descend(saveValues)
	} else {
		bt.Ascend(saveValues)
	}

	return &BTreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bti *BTreeIterator) Rewind() {
	bti.currIndex = 0
}

func (bti *BTreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *BTreeIterator) Next() {
	bti.currIndex++
}

func (bti *BTreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

func (bti *BTreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

func (bti *BTreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

func (bti *BTreeIterator) Close() {
	bti.values = nil
}

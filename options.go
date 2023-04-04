package bitcask_go

import "os"

// IndexerType 索引类型
type IndexerType = int8

const (
	// Btree 索引
	Btree IndexerType = iota + 1

	// ART 自适应基数索引树
	ART
)

// Options 数据库启动配置项
type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	// 索引类型
	IndexType IndexerType
}

// DefaultOptions 数据库启动默认配置项
var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    Btree,
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// 遍历前缀为指定值的 key，默认为空
	prefix []byte

	// 是否逆序遍历，默认 false
	Reverse bool
}

// DefaultIteratorOptions 索引迭代器默认配置项
var DefaultIteratorOptions = IteratorOptions{
	prefix:  nil,
	Reverse: false,
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次中最大数据量
	MaxBatchNum uint

	// 提交时是否持久化
	syncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	syncWrites:  true,
}

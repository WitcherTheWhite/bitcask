package fio

const DataFilePerm = 0644

// I0Manager IoManager 抽象 IO 管理接口，可以介入不同的 IO 类型，目前支持标准文件 IO
type I0Manager interface {

	// Read 从文件给定位置读取相应的数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取到文件大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager，目前只支持标准 FileIO
func NewIOManager(fileName string) (I0Manager, error) {
	return NewFileIOManager(fileName)
}

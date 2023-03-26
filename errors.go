package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotFound       = errors.New("datafile not found")
	ErrDataDirectoryCorrupted = errors.New("the database directory may corrupted")
)

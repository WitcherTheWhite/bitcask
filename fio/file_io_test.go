package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(fileName string) {
	if err := os.RemoveAll(fileName); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join(".", "a.data")
	fio, err := NewFileIOManager(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join(".", "b.data")
	fio, err := NewFileIOManager(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	n, err = fio.Write([]byte("witcher"))
	assert.Nil(t, err)
	assert.Equal(t, 7, n)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join(".", "c.data")
	fio, err := NewFileIOManager(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("white"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("witcher"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)

	b2 := make([]byte, 7)
	n, err = fio.Read(b2, 5)
	assert.Nil(t, err)
	assert.Equal(t, 7, n)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join(".", "d")
	fio, err := NewFileIOManager(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join(".", "e")
	fio, err := NewFileIOManager(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

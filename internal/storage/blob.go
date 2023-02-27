package storage

import (
	"os"
	// "log"
	"math"
	"bytes"
	"math/big"
	// "errors"
	// "syscall"
	"path/filepath"
	// "encoding/hex"
	FA "github.com/detailyang/go-fallocate"
)

const (
	FILES_PERM = 0750
)

var (
	BlobRecordInternalError = &BlobError{ "internal record error" }
	BlobRecordExceedsSize = &BlobError{ "record value exceeds size" }
	BlobRecordNotFound = &BlobError{ "record not found" }
	BlobIncorrectKeySize = &BlobError{ "key size is incorrect" }
	BlobCorrupted = &BlobError{ "wrong size" }
	BlobAlreadyInitialized = &BlobError{ "already initialized" }

	zero = big.NewInt(0)
)

type BlobError struct{ msg string }
func (err BlobError) Error() string { return err.msg }

//
// BLOB is a key-value storage on a disk
//
type Blob struct {
	Path string
	KeySize uint64 // bytes len
		keySize uint // --- ^^ alias
	ValueSize uint64 // bytes len
	Capacity uint8 // 2 ** Capacity records, 0 means full range mapping
		capacity uint // --- ^^ alias
	recordSize int64
	capacitySize int64
	shift uint
	recordsCount int64
	shrinked bool
	file *os.File
}

//
// get position by key value
//
func (this *Blob) SlotOf(key *big.Int) (pos int64) {
	k := new(big.Int).Set(key)
	if this.shift > 0 {
		k.Rsh(k, this.shift)
	}
	pos = k.Int64()
	return
}

func (this *Blob) ReadAt(address int64, data *[]byte) error {
	if _, err := this.file.ReadAt(*data, address * this.recordSize); err != nil {
		return err
	}
	return nil
}

func (this *Blob) WriteAt(address int64, data *[]byte) error {
	if _, err := this.file.WriteAt(*data, address * this.recordSize); err != nil {
		return err
	}
	return nil
}

func (this *Blob) Set(key *big.Int, value *[]byte) (int64, uint8, error) {
	valueReader := bytes.NewReader(*value)
	valueLen := uint64(valueReader.Len())

	data := make([]byte, this.recordSize)
	i := this.SlotOf(key)
	iters := uint8(0)

	if valueLen > this.ValueSize {
		return 0, iters, BlobRecordExceedsSize
	}

	if !this.shrinked {
		if _, err := valueReader.Read(data[this.ValueSize - valueLen:]); err != nil {
			return i, iters, err
		}
		if err := this.WriteAt(i, &data); err != nil {
			return i, iters, err
		}
		return i, iters, nil
	}

	keyData := key.Bytes()
	// log.Printf("%x: %x", keyData, *value)
	keyDataSize := uint64(len(keyData))
	if _, err := bytes.NewReader(keyData).Read(data[this.KeySize - keyDataSize:]); err != nil {
		return i, iters, err
	}
	if _, err := valueReader.Read(data[uint64(this.recordSize) - valueLen:]); err != nil {
		return i, iters, err
	}
	// log.Printf("data: %x", data)

	recordKey := big.NewInt(0)
	recordKeyData := make([]byte, this.keySize)
	for {
		iters += 1
		if err := this.ReadAt(i, &recordKeyData); err != nil {
			return i, iters, err
		}
		recordKey.SetBytes(recordKeyData)
		if (key.Cmp(recordKey) == 0) || (recordKey.Cmp(zero) == 0) {
			if err := this.WriteAt(i, &data); err != nil {
				return i, iters, err
			}
			return i, iters, nil
		}
		if iters > this.Capacity {
			break
		}
		i += 1
	}
	return i, iters, BlobRecordNotFound
}

func (this *Blob) Get(key *big.Int) ([]byte, int64, uint8, error) {
	data := make([]byte, this.recordSize)
	i := this.SlotOf(key)
	iters := uint8(0)

	recordKey := big.NewInt(0)
	for {
		iters += 1
		if err := this.ReadAt(i, &data); err != nil {
			return nil, 0, iters, err
		}

		// log.Printf("[%d] bytes @ %d: %s", this.recordSize, i, hex.EncodeToString(data))

		if !this.shrinked {
			return data, i, iters, nil
		}

		recordKey.SetBytes(data[0:this.KeySize])
		if key.Cmp(recordKey) == 0 {
			return data[this.KeySize:], i, iters, nil
		}

		if iters > this.Capacity {
			break
		}
		i += 1
	}
	return nil, i, iters, BlobRecordNotFound
}

//
// count of records in blob
//
func (this *Blob) RecordsCount() int64 {
	return this.recordsCount
}

//
// total bytes should be allocated for blob
//
func (this *Blob) CapacitySize() int64 { // bytes len
	return this.capacitySize
}

func (this *Blob) Init() error {
	this.keySize = uint(this.KeySize)
	this.capacity = uint(this.Capacity)
	this.shift = 0
	if this.keySize * 8 > this.capacity {
		this.shift = this.keySize * 8 - this.capacity
	}
	if this.Capacity == 0 {
		this.recordsCount = int64(math.Pow(2, float64(8 * this.KeySize)))
		this.recordSize = int64(this.ValueSize)
		this.shrinked = false
	} else {
		this.recordsCount = int64(math.Pow(2, float64(this.Capacity)))
		this.recordSize = int64(this.KeySize + this.ValueSize)
		this.shrinked = true
	}
	this.capacitySize = this.recordSize * this.recordsCount // TODO: refactor to avoid loss on huge blobs
	if this.file != nil {
		return BlobAlreadyInitialized
	}
	dir := filepath.Dir(this.Path)
	err := os.MkdirAll(dir, FILES_PERM)
	if err != nil && !os.IsExist(err) {
		return err
	}

	f, err := os.OpenFile(this.Path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	this.file = f

	stat, err := os.Stat(this.Path)
	if err != nil {
		return err
	}

	if stat.Size() <= 0 {
		if err := FA.Fallocate(f, 0, this.capacitySize); err != nil {
			return err
		}
	}

	stat, err = os.Stat(this.Path)
	if err != nil {
		return err
	}

	if stat.Size() != this.capacitySize {
		return BlobCorrupted
	}
	return nil
}

func (this *Blob) Destroy() error {
	if err := os.Remove(this.Path); err != nil {
		return err
	}
	return nil
}

func (this *Blob) Close() error {
	return this.file.Close()
}

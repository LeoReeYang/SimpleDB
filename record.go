package SimpleDB

import (
	"hash/crc64"
	"sync"
	"unsafe"

	"golang.org/x/exp/constraints"
)

var TimeStampCount uint64 = 0
var uncompacted uint64 = 0

type ValueType uint64

const (
	NewValue     ValueType = 1
	RemoveValue  ValueType = 0
	HeaderSize   int       = 40
	InfoHeadSize int       = 32
)

type Bitcask struct {
	Index        map[string]ValueIndex //key -> info
	Loggers      map[string]*Logger    //filename -> logger
	WorkLogger   *Logger
	CompactMutex sync.Mutex
	RWmutex      sync.RWMutex
}

type Record struct {
	TimeStamp, KeySize, ValueSize uint64
	ValueType                     ValueType
	Key                           []byte
	Value                         []byte
	CRC                           uint64
}

func (r *Record) BuildBuffer() []byte {

	totalSize := HeaderSize + len(r.Key) + len(r.Value)

	buffer := make([]byte, 0, totalSize)

	Append(&buffer, uint64(r.TimeStamp))
	Append(&buffer, uint64(r.KeySize))
	Append(&buffer, uint64(r.ValueSize))
	Append(&buffer, uint64(r.ValueType))

	buffer = append(buffer, r.Key...)
	buffer = append(buffer, r.Value...)

	Append(&buffer, uint64(GetCheckSum(buffer)))

	return buffer
}

func newRecord(tstmp, keysize, valuesize uint64, valueTp ValueType, key, value string) *Record {
	return &Record{
		TimeStamp: tstmp,
		KeySize:   keysize,
		ValueSize: valuesize,
		Key:       []byte(key),
		Value:     []byte(value),
	}
}

func Append[T constraints.Integer](buf *[]byte, data T) {
	size := int(unsafe.Sizeof(data))
	for i := 0; i < size; i++ {
		*buf = append(*buf, byte(data>>(i*8)))
	}
}

func GetCheckSum(buf []byte) uint64 {
	return crc64.Checksum(buf, crc64.MakeTable(crc64.ISO))
}

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
	Index   map[string]ValueIndex //key -> info
	Loggers map[string]*Logger    //filename -> logger

	backupIndex   map[string]ValueIndex
	backupLoggers map[string]*Logger
	WorkLogger    *Logger
	CompactMutex  sync.Mutex
	RWmutex       sync.RWMutex
	isCompact     bool
	maxfileid     uint64
}

func NewBitcask() *Bitcask {
	bitcask := &Bitcask{
		Index:   make(map[string]ValueIndex, 0),
		Loggers: make(map[string]*Logger, 0),

		backupIndex: make(map[string]ValueIndex, 0),
		// backupLoggers: make(map[string]*Logger, 0),
		WorkLogger: &Logger{
			LogName:  "",
			Fd:       nil,
			FileSize: 0,
		},
	}
	bitcask.Recovery()

	return bitcask
}

type Record struct {
	TimeStamp, KeySize, ValueSize uint64
	ValueType                     ValueType
	Key                           []byte
	Value                         []byte
	CRC                           uint64
}

type RecordHead struct {
	TimeStamp, KeySize, ValueSize uint64
	ValueType                     ValueType
}

func newRecordHead() RecordHead {
	return RecordHead{}
}

//return the rawbytes data from a Record
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
	record := &Record{
		TimeStamp: tstmp,
		KeySize:   keysize,
		ValueSize: valuesize,
		ValueType: valueTp,
		Key:       []byte(key),
		Value:     []byte(value),
	}

	return record
}

func Append[T constraints.Unsigned](buf *[]byte, data T) {
	size := int(unsafe.Sizeof(data))
	for i := 0; i < size; i++ {
		*buf = append(*buf, byte(data>>(i*8)))
	}
}

func GetCheckSum(buf []byte) uint64 {
	return crc64.Checksum(buf, crc64.MakeTable(crc64.ISO))
}

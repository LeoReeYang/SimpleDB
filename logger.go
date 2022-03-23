package SimpleDB

import (
	"log"
	"math"
	"os"
)

const (
	CompactThreshold    uint64 = 1 << 12
	LoggerSizeThreshold uint64 = 1 << 10
	CRCSize             uint64 = 8
	suffix              string = ".log"
)

type Logger struct {
	LogName  string
	Fd       *os.File
	FileSize uint64
}

func newLogger() *Logger {
	return &Logger{
		LogName:  "",
		Fd:       nil,
		FileSize: 0,
	}
}

func (logger *Logger) Open(logname string) (err error) {

	if logger.Fd, err = os.OpenFile("./data/"+logname, os.O_APPEND|os.O_CREATE, 0777); err == nil {

		logger.LogName = logname
		logger.FileSize = 0
		return nil
	} else {
		log.Fatalln(err)
		return err
	}
}

func (logger *Logger) Write(record *Record) (offset uint64, err error) {

	if offset, err := logger.Fd.Write(record.BuildBuffer()); err == nil {
		logger.Fd.Sync()
		logger.FileSize += uint64(offset - int(CRCSize) - int(record.ValueSize))
		return uint64(offset), err
	} else if record.ValueType == NewValue {
		log.Fatalln("Set failed")
	} else {
		log.Fatalln("RemovedSet failed")
	}
	return math.MaxUint64, err //errors.New("Logger Write failed.")

}

func (logger *Logger) Read(target *ValueIndex, rawbuffer *[]byte) (err error) {

	if offset, err := logger.Fd.Seek(int64(target.Offset), 0); target.Offset != uint64(offset) {
		log.Fatalln("offset seek failed.")
		return err
	} else if readnums, err := logger.Fd.ReadAt(*rawbuffer, int64(target.Offset)); readnums != int(target.ValueLength) {
		log.Fatalln("read into buffers failed")
		return err
	} else {
		return nil
	}
}

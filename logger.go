package SimpleDB

import (
	"log"
	"math"
	"os"
)

const (
	CompactThreshold    uint64 = 1 << 8
	LoggerSizeThreshold uint64 = 1 << 6
	CRCSize             uint64 = 8
	suffix              string = ".log"
	prefix              string = "./data/"
)

type Logger struct {
	LogName  string
	Fd       *os.File
	FileSize uint64
}

func newLogger(fname string) *Logger {
	newlogger := &Logger{
		LogName:  fname,
		Fd:       nil,
		FileSize: 0,
	}
	err := newlogger.Open(fname)
	if err != nil {
		log.Fatal("Open new .log failed.", err)
	}
	return newlogger
}

func (logger *Logger) Open(logname string) (err error) {

	if logger.Fd, err = os.OpenFile(logname, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755); err == nil {
		logger.LogName = logname
		logger.FileSize = 0
		return nil
	} else {
		log.Fatalln(err)
		return err
	}
}

//Logger write a Record to the file handled by it,return the value offset
func (logger *Logger) Write(record *Record) (offset uint64, err error) {

	posNow, err := logger.Fd.Seek(0, 1)
	if err != nil {
		log.Fatal("Write seek pos failed.", err)
	}

	if writeNums, err := logger.Fd.Write(record.BuildBuffer()); err == nil {
		logger.Fd.Sync()

		logger.FileSize += uint64(writeNums)
		return uint64(int(posNow) + writeNums - int(CRCSize) - int(record.ValueSize)), err
	} else if record.ValueType == NewValue {
		log.Fatalln("Set failed")
	} else {
		log.Fatalln("RemovedSet failed")
	}
	return math.MaxUint64, err //errors.New("Logger Write failed.")
}

func (logger *Logger) Read(target *ValueIndex, rawbuffer []byte) (err error) {

	if offset, err := logger.Fd.Seek(int64(target.Offset), 0); target.Offset != uint64(offset) {
		log.Fatalln("offset seek failed.")
		return err
	} else if readnums, err := logger.Fd.ReadAt(rawbuffer, int64(target.Offset)); readnums != int(target.ValueLength) {

		log.Fatalln("Logger.Read() read into buffers failed.", target, logger, err)
	}

	return nil
}

// fmt.Println("logSize: ", logger.FileSize)
// fmt.Println("writeNums : ", writeNums)

// fmt.Println("logSize : ", logger.FileSize)
// fmt.Println("offsets : ", writeNums-int(CRCSize)-int(record.ValueSize))
// fmt.Println(target)
// log.Fatal(err)

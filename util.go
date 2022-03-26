package SimpleDB

import (
	"bytes"
	"io/fs"
	"io/ioutil"
	"log"
	"math"
	"strconv"
	"strings"
)

func GetLogId(fname string) uint64 {
	if _, suf, found := strings.Cut(fname, prefix); found {
		if pref, _, f := strings.Cut(suf, ".log"); f {
			if pre, err := strconv.Atoi(pref); err == nil {
				return uint64(pre)
			}
		}
	}
	return math.MaxUint64
}

func (this *Bitcask) GetRecordSize(key *string) uint64 {
	return uint64(HeaderSize) + uint64(len(*key)) + this.Index[*key].ValueLength
}

func (this *Bitcask) UpdataIndex(key *string, valueOffset uint64, valueSize uint64) {
	tempIndex := newIndex(this.WorkLogger.LogName, valueOffset, valueSize)
	this.Index[*key] = *tempIndex
}

func (this *Bitcask) CheckUncompacted(key *string) {
	if _, ok := this.Index[*key]; ok {
		if uncompacted += this.GetRecordSize(key); uncompacted >= CompactThreshold {
			uncompacted = 0
			this.Compact(this.WorkLogger.LogName)
		}
	}
}

func (this *Bitcask) switchLogger() {
	if this.WorkLogger.FileSize > LoggerSizeThreshold {

		newLogFileName := GetNewLogName(this.WorkLogger.LogName)

		nLogger := newLogger(newLogFileName)

		this.Loggers[nLogger.LogName] = nLogger
		this.WorkLogger = nLogger
	}
}

func GetNewLogName(oldname string) (newname string) {
	newLogId := GetLogId(oldname) + 1
	newLogFileName := prefix + strconv.Itoa(int(newLogId)) + suffix
	return newLogFileName
}

func Atoi(b []byte) {
	panic("unimplemented")
}

func (this *Bitcask) fileInit(file fs.DirEntry) (cont []byte, contSize, pos int) {

	if err := this.WorkLogger.Open("./data/" + file.Name()); err != nil {
		log.Fatalln("recovery open log file failed.")
	}

	content, err := ioutil.ReadAll(this.WorkLogger.Fd)
	if err != nil {
		log.Fatalln("Recovery read into memory failed.", err)
	}

	contentSize := len(content)
	this.WorkLogger.FileSize = uint64(contentSize)

	return content, contentSize, 0
}

func (this *Bitcask) traverseFile(r *bytes.Reader, recordHead *RecordHead, pos *int, contentSize int, checksum *uint64, kv map[string]string) {
	for *pos < contentSize {

		this.readRecordHead(r, pos, recordHead)

		key := make([]byte, recordHead.KeySize)
		value := make([]byte, recordHead.ValueSize)

		this.readKey(r, pos, recordHead, key)

		if recordHead.ValueType == NewValue {
			this.newValueHandle(r, pos, recordHead, value, checksum, key, kv)
		} else {
			this.removeHandle(r, recordHead, key, value, checksum, kv)
		}
	}
}

// for pos < contentSize {

// 	this.readRecordHead(r, &pos, &recordHead)

// 	key := make([]byte, recordHead.KeySize)
// 	value := make([]byte, recordHead.ValueSize)

// 	this.readKey(r, &pos, &recordHead, key)

// 	if recordHead.ValueType == NewValue {
// 		this.newValueHandle(r, &pos, &recordHead, value, &checksum, key, kv)
// 	} else {
// 		this.removeHandle(r, &recordHead, key, value, &checksum, kv)
// 	}
// }

func switchLogger(temp_logger *Logger, new_logs map[string]*Logger) {

	if temp_logger.FileSize > LoggerSizeThreshold {
		new_logs[temp_logger.LogName] = temp_logger

		newLogId := GetLogId(temp_logger.LogName) + 1
		newLogFileName := strconv.Itoa(int(newLogId)) + suffix //get "id.log"

		temp_logger.Open(newLogFileName)
	}
}

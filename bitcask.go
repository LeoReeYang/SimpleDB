package SimpleDB

import (
	"bytes"
	"encoding/binary"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
)

func (this *Bitcask) Set(key, value *string) {

	record := newRecord(GetNewTimeStamp(), uint64(len(*key)), uint64(len(*value)), NewValue, *key, *value)
	this.CheckUncompacted(key)

	this.RWmutex.Lock()
	this.LoggerWrite(key, record)
	this.RWmutex.Unlock()
}

func (this *Bitcask) Get(key *string, rawbuffer []byte) {

	this.RWmutex.RLock()
	this.LoggerRead(key, rawbuffer)
	this.RWmutex.RUnlock()
}

func (this *Bitcask) Remove(key *string) {

	record := newRecord(GetNewTimeStamp(), uint64(len(*key)), 0, RemoveValue, *key, "")

	this.RWmutex.Lock()
	this.CheckUncompacted(key)
	this.LoggerWrite(key, record)

	this.RWmutex.Unlock()
}

func (this *Bitcask) RecoveryInit() (getfiles []fs.DirEntry, err error) {
	uncompacted = 0
	this.WorkLogger.Open("0.log")
	this.Loggers["0.log"] = this.WorkLogger

	files, err := os.ReadDir("./data")
	if err != nil {
		log.Fatalln(err)
		return files, err
	}

	return files, nil
}

func (this *Bitcask) Recovery() {

	kv := make(map[string]string)

	files, err := this.RecoveryInit()
	if err != nil {
		log.Fatalln("Open 0.log failed,Check /data dir.")
		return
	}

	for _, file := range files {
		pos := 0
		err := this.WorkLogger.Open(file.Name())
		if err != nil {
			log.Fatalln("recovery open log file failed.")
		}
		content, err := ioutil.ReadAll(this.WorkLogger.Fd)
		if err != nil {
			log.Fatalln("Recovery read into memory failed.", err)
		}

		contentSize := len(content)

		r := bytes.NewReader(content)

		var RecordHead struct {
			TimeStamp, KeySize, ValueSize uint64
			ValueType                     ValueType
		}

		for pos != contentSize {
			if err := binary.Read(r, binary.LittleEndian, &RecordHead); err != nil {
				log.Fatalln("read head failed.", err)
			}
			pos += InfoHeadSize

			key := make([]byte, RecordHead.KeySize)
			value := make([]byte, RecordHead.ValueSize)
			var chechsum uint64

			if err := binary.Read(r, binary.LittleEndian, &key); err != nil {
				log.Fatalln("read key failed.", err)
			}
			pos += int(RecordHead.KeySize)

			if RecordHead.ValueType == NewValue {
				offset := pos
				if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
					log.Fatalln("read key failed.", err)
				}
				if err := binary.Read(r, binary.LittleEndian, &chechsum); err != nil {
					log.Fatalln("read key failed.", err)
				}

				pos += InfoHeadSize + int(RecordHead.KeySize) + int(RecordHead.ValueSize) + int(CRCSize)

				if _, ok := this.Index[string(key)]; ok {
					uncompacted += uint64(HeaderSize) + RecordHead.KeySize + this.Index[string(key)].ValueLength
				}

				this.Index[string(key)] = *newIndex(this.WorkLogger.LogName, uint64(offset), RecordHead.ValueSize)
				kv[string(key)] = string(value)

			} else {
				uncompacted += uint64(InfoHeadSize) + RecordHead.KeySize + CRCSize

				delete(this.Index, string(key))
				delete(kv, string(key))

				if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
					log.Fatalln("read key failed.", err)
				}
				if err := binary.Read(r, binary.LittleEndian, &chechsum); err != nil {
					log.Fatalln("read key failed.", err)
				}
			}
		}

	}

	if uncompacted >= CompactThreshold {
		this.Compact()
	}
}

func (this *Bitcask) Compact() {
	this.CompactMutex.Lock()
	defer this.CompactMutex.Unlock()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func(logs map[string]*Logger, indexNow map[string]ValueIndex) {

		new_logs := make(map[string]*Logger)
		new_index := make(map[string]ValueIndex)

		for key, index := range indexNow {

			buf := make([]byte, index.ValueLength)
			err := logs[index.LogName].Read(&index, &buf)
			if err != nil {
				return
			}

			temp_logger := newLogger()

			if err := temp_logger.Open("1000.log"); err != nil {
				log.Fatalln("new log file open failed.")
			}

			record := newRecord(GetNewTimeStamp(), uint64(len(key)), index.ValueLength, NewValue, key, string(buf))

			if offset, err := temp_logger.Write(record); err == nil {
				temp_index := newIndex(temp_logger.LogName, offset, record.ValueSize)

				new_index[key] = *temp_index
				new_logs[temp_logger.LogName] = temp_logger

				switchLogger(temp_logger, new_logs)

				if temp_logger.FileSize > LoggerSizeThreshold {
					new_logs[temp_logger.LogName] = temp_logger

					newLogId := GetLogId(temp_logger.LogName) + 1
					newLogFileName := strconv.Itoa(int(newLogId)) + suffix //get "id.log"

					temp_logger.Open(newLogFileName)
				}

			} else {
				log.Fatalln("new log file write failed.")
			}
		}
		//Update the new logs and index
		for key, vindex := range new_index {
			this.Index[key] = vindex
		}
		for logname, logger := range new_logs {
			this.Loggers[logname] = logger
		}
		wg.Done()
	}(this.Loggers, this.Index)

	wg.Wait()
}

func (this *Bitcask) CheckUncompacted(key *string) {
	if _, ok := this.Index[*key]; ok {
		if uncompacted += this.GetRecordSize(key); uncompacted >= CompactThreshold {
			uncompacted = 0
			this.Compact()
		}
	}
}

func (this *Bitcask) UpdataIndex(key *string, valueOffset uint64, valueSize uint64) {
	tempIndex := newIndex(this.WorkLogger.LogName, valueOffset, valueSize)
	this.Index[*key] = *tempIndex
}

func (this *Bitcask) switchLogger() {
	if this.WorkLogger.FileSize > LoggerSizeThreshold {
		this.Loggers[this.WorkLogger.LogName] = this.WorkLogger

		newLogId := GetLogId(this.WorkLogger.LogName) + 1
		newLogFileName := strconv.Itoa(int(newLogId)) + suffix

		this.WorkLogger.Open(newLogFileName)
	}
}

func switchLogger(temp_logger *Logger, new_logs map[string]*Logger) {

	if temp_logger.FileSize > LoggerSizeThreshold {
		new_logs[temp_logger.LogName] = temp_logger

		newLogId := GetLogId(temp_logger.LogName) + 1
		newLogFileName := strconv.Itoa(int(newLogId)) + suffix //get "id.log"

		temp_logger.Open(newLogFileName)
	}
}

func (this *Bitcask) LoggerRead(key *string, rawbuffer []byte) {

	index := this.Index[*key]
	theLogger := this.Loggers[index.LogName]
	rawbuffer = make([]byte, this.Index[*key].ValueLength)

	theLogger.Read(&index, &rawbuffer)
}

func (this *Bitcask) LoggerWrite(key *string, record *Record) {

	if offset, err := this.WorkLogger.Write(record); err == nil {
		this.UpdataIndex(key, offset, uint64(record.ValueSize))
		this.switchLogger()
	}
}

func GetNewTimeStamp() uint64 {
	temp := TimeStampCount
	TimeStampCount++
	return temp
}

func Atoi(b []byte) {
	panic("unimplemented")
}

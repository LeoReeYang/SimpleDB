package SimpleDB

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
)

func (this *Bitcask) Set(key, value *string) (err error) {

	record := newRecord(GetNewTimeStamp(), uint64(len(*key)), uint64(len(*value)), NewValue, *key, *value)
	this.CheckUncompacted(key)

	this.RWmutex.Lock()
	err = this.LoggerWrite(key, record)
	this.RWmutex.Unlock()

	return err
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

func (this *Bitcask) Recovery() {

	var checksum uint64
	kv := make(map[string]string)
	recordHead := newRecordHead()

	files := this.RecoveryInit()

	for _, file := range files {

		content, contentSize, pos := this.fileInit(file)

		r := bytes.NewReader(content)

		for pos < contentSize {

			this.readRecordHead(r, &pos, &recordHead)

			key := make([]byte, recordHead.KeySize)
			value := make([]byte, recordHead.ValueSize)

			this.readKey(r, &pos, &recordHead, key)

			if recordHead.ValueType == NewValue {
				this.newValueHandle(r, &pos, &recordHead, value, &checksum, key, kv)
			} else {
				this.removeHandle(r, &recordHead, key, value, &checksum, kv)
			}
		}

		this.Loggers[this.WorkLogger.LogName] = this.WorkLogger
	}

	if uncompacted >= CompactThreshold {
		this.Compact()
	}
}

func (this *Bitcask) readLeft(r *bytes.Reader, pos *int, recordHead *RecordHead, value []byte, checksum *uint64) (offset uint64) {
	offsets := *pos

	if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
		log.Fatalln("recovery read value failed.", err)
	}
	if err := binary.Read(r, binary.LittleEndian, checksum); err != nil {
		log.Fatalln("recovery read checksum failed.", err)
	}
	*pos += int(recordHead.ValueSize) + int(CRCSize)

	return uint64(offsets)
}

func (this *Bitcask) readKey(r *bytes.Reader, pos *int, recordHead *RecordHead, key []byte) (err error) {
	if err := binary.Read(r, binary.LittleEndian, key); err != nil {
		log.Fatalln("read key failed.", err)
	}
	*pos += int(recordHead.KeySize)

	return err
}

func (this *Bitcask) readRecordHead(r *bytes.Reader, pos *int, recordHead *RecordHead) (err error) {
	if err := binary.Read(r, binary.LittleEndian, recordHead); err != nil {
		log.Fatalln("read head failed.", err)
	}
	*pos += InfoHeadSize

	return err
}

func (this *Bitcask) newValueHandle(r *bytes.Reader, pos *int, recordHead *RecordHead, value []byte, checksum *uint64, key []byte, kv map[string]string) (err error) {
	offset := this.readLeft(r, pos, recordHead, value, checksum)

	if _, ok := this.Index[string(key)]; ok {
		uncompacted += uint64(HeaderSize) + recordHead.KeySize + this.Index[string(key)].ValueLength
	}

	// this.Index[string(key)] = *newIndex(this.WorkLogger.LogName, uint64(offset), recordHead.ValueSize)

	key_str := string(key)

	this.UpdataIndex(&key_str, uint64(offset), recordHead.ValueSize)

	kv[string(key)] = string(value)

	return err
}

func (this *Bitcask) removeHandle(r *bytes.Reader, recordHead *RecordHead, key, value []byte, checksum *uint64, kv map[string]string) (err error) {
	uncompacted += uint64(InfoHeadSize) + recordHead.KeySize + CRCSize

	delete(this.Index, string(key))
	delete(kv, string(key))

	if err := binary.Read(r, binary.LittleEndian, value); err != nil {
		log.Fatalln("read key failed.", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &checksum); err != nil {
		log.Fatalln("read key failed.", err)
	}

	return err
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
			err := logs[index.LogName].Read(&index, buf)

			if err != nil {
				return
			}

			temp_logger := newLogger()

			if err := temp_logger.Open("10000.log"); err != nil {
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

	if value, exsit := this.Index[*key]; exsit {
		index := value
		theLogger := this.Loggers[index.LogName]
		theLogger.Read(&index, rawbuffer)
	} else {
		fmt.Println(key, " not Exsit!")
	}
}

func (this *Bitcask) LoggerWrite(key *string, record *Record) (err error) {

	if offset, err := this.WorkLogger.Write(record); err == nil {
		this.UpdataIndex(key, offset, uint64(record.ValueSize))
		this.switchLogger()
		return nil
	}
	return err
}

func (this *Bitcask) RecoveryInit() (getfiles []fs.DirEntry) {

	uncompacted = 0

	// testpath, err := os.MkdirTemp("", "data")
	// if errs := os.Mkdir("./data", fs.FileMode(os.O_CREATE)); errs != nil {
	// 	log.Fatal(errs)
	// }
	// testpath := "./data"

	// if err != nil {
	// 	log.Fatal(err)
	// }

	files, err := os.ReadDir("./data")

	testpath := "./data/0.log"

	if err != nil {
		log.Fatalln(err)
		// return files
	}

	if len(files) == 0 {
		this.WorkLogger.Open(testpath)
		this.Loggers[testpath] = this.WorkLogger
	}
	return files
}

func GetNewTimeStamp() uint64 {
	temp := TimeStampCount
	TimeStampCount++
	return temp
}

func Atoi(b []byte) {
	panic("unimplemented")
}

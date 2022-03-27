package SimpleDB

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

func (this *Bitcask) Set(key, value string) (err error) {

	record := newRecord(GetNewTimeStamp(), uint64(len(key)), uint64(len(value)), NewValue, key, value)

	this.RWmutex.Lock()
	this.CheckUncompacted(key)
	err = this.LoggerWrite(this.WorkLogger, key, record)
	this.RWmutex.Unlock()

	return err
}

func (this *Bitcask) Get(key string) (val string, err error) {

	this.RWmutex.RLock()

	vals, err := this.LoggerRead(key)
	if err != nil {
		log.Fatal(err)
		return "", err
	}
	this.RWmutex.RUnlock()
	return vals, nil
}

func (this *Bitcask) Remove(key string) {

	record := newRecord(GetNewTimeStamp(), uint64(len(key)), 0, RemoveValue, key, "")

	this.RWmutex.Lock()
	this.CheckUncompacted(key)
	this.LoggerWrite(this.WorkLogger, key, record)

	this.RWmutex.Unlock()
}

func (this *Bitcask) Recovery() {

	var checksum uint64
	kv := make(map[string]string)
	recordHead := newRecordHead()

	files := this.RecoveryInit()

	for _, file := range files {

		content, contentSize, pos := this.fileInit(file)

		r := bytes.NewReader(content)

		this.traverseFile(r, &recordHead, &pos, contentSize, &checksum, kv)

		this.Loggers[this.WorkLogger.LogName] = this.WorkLogger
	}

	if uncompacted >= CompactThreshold {
		fn := this.WorkLogger.LogName
		this.Compact(fn)
	}

	if len(files) != 0 {
		newLogId := GetLogId(this.WorkLogger.LogName) + 1
		newLogFileName := prefix + strconv.Itoa(int(newLogId)) + suffix

		nLogger := newLogger(newLogFileName)

		this.Loggers[nLogger.LogName] = nLogger
		this.WorkLogger = nLogger

		// this.WorkLogger.Open(newLogFileName)
		// this.Loggers[this.WorkLogger.LogName] = this.WorkLogger
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

	this.UpdataIndex(key_str, uint64(offset), recordHead.ValueSize)

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

func (this *Bitcask) Compact(fn string) {

	this.CompactMutex.Lock()
	defer this.CompactMutex.Unlock()

	wg := sync.WaitGroup{}
	wg.Add(1)

	new_logs := make(map[string]*Logger)
	new_index := make(map[string]ValueIndex)
	temp_logger := newLogger(prefix + "10000.log")

	go func(logs map[string]*Logger, indexNow map[string]ValueIndex) {

		//traverse and copy the logs and index
		this.RWmutex.RLock()
		for fn, v := range logs {
			new_logs[fn] = v
		}
		for key, val := range indexNow {
			new_index[key] = val
		}
		this.RWmutex.RUnlock()

		for key, index := range indexNow {

			buf := make([]byte, index.ValueLength)

			if err := logs[index.LogName].Read(&index, buf); err != nil {
				return
			}

			record := newRecord(GetNewTimeStamp(), uint64(len(key)), index.ValueLength, NewValue, key, string(buf))

			if offset, err := temp_logger.Write(record); err == nil {
				temp_index := newIndex(temp_logger.LogName, offset, record.ValueSize)

				new_index[key] = *temp_index
				new_logs[temp_logger.LogName] = temp_logger

				switchLogger(temp_logger, new_logs)

			} else {
				log.Fatalln("new log file write failed.")
			}
		}

		//Update the new logs and index
		func() {
			this.RWmutex.Lock()
			for key, vindex := range new_index {
				this.Index[key] = vindex
			}
			for logname, logger := range new_logs {
				this.Loggers[logname] = logger
			}
			this.RWmutex.Unlock()
		}()

		wg.Done()
	}(this.Loggers, this.Index)

	wg.Wait()
}

func (this *Bitcask) LoggerRead(key string) (val string, err error) {
	var errs error
	if value, exsit := this.Index[key]; exsit {
		rawbuffer := make([]byte, value.ValueLength)

		theLogger := this.Loggers[value.LogName]
		err := theLogger.Read(&value, rawbuffer)

		if err != nil {
			fmt.Println(key, " not Exsit!")
			errs = err
			return "", err
		}
		return string(rawbuffer), nil
	}
	return "", errs
}

func (this *Bitcask) LoggerWrite(logger *Logger, key string, record *Record) (err error) {

	if offset, err := this.WorkLogger.Write(record); err == nil {
		this.UpdataIndex(key, offset, uint64(record.ValueSize))
		this.switchLogger()
		return nil
	}
	return err
}

func (this *Bitcask) RecoveryInit() (getfiles []fs.DirEntry) {

	// testpath, err := os.MkdirTemp("", "data")
	// if errs := os.Mkdir("./data"); errs != nil {
	// 	log.Fatal(errs)
	// }
	// testpath := "./data"

	uncompacted = 0

	files, err := os.ReadDir(prefix) //"./server/data"
	if err != nil {
		log.Fatalln(err)
		return
	}

	path := "0.log"

	if len(files) == 0 {
		this.WorkLogger.Open(prefix + path)
		this.Loggers[this.WorkLogger.LogName] = this.WorkLogger
	}
	return files
}

func GetNewTimeStamp() uint64 {
	return atomic.AddUint64(&TimeStampCount, 1)
}

func (this *Bitcask) WriteTest() {

	key := "1"

	value := "0"
	for i := 0; i < 4040; i++ {
		value = value + "0"
	}

	for i := 0; i < 10000; i++ {
		this.Set(key, value)
	}

}

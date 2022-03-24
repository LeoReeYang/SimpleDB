package SimpleDB

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestSet(t *testing.T) {
	key, value := "1", "cy is god!"

	bitcask := newBitcask()

	err := bitcask.Set(&key, &value)

	buf := make([]byte, len(value))

	bitcask.Get(&key, buf)

	fmt.Println(string(buf))

	if err != nil || string(buf) != value {
		t.Fatal("false.")
	}

	// if err != nil {
	// 	t.Fatalf(`Set(%v ,%v) = %v `, key, value, err)
	// 	t.Fatal("failed.")
	// }
}

func TestBuildBuffer(t *testing.T) {
	key, value := "1", "cy is god!"
	record := newRecord(GetNewTimeStamp(), 1, uint64(len(value)), NewValue, key, value)

	// bitcask := newBitcask()

	buf := make([]byte, 0)

	buf = record.BuildBuffer()

	fmt.Println(buf)
}

func TestLoggerWrite(t *testing.T) {
	key, value := "1", "cy is god!"
	bitcask := newBitcask()
	record := newRecord(GetNewTimeStamp(), 1, uint64(len(value)), NewValue, key, value)

	err := bitcask.LoggerWrite(&key, record)

	fmt.Println("\n", bitcask.WorkLogger.LogName)
	fmt.Println(record)

	if err != nil {
		t.Fatalf(`LoggerWrite(%v ,%v),err: %v`, &key, record, err)
		t.Fatal("failed.")
	}
}

func TestGet(t *testing.T) {
	key, value := "1", "cy is god!"
	bitcask := newBitcask()

	err := bitcask.Set(&key, &value)

	buf := make([]byte, 0)

	bitcask.Get(&key, buf)

	fmt.Println(string(buf))

	if err != nil || string(buf) != value {
		t.Fatal("false.")
	}
}

func TestRecovery(t *testing.T) {

	// err := os.Mkdir("./data1", 0750)
	// _, err := os.MkdirTemp("./", "data")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer os.RemoveAll("./data1")

	// key1 := "0"
	// key2 := "1"
	value1 := "cy is god!!"
	value2 := "cy is god.."

	bitcask := newBitcask()

	buf := make([]byte, len(value1))

	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		fmt.Println(key)

		if (i % 2) == 1 {
			fmt.Println(i % 2)
			bitcask.Set(&key, &value1)
		} else {
			fmt.Println(i % 2)
			bitcask.Set(&key, &value2)
		}

	}

	for i := 0; i < 10; i++ {
		key := strconv.Itoa(i)
		bitcask.Get(&key, buf)
		fmt.Println(string(buf))
	}
	os.Remove("./data/0.log")

}

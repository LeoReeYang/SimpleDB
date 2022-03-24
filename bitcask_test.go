package SimpleDB

import (
	"fmt"
	"os"
	"strconv"
	"sync"
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

	err := bitcask.LoggerWrite(bitcask.WorkLogger, &key, record)

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

	os.Mkdir("./data", 0755)
	defer os.RemoveAll("./data")

	value1 := "cy is god!!"
	value2 := "cy is god.."

	bitcask := newBitcask()

	buf := make([]byte, len(value1))

	wg := sync.WaitGroup{}

	var value = ""

	for i := 0; i < 5; i++ {
		wg.Add(2)

		go func() {
			for i := 0; i < 5; i++ {
				key := strconv.Itoa(i)

				if (i % 2) == 1 {
					value = value1
				} else {
					value = value2
				}

				bitcask.Set(&key, &value)
				fmt.Printf("key: %v write into value: %v  .\n", key, value)
			}
			wg.Done()
		}()

		go func() {
			// time.Sleep(time.Second)

			for j := 0; j < 5; j++ {
				key := strconv.Itoa(j)
				bitcask.Get(&key, buf)

				fmt.Println(key, string(buf))
			}
			wg.Done()
		}()
	}
	wg.Wait()

	for j := 0; j < 5; j++ {
		key := strconv.Itoa(j)
		bitcask.Get(&key, buf)

		fmt.Println(key, string(buf))
	}

}

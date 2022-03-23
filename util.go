package SimpleDB

import (
	"strconv"
	"strings"
)

func GetLogId(fname string) uint64 {
	if prefix, _, found := strings.Cut(fname, ".log"); found {
		if pre, err := strconv.Atoi(prefix); err == nil {
			return uint64(pre)
		}
	}
	return 0
}

func (this *Bitcask) GetRecordSize(key *string) uint64 {
	return uint64(HeaderSize) + uint64(len(*key)) + this.Index[*key].ValueLength
}

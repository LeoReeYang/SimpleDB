package SimpleDB

type ValueIndex struct {
	LogName             string
	Offset, ValueLength uint64
}

func newIndex(logname string, offset, length uint64) *ValueIndex {
	return &ValueIndex{
		LogName:     logname,
		Offset:      offset,
		ValueLength: length,
	}
}

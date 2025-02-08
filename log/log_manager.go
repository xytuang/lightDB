package log_manager

import (
	"../file/file_manager"
)

/**
* LOG MANAGER DEFINITION
*/
type LogMgr struct {
	fm *FileMgr
	logfile string
}

type GenericIterator[T any] struct {
    data  []T
    index int
}

func NewGenericIterator[T any](data []T) *GenericIterator[T] {
    return &GenericIterator[T]{data: data, index: 0}
}

func (it *GenericIterator[T]) HasNext() bool {
    return it.index < len(it.data)
}

func (it *GenericIterator[T]) Next() T {
    if it.HasNext() {
        element := it.data[it.index]
        it.index++
        return element
    }
    var zeroValue T
    return zeroValue
}

func NewLogMgr(fm *FileMgr, logfile string) *LogMgr {
	return &LogMgr{fm: fm, logfile: logfile}
}

/**
Returns log sequence number
*/
func (lm *LogMgr) Append(rec []byte) int {
}

/**
Forces a specific log record to disk
*/
func (lm *LogMgr) Flush(lsn int) {
}

func (lm *logMgr) NewIterator() *GenericIterator[byte]{
	return NewGenericIterator[byte]
}




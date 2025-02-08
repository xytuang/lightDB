package log

import (
	"lightDB/file"
	"fmt"
)

/**
Iterator Class to implement LogMgr
*/
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

/**
* LOG MANAGER DEFINITION
*/
type LogMgr struct {
	fm *file.FileMgr
	logfile string
	logpage *file.Page
	currentBlk *file.BlockId
	latestLSN int
	lastSavedLSN int
}

func NewLogMgr(fm *file.FileMgr, logfile string) (*LogMgr, error) {
	logpage := file.NewPage(fm.Blocksize())
	logsize, err := fm.CheckLength(logfile)

	if err != nil {
		fmt.Printf("Could not check length of file %s\n", logfile)
		return nil, err
	}

	var currentBlk *file.BlockId
	if logsize == 0 {
		currentBlk, err = appendNewBlock(fm, logfile, logpage)

		if err != nil {
			fmt.Printf("Could not check length of file %s\n", logfile)
			return nil, err
		}

	} else {
		currentBlk = file.NewBlock(logfile, logsize - 1)
		fm.Read(currentBlk, logpage)
	}
	return &LogMgr{fm: fm, logfile: logfile, logpage: logpage, currentBlk: currentBlk, latestLSN: 0, lastSavedLSN: 0}, nil
}

func appendNewBlock(fm *file.FileMgr, logfile string, logpage *file.Page) (*file.BlockId, error) {
	blk, err := fm.Append(logfile)

	if err != nil  {
		fmt.Printf("Could not append")
		return nil, err
	}
	logpage.SetInt(0, fm.Blocksize())
	fm.Write(blk, logpage)
	return blk, nil
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

func (lm *LogMgr) NewIterator() *GenericIterator[byte]{
	return NewGenericIterator[byte]
}




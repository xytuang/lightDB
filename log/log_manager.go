package log

import (
	"lightDB/file"
	"fmt"
	"sync"
	"unsafe"
)

/**
LogIterator Class to implement LogMgr
*/
type LogIterator struct {
	fm *file.FileMgr
	blk *file.BlockId
	page *file.Page
	currentPos int
	boundary int
}

func NewLogIterator(fm *file.FileMgr, blk *file.BlockId) *LogIterator {
	b := make([]byte, fm.Blocksize())
	p := file.NewPageFromBytes(b)

	it := &LogIterator{fm: fm, blk: blk, page: p, currentPos: 0, boundary: 0}
	it.moveToBlock()
	return it
}

/**
We read backwards from the page because the log is created backwards ie. from byte n to byte 0
*/
func (it *LogIterator) HasNext() bool {
	return it.currentPos < it.fm.Blocksize() || it.blk.Blknum() > 0
}

func (it *LogIterator) Next() []byte {
	if it.currentPos == it.fm.Blocksize() {
		/**
		Get the previous block
		*/
		it.blk = file.NewBlock(it.blk.Filename(), it.blk.Blknum() - 1)
		it.moveToBlock()
	}

	rec := it.page.GetBytes(it.currentPos)
	it.currentPos += int(unsafe.Sizeof(int64(0))) + len(rec)
	return rec
}

/**
PRIVATE METHODS FOR LOG ITERATOR
*/

func (it *LogIterator) moveToBlock() {
	err := it.fm.Read(it.blk, it.page)

	if err != nil {
		fmt.Printf("Error when reading from block %v\n", err)
	}

	it.boundary = it.page.GetInt(0)
	it.currentPos = it.boundary
}

/**
* LOG MANAGER DEFINITION
*/
type LogMgr struct {
	fm *file.FileMgr
	logfile string
	logpage *file.Page //This points to the same Page throughout the lifetime of a LogMgr
	currentBlk *file.BlockId //This changes whenever we flush logpage to block on disc indicated by currentBlk
	latestLSN int
	lastSavedLSN int
	mu sync.Mutex
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
		//New logfile, need to append new block to it
		currentBlk, err = appendNewBlock(fm, logfile, logpage)

		if err != nil {
			fmt.Printf("Could not check length of file %s\n", logfile)
			return nil, err
		}

	} else {
		//Get last block of the log file
		currentBlk = file.NewBlock(logfile, logsize - 1)
		fm.Read(currentBlk, logpage)
	}
	return &LogMgr{fm: fm, logfile: logfile, logpage: logpage, currentBlk: currentBlk, latestLSN: 0, lastSavedLSN: 0}, nil
}


/**
Returns log sequence number of new record rec
*/
func (lm *LogMgr) Append(logrec []byte) int {
	lm.mu.Lock()
	/**
	We always store the end position of the logpage as an int at byte 0
	*/
	boundary := lm.logpage.GetInt(0)
	recsize := len(logrec)
	bytesNeeded := recsize + int(unsafe.Sizeof(int64(0)))

	/**
	Record is longer than number of free bytes on logpage
	*/
	if (boundary - bytesNeeded < int(unsafe.Sizeof(int64(0)))) {
		lm.flush()
		var err error
		lm.currentBlk, err = appendNewBlock(lm.fm, lm.logfile, lm.logpage)

		if err != nil {
			fmt.Printf("Error when appending log record: %v\n", err)
			lm.mu.Unlock()
			return 0
		}
		boundary = lm.logpage.GetInt(0)
	}

	recpos := boundary - bytesNeeded
	lm.logpage.SetBytes(recpos, logrec)
	lm.logpage.SetInt(0, recpos)
	lm.latestLSN += 1
	lm.mu.Unlock()
	return lm.latestLSN
}

/**
Forces a specific log record to disk
*/
func (lm *LogMgr) Flush(lsn int) {
	if lsn >= lm.lastSavedLSN {
		lm.flush()
	}
}

func (lm *LogMgr) NewIterator() *LogIterator {
	lm.flush()
	return NewLogIterator(lm.fm, lm.currentBlk)
}


/**
PRIVATE FUNCTIONS FOR LOG MGR START HERE
*/
func appendNewBlock(fm *file.FileMgr, logfile string, logpage *file.Page) (*file.BlockId, error) {
	//blk is of type *BlockId
	blk, err := fm.Append(logfile)

	if err != nil  {
		fmt.Printf("Could not append")
		return nil, err
	}
	/**
	Writes blocksize from bytes 0 to 3
	*/
	logpage.SetInt(0, fm.Blocksize())

	/**
	Now write logpage to disk
	*/
	fm.Write(blk, logpage)
	return blk, nil
}

func (lm *LogMgr) flush() {
	lm.fm.Write(lm.currentBlk, lm.logpage)
	lm.lastSavedLSN = lm.latestLSN
}

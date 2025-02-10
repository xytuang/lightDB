package buffer

import (
	"lightDB/file"
	"lightDB/log"
)

type BufferHeader struct {
	fm *file.FileMgr
	lm *log.LogMgr
	contents *file.Page
	blk *file.BlockId
	pins int
	txNum int
	lsn int
}

func NewBufferHeader(fm *file.FileMgr, lm *log.LogMgr) *BufferHeader {
	contents := file.NewPage(fm.Blocksize())
	return &BufferHeader{fm: fm, lm: lm, contents: contents, blk: nil, pins: 0, txNum: -1, lsn: -1}
}

func (bh *BufferHeader) Contents() *file.Page {
	return bh.contents
}

func (bh *BufferHeader) Block() *file.BlockId {
	return bh.blk
}

/**
This function is only called by transactions that modify the contents of the buffer
*/
func (bh *BufferHeader) SetModified(txNum int, lsn int) {
	bh.txNum = txNum
	if bh.lsn >= 0 {
		bh.lsn = lsn
	}
}

func (bh *BufferHeader) IsPinned() bool {
	return bh.pins > 0 
}

func (bh *BufferHeader) ModifyingTx() int {
	return bh.txNum
}

func (bh *BufferHeader) assignToBlock(b *file.BlockId) {
	bh.flush()
	bh.blk = b
	bh.fm.Read(b, bh.contents)
	bh.pins = 0
}

/**
If txNum is ever >= 0, then it has been modified
Note that we don't check bh.pins in this function because
we assume that the page to be flushed is already unpinned.

Choosing a buffer to replace (aka unpinned buffer) is dealt with by the BufferManager and not the BufferHeader
*/
func (bh *BufferHeader) flush() {
	if bh.txNum >= 0 {
		bh.lm.Flush(bh.lsn)
		bh.fm.Write(bh.blk, bh.contents)
		bh.txNum = -1
	}
}

func (bh *BufferHeader) Pin() {
	bh.pins += 1
}

func (bh *BufferHeader) Unpin() {
	bh.pins -= 1
}


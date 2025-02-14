package buffer

import (
	"lightDB/file"
	"lightDB/log"
	"sync"
	"sync/atomic"
	"time"
	"errors"
)

var MAX_SECONDS int = 2

type BufferMgr struct {
	bufferPool []*BufferHeader
	numAvailable atomic.Int64
	mu sync.Mutex
}

func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numBuffs int) *BufferMgr {
	bufferPool := make([]*BufferHeader, numBuffs)
	for i := 0; i < numBuffs; i++ {
		bufferPool[i] = NewBufferHeader(fm, lm)
	}
	var numAvailable atomic.Int64
	numAvailable.Store(int64(numBuffs))
	return &BufferMgr{bufferPool: bufferPool, numAvailable: numAvailable}
}

/**
Don't need mutex here because count is atomic
*/
func (bm *BufferMgr) AvailableCount() int64 {
	return bm.numAvailable.Load()
}

func (bm *BufferMgr) FlushAll(txNum int) {
	bm.mu.Lock()
	for i := 0; i < len(bm.bufferPool); i++ {
		buff := bm.bufferPool[i]
		if buff.ModifyingTx() == txNum {
			buff.Flush()
		}
	}
	bm.mu.Unlock()
}

func (bm *BufferMgr) Unpin(buff *BufferHeader) {
	bm.mu.Lock()
	buff.Unpin()

	if !buff.IsPinned() {
		bm.numAvailable.Add(1)
	}
	bm.mu.Unlock()
}

/**
Try to pin blk to some page in the buffer pool for a maximum duration of MAX_SECONDS * time.Second

If blk is not pinned after that duration, return an error
*/
func (bm *BufferMgr) Pin(blk *file.BlockId) (*BufferHeader, error) {
	bm.mu.Lock()
	start := time.Now()
	buff := bm.tryToPin(blk)

	for buff == nil && !bm.waitingTooLong(start) {
		bm.mu.Unlock()
		time.Sleep(time.Duration(MAX_SECONDS) * time.Second)
		bm.mu.Lock()
		buff = bm.tryToPin(blk)
	}

	if buff == nil {
		bm.mu.Unlock()
		return nil, errors.New("No available page in buffer pool!")
	}
	bm.mu.Unlock()
	return buff, nil
}

func (bm *BufferMgr) waitingTooLong(start time.Time) bool {
	return time.Since(start) > (time.Duration(MAX_SECONDS) * time.Second)
}

/**
This function is always called with bm.mu locked
*/
func (bm *BufferMgr) tryToPin(blk *file.BlockId) *BufferHeader {
	buff := bm.findExistingBuffer(blk)

	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		
		if buff == nil {
			return nil
		}

		buff.AssignToBlock(blk)
	}

	if !buff.IsPinned() {
		bm.numAvailable.Add(-1)
	}

	buff.Pin()

	return buff
}

func (bm *BufferMgr) findExistingBuffer(blk *file.BlockId) *BufferHeader {
	for i := 0; i < len(bm.bufferPool); i++ {
		b := bm.bufferPool[i].Block()

		if b == nil{
			continue
		}

		if blk.Blknum() == b.Blknum() {
			return bm.bufferPool[i]
		}
	}

	return nil
}

func (bm *BufferMgr) chooseUnpinnedBuffer() *BufferHeader {
	for i := 0; i < len(bm.bufferPool); i++ {
		if !bm.bufferPool[i].IsPinned() {
			return bm.bufferPool[i]
		}
	}

	return nil
}



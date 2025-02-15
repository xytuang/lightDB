package transactions

import (
	"context"
	"errors"
	"lightDB/common"
	"lightDB/file"
	"sync"
	"time"
)

type LockTable struct {
	locks map[*file.BlockId]int
	mu sync.Mutex
	cond *sync.Cond
}

func NewLockTable() *LockTable {
	locks := make(map[*file.BlockId]int)
	lockTable := &LockTable{locks: locks}
	lockTable.cond = sync.NewCond(&lockTable.mu)
	return lockTable
}

func (lockTable *LockTable) SLock(blk *file.BlockId, ctx context.Context) error {
	ticker := time.NewTicker(common.POLL_INTERVAL)
	defer ticker.Stop()

	for {
		lockTable.mu.Lock()
		if (!lockTable.hasXLock(blk)) {
			val := lockTable.getLockVal(blk)
			lockTable.locks[blk] = val + 1
			lockTable.mu.Unlock()
			return nil
		}
		lockTable.mu.Unlock()
		select {
		case <-ctx.Done():
			return errors.New("SLock not available")
		case <-ticker.C:
		}

	}
}


func (lockTable *LockTable) XLock(blk *file.BlockId, ctx context.Context) error {
	ticker := time.NewTicker(common.POLL_INTERVAL)
	defer ticker.Stop()

	for {
		lockTable.mu.Lock()
		if (!lockTable.hasOtherSLocks(blk)) {
			lockTable.locks[blk] = -1
			lockTable.mu.Unlock()
			return nil
		}
		lockTable.mu.Unlock()
		select {
		case <-ctx.Done():
			return errors.New("XLock not available")
		case <-ticker.C:
		}

	}
}

func (lockTable *LockTable) Unlock(blk *file.BlockId) {
	lockTable.mu.Lock()
	defer lockTable.mu.Unlock()

	val := lockTable.getLockVal(blk)
	if val > 1 {
		lockTable.locks[blk] -= 1
	} else {
		delete(lockTable.locks, blk)
		lockTable.cond.Broadcast()
	}
}

func (lockTable *LockTable) hasXLock(blk *file.BlockId) bool {
	return lockTable.getLockVal(blk) < 0
}

func (lockTable *LockTable) hasOtherSLocks(blk *file.BlockId) bool {
	return lockTable.getLockVal(blk) > 1
}

func (lockTable *LockTable) getLockVal(blk *file.BlockId) int {
	val, err := lockTable.locks[blk]

	if !err {
		return 0
	}
	return val
}


package transactions

import (
	"context"
	"lightDB/common"
	"lightDB/file"
	"time"
)

var locktable *LockTable = NewLockTable()

type ConcurrencyMgr struct {
	locks map[*file.BlockId]string
}

func NewConcurrencyMgr() *ConcurrencyMgr{
	locks := make(map[*file.BlockId]string)
	return &ConcurrencyMgr{locks: locks}
}

func (concurrencyMgr *ConcurrencyMgr) SLock(blk *file.BlockId) error {
	_, err := concurrencyMgr.locks[blk]
	if !err {
		ctx, cancel := context.WithTimeout(context.Background(),time.Duration(common.MAX_SECONDS) * time.Second)
		defer cancel()
		failure := locktable.SLock(blk, ctx)
		if failure != nil {
			return failure
		}
		concurrencyMgr.locks[blk] = "S"
	}
	/*
	If we reach here, that means the transaction already has an SLock on blk
	*/
	return nil
}


func (concurrencyMgr *ConcurrencyMgr) XLock(blk *file.BlockId) error {
	if !concurrencyMgr.hasXLock(blk) {
		failure := concurrencyMgr.SLock(blk)

		if failure != nil {
			return failure
		}

		ctx, cancel := context.WithTimeout(context.Background(),time.Duration(common.MAX_SECONDS) * time.Second)
		defer cancel()
		failure = locktable.XLock(blk, ctx)


		if failure != nil {
			return failure
		}

		concurrencyMgr.locks[blk] = "X"
	}
	/*
	If we reach here, that means the transaction already has an SLock on blk
	*/
	return nil
}

func (concurrencyMgr *ConcurrencyMgr) Release(blk *file.BlockId) {
	for blk := range concurrencyMgr.locks {
		locktable.Unlock(blk)
	}
}

func (concurrencyMgr *ConcurrencyMgr) hasXLock(blk *file.BlockId) bool {
	locktype, err := concurrencyMgr.locks[blk]
	return !err && locktype == "X"
}

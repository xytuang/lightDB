package transactions

import (
	"lightDB/buffer"
	"lightDB/log"
)

type RecoveryMgr struct {
	lm *log.LogMgr
	bm *buffer.BufferMgr
	tx *Transaction
	txnum int
}

func NewRecoveryMgr(lm *log.LogMgr, bm *buffer.BufferMgr, tx *Transaction, txnum int) *RecoveryMgr {
	return &RecoveryMgr{lm: lm, bm: bm, tx: tx, txnum: txnum}
}





package transactions

import (
	"lightDB/buffer"
	"lightDB/file"
	"lightDB/log"
	"sync/atomic"
)
var nextTxnNumber atomic.Int64
var END_OF_FILE int = -1

type Transaction struct {
	txnNum int
	rm *RecoveryMgr
	cm *ConcurrencyMgr
	bm *buffer.BufferMgr
	fm *file.FileMgr
	buffers *buffer.BufferList
}

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Transaction {
	txnNum := nextTxnNumber.Add(1)
	cm := NewConcurrencyMgr()
	buffers := buffer.NewBufferList(bm)
	tx := &Transaction{txnNum: int(txnNum), rm: nil, cm: cm, bm: bm, fm: fm, buffers: buffers}
	rm := NewRecoveryMgr(lm, bm, tx, int(txnNum))
	tx.rm = rm
	return tx
}

func (tx *Transaction) Commit() {

}

func (tx *Transaction) Rollback() {
}

func (tx *Transaction) Recover() {
}


func (tx *Transaction) Pin(blk *file.BlockId) {
	tx.buffers.Pin(blk)
}


func (tx *Transaction) Unpin(blk *file.BlockId) {
	tx.buffers.Unpin(blk)
}


func (tx *Transaction) GetInt(blk *file.BlockId, offset int) (int, error) {
	err := tx.cm.SLock(blk)
	if err != nil {
		return 0, err
	}
	buff := tx.buffers.GetBuffer(blk)

	return buff.Contents().GetInt(offset), nil
}

func (tx *Transaction) GetString(blk *file.BlockId, offset int) (string, error) {
	err := tx.cm.SLock(blk)
	if err != nil {
		return "", err
	}
	buff := tx.buffers.GetBuffer(blk)

	return buff.Contents().GetString(offset), nil
}

func (tx *Transaction) SetInt(blk *file.BlockId, offset int, val int, okToLog bool) error {
	err := tx.cm.XLock(blk)
	if err != nil {
		return err
	}
	lsn := -1

	buff := tx.buffers.GetBuffer(blk)
	if okToLog {
		lsn = tx.rm.SetString(buff, offset, val)
	}

	p := buff.Contents()
	p.SetInt(offset, val)
	buff.SetModified(tx.txnNum, lsn)
	return nil
}

func (tx *Transaction) SetString(blk *file.BlockId, offset int, val string, okToLog bool) {
}

func (tx *Transaction) AvailableBufs() {
}

func (tx *Transaction) Size(filename string) {
}

func (tx *Transaction) Append(filename string) *file.BlockId {
	return nil
}

func (tx *Transaction) Blocksize() int {
	return 0
}

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
	var startRecord StartRecord
	startRecord.WriteToLog(lm, txnum)
	return &RecoveryMgr{lm: lm, bm: bm, tx: tx, txnum: txnum}
}

/*
Write Ahead Logging
*/
func (recoveryMgr *RecoveryMgr) Commit() {
	var commitRecord CommitRecord
	lsn := commitRecord.WriteToLog(recoveryMgr.lm, recoveryMgr.txnum)
	recoveryMgr.lm.Flush(lsn)
	/*
	Switch to steal no-force
	recoveryMgr.bm.FlushAll(recoveryMgr.txnum)
	*/
}

func (recoveryMgr *RecoveryMgr) Rollback() {
	var rollbackRecord RollbackRecord
	lsn := rollbackRecord.WriteToLog(recoveryMgr.lm, recoveryMgr.txnum)
	recoveryMgr.lm.Flush(lsn)

	recoveryMgr.doRollback()
}

func (recoveryMgr *RecoveryMgr) Recover() {
	recoveryMgr.doRecover()
	var rollbackRecord RollbackRecord
	lsn := rollbackRecord.WriteToLog(recoveryMgr.lm, recoveryMgr.txnum)
	recoveryMgr.lm.Flush(lsn)
	recoveryMgr.bm.FlushAll(recoveryMgr.txnum)
}

func (recoveryMgr *RecoveryMgr) SetInt(buff *buffer.BufferHeader, offset int, oldval int, newval int) int {
	var setIntRecord SetIntRecord
	return setIntRecord.WriteToLog(recoveryMgr.lm, recoveryMgr.txnum, buff.Block(), offset, oldval, newval)
}

func (recoveryMgr *RecoveryMgr) SetString(buff *buffer.BufferHeader, offset int, oldval string, newval string) int {
	var setStringRecord SetStringRecord
	return setStringRecord.WriteToLog(recoveryMgr.lm, recoveryMgr.txnum, buff.Block(), offset, oldval, newval)
}

func (recoveryMgr *RecoveryMgr) doRollback() {
	it := recoveryMgr.lm.NewIterator()

	for it.HasNext() {
		bytes := it.Next()
		rec := CreateLogRecord(bytes)
		if rec.GetTxnum() == recoveryMgr.txnum {
			if rec.Op() == LogRecordOps["START"] {
				return
			}
			rec.Undo(recoveryMgr.tx)
		}

	}
}

func (recoveryMgr *RecoveryMgr) doRecover() {
	finishedTxs := make(map[int]bool)
	redoRecords := make([]LogRecord, 0, 10)
	it := recoveryMgr.lm.NewIterator()

	for it.HasNext() {
		bytes := it.Next()
		rec := CreateLogRecord(bytes)
		if (rec.Op() == LogRecordOps["CHECKPOINT"]) {
			break
		}
		if (rec.Op() == LogRecordOps["COMMIT"] || rec.Op() == LogRecordOps["ROLLBACK"]) {
			finishedTxs[rec.GetTxnum()] = true
			redoRecords = append([]LogRecord{rec}, redoRecords...)
			continue
		}

		_, ok := finishedTxs[rec.GetTxnum()]

		if !ok {
			rec.Undo(recoveryMgr.tx)
		}
	}

	for _, record := range redoRecords {
		record.Redo(recoveryMgr.tx)
	}
}

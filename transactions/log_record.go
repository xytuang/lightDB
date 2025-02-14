package transactions

import (
	"lightDB/file"
	"lightDB/log"
)

var LogRecordOps = map[string]int {
	"CHECKPOINT"	:0,
	"START"		:1,
	"COMMIT"	:2,
	"ROLLBACK"	:3,
	"SETINT"	:4,
	"SETSTRING"	:5,
}

type LogRecord interface {
	Op() int
	GetTxnum() int
	Undo(tx *Transaction)
	Redo(tx *Transaction)
}

func CreateLogRecord(bytes []byte) LogRecord {
	p := file.NewPageFromBytes(bytes)
	switch p.GetInt(0) {
	case LogRecordOps["CHECKPOINT"]:
		return NewCheckPointRecord()

	case LogRecordOps["START"]:
		return NewStartRecord(p)

	case LogRecordOps["COMMIT"]:
		return NewCommitRecord(p)

	case LogRecordOps["ROLLBACK"]:
		return NewRollbackRecord(p)

	case LogRecordOps["SETINT"]:
		return NewSetIntRecord(p)

	case LogRecordOps["SETSTRING"]:
		return NewSetStringRecord(p)
	default:
		return nil
	}
}

/*
CHECKPOINT DEFINITION
*/
type CheckPointRecord struct {
}

func NewCheckPointRecord() *CheckPointRecord {
	return &CheckPointRecord{}
}

func (checkpointRec *CheckPointRecord) Op() int {
	return LogRecordOps["START"]
}

func (checkpointRec *CheckPointRecord) GetTxnum() int {
	return 0
}

func (checkpointRec *CheckPointRecord) Undo(tx *Transaction) {
}


func (checkpointRec *CheckPointRecord) Redo(tx *Transaction) {
}

func (CheckPointRecord) WriteToLog(lm *log.LogMgr) int {
	reclen := file.IntSize
	bytes := make([]byte, reclen)
	page := file.NewPageFromBytes(bytes)
	page.SetInt(0, LogRecordOps["CHECKPOINT"])
	return lm.Append(bytes)
}

/*
START DEFINITION
*/
type StartRecord struct {
	txnum int
}

func NewStartRecord(page *file.Page) *StartRecord {
	tpos := file.IntSize
	txnum := page.GetInt(tpos)

	return &StartRecord{txnum: txnum}
}

func (startRec *StartRecord) Op() int {
	return LogRecordOps["START"]
}

func (startRec *StartRecord) GetTxnum() int {
	return startRec.txnum
}

func (startRec *StartRecord) Undo(tx *Transaction) {
}

func (startRec *StartRecord) Redo(tx *Transaction) {
}

func (StartRecord) WriteToLog(lm *log.LogMgr, txnum int) int {
	tpos := file.IntSize

	reclen := tpos + file.IntSize
	bytes := make([]byte, reclen)
	page := file.NewPageFromBytes(bytes)
	page.SetInt(0, LogRecordOps["START"])
	page.SetInt(tpos, txnum)
	return lm.Append(bytes)
}
/*
COMMIT DEFINITION
*/
type CommitRecord struct {
	txnum int
}

func NewCommitRecord(page *file.Page) *CommitRecord {
	tpos := file.IntSize
	txnum := page.GetInt(tpos)

	return &CommitRecord{txnum: txnum}
}

func (commitRec *CommitRecord) Op() int {
	return LogRecordOps["COMMIT"]
}

func (commitRec *CommitRecord) GetTxnum() int {
	return commitRec.txnum
}

func (commitRec *CommitRecord) Undo(tx *Transaction) {
}

func (commitRec *CommitRecord) Redo(tx *Transaction) {
}

func (CommitRecord) WriteToLog(lm *log.LogMgr, txnum int) int {
	tpos := file.IntSize

	reclen := tpos + file.IntSize
	bytes := make([]byte, reclen)
	page := file.NewPageFromBytes(bytes)
	page.SetInt(0, LogRecordOps["COMMIT"])
	page.SetInt(tpos, txnum)

	return lm.Append(bytes)
}
/*
ROLLBACK DEFINITION
*/
type RollbackRecord struct {
	txnum int
}

func NewRollbackRecord(page *file.Page) *RollbackRecord {
	tpos := file.IntSize
	txnum := page.GetInt(tpos)

	return &RollbackRecord{txnum: txnum}
}

func (rollbackRec *RollbackRecord) Op() int {
	return LogRecordOps["ROLLBACK"]
}

func (rollbackRec *RollbackRecord) GetTxnum() int {
	return rollbackRec.txnum
}

func (rollbackRec *RollbackRecord) Undo(tx *Transaction) {
}

func (rollbackRec *RollbackRecord) Redo(tx *Transaction) {
}

func (RollbackRecord) WriteToLog(lm *log.LogMgr, txnum int) int {
	tpos := file.IntSize

	reclen := tpos + file.IntSize
	bytes := make([]byte, reclen)
	page := file.NewPageFromBytes(bytes)
	page.SetInt(0, LogRecordOps["ROLLBACK"])
	page.SetInt(tpos, txnum)

	return lm.Append(bytes)
}

/*
SET INT DEFINITION
*/
type SetIntRecord struct {
	txnum int
	offset int
	oldval int
	newval int
	blk *file.BlockId
}

func NewSetIntRecord(page *file.Page) *SetIntRecord {
	tpos := file.IntSize
	txnum := page.GetInt(tpos)
	fpos := tpos + file.IntSize
	filename := page.GetString(fpos)
	blkpos := fpos + file.MaxLength(len(filename))
	blknum := page.GetInt(blkpos)
	offsetpos := blkpos + file.IntSize
	offset := page.GetInt(offsetpos)
	oldvalpos := offsetpos + file.IntSize
	oldval := page.GetInt(oldvalpos)
	newvalpos := oldvalpos + file.IntSize
	newval := page.GetInt(newvalpos)

	return &SetIntRecord{txnum: txnum, offset: offset, oldval: oldval, newval:newval, blk: file.NewBlock(filename, blknum) }
}

func (setIntRec *SetIntRecord) Op() int {
	return LogRecordOps["SETINT"]
}

func (setIntRec *SetIntRecord) GetTxnum() int {
	return setIntRec.txnum
}

func (setIntRec *SetIntRecord) Undo(tx *Transaction) {
	tx.Pin(setIntRec.blk)
	tx.SetInt(setIntRec.blk, setIntRec.offset, setIntRec.oldval, true)
	tx.Unpin(setIntRec.blk)
}

func (setIntRec *SetIntRecord) Redo(tx *Transaction) {
	tx.Pin(setIntRec.blk)
	tx.SetInt(setIntRec.blk, setIntRec.offset, setIntRec.newval, true)
	tx.Unpin(setIntRec.blk)
}

func (SetIntRecord) WriteToLog(lm *log.LogMgr, txnum int, blk *file.BlockId, offset int, oldval int, newval int) int {
	tpos := file.IntSize
	fpos := tpos + file.IntSize
	blkpos := fpos + file.MaxLength(len(blk.Filename()))
	offsetpos := blkpos + file.IntSize
	oldvalpos := offsetpos + file.IntSize
	newvalpos := oldvalpos + file.IntSize

	reclen := newvalpos + file.IntSize
	bytes := make([]byte, reclen)
	page := file.NewPageFromBytes(bytes)
	page.SetInt(0, LogRecordOps["SETINT"])
	page.SetInt(tpos, txnum)
	page.SetString(fpos, blk.Filename())
	page.SetInt(blkpos, blk.Blknum())
	page.SetInt(offsetpos, offset)
	page.SetInt(oldvalpos, oldval)
	page.SetInt(newvalpos, newval)

	return lm.Append(bytes)
}




/*
SET STRING DEFINTION
*/
type SetStringRecord struct {
	txnum int
	offset int
	oldval string
	newval string
	blk *file.BlockId
}

func NewSetStringRecord(page *file.Page) *SetStringRecord {
	tpos := file.IntSize
	txnum := page.GetInt(tpos)
	fpos := tpos + file.IntSize
	filename := page.GetString(fpos)
	blkpos := fpos + file.MaxLength(len(filename))
	blknum := page.GetInt(blkpos)
	offsetpos := blkpos + file.IntSize
	offset := page.GetInt(offsetpos)
	oldvalpos := offsetpos + file.IntSize
	oldval := page.GetString(oldvalpos)
	newvalpos := offsetpos + file.IntSize
	newval := page.GetString(newvalpos)

	return &SetStringRecord{txnum: txnum, offset: offset, oldval: oldval, newval: newval, blk: file.NewBlock(filename, blknum) }
}

func (setStringRec *SetStringRecord) Op() int {
	return LogRecordOps["SETSTRING"]
}

func (setStringRec *SetStringRecord) GetTxnum() int {
	return setStringRec.txnum
}

func (setStringRec *SetStringRecord) Undo(tx *Transaction) {
	tx.Pin(setStringRec.blk)
	tx.SetString(setStringRec.blk, setStringRec.offset, setStringRec.oldval, true)
	tx.Unpin(setStringRec.blk)
}

func (setStringRec *SetStringRecord) Redo(tx *Transaction) {
	tx.Pin(setStringRec.blk)
	tx.SetString(setStringRec.blk, setStringRec.offset, setStringRec.newval, true)
	tx.Unpin(setStringRec.blk)
}

func (SetStringRecord) WriteToLog(lm *log.LogMgr, txnum int, blk *file.BlockId, offset int, oldval string, newval string) int {
	tpos := file.IntSize
	fpos := tpos + file.IntSize
	blkpos := fpos + file.MaxLength(len(blk.Filename()))
	offsetpos := blkpos + file.IntSize
	oldvalpos := offsetpos + file.IntSize
	newvalpos := oldvalpos + file.MaxLength(len(oldval))


	reclen := newvalpos + file.MaxLength(len(newval))

	bytes := make([]byte, reclen)
	page := file.NewPageFromBytes(bytes)
	page.SetInt(0, LogRecordOps["SETSTRING"])
	page.SetInt(tpos, txnum)
	page.SetString(fpos, blk.Filename())
	page.SetInt(blkpos, blk.Blknum())
	page.SetInt(offsetpos, offset)
	page.SetString(oldvalpos, oldval)
	page.SetString(newvalpos, newval)

	return lm.Append(bytes)
}


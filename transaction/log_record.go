package transaction

import (
	"lightDB/file"
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
	op() int
	txNum() int
	undo(txnum int)
}

func CreateLogRecord(bytes []byte) (*LogRecord) {
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


type SetStringRecord struct {
	txnum int
	offset int
	val string
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
	valpos := offsetpos + file.IntSize
	val := page.GetString(valpos)

	return &SetStringRecord{txnum: txnum, offset: offset, val: val, blk: file.NewBlock(filename, blknum) }
}

func (setStringRec *SetStringRecord) op() int {
	return LogRecord["SETSTRING"]
}

func (setStringRec *SetStringRecord) txnum() int {
	return setStringRec.txnum
}

func (setStringRec *SetStringRecord) undo(tx *Transaction) {
	tx.Pin()
}


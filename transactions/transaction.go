package transactions

import (
	"lightDB/file"
	"lightDB/log"
	"lightDB/buffer"
)

type Transaction struct {
}

func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) {

}

func (tx *Transaction) Commit() {
}

func (tx *Transaction) Rollback() {
}

func (tx *Transaction) Recover() {
}


func (tx *Transaction) Pin(blk *file.BlockId) {
}


func (tx *Transaction) Unpin(blk *file.BlockId) {
}


func (tx *Transaction) GetInt(blk *file.BlockId, offset int) {
}

func (tx *Transaction) GetString(blk *file.BlockId, offset int) {
}

func (tx *Transaction) SetInt(blk *file.BlockId, offset int, val int, okToLog bool) {
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

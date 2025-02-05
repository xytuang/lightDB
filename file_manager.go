package file_manager


import (
	"encoding/binary"
	"bytes"
	"fmt"
)
/*
BLOCK DEFINITION
Create a reference to the blknumth block in Filename
*/
type BlockId struct {
	Filename string
	Blknum int
}

func NewBlock(filename string, blknum int) *BlockId {
	return &BlockId{Filename: filename, Blknum: blknum}
}

func (b *BlockId) Equals(other *BlockId) {
	return (b.Filename == other.Filename) && (b.Blknum == other.Blknum)
}

func (b *BlockId) String() string {
	return fmt.Sprintf("[%s, block %d]",b.Filename, b.Blknum)
}

/*
PAGE DEFINITION
*/

type Page struct {
	data []byte
}

func NewPage(blocksize int) *Page {
	return &Page{data: make([]byte, blocksize)}
}

func NewPageFromBytes(bytes []byte) *Page {
	return &Page{data: bytes}
}

func (p *Page) GetInt(offset int) int {
	return int(binary.BigEndian.Uint32(p.data[offset:]))
}

func (p *Page) GetBytes(offset int) []byte {
	return p.data[offset:offset + length]
}

func (p *Page) GetString(offset int) string {
	return string(p.data[offset:offset + length])
}

func (p *Page) SetInt(offset int, val int) {
	binary.BigEndian.PutUint32(p.data[offset:], uint32(val))
}

func (p *Page) SetBytes(offset int, val []byte) {
	copy(p.data[offset:], val)
}

func (p *Page) SetString(offset int, val string) {
	copy(p.data[offset:], []byte(val))
}

func (p *Page) MaxLength(strlen int) int {
	return strlen
}

/* FILE MANAGER DEFINITION */
/**
* Only one FileMgr object exists in a single instance and it is created on system startup
*/
type FileMgr struct {
	DbDirectory string /* name of folder that contains files for database */
	Blocksize int /* how big a single block is */
}

func NewFileMgr(dbDirectory string, blocksize int) *FileMgr {
	return &FileMgr{DbDirectory: dbDirectory, Blocksize: blocksize}
}

/*
Read contents of blk into page
*/
func (f *FileMgr) Read(blk *BlockId, page *Page) {
}

/*
Writes contents of page to blk
*/
func (f *FileMgr) Write(blk *BlockId, page *Page) {
}

func (f *FileMgr) AppendBlock(filename string) *BlockId {
}

/*
Called when creating new FileMgr (ie. on startup)
If need to create new directory for db files, return true
Else false
*/
func (f *FileMgr) IsNew() bool {
}

func (f *FileMgr) Length(filename string) int {
}

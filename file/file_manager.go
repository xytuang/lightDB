package file_manager


import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"strings"
	"unsafe"
)

const IntSize = int(unsafe.Sizeof(int(1)))
/*
BLOCK DEFINITION
Create a reference to the blknumth block in Filename
*/
type BlockId struct {
	filename string
	blknum int
}

func NewBlock(filename string, blknum int) *BlockId {
	return &BlockId{filename: filename, blknum: blknum}
}

func (b *BlockId) Equals(other *BlockId) bool {
	return (b.Filename() == other.Filename()) && (b.Blknum() == other.Blknum())
}

func (b *BlockId) String() string {
	return fmt.Sprintf("[%s, block %d]",b.Filename, b.Blknum)
}

func (b *BlockId) Filename() string {
	return b.filename
}

func (b *BlockId) Blknum() int {
	return b.blknum
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

func bytesToInt(b []byte) int64 {
	v := binary.LittleEndian.Uint64(b)
	return int64(v)
}

func (p *Page) GetInt(offset int) int {
	return int(binary.BigEndian.Uint32(p.data[offset:]))
}

func (p *Page) GetBytes(offset int) []byte {
	start := offset + IntSize
	length := bytesToInt(p.data)
	return p.data[start:start + int(length)]
}

func (p *Page) GetString(offset int) string {
	return string(p.GetBytes(offset))
}

func (p *Page) SetInt(offset int, val int) {
	binary.LittleEndian.PutUint64(p.data[offset:], uint64(val))
}

func (p *Page) SetBytes(offset int, val []byte) {
	binary.LittleEndian.PutUint64(p.data[offset:], uint64(len(val)))
	copy(p.data[offset + IntSize:], val)
}

func (p *Page) SetString(offset int, val string) {
	p.SetBytes(offset, []byte(val))
}

func (p *Page) MaxLength(strlen int) int {
	return strlen
}

/* FILE MANAGER DEFINITION */
/**
* Only one FileMgr object exists in a single instance and it is created on system startup
*/
type FileMgr struct {
	dbDirectory *os.File /* name of folder that contains files for database */
	blocksize int /* how big a single block is */
	openFiles map[string]*os.File
	mu sync.Mutex
}

func NewFileMgr(dbDirectoryName string, blocksize int) *FileMgr {
	_, err := os.Stat(dbDirectoryName)
	if os.IsNotExist(err) {
		err := os.MkdirAll(dbDirectoryName, os.ModePerm)

		if err != nil {
			fmt.Println("Failed to create directory: %v\n", err)
			return nil
		}

		fmt.Println("Directory created:", dbDirectoryName)
	} else if err != nil {
		fmt.Println("Error checking existing directory: %v\n", err)
		return nil
	}

	dbDirectory, err := os.Open(dbDirectoryName)

	if err != nil {
		fmt.Println("Failed to open existing directory: %v\n", err)
		return nil
	}

	files, err := dbDirectory.Readdir(-1)

	if err != nil {
		fmt.Println("Error reading directory")
	}

	for _,file := range files {
		if strings.Contains(file.Name(), "temp") {
			filePath := generateFilePath(dbDirectoryName, file.Name())
			err := os.Remove(filePath)

			if err != nil {
				fmt.Println("Error deleting file %s", filePath)
			} else {
				fmt.Println("Deleted file %s", filePath)
			}
		}
	}
	return &FileMgr{
		dbDirectory: dbDirectory,
		blocksize: blocksize,
		openFiles: make(map[string]*os.File),
	}
}

/*
Read contents of blk into page
*/
func (f *FileMgr) Read(blk *BlockId, page *Page) error {
	f.mu.Lock()

	file, err := f.getDBFile(blk.Filename())

	if err != nil {
		f.mu.Unlock()
		return err
	}

	_, err = file.Seek(int64(blk.Blknum()) * int64(f.blocksize), 0)

	if err != nil {
		f.mu.Unlock()
		return err
	}

	_, err = file.Read(page.data)

	if err != nil {
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()
	return nil
}

/*
Writes contents of page to blk
*/
func (f *FileMgr) Write(blk *BlockId, page *Page) error {
	f.mu.Lock()

	file, err := f.getDBFile(blk.Filename())

	if err != nil {
		f.mu.Unlock()
		return err
	}

	_, err = file.Seek(int64(blk.Blknum()) * int64(f.blocksize), 0)

	if err != nil {
		f.mu.Unlock()
		return err
	}

	_, err = file.Write(page.data)

	if err != nil {
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()
	return nil
}

func (f *FileMgr) Append(filename string) (*BlockId, error) {
	f.mu.Lock()

	file, err := f.getDBFile(filename)

	if err != nil {
		f.mu.Unlock()
		return nil, err
	}

	newBlkNum, err := f.Length(file)

	if err != nil {
		f.mu.Unlock()
		return nil, err
	}

	blk := NewBlock(filename, newBlkNum)

	b := make([]byte, f.blocksize)

	_, err = file.Seek(int64(blk.Blknum()) * int64(f.blocksize), 0)

	if err != nil {
		f.mu.Unlock()
		return nil, err
	}

	_, err = file.Write(b)

	if err != nil {
		f.mu.Unlock()
		return nil, err
	}
	f.mu.Unlock()
	return blk, nil
}

func generateFilePath(directoryName string, filename string) string {
	filePath := fmt.Sprintf("%s/%s", directoryName, filename)
	return filePath
}

func (f *FileMgr) getDBFile(filename string) (*os.File, error)  {
	_, ok := f.openFiles[filename]

	if !ok {
		filePath := generateFilePath(f.dbDirectory.Name(), filename)
		tmpFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			return nil, err
		}

		f.openFiles[filename] = tmpFile
	}

	file := f.openFiles[filename]
	return file, nil
}

func (f *FileMgr) Length(file *os.File) (int, error) {
	fi, err := file.Stat()

	if err != nil {
		return 0, err
	}
	return int(fi.Size()) / int(f.blocksize) , nil
}

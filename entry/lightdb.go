package lightdb


import (
	"bufio"
	"fmt"
	"os"
)

type LightDB struct {
	fm *FileManager
	lm *LogManager
}

func StartLightDB(dbDirectoryName string, blocksize int, numblocks int) *LightDB {
	fm := NewFileMgr(dbDirectoryName, blocksize)

	/**
	Keep prompting user until we set up a valid FileMgr
	*/
	while (fm == nil) {
		fmt.Println("%s is not a valid directory", dbDirectoryName)
		fmt.Println("Enter another directory")
		reader := bufio.NewReader(os.Stdin)
		dbDirectoryName, _ = reader.ReadString('\n')
		dbDirectoryName = dbDirectoryName[:len(dbDirectoryName) - 1]
		fm = NewFileMgr(dbDirectoryName, blocksize)
	}

	lm := NewLogMgr(fm, "logfile")

	return &LightDB{fm: fm, lm: lm}
}

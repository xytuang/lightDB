package entry

import (
	"bufio"
	"fmt"
	"os"
	"lightDB/file"
	"lightDB/log"
)

type LightDB struct {
	fm *file.FileMgr
	lm *log.LogMgr
}

func StartLightDB(dbDirectoryName string, blocksize int, numblocks int) *LightDB {
	fm := file.NewFileMgr(dbDirectoryName, blocksize)

	/**
	Keep prompting user until we set up a valid FileMgr
	*/
	for fm == nil {
		fmt.Printf("%s is not a valid directory", dbDirectoryName)
		fmt.Println("Enter another directory")
		reader := bufio.NewReader(os.Stdin)
		dbDirectoryName, _ = reader.ReadString('\n')
		dbDirectoryName = dbDirectoryName[:len(dbDirectoryName) - 1]
		fm = file.NewFileMgr(dbDirectoryName, blocksize)
	}

	lm, _ := log.NewLogMgr(fm, "logfile")



	return &LightDB{fm: fm, lm: lm}
}

func (db *LightDB) LogMgr() *log.LogMgr {
	return db.lm
}

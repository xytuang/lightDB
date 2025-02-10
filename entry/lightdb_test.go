package entry

import (
	"testing"
	"fmt"
	"lightDB/file"
	"lightDB/log"
)

func TestLightDB(t *testing.T) {
	db := StartLightDB("logtest", 400, 8)
	lm := db.LogMgr()

	createRecords(lm, 1 , 35)
	printLogRecords(lm, "the log file now has these records")
	createRecords(lm, 36, 70)
	lm.Flush(65)
	printLogRecords(lm, "the log file now has these records")
}

func createRecords(lm *log.LogMgr, start int, end int) {
	for start <= end {
		rec := createLogRecord(fmt.Sprintf("record%d",start), start + 100)
		lsn := lm.Append(rec)
		fmt.Printf("LSN: %d\n", lsn)
		start += 1
	}
}

func createLogRecord(s string, n int) []byte {
	numBytes := file.MaxLength(len(s)) + 8
	b := make([]byte, numBytes)
	page := file.NewPageFromBytes(b)
	page.SetString(0, s)
	page.SetInt(file.MaxLength(len(s)), n)
	return b
}

func printLogRecords(lm *log.LogMgr, s string) {
	fmt.Printf("%s\n", s)
	it := lm.NewIterator()

	for it.HasNext() {
		rec := it.Next()
		p := file.NewPageFromBytes(rec)
		recString := p.GetString(0)
		npos := file.MaxLength(len(recString))
		val := p.GetInt(npos)
		fmt.Printf("[ %s , %d]", recString, val)
	}
}


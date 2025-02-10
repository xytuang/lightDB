package entry

import (
	"testing"
	"fmt"
	"lightDB/entry"
	"lightDB/file"
	"lightDB/buffer"
)

func TestBufferMgr(t *testing.T) {
	db := entry.StartLightDB("logtest", 400, 3)
	bm := db.BufferMgr()

	buff := make([]*buffer.BufferHeader, 6)

	buff[0], _ = bm.Pin(file.NewBlock("testfile", 0))
	buff[1], _ = bm.Pin(file.NewBlock("testfile", 1))
	buff[2], _ = bm.Pin(file.NewBlock("testfile", 2))

	bm.Unpin(buff[1])
	buff[1] = nil

	buff[3], _ = bm.Pin(file.NewBlock("testfile", 0))
	buff[4], _ = bm.Pin(file.NewBlock("testfile", 1))

	fmt.Printf("Available buffers: %d\n", bm.AvailableCount())

	fmt.Println("Attempting to pin block 3...")
	_, err := bm.Pin(file.NewBlock("testfile", 3))
	if err != nil {
		fmt.Printf("Exception: %s\n", err)
	}

	bm.Unpin(buff[2])
	buff[2] = nil

	buff[5], _ = bm.Pin(file.NewBlock("testfile", 3))

	fmt.Println("Final Buffer Allocation:")
	for i := 0; i < len(buff); i++ {
		if buff[i] != nil {
			fmt.Printf("buff[%d] pinned to block %d\n", i, buff[i].Block().Blknum())
		}
	}
}

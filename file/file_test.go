package file

import (
	"testing"
)

func TestFileManager(t *testing.T) {
	fm := NewFileMgr("dbtest", 400)

	if fm == nil {
		t.Errorf("Failed to make file mgr")
	}

	blk := NewBlock("testfile", 2)
	p1 := NewPage(fm.Blocksize())

	pos1 := 88
	expectedString := "abcdefghijklm"
	expectedInt := 345

	err := p1.SetString(pos1, expectedString)

	if err != nil {
		t.Errorf("err: %v\n", err)
	}
	size := p1.MaxLength(len(expectedString))
	pos2 := pos1 + size

	err = p1.SetInt(pos2, expectedInt)

	if err != nil {
		t.Errorf("err: %v\n", err)
	}

	fm.Write(blk, p1)

	p2 := NewPage(fm.Blocksize())
	fm.Read(blk, p2)

	// Assertions
	if got := p2.GetInt(pos2); got != expectedInt {
		t.Errorf("offset %d: expected %d, got %d", pos2, expectedInt, got)
	}

	if got := p2.GetString(pos1); got != expectedString {
		t.Errorf("offset %d: expected %s, got %s", pos1, expectedString, got)
	}
}


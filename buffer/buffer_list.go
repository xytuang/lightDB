package buffer

import "lightDB/file"

type BufferList struct {
	buffers map[*file.BlockId]*BufferHeader
	pins []*file.BlockId
	bm *BufferMgr
}

func NewBufferList(bm *BufferMgr) *BufferList {
	buffers := make(map[*file.BlockId]*BufferHeader)
	pins := make([]*file.BlockId, 0)
	return &BufferList{buffers: buffers, pins: pins, bm: bm}
}

func (buffList *BufferList) GetBuffer(blk *file.BlockId) *BufferHeader {
	return buffList.buffers[blk]
}

func (buffList *BufferList) Pin(blk *file.BlockId) {
	buff,_  := buffList.bm.Pin(blk)
	buffList.buffers[blk] = buff
	buffList.pins = append(buffList.pins, blk)
}


func (buffList *BufferList) Unpin(blk *file.BlockId) {
	buff := buffList.buffers[blk]
	buffList.bm.Unpin(buff)
	buffList.pins = remove(buffList.pins, blk)
	if !contains(buffList.pins, blk) {
		delete(buffList.buffers, blk)
	}
}

func (buffList *BufferList) UnpinAll() {
	for _, val := range buffList.pins {
		buff := buffList.buffers[val]
		buffList.bm.Unpin(buff)
	}

	for k := range buffList.buffers {
		delete(buffList.buffers, k)
	}
	buffList.pins = nil
}

func contains(slice []*file.BlockId, blk *file.BlockId) bool {
	/*
	TODO: ERROR CHECKING
	*/
	for _, val := range slice {
		if val == blk {
			return true
		}
	}
	return false
}

func remove(slice []*file.BlockId, blk *file.BlockId) []*file.BlockId{
	/*
	TODO: ERROR CHECKING
	*/
	for i,v := range slice {
		if v == blk {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

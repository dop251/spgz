package spgz

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
)

const (
	headerMagic = "SPGZ0001"
	headerSize  = 4096

	writeQueueSize = 32
)

const (
	DefBlockSize = 128*1024 - 1
	MinBlockSize = 3*4096 - 1
)

const (
	blkUncompressed byte = iota
	blkCompressed
)

var (
	ErrInvalidFormat         = errors.New("invalid file format")
	ErrFileIsDirectory       = errors.New("file cannot be a directory")
	ErrWriteOnlyNotSupported = errors.New("write only mode is not supported")
)

type compressor interface {
	CompressAsync(src []byte, dst *[]byte, result chan<- error)
}

type gzipCompressor struct {
	writers chan *gzip.Writer
}

func (c *gzipCompressor) CompressAsync(src []byte, dst *[]byte, result chan<- error) {
	go func() {
		zw := <-c.writers
		defer func() {
			c.writers <- zw
		}()

		buf := bytes.NewBuffer(*dst)
		zw.Reset(buf)

		reader := bytes.NewBuffer(src)

		buf.WriteByte(blkCompressed)

		_, err := io.Copy(zw, reader)
		if err != nil {
			result <- err
			return
		}
		err = zw.Close()
		if err != nil {
			result <- err
			return
		}
		*dst = buf.Bytes()
		result <- nil
	}()
}

func newGzipCompressor(writers int) *gzipCompressor {
	c := &gzipCompressor{
		writers: make(chan *gzip.Writer, writers),
	}
	for i := 0; i < writers; i++ {
		c.writers <- gzip.NewWriter(nil)
	}
	return c
}

// ErrCorruptCompressedBlock is returned when a compressed block involved in a read or a write operation is
// corrupt (for example as a result of a partial write). Note, this is not a media error.
//
// Any read operation intersecting with the range specified by Offset() and Size(() will fail.
// Any write operation intersecting, but not fully covering the range, will also fail (therefore in order to make
// the file usable again one must overwrite the entire range in a single write operation).
type ErrCorruptCompressedBlock struct {
	offset, size int64

	err error
}

func (e *ErrCorruptCompressedBlock) Error() string {
	return fmt.Sprintf("corrupted compressed block at %d, size %d: %v", e.offset, e.size, e.err)
}

func (e *ErrCorruptCompressedBlock) Unwrap() error {
	return e.err
}

func (e *ErrCorruptCompressedBlock) Offset() int64 {
	return e.offset
}

func (e *ErrCorruptCompressedBlock) Size() int64 {
	return e.size
}

type block struct {
	f                   *SpgzFile
	num                 int64
	data                []byte
	rawBlock, dataBlock []byte
	ch                  chan error
	// True when data is pointing to rawBlock rather than dataBlock.
	dataIsRaw bool
	dirty     bool
	// When data is written to a block that is currently not loaded it's not loaded immediately
	// (in anticipation that it will be overwritten in full so no loading will be necessary).
	// It's only loaded when it's read or written beyond the current data slice. When it is, this flag is set.
	full bool
}

type SpgzFile struct {
	sync.Mutex
	f         SparseFile
	blockSize int64
	readOnly  bool

	block    block
	tmpBlock block

	gzReader   *gzip.Reader
	compressor compressor

	// Contains a list of consecutive blocks ready to be written. When the queue is flushed, first a single
	// hole is punched across the whole area, then individual blocks are written. This helps reduce the number
	// of fallocate() calls which seem to be quite expensive, at least on btrfs.
	writeQueue []block
	offset     int64
	size       int64
	loaded     bool
}

func (b *block) init(f *SpgzFile) {
	b.f = f
	b.dirty = false
	b.dataIsRaw = false
}

// Completes a partially overwritten block by reading the rest.
// b.data at this point contains the overwritten part.
func (b *block) readTail() error {
	if !b.full && int64(len(b.data)) < b.f.blockSize {
		return b.read(true)
	}
	return nil
}

func (b *block) read(tail bool) error {
	if tail {
		b.prepareWrite()
	}
	if b.rawBlock == nil {
		b.rawBlock = make([]byte, b.f.blockSize+1)
	} else {
		b.rawBlock = b.rawBlock[:b.f.blockSize+1]
	}

	if b.dataBlock == nil {
		b.dataBlock = make([]byte, b.f.blockSize)
	}
	n, err := b.f.f.ReadAt(b.rawBlock, headerSize+b.num*(b.f.blockSize+1))
	if err != nil {
		if err == io.EOF {
			if n > 0 {
				b.rawBlock = b.rawBlock[:n]
				err = nil
			} else {
				b.full = true
				if !tail {
					b.data = b.dataBlock[:0]
					b.dataIsRaw = false
					b.dirty = false
					return err
				} else {
					return nil
				}
			}
		} else {
			if !tail {
				b.data = b.dataBlock[:0]
				b.dataIsRaw = false
			}
			return err
		}
	}

	switch b.rawBlock[0] {
	case blkUncompressed:
		if tail {
			if len(b.rawBlock)-1 > len(b.data) {
				l := len(b.data)
				b.data = b.data[:len(b.rawBlock)-1]
				copy(b.data[l:], b.rawBlock[l+1:])
			}
		} else {
			b.data = b.rawBlock[1:]
			b.dataIsRaw = true
		}
	case blkCompressed:
		err = b.loadCompressed(tail)
	}

	if err == nil {
		b.full = true
	}
	if !tail {
		b.dirty = false
	}

	return err
}

func (b *block) load(num int64) error {
	b.num = num
	return b.read(false)
}

func (b *block) createCorruptBlockError(err error) *ErrCorruptCompressedBlock {
	var size int64
	if int64(len(b.rawBlock)) == b.f.blockSize+1 {
		size = b.f.blockSize
	}
	return &ErrCorruptCompressedBlock{
		offset: b.num * b.f.blockSize,
		size:   size,
		err:    err,
	}
}

func (b *block) loadCompressed(tail bool) error {
	z := b.f.gzReader
	err := z.Reset(bytes.NewBuffer(b.rawBlock[1:]))
	if err != nil {
		return b.createCorruptBlockError(err)
	}
	z.Multistream(false)

	if !tail {
		b.data = b.dataBlock[:0]
		b.dataIsRaw = false
	}
	buf := bytes.NewBuffer(b.data)

	if len(b.data) > 0 {
		_, err := io.CopyN(io.Discard, z, int64(len(b.data)))
		if err != nil && err != io.EOF {
			return b.createCorruptBlockError(err)
		}

	}
	_, err = io.Copy(buf, z)
	if err != nil {
		return b.createCorruptBlockError(err)
	}
	b.data = buf.Bytes()

	l := int64(len(b.data))
	if l < b.f.blockSize {
		lastBlockNum := (b.f.size - headerSize) / (b.f.blockSize + 1)
		if lastBlockNum > b.num {
			b.data = b.data[:b.f.blockSize]
			for i := l; i < b.f.blockSize; i++ {
				b.data[i] = 0
			}
		}
	}

	return nil
}

func (b *block) prepareStore() (err error) {
	var curOffset int64

	if len(b.data) == 0 {
		curOffset = headerSize + b.num*(b.f.blockSize+1)
	} else if IsBlockZero(b.data) {
		curOffset = headerSize + b.num*(b.f.blockSize+1) + int64(len(b.data)) + 1
		if int64(len(b.data)) == b.f.blockSize {
			if b.f.size < curOffset {
				err = b.f.rawTruncate(curOffset)
				if err != nil {
					return
				}
			}
		}
	} else {
		b.prepareWrite()
		if b.rawBlock == nil {
			b.rawBlock = make([]byte, 0, b.f.blockSize+1)
		} else {
			b.rawBlock = b.rawBlock[:0]
		}
		b.ch = make(chan error, 1)
		b.f.compressor.CompressAsync(b.data, &b.rawBlock, b.ch)
		return nil
	}
	b.dirty = false

	if int64(len(b.data)) < b.f.blockSize {
		// last block
		err = b.f.rawTruncate(curOffset)
	}
	return
}

func (b *block) store() (err error) {
	if !b.dirty {
		return
	}
	if b.ch == nil {
		err = b.prepareStore()
		if err != nil {
			return
		}
		if !b.dirty {
			return nil
		}
	}
	err = <-b.ch
	b.ch = nil
	if err != nil {
		return
	}
	n := len(b.rawBlock)
	var curOffset int64
	if n+1 < len(b.data)-2*4096 { // save at least 2 blocks
		curOffset = headerSize + b.num*(b.f.blockSize+1) + int64(n)
	} else {
		b.rawBlock = b.rawBlock[:len(b.data)+1]
		b.rawBlock[0] = blkUncompressed
		copy(b.rawBlock[1:], b.data)
		curOffset = headerSize + b.num*(b.f.blockSize+1) + int64(len(b.data)) + 1
	}
	_, err = b.f.rawWriteAt(b.rawBlock, headerSize+b.num*(b.f.blockSize+1))
	if err != nil {
		return err
	}
	b.dirty = false

	if int64(len(b.data)) < b.f.blockSize {
		// last block
		err = b.f.rawTruncate(curOffset)
	}

	return
}

func (b *block) prepareWrite() {
	if b.dataIsRaw {
		if b.dataBlock == nil {
			b.dataBlock = make([]byte, len(b.data), b.f.blockSize)
		} else {
			b.dataBlock = b.dataBlock[:len(b.data)]
		}
		copy(b.dataBlock, b.data)
		b.data = b.dataBlock
		b.dataIsRaw = false
	}
}

// BlockSize returns the block size for optimal I/O. Reads and writes should be multiples of the block size and
// should be aligned with block boundaries.
func (f *SpgzFile) BlockSize() int64 {
	return f.blockSize
}

func (f *SpgzFile) Read(buf []byte) (n int, err error) {
	f.Lock()
	err = f.loadAt(f.offset)
	if err != nil {
		f.Unlock()
		return 0, err
	}
	o := f.offset - f.block.num*f.blockSize
	n = copy(buf, f.block.data[o:])
	f.offset += int64(n)
	if n == 0 {
		err = io.EOF
	}
	f.Unlock()
	return
}

func (f *SpgzFile) ReadAt(buf []byte, offset int64) (n int, err error) {
	if offset < 0 {
		return 0, os.ErrInvalid
	}
	f.Lock()
	for n < len(buf) {
		err = f.loadAt(offset)
		if err != nil {
			f.Unlock()
			return
		}
		o := offset - f.block.num*f.blockSize
		n1 := copy(buf[n:], f.block.data[o:])
		n += n1
		offset += int64(n1)
	}
	f.Unlock()
	return
}

func (f *SpgzFile) loadBlockFromQueue(num int64, block *block, full bool) (bool, error) {
	if len(f.writeQueue) > 0 {
		// Try to copy from the writeQueue if the block is there
		idx := num - f.writeQueue[0].num
		if idx >= 0 && idx < int64(len(f.writeQueue)) {
			b := &f.writeQueue[idx]
			if full {
				err := b.readTail()
				if err != nil {
					return true, err
				}
			}
			if block.dataBlock == nil {
				block.dataBlock = make([]byte, 0, f.blockSize)
				block.data = block.dataBlock
				block.dataIsRaw = false
			}
			block.data = block.data[:len(b.data)]
			copy(block.data, b.data)
			block.num = num
			block.dirty = false
			block.full = b.full
			return true, nil
		}
	}
	return false, nil
}

func (f *SpgzFile) loadBlock(num int64, block *block) error {
	if found, err := f.loadBlockFromQueue(num, block, true); found {
		return err
	}
	return block.load(num)
}

func (f *SpgzFile) loadNum(num int64) error {
	if f.loaded && f.block.dirty {
		err := f.storeBlock()
		if err != nil {
			return err
		}
	}
	err := f.loadBlock(num, &f.block)
	f.loaded = err == nil || err == io.EOF
	return err
}

func (f *SpgzFile) loadAt(offset int64) error {
	num := offset / f.blockSize
	if num != f.block.num || !f.loaded {
		return f.loadNum(num)
	}
	return f.block.readTail()
}

// moves the data and properties from the other block invalidating it
func (b *block) assign(other *block) {
	b.f = other.f
	b.num = other.num
	b.data, other.data = other.data, b.data
	b.rawBlock, other.rawBlock = other.rawBlock, b.rawBlock
	b.dataBlock, other.dataBlock = other.dataBlock, b.dataBlock
	b.dataIsRaw = other.dataIsRaw
	b.dirty = other.dirty
	b.full = other.full
}

func (f *SpgzFile) flushWriteQueue() error {
	if len(f.writeQueue) > 0 {
		for i := range f.writeQueue {
			b := &f.writeQueue[i]
			err := b.readTail()
			if err != nil {
				return err
			}
			err = b.prepareStore()
			if err != nil {
				return err
			}
		}
		holeStart := headerSize + f.writeQueue[0].num*(f.blockSize+1)
		holeSize := int64(len(f.writeQueue)) * (f.blockSize + 1)
		err := f.f.PunchHole(holeStart, holeSize)
		if err != nil {
			return err
		}
		for i := range f.writeQueue {
			err := f.writeQueue[i].store()
			if err != nil {
				return err
			}
		}
		f.writeQueue = f.writeQueue[:0]
	}
	return nil
}

func (f *SpgzFile) storeBlock() error {
	var idx int64
	if len(f.writeQueue) > 0 {
		idx = f.block.num - f.writeQueue[0].num
		if len(f.writeQueue) == cap(f.writeQueue) || idx < 0 || idx > int64(len(f.writeQueue)) {
			err := f.flushWriteQueue()
			if err != nil {
				return err
			}
			idx = 0
		}
	}
	if idx == int64(len(f.writeQueue)) {
		if len(f.writeQueue) > 0 {
			lastBlock := &f.writeQueue[len(f.writeQueue)-1]
			if int64(len(lastBlock.data)) < f.blockSize {
				err := lastBlock.expandToFullSize()
				if err != nil {
					return err
				}
			}
		}
		f.writeQueue = f.writeQueue[:idx+1]
	}
	f.writeQueue[idx].assign(&f.block)
	f.loaded = false
	return nil
}

func (f *SpgzFile) write(buf []byte, offset int64) (n int, err error) {
	if f.readOnly {
		return 0, os.ErrPermission
	}
	for len(buf) > 0 {
		num := offset / f.blockSize
		o := offset - num*f.blockSize

		if num != f.block.num || !f.loaded {
			if f.loaded && f.block.dirty {
				err = f.storeBlock()
				if err != nil {
					return
				}
			}
			if found, err := f.loadBlockFromQueue(num, &f.block, false); found {
				if err != nil {
					return 0, err
				}
			} else {
				f.block.num = num
				if f.block.dataBlock == nil {
					f.block.dataBlock = make([]byte, 0, f.blockSize)
				}
				f.block.data = f.block.dataBlock[:0]
				f.block.dataIsRaw = false
				f.block.dirty = false
				f.block.full = false
			}
			f.loaded = true
		}
		if o > int64(len(f.block.data)) {
			err = f.block.readTail()
			if err != nil {
				return
			}
		}

		newBlockSize := o + int64(len(buf))
		if newBlockSize > f.blockSize {
			newBlockSize = f.blockSize
		}
		l := int64(len(f.block.data))
		if newBlockSize > l {
			f.block.data = f.block.data[:newBlockSize]
			for i := l; i < o; i++ {
				f.block.data[i] = 0
			}
		}
		nn := copy(f.block.data[o:], buf)
		f.block.dirty = true
		n += nn
		offset += int64(nn)
		buf = buf[nn:]
	}

	return
}

func (f *SpgzFile) Write(buf []byte) (n int, err error) {
	f.Lock()
	n, err = f.write(buf, f.offset)
	f.offset += int64(n)
	f.Unlock()
	return
}

func (f *SpgzFile) WriteAt(buf []byte, offset int64) (n int, err error) {
	if offset < 0 {
		return 0, os.ErrInvalid
	}
	f.Lock()
	n, err = f.write(buf, offset)
	f.Unlock()
	return
}

func (f *SpgzFile) rawTruncate(size int64) error {
	if f.size == size {
		return nil
	}
	err := f.f.Truncate(size)
	if err == nil {
		f.size = size
	}
	return err
}

func (f *SpgzFile) rawWriteAt(b []byte, o int64) (int, error) {
	n, err := f.f.WriteAt(b, o)
	newSize := o + int64(n)
	if newSize > f.size {
		f.size = newSize
	}
	return n, err
}

// When a block is discarded before its contents is written because it falls in a hole we still need to ensure
// the file is expanded to the same size as if the block was actually written.
func (f *SpgzFile) expandToBlockSize(b *block) error {
	if int64(len(b.data)) < f.blockSize {
		return f.rawTruncate(headerSize + b.num*(f.blockSize+1) + int64(len(b.data)) + 1)
	}
	reqSize := headerSize + (b.num+1)*(f.blockSize+1)
	if f.size < reqSize {
		return f.rawTruncate(reqSize)
	}
	return nil
}

func (f *SpgzFile) PunchHole(offset, size int64) error {
	if offset < 0 || size < 0 {
		return os.ErrInvalid
	}
	if size == 0 {
		return nil
	}
	startZeroBlock := offset / f.blockSize
	l := offset - startZeroBlock*f.blockSize
	if l > 0 {
		startZeroBlock++
	}
	endZeroBlock := (offset + size) / f.blockSize
	f.Lock()
	defer f.Unlock()
	if len(f.writeQueue) > 0 {
		// Adjusting the writeQueue either by filling the affected blocks with zeros or by discarding tail blocks
		startIdx := startZeroBlock - f.writeQueue[0].num
		bl := int64(len(f.writeQueue))
		if startIdx < bl {
			if endIdx := endZeroBlock - f.writeQueue[0].num; endIdx > 0 {
				if startIdx < 0 {
					startIdx = 0
				}
				if endIdx >= bl {
					// The tail can be discarded
					err := f.expandToBlockSize(&f.writeQueue[len(f.writeQueue)-1])
					if err != nil {
						return err
					}
					f.writeQueue = f.writeQueue[:startIdx]
				} else {
					sec := f.writeQueue[startIdx:endIdx]
					for i := range sec {
						b := &sec[i]
						for j := range b.data {
							b.data[j] = 0
						}
					}
				}
			}
		}
	}
	if f.loaded && f.block.num >= startZeroBlock && f.block.num < endZeroBlock {
		// The currently loaded block falls in the hole, discard it
		err := f.expandToBlockSize(&f.block)
		if err != nil {
			return err
		}
		f.loaded = false
	}
	if l > 0 {
		err := f.loadAt(offset)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		if int64(len(f.block.data)) <= l {
			return nil
		}
		tail := f.block.data[l:]
		if int64(len(tail)) > size {
			tail = tail[:size]
		}
		for i := range tail {
			tail[i] = 0
		}
		f.block.dirty = true
		l = int64(len(tail))
		offset += l
		size -= l
	}

	if blocks := endZeroBlock - startZeroBlock; blocks > 0 {
		err := f.f.PunchHole(headerSize+startZeroBlock*(f.blockSize+1), blocks*(f.blockSize+1))
		if err != nil {
			return err
		}
		l = blocks * f.blockSize
		offset += l
		size -= l
	}
	if size > 0 {
		err := f.loadAt(offset)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		l := int(size)
		if l > len(f.block.data) {
			l = len(f.block.data)
		}
		head := f.block.data[:l]
		for i := range head {
			head[i] = 0
		}
		f.block.dirty = true
	}

	return nil
}

func (f *SpgzFile) Size() (int64, error) {
	f.Lock()
	defer f.Unlock()
	var lastBlock *block
	// Try to get the last block either from the writeQueue or from the currently loaded block
	if len(f.writeQueue) > 0 {
		lastBlock = &f.writeQueue[len(f.writeQueue)-1]
	}
	if f.loaded && (lastBlock == nil || f.block.num >= lastBlock.num) {
		lastBlock = &f.block
	}
	if lastBlock != nil && int64(len(lastBlock.data)) == f.blockSize {
		lastBlock = nil
	}

	if lastBlock == nil {
		o := f.size
		if f.size < headerSize {
			o = headerSize
		}
		lastBlockNum := (o - headerSize) / (f.blockSize + 1)
		lastBlock = &f.tmpBlock

		err := f.loadBlock(lastBlockNum, lastBlock)
		if err != nil {
			if err == io.EOF {
				return lastBlockNum * f.blockSize, nil
			}
			return 0, err
		}
	} else {
		err := lastBlock.readTail()
		if err != nil {
			return 0, err
		}
	}

	return lastBlock.num*f.blockSize + int64(len(lastBlock.data)), nil
}

func (f *SpgzFile) Seek(offset int64, whence int) (int64, error) {
	newOffset := f.offset
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset += offset
	case io.SeekEnd:
		size, err := f.Size()
		if err != nil {
			return newOffset, err
		}
		newOffset = size + offset
	default:
		return newOffset, os.ErrInvalid
	}
	if newOffset < 0 {
		return newOffset, os.ErrInvalid
	}
	f.offset = newOffset
	return newOffset, nil
}

// Expand the block to full size by padding it with zeros. This is done when the block used to be the last one,
// but there is another one written after it.
func (b *block) expandToFullSize() error {
	err := b.readTail()
	if err != nil {
		return err
	}
	tail := b.data[len(b.data):b.f.blockSize]
	if len(tail) > 0 {
		for i := range tail {
			tail[i] = 0
		}
		b.data = b.data[:b.f.blockSize]
		b.dirty = true
	}
	return nil
}

func (f *SpgzFile) Truncate(size int64) error {
	blockNum := size / f.blockSize
	newLen := int(size - blockNum*f.blockSize)
	var b *block

	f.Lock()
	defer f.Unlock()
	if f.loaded {
		if f.block.num == blockNum {
			b = &f.block
		} else if f.block.num < blockNum {
			err := f.block.expandToFullSize()
			if err != nil {
				return err
			}
		} else {
			f.loaded = false
		}
	}
	if len(f.writeQueue) > 0 {
		idx := blockNum - f.writeQueue[0].num
		if idx < 0 {
			f.writeQueue = f.writeQueue[:0]
		} else {
			if idx < int64(len(f.writeQueue)) {
				if newLen > 0 {
					f.writeQueue = f.writeQueue[:idx+1]
					if b == nil {
						b = &f.writeQueue[idx]
					}
				} else {
					f.writeQueue = f.writeQueue[:idx]
				}
			}
			idx--
			if idx >= 0 && idx < int64(len(f.writeQueue)) {
				err := f.writeQueue[idx].expandToFullSize()
				if err != nil {
					return err
				}
			}
		}
	}
	if newLen > 0 {
		if b == nil {
			err := f.loadNum(blockNum)
			if err != nil {
				if err != io.EOF {
					return err
				}
				return f.rawTruncate(headerSize + blockNum*(f.blockSize+1) + int64(newLen) + 1)
			}
			b = &f.block
		} else if len(b.data) < newLen {
			err := b.readTail()
			if err != nil {
				return err
			}
		}
		if newLen != len(b.data) {
			if newLen > len(b.data) {
				tail := b.data[len(b.data):newLen]
				for i := range tail {
					tail[i] = 0
				}
			}
			b.data = b.data[:newLen]
			b.dirty = true
		}
		if !b.dirty {
			return nil
		}
	}
	return f.rawTruncate(headerSize + blockNum*(f.blockSize+1))
}

func (f *SpgzFile) WriteTo(w io.Writer) (n int64, err error) {
	f.Lock()
	defer f.Unlock()

	for {
		err = f.loadAt(f.offset)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}
		buf := f.block.data[f.offset-f.block.num*f.blockSize:]
		if len(buf) == 0 {
			return
		}
		var written int
		written, err = w.Write(buf)
		f.offset += int64(written)
		n += int64(written)
		if err != nil {
			return
		}
	}
}

func (f *SpgzFile) ReadFrom(rd io.Reader) (n int64, err error) {
	if f.readOnly {
		return 0, os.ErrPermission
	}
	f.Lock()
	defer f.Unlock()

	for {
		err = f.loadAt(f.offset)
		if err != nil {
			if err != io.EOF {
				return
			}
		}
		o := int(f.offset - f.block.num*f.blockSize)
		buf := f.block.data[o:f.blockSize]
		var r int
		r, err = rd.Read(buf)
		nl := o + r
		l := len(f.block.data)
		if nl > l {
			f.block.data = f.block.data[:nl]
		}
		if l < o {
			a := f.block.data[l:o]
			for i := range a {
				a[i] = 0
			}
		}
		f.offset += int64(r)
		n += int64(r)
		f.block.dirty = true
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return
		}
	}
}

func (f *SpgzFile) flush() error {
	if f.loaded && f.block.dirty {
		err := f.storeBlock()
		if err != nil {
			return err
		}
	}
	return f.flushWriteQueue()
}

func (f *SpgzFile) Sync() error {
	f.Lock()
	defer f.Unlock()
	err := f.flush()
	if err != nil {
		return err
	}
	return f.f.Sync()
}

func (f *SpgzFile) Close() error {
	f.Lock()
	defer f.Unlock()

	err := f.flush()
	if err != nil {
		return err
	}

	return f.f.Close()
}

func (f *SpgzFile) checkPunchHole() error {
	// Check if punching holes is supported
	off, err := f.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if err := f.f.PunchHole(off, 4096); err != nil {
		return err
	}

	_, err = f.f.Seek(0, io.SeekStart)
	return err
}

func (f *SpgzFile) init(flag int, blockSize int64) error {
	if flag&os.O_WRONLY != 0 {
		return ErrWriteOnlyNotSupported
	}
	f.readOnly = flag&os.O_RDWR == 0
	// Trying to read the header
	buf := make([]byte, len(headerMagic)+4)
	_, err := io.ReadFull(f.f, buf)
	if err != nil {
		if err == io.EOF {
			// Empty file
			if flag&os.O_RDWR != 0 && flag&os.O_CREATE != 0 {
				err = f.checkPunchHole()
				if err != nil {
					return err
				}
				if blockSize == 0 {
					blockSize = DefBlockSize
				} else {
					blockSize = ((blockSize + 1) &^ 0xFFF) - 1
					if blockSize < MinBlockSize {
						blockSize = MinBlockSize
					}
				}
				copy(buf, headerMagic)
				binary.LittleEndian.PutUint32(buf[len(headerMagic):], uint32((blockSize+1)/4096))
				_, err = f.f.Write(buf)
				if err != nil {
					return err
				}
			} else {
				return ErrInvalidFormat
			}
		}
		if err == io.ErrUnexpectedEOF {
			return ErrInvalidFormat
		}
		if err != nil {
			return err
		}
	} else {
		if string(buf[:len(headerMagic)]) != headerMagic {
			return ErrInvalidFormat
		}
		if !f.readOnly {
			err = f.checkPunchHole()
			if err != nil {
				return err
			}
		}
		bs := binary.LittleEndian.Uint32(buf[8:])
		blockSize = int64(bs*4096) - 1
	}
	f.size, err = f.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	f.blockSize = blockSize
	f.block.init(f)
	f.tmpBlock.init(f)
	f.writeQueue = make([]block, 0, writeQueueSize)
	f.gzReader = new(gzip.Reader)
	f.compressor = newGzipCompressor(runtime.NumCPU())
	return nil
}

// OpenFile opens a file as SpgzFile with block size of DefBlockSize. See OpenFileSize for more details.
func OpenFile(name string, flag int, perm os.FileMode) (f *SpgzFile, err error) {
	return OpenFileSize(name, flag, perm, 0)
}

// OpenFileSize opens a file as SpgzFile. See NewFromFileSize for more details.
func OpenFileSize(name string, flag int, perm os.FileMode, blockSize int64) (f *SpgzFile, err error) {
	var ff *os.File
	if flag&os.O_WRONLY != 0 {
		return nil, ErrWriteOnlyNotSupported
	}
	ff, err = os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	f = &SpgzFile{
		f: NewSparseFile(ff),
	}

	err = f.init(flag, blockSize)
	if err != nil {
		_ = f.f.Close()
		return nil, err
	}

	return f, nil
}

// NewFromFile creates a new SpgzFile from the given *os.File with desired block size of DefBlockSize.
// See NewFromFileSize for more details.
func NewFromFile(file *os.File, flag int) (f *SpgzFile, err error) {
	return NewFromFileSize(file, flag, 0)
}

// NewFromFileSize creates a new SpgzFile from the given *os.File. File position must be 0.
//
// The blockSize parameter specifies the desired block size for newly created files. The actual block size will be
// DefBlockSize if the supplied value is 0, otherwise ((blockSize+1) &^ 0xFFF)-1 or MinBlockSize whichever is greater. For
// optimal I/O performance reads and writes should be sized in multiples of the actual block size and aligned at the
// block size boundaries. This can be achieved by using bufio.Reader and bufio.Writer.
// This parameter is ignored for already initialised files. The current block size can be retrieved using
// SpgzFile.BlockSize().
//
// The flag parameter is similar to that of os.OpenFile().
// If the file is empty it will be initialised if os.O_CREATE and os.O_RDWR are set, otherwise
// ErrInvalidFormat is returned. Write-only mode (os.O_WRONLY) is not supported.
//
// Returns ErrPunchHoleNotSupported if opening for writing and the underlying OS or filesystem does not support
// punching holes (as space-saving will not be possible in this case).
func NewFromFileSize(file *os.File, flag int, blockSize int64) (f *SpgzFile, err error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, ErrFileIsDirectory
	}

	return NewFromSparseFileSize(NewSparseFile(file), flag, blockSize)
}

func NewFromSparseFile(file SparseFile, flag int) (f *SpgzFile, err error) {
	return NewFromSparseFileSize(file, flag, 0)
}

func NewFromSparseFileSize(file SparseFile, flag int, blockSize int64) (f *SpgzFile, err error) {
	f = &SpgzFile{
		f: file,
	}

	err = f.init(flag, blockSize)
	if err != nil {
		return nil, err
	}

	return f, nil
}

package spgz

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	headerMagic = "SPGZ0001"
	headerSize  = 4096
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
	blockIsRaw          bool
	dirty               bool
}

type SpgzFile struct {
	sync.Mutex
	f         SparseFile
	blockSize int64
	readOnly  bool

	block  block
	loaded bool
	offset int64
}

func (b *block) init(f *SpgzFile) {
	b.f = f
	b.dirty = false
	b.blockIsRaw = false
}

func (b *block) load(num int64) error {
	b.num = num
	if b.rawBlock == nil {
		b.rawBlock = make([]byte, b.f.blockSize+1)
	} else {
		b.rawBlock = b.rawBlock[:b.f.blockSize+1]
	}

	if b.dataBlock == nil {
		b.dataBlock = make([]byte, b.f.blockSize)
	}

	n, err := b.f.f.ReadAt(b.rawBlock, headerSize+num*(b.f.blockSize+1))
	if err != nil {
		if err == io.EOF {
			if n > 0 {
				b.rawBlock = b.rawBlock[:n]
				err = nil
			} else {
				b.data = b.dataBlock[:0]
				b.blockIsRaw = false
				b.dirty = false
				return err
			}
		} else {
			b.data = b.dataBlock[:0]
			b.blockIsRaw = false
			return err
		}
	}

	switch b.rawBlock[0] {
	case blkUncompressed:
		b.data = b.rawBlock[1:]
		b.blockIsRaw = true
	case blkCompressed:
		err = b.loadCompressed()
	}

	b.dirty = false

	return err
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

func (b *block) loadCompressed() error {
	z, err := gzip.NewReader(bytes.NewBuffer(b.rawBlock[1:]))
	if err != nil {
		return b.createCorruptBlockError(err)
	}
	z.Multistream(false)

	buf := bytes.NewBuffer(b.dataBlock[:0])

	_, err = io.Copy(buf, z)
	if err != nil {
		return b.createCorruptBlockError(err)
	}
	b.data = buf.Bytes()
	b.blockIsRaw = false

	l := int64(len(b.data))
	if l < b.f.blockSize {
		o, err := b.f.f.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		lastBlockNum := (o - headerSize) / (b.f.blockSize + 1)
		if lastBlockNum > b.num {
			b.data = b.data[:b.f.blockSize]
			for i := l; i < b.f.blockSize; i++ {
				b.data[i] = 0
			}
		}
	}

	return nil
}

func (b *block) store(truncate bool) (err error) {
	var curOffset int64

	if len(b.data) == 0 {
		curOffset = headerSize + b.num*(b.f.blockSize+1)
	} else if IsBlockZero(b.data) {
		err = b.f.f.PunchHole(headerSize+b.num*(b.f.blockSize+1), int64(len(b.data))+1)
		if err != nil {
			return err
		}
		curOffset = headerSize + b.num*(b.f.blockSize+1) + int64(len(b.data)) + 1
	} else {
		b.prepareWrite()

		buf := bytes.NewBuffer(b.rawBlock[:0])

		reader := bytes.NewBuffer(b.data)

		buf.WriteByte(blkCompressed)

		w := gzip.NewWriter(buf)
		_, err = io.Copy(w, reader)
		if err != nil {
			return err
		}
		err = w.Close()
		if err != nil {
			return err
		}
		bb := buf.Bytes()
		n := len(bb)
		if n+1 < len(b.data)-2*4096 { // save at least 2 blocks
			_, err = b.f.f.WriteAt(bb, headerSize+b.num*(b.f.blockSize+1))
			if err != nil {
				return err
			}

			curOffset = headerSize + b.num*(b.f.blockSize+1) + int64(n)
		} else {
			buf.Reset()
			buf.WriteByte(blkUncompressed)
			buf.Write(b.data)
			_, err = b.f.f.WriteAt(buf.Bytes(), headerSize+b.num*(b.f.blockSize+1))
			curOffset = headerSize + b.num*(b.f.blockSize+1) + int64(len(b.data)) + 1
		}
	}

	if err != nil {
		return err
	}

	b.dirty = false

	if truncate {
		err = b.f.f.Truncate(curOffset)
	} else {
		if int64(len(b.data)) < b.f.blockSize {
			// last block
			err = b.f.f.Truncate(curOffset)
		} else {
			endOfBlock := headerSize + (b.num+1)*(b.f.blockSize+1)
			if holeSize := endOfBlock - curOffset; holeSize > 0 {
				err = b.f.f.PunchHole(curOffset, holeSize)
			}
		}
	}

	return
}

func (b *block) prepareWrite() {
	if b.blockIsRaw {
		if b.dataBlock == nil {
			b.dataBlock = make([]byte, len(b.data), b.f.blockSize)
		} else {
			b.dataBlock = b.dataBlock[:len(b.data)]
		}
		copy(b.dataBlock, b.data)
		b.data = b.dataBlock
		b.blockIsRaw = false
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

func (f *SpgzFile) loadAt(offset int64) error {
	num := offset / f.blockSize
	if num != f.block.num || !f.loaded {
		if f.loaded && f.block.dirty {
			err := f.block.store(false)
			if err != nil {
				return err
			}
		}
		err := f.block.load(num)
		f.loaded = err == nil || err == io.EOF
		return err
	}
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
				err = f.block.store(false)
				if err != nil {
					return
				}
			}
			if o == 0 && int64(len(buf)) >= f.blockSize {
				// The block is overwritten completely, there is no need to load it first
				f.block.num = num
				if f.block.dataBlock == nil {
					f.block.dataBlock = make([]byte, f.blockSize)
				}
				f.block.data = f.block.dataBlock[:0]
			} else {
				err = f.block.load(num)
				if err != nil {
					if err != io.EOF {
						return
					}
					err = nil
				}
			}
			f.loaded = true
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
	f.Lock()
	n, err = f.write(buf, offset)
	f.Unlock()
	return
}

func (f *SpgzFile) PunchHole(offset, size int64) error {
	num := offset / f.blockSize
	l := offset - num*f.blockSize
	f.Lock()
	defer f.Unlock()
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
		num++
	}

	blocks := size / f.blockSize

	if blocks > 0 {
		err := f.f.PunchHole(headerSize+num*(f.blockSize+1), blocks*(f.blockSize+1))
		if err != nil {
			return err
		}
		if f.loaded && f.block.num >= num && f.block.num < num+blocks {
			// The currently loaded block falls in the hole, discard it
			f.loaded = false
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
	o, err := f.f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if o < headerSize {
		o = headerSize
	}
	lastBlockNum := (o - headerSize) / (f.blockSize + 1)
	f.Lock()
	defer f.Unlock()
	if f.loaded && f.block.num >= lastBlockNum {
		return f.block.num*f.blockSize + int64(len(f.block.data)), nil
	}

	b := &block{
		f: f,
	}

	err = b.load(lastBlockNum)

	if err != nil && err != io.EOF {
		return 0, err
	}
	return lastBlockNum*f.blockSize + int64(len(b.data)), nil
}

func (f *SpgzFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
		return f.offset, nil
	case io.SeekCurrent:
		f.offset += offset
		return f.offset, nil
	case io.SeekEnd:
		size, err := f.Size()
		if err != nil {
			return f.offset, err
		}
		f.offset = size + offset
		return f.offset, nil
	}
	return f.offset, os.ErrInvalid
}

func (f *SpgzFile) Truncate(size int64) error {
	blockNum := size / f.blockSize
	newLen := int(size - blockNum*f.blockSize)
	var b *block

	f.Lock()
	defer f.Unlock()

	if f.loaded && f.block.num == blockNum {
		b = &f.block
	} else {
		if f.loaded && f.block.num < blockNum && int64(len(f.block.data)) < f.blockSize {
			// The last block which is loaded is no longer last, expand it with zeros
			tail := f.block.data[len(f.block.data):f.blockSize]
			for i := range tail {
				tail[i] = 0
			}
			f.block.data = f.block.data[:f.blockSize]
			f.block.dirty = true
		}
		if newLen != 0 {
			b = &block{
				f: f,
			}
			err := b.load(blockNum)
			if err != nil {
				if err == io.EOF {
					b = nil
				} else {
					return err
				}
			}
		}
		if b == nil {
			// Truncating beyond the current EOF (i.e. expanding)
			if newLen > 0 {
				// Add one byte (which will be zero) to indicate an uncompressed block
				newLen++
			}
			err := f.f.Truncate(headerSize + blockNum*(f.blockSize+1) + int64(newLen))
			if err != nil {
				return err
			}
			if f.loaded && f.block.num > blockNum {
				f.loaded = false
			}
			return nil
		}
	}

	if b == &f.block {
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
	} else {
		b.data = b.data[:newLen]
		err := b.store(true)
		if err != nil {
			return err
		}
	}

	if f.loaded && f.block.num > blockNum {
		f.loaded = false
	}

	return nil
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

func (f *SpgzFile) Sync() error {
	f.Lock()
	defer f.Unlock()

	if f.block.dirty {
		err := f.block.store(false)
		if err != nil {
			return err
		}
	}
	return f.f.Sync()
}

func (f *SpgzFile) Close() error {
	f.Lock()
	defer f.Unlock()

	if f.block.dirty {
		err := f.block.store(false)
		if err != nil {
			return err
		}
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
	f.blockSize = blockSize
	f.block.init(f)
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
		f.f.Close()
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

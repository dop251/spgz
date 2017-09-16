package spgz

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"log"
)

const (
	headerMagic = "SPGZ0001"
	headerSize  = 4096
)

const (
	defBlockSize = 128 * 1024 - 1
)

const (
	blkUncompressed byte = iota
	blkCompressed
)

var (
	ErrInvalidFormat         = errors.New("Invalid file format")
	ErrPunchHoleNotSupported = errors.New("The filesystem does not support punching holes. Use xfs or ext4")
	ErrFileIsDirectory       = errors.New("File cannot be a directory")
)

type block struct {
	f                   *compFile
	num                 int64
	data                []byte
	rawBlock, dataBlock []byte
	blockIsRaw          bool
	dirty               bool
}

type compFile struct {
	sync.Mutex
	f         SparseFile
	blockSize int64
	block     block
	loaded    bool

	offset int64
}

func (b *block) init(f *compFile) {
	b.f = f
	b.dirty = false
	b.blockIsRaw = false
}

func (b *block) load(num int64) error {
	// log.Printf("Loading block %d", num)
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
	// log.Printf("Loaded, size %d\n", len(b.data))
	return err

}

func (b *block) loadCompressed() error {
	// log.Println("Block is compressed")
	z, err := gzip.NewReader(bytes.NewBuffer(b.rawBlock[1:]))
	if err != nil {
		return err
	}
	z.Multistream(false)

	buf := bytes.NewBuffer(b.dataBlock[:0])

	_, err = io.Copy(buf, z)
	if err != nil {
		return err
	}
	b.data = buf.Bytes()
	b.blockIsRaw = false

	l := int64(len(b.data))
	if l < b.f.blockSize {
		o, err := b.f.f.Seek(0, os.SEEK_END)
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
	// log.Printf("Storing block %d", b.num)

	var curOffset int64

	if len(b.data) == 0 {
		curOffset = headerSize + b.num*(b.f.blockSize+1)
	} else if IsBlockZero(b.data) {
		// log.Println("Block is all zeroes")
		err = b.f.f.PunchHole(headerSize+b.num*(b.f.blockSize+1), int64(len(b.data))+1)
		if err != nil {
			err = ErrPunchHoleNotSupported
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
			// log.Printf("Storing compressed, size %d\n", n - 1)
			_, err = b.f.f.WriteAt(bb, headerSize+b.num*(b.f.blockSize+1))
			if err != nil {
				return err
			}

			curOffset = headerSize + b.num*(b.f.blockSize+1) + int64(n)
		} else {
			// log.Println("Storing uncompressed")
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
		var o int64
		o, err = b.f.f.Seek(0, os.SEEK_END)
		if err != nil {
			return err
		}
		if o < curOffset {
			err = b.f.f.Truncate(curOffset)
		} else if o > curOffset {
			endOfBlock := headerSize+(b.num+1)*(b.f.blockSize+1)
			if o < endOfBlock {
				err = b.f.f.Truncate(curOffset)
			}
			if holesize := endOfBlock - curOffset; holesize > 0 {
				err = b.f.f.PunchHole(curOffset, endOfBlock - curOffset)
				if err != nil {
					log.Printf("offset: %d, length: %d", curOffset, endOfBlock - curOffset)
					err = ErrPunchHoleNotSupported
				}
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

func (f *compFile) Read(buf []byte) (n int, err error) {
	// log.Printf("Read %d bytes at %d\n", len(buf), f.offset)
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

func (f *compFile) ReadAt(buf []byte, offset int64) (n int, err error) {
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

func (f *compFile) loadAt(offset int64) error {
	num := offset / f.blockSize
	if num != f.block.num || !f.loaded {
		if f.block.dirty {
			err := f.block.store(false)
			if err != nil {
				return err
			}
		}
		err := f.block.load(num)
		f.loaded = true
		return err
	}
	return nil
}

func (f *compFile) write(buf[] byte, offset int64) (n int, err error) {
	for len(buf) > 0 {
		// log.Printf("Writing %d bytes\n", len(buf))
		err = f.loadAt(offset)
		if err != nil {
			if err != io.EOF {
				return
			}
			err = nil
		}

		o := offset - f.block.num*f.blockSize
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

func (f *compFile) Write(buf []byte) (n int, err error) {
	f.Lock()
	n, err = f.write(buf, f.offset)
	f.offset += int64(n)
	f.Unlock()
	return
}

func (f *compFile) WriteAt(buf []byte, offset int64) (n int, err error) {
	f.Lock()
	n, err = f.write(buf, offset)
	f.Unlock()
	return
}

func (f *compFile) PunchHole(offset, size int64) error {
	num := offset / f.blockSize
	l := offset - num * f.blockSize
	blocks := size / f.blockSize
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
		for i := range tail {
			tail[i] = 0
		}
		f.block.dirty = true
		l = int64(len(f.block.data)) - l
		offset += l
		size -= l
		num++
	} else {
		if f.loaded && f.block.num >= num && f.block.num < num + blocks {
			// The currently loaded block falls in the hole, discard it
			f.loaded = false
		}
	}

	if blocks > 0 {
		err := f.f.PunchHole(headerSize+(num)*(f.blockSize+1), blocks*(f.blockSize+1))
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

func (f *compFile) Size() (int64, error) {
	o, err := f.f.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, err
	}
	if o <= headerSize {
		return 0, nil
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

func (f *compFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case os.SEEK_SET:
		f.offset = offset
		return f.offset, nil
	case os.SEEK_CUR:
		f.offset += offset
		return f.offset, nil
	case os.SEEK_END:
		size, err := f.Size()
		if err != nil {
			return f.offset, err
		}
		f.offset = size + offset
		return f.offset, nil
	}
	return f.offset, os.ErrInvalid
}

func (f *compFile) Truncate(size int64) error {
	blockNum := size / f.blockSize
	var b *block
	f.Lock()
	if f.loaded && f.block.num == blockNum {
		b = &f.block
	} else {
		b = &block{
			f: f,
		}
		err := b.load(blockNum)
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				f.Unlock()
				return err
			}
		}
	}

	newLen := int(size - blockNum*f.blockSize)

	b.data = b.data[:newLen]
	err := b.store(true)

	if f.loaded && f.block.num > blockNum {
		f.loaded = false
	}

	f.Unlock()
	return err
}

func (f *compFile) WriteTo(w io.Writer) (n int64, err error) {
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

func (f *compFile) ReadFrom(rd io.Reader) (n int64, err error) {
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
		if nl > len(f.block.data) {
			f.block.data = f.block.data[:nl]
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

func (f *compFile) Sync() error {
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

func (f *compFile) Close() error {
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

func (f *compFile) init(flag int, blockSize int64) error {
	blockSize &= 0xffffffffffff000
	if blockSize == 0 {
		blockSize = defBlockSize
	} else {
		blockSize--
	}

	f.block.init(f)

	// Trying to read the header
	buf := make([]byte, len(headerMagic)+4)

	_, err := io.ReadFull(f.f, buf)
	if err != nil {
		if err == io.EOF {
			// Empty file
			if flag&os.O_WRONLY != 0 || flag&os.O_RDWR != 0 {
				w := bytes.NewBuffer(buf[:0])
				w.WriteString(headerMagic)
				binary.Write(w, binary.LittleEndian, uint32((blockSize+1)/4096))
				_, err = f.f.Write(w.Bytes())
				if err != nil {
					return err
				}
				f.blockSize = blockSize
				return nil
			}
		}
		if err == io.ErrUnexpectedEOF {
			return ErrInvalidFormat
		}
		return err
	}
	if string(buf[:8]) != headerMagic {
		return ErrInvalidFormat
	}
	w := bytes.NewBuffer(buf[8:])
	var bs uint32
	binary.Read(w, binary.LittleEndian, &bs)
	f.blockSize = int64(bs*4096) - 1
	return nil
}

func OpenFile(name string, flag int, perm os.FileMode) (f *compFile, err error) {
	return OpenFileSize(name, flag, perm, 0)
}

func OpenFileSize(name string, flag int, perm os.FileMode, blockSize int64) (f *compFile, err error) {
	var ff *os.File
	ff, err = os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	f = &compFile{
		f: NewSparseFile(ff),
	}

	err = f.init(flag, blockSize)
	if err != nil {
		f.f.Close()
		return nil, err
	}

	return f, nil
}

func NewFromFile(file *os.File, flag int) (f *compFile, err error) {
	return NewFromFileSize(file, flag, 0)
}

func NewFromFileSize(file *os.File, flag int, blockSize int64) (f *compFile, err error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, ErrFileIsDirectory
	}

	f = &compFile{
		f: NewSparseFile(file),
	}

	err = f.init(flag, blockSize)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func NewFromSparseFile(file SparseFile, flag int) (f *compFile, err error) {
	return NewFromSparseFileSize(file, flag, 0)
}

func NewFromSparseFileSize(file SparseFile, flag int, blockSize int64) (f *compFile, err error) {
	f = &compFile{
		f: file,
	}

	err = f.init(flag, blockSize)
	if err != nil {
		return nil, err
	}

	return f, nil
}

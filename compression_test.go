package spgz

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime/debug"
	"testing"
)

func TestCompressedWrite(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	ct.truncate(0)

	buf := make([]byte, 1*1024*1024)
	ct.write(buf)

	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}
	ct.write(buf)

	o := ct.seek(0, io.SeekEnd)
	if o != 2*1024*1024 {
		t.Fatalf("Unexpected size: %d", o)
	}

	ct.check()
}

func TestCompressedWrite1(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	ct.truncate(0)

	buf := make([]byte, 2*1024*1024)
	rand.Read(buf)
	ct.write(buf)
	ct.reopen(true)
	ct.check()
}

func TestCompressedOverwriteShrink(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize
	ct.writeRandomBlock(0, int(blockSize/2))
	ct.writeUniformBlock(blockSize/2, int(blockSize/2)-2, 0)
	ct.reopen(false)
	ct.writeUniformBlock(0, int(blockSize)-1, 'x')
	ct.check()
}

func TestCompressedTruncate(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)

	buf := make([]byte, 1*1024*1024)
	rand.Read(buf)
	ct.write(buf)

	ct.truncate(163999)
	ct.checkSize(163999)

	ct.truncate(63999)
	ct.checkSize(63999)

	ct.truncate(ct.sf.blockSize)
	ct.checkSize(ct.sf.blockSize)

	ct.check()
}

func TestTruncateExpandWithDirtyLastBlock(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)

	block := make([]byte, MinBlockSize+1000)
	rand.Read(block)
	ct.write(block)

	ct.truncate(MinBlockSize*2 + 1000)

	ct.seek(0, io.SeekStart)

	ct.readFull(block[:MinBlockSize]) // flush the 2nd block

	ct.checkSize(MinBlockSize*2 + 1000)

	ct.check()
}

func TestTruncateNoChange(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)
	ct.writeRandomBlock(0, MinBlockSize*3/2)
	ct.sync()
	ct.truncate(MinBlockSize * 3 / 2)
	ct.check()
}

func TestTruncateExpandWithinBlock(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)

	block := make([]byte, MinBlockSize+1000)
	rand.Read(block)
	ct.write(block)

	ct.truncate(MinBlockSize + 2000)

	buf := make([]byte, 1000)
	ct.readAt(buf, MinBlockSize+1000)
	if !IsBlockZero(buf) {
		t.Fatal("not zero")
	}

	ct.check()
}

func TestTruncateExactlyAtBlockBoundary(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)

	block := make([]byte, MinBlockSize*2+1000)
	rand.Read(block)

	ct.write(block)

	ct.readAt(block[:MinBlockSize], 0)
	ct.truncate(MinBlockSize * 2)
	ct.checkSize(MinBlockSize * 2)

	ct.check()
}

func TestTruncate1(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize

	ct.writeUniformBlock(0, int(blockSize/2), 'x')
	ct.writeUniformBlock(blockSize, int(blockSize), 'x')

	ct.truncate(blockSize/2 + 10)
	ct.reopen(true)
	ct.checkSize(blockSize/2 + 10)

	ct.check()
}

func TestTruncate2(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize

	ct.writeUniformBlock(0, int(blockSize), 'x')
	ct.writeUniformBlock(blockSize, int(blockSize/2), 'x')
	ct.writeUniformBlock(blockSize*2, int(blockSize), 'x')

	ct.reopen(false)

	buf1 := make([]byte, 100)
	ct.writeAt(buf1, 0)

	ct.truncate(blockSize*3/2 + 10)
	ct.reopen(true)
	ct.checkSize(blockSize*3/2 + 10)

	ct.check()
}

func TestTruncateInvalidateCurrentBlock(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize

	ct.writeUniformBlock(0, int(blockSize*2), 'x')
	ct.sync()

	buf1 := make([]byte, int(blockSize))
	ct.readAt(buf1, blockSize)
	ct.truncate(blockSize / 2)

	n, err := ct.sf.ReadAt(buf1, blockSize)
	if err != io.EOF {
		t.Fatal("expected EOF")
	}
	if n != 0 {
		t.Fatal(n)
	}

	ct.writeUniformBlock(blockSize, int(blockSize), 'x')
	ct.writeUniformBlock(blockSize*2, int(blockSize), 'x')

	ct.truncate(blockSize)
	ct.checkSize(blockSize)

	ct.sync()
	ct.checkSize(blockSize)

	ct.check()
}

func TestTruncateLastBlockInWriteQueue(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize

	ct.writeUniformBlock(0, int(blockSize), 'x')
	ct.writeUniformBlock(blockSize, int(blockSize), 'x')

	buf1 := make([]byte, blockSize)

	ct.readAt(buf1, 0)
	ct.truncate(blockSize + blockSize/2)
	ct.checkSize(blockSize + blockSize/2)

	ct.sync()
	ct.checkSize(blockSize + blockSize/2)

	ct.check()
}

func TestTruncateAndZero(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize

	ct.writeUniformBlock(0, int(blockSize), 'x')
	ct.sync()

	buf1 := make([]byte, blockSize)

	ct.writeAt(buf1, 0)
	ct.sync()

	ct.truncate(blockSize / 2)

	ct.check()
}

func TestTruncateAndZeroBlockNotInCache(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)
	ct.writeUniformBlock(0, MinBlockSize/2, 0)
	ct.writeUniformBlock(MinBlockSize/2, MinBlockSize/2, 'z')
	ct.sync()
	ct.truncate(MinBlockSize / 2)
	ct.check()
}

func TestOverwriteLastCompressedWithZeros(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize

	ct.writeUniformBlock(0, int(blockSize), 'x')
	ct.sync()

	buf1 := make([]byte, blockSize)

	ct.writeAt(buf1, 0)

	ct.check()
}

func TestTruncateCurrentBlockInQueue(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize

	ct.writeUniformBlock(0, int(blockSize*2), 'x')

	buf1 := make([]byte, blockSize)
	ct.readAt(buf1, 0)

	ct.truncate(blockSize / 2)

	ct.writeUniformBlock(0, 100, 'x')

	ct.sync()
	ct.checkSize(blockSize / 2)

	ct.check()
}

func TestPunchHole(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)

	buf := make([]byte, 1*1024*1024)
	rand.Read(buf)
	ct.write(buf)

	ct.punchHole(100, 200)

	buf1 := append([]byte(nil), buf...)
	for i := 100; i < 300; i++ {
		buf1[i] = 0
	}

	ct.seek(0, io.SeekStart)
	buf2, err := io.ReadAll(ct.sf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf2, buf1) {
		t.Fatal("not equal")
	}

	ct.punchHole(3333, 777777)
	for i := 3333; i < 3333+777777; i++ {
		buf1[i] = 0
	}

	ct.punchHole(6*DefBlockSize, DefBlockSize)
	for i := 6 * DefBlockSize; i < 7*DefBlockSize; i++ {
		buf1[i] = 0
	}

	ct.punchHole(7*DefBlockSize+222, 10*DefBlockSize)
	for i := 7*DefBlockSize + 222; i < len(buf1); i++ {
		buf1[i] = 0
	}

	ct.seek(0, io.SeekStart)
	buf2, err = io.ReadAll(ct.sf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf2, buf1) {
		t.Fatal("not equal")
	}

	ct.reopen(true)

	buf2, err = io.ReadAll(ct.sf)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf2, buf1) {
		t.Fatal("not equal after reopen")
	}

	ct.check()
}

func TestPunchHoleKeepSize(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize

	buf := make([]byte, blockSize)
	ct.writeAt(buf, 0)

	ct.sync()

	ct.writeAt(buf[:len(buf)/2], blockSize)

	ct.punchHole(0, blockSize*2)
	ct.checkSize(blockSize + blockSize/2)

	ct.writeAt(buf, blockSize)

	ct.punchHole(0, 3*blockSize)
	ct.checkSize(2 * blockSize)

	ct.check()
}

func TestReadFromDirtyBuffer(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)

	buf := bytes.Repeat([]byte{'x'}, MinBlockSize+1)

	ct.write(buf)

	ct.seek(MinBlockSize+1001, io.SeekStart)

	ct.readFrom(buf)

	ct.reopen(true)
	ct.check()
}

func TestCorruptedCompression(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)
	ct.writeUniformBlock(0, MinBlockSize*2, 'x')

	ct.sync()

	// corrupting the first block
	zeroBuf := make([]byte, 1000)
	_, err := ct.f.WriteAt(zeroBuf, headerSize+16)
	if err != nil {
		t.Fatal(err)
	}

	rbuf := make([]byte, MinBlockSize)
	n, err := ct.sf.ReadAt(rbuf, 0)
	if err == nil {
		t.Fatal("err is nil")
	}
	if n != 0 {
		t.Fatalf("n is not 0: %d", n)
	}
	var ce *ErrCorruptCompressedBlock
	if errors.As(err, &ce) {
		if o := ce.Offset(); o != 0 {
			t.Fatalf("offset: %d", o)
		}
		if s := ce.Size(); s != MinBlockSize {
			t.Fatalf("size: %d", s)
		}
	} else {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Attempting to re-write part of the corrupted block
	buf := bytes.Repeat([]byte{'x'}, MinBlockSize)
	_, err = ct.sf.WriteAt(buf, 100)
	if err == nil {
		t.Fatal("err is nil")
	}

	// Attempting to re-write the whole corrupted block
	ct.writeAt(buf, 0)

	ct.check()
}

func TestCorruptedCompressionHeader(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)
	ct.writeUniformBlock(0, MinBlockSize*2, 'x')

	ct.sync()

	if ct.f.data[4096] != 1 {
		t.Fatal("first block is not compressed")
	}

	// corrupting the first block gzip header
	ct.f.data[4098] = ^ct.f.data[4098]

	rbuf := make([]byte, MinBlockSize)
	n, err := ct.sf.ReadAt(rbuf, 0)
	if err == nil {
		t.Fatal("err is nil")
	}
	if n != 0 {
		t.Fatalf("n is not 0: %d", n)
	}
	var ce *ErrCorruptCompressedBlock
	if errors.As(err, &ce) {
		if o := ce.Offset(); o != 0 {
			t.Fatalf("offset: %d", o)
		}
		if s := ce.Size(); s != MinBlockSize {
			t.Fatalf("size: %d", s)
		}
	} else {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestCorruptedLastBlock(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)
	buf := bytes.Repeat([]byte{'x'}, MinBlockSize)

	ct.writeAt(buf, 0)
	ct.writeAt(buf[:MinBlockSize-200], MinBlockSize)
	ct.sync()

	// corrupting the last block
	zeroBuf := make([]byte, 1000)
	_, err := ct.f.WriteAt(zeroBuf, headerSize+MinBlockSize+16)
	if err != nil {
		t.Fatal(err)
	}

	s, err := ct.sf.Size()
	if err == nil {
		t.Fatal("no error")
	}

	// Truncating to fix the error
	ct.truncate(MinBlockSize)

	// Attempting to write after the end of file
	ct.writeAt(buf[:1], s)

	ct.check()
}

func testShrinkLastBlock(t *testing.T, lastBlockSize int64, compressed bool) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)

	buf := make([]byte, MinBlockSize)
	if compressed {
		for i := range buf {
			buf[i] = 'x'
		}
	} else {
		rand.Read(buf)
	}
	ct.write(buf)

	ct.write(buf[:lastBlockSize])

	ct.reopen(false)
	ct.checkSize(MinBlockSize + lastBlockSize)

	ct.writeAt(buf[:lastBlockSize/2], MinBlockSize)

	ct.reopen(false)
	ct.checkSize(MinBlockSize + lastBlockSize)

	ct.truncate(MinBlockSize)
	ct.checkSize(MinBlockSize)

	ct.check()
}

func TestShrinkLastBlock(t *testing.T) {
	t.Run("uncompressed_short", func(t *testing.T) {
		testShrinkLastBlock(t, 1000, false)
	})

	t.Run("compressed_short", func(t *testing.T) {
		testShrinkLastBlock(t, 1000, true)
	})

	t.Run("uncompressed_full", func(t *testing.T) {
		testShrinkLastBlock(t, MinBlockSize, false)
	})

	t.Run("compressed_full", func(t *testing.T) {
		testShrinkLastBlock(t, MinBlockSize, true)
	})
}

func TestSize(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	ct.checkSize(0)

	ct.writeRandomBlock(0, 4096)
	ct.checkSize(4096)
}

func TestSizePartialBlock(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize
	ct.writeRandomBlock(0, int(blockSize))
	ct.reopen(false)
	ct.writeRandomBlock(0, int(blockSize/2))
	ct.checkSize(blockSize)
}

func TestLastCompressedShortExpand(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize
	buf := bytes.Repeat([]byte{'x'}, int(blockSize-7))
	ct.write(buf)
	ct.reopen(false)
	ct.seek(blockSize, io.SeekStart)

	block1 := []byte("Second block")
	ct.writeAt(block1, blockSize)

	ct.reopen(true)
	ct.check()
}

func TestPartialRead(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize
	ct.writeRandomBlock(0, int(blockSize))
	ct.sync()
	ct.writeUniformBlock(0, 100, 'z')
	ct.writeUniformBlock(200, 100, 'z')
	ct.check()
}

func TestPartialOverwriteInQueue(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)
	ct.writeRandomBlock(0, MinBlockSize*2)
	ct.writeRandomBlock(0, 1000)
	ct.check()
}

func TestOverwriteUncompressed(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize
	ct.writeRandomBlock(0, int(blockSize))
	ct.sync()
	buf := make([]byte, blockSize)
	ct.readAt(buf, 0)
	ct.writeUniformBlock(0, int(blockSize), 'x')
	ct.reopen(true)
	ct.check()
}

func TestOverwriteExpand(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	ct.writeUniformBlock(0, 1000, 'x')
	ct.sync()
	ct.writeUniformBlock(0, 2000, 'z')
	ct.sync()
	ct.check()
}

func TestPartialOverwriteReadFromQueue(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(MinBlockSize, false)
	ct.writeRandomBlock(0, MinBlockSize*3/2)
	ct.sync()
	ct.writeRandomBlock(MinBlockSize, 1000)
	ct.checkNoSync()
}

func TestPartialReadTruncate(t *testing.T) {
	ct := &cmpTest{t: t}

	ct.open(0, false)
	blockSize := ct.sf.blockSize
	ct.writeRandomBlock(0, int(blockSize)/2)
	ct.reopen(false)
	ct.writeRandomBlock(0, int(blockSize)/4)
	ct.truncate(blockSize / 4)
	ct.check()
}

func zeroTest1(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

var zeroBuf = make([]byte, 1*1024*1024)

func zeroTest2(buf []byte) bool {
	return bytes.Equal(buf, zeroBuf)
}

func BenchmarkZeroTest1(b *testing.B) {
	buf := make([]byte, 1*1024*1024)

	for i := 0; i < b.N; i++ {
		zeroTest1(buf)
	}

}

func BenchmarkZeroTest2(b *testing.B) {
	buf := make([]byte, 1*1024*1024)

	for i := 0; i < b.N; i++ {
		zeroTest2(buf)
	}

}

func BenchmarkZeroTest3(b *testing.B) {
	buf := make([]byte, 1*1024*1024)

	for i := 0; i < b.N; i++ {
		IsBlockZero(buf)
	}

}

func TestIsBlockZero(t *testing.T) {
	// Make sure it crosses the page boundary
	a := make([]byte, 4100)
	for i := 0; i < len(a); i++ {
		a[i] = 0xff
	}
	buf := a[len(a)-5:]
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	if !IsBlockZero(buf) {
		t.Fatal("cross-page block of 5 is not zero")
	}
	buf[3] = 0xff
	if IsBlockZero(buf) {
		t.Fatal("cross-page block of 5 is zero")
	}

	for i := 0; i < len(a); i++ {
		a[i] = 0xff
	}
	buf = a[4096-8 : 4096-8+5]
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	if !IsBlockZero(buf) {
		t.Fatal("end-of-page block of 5 is not zero")
	}

	buf = make([]byte, 11)
	if !IsBlockZero(buf) {
		t.Fatal("block of 11 is not zero")
	}
	buf[10] = 1
	if IsBlockZero(buf) {
		t.Fatal("block of 11 is zero")
	}

	buf = make([]byte, 111)
	if !IsBlockZero(buf) {
		t.Fatal("block of 11 is not zero")
	}

	buf[0] = 1
	if IsBlockZero(buf) {
		t.Fatal("block of 111 is zero")
	}

	buf[0] = 0
	buf[110] = 1
	if IsBlockZero(buf) {
		t.Fatal("block of 111 is zero (1)")
	}

	buf[110] = 0
	buf[51] = 1
	if IsBlockZero(buf) {
		t.Fatal("block of 111 is zero (2)")
	}

	if !IsBlockZero(buf[:0]) {
		t.Fatal("empty block is not zero")
	}
}

// Utilities

type memSparseFile struct {
	data   []byte
	offset int64
}

func (s *memSparseFile) Read(buf []byte) (n int, err error) {
	if s.offset >= int64(len(s.data)) {
		err = io.EOF
		return
	}
	n = copy(buf, s.data[s.offset:])
	s.offset += int64(n)
	return
}

func (s *memSparseFile) ensureSize(newSize int64) {
	if newSize > int64(len(s.data)) {
		if newSize <= int64(cap(s.data)) {
			l := int64(len(s.data))
			s.data = s.data[:newSize]
			for i := l; i < newSize; i++ {
				s.data[i] = 0
			}
		} else {
			d := make([]byte, newSize)
			copy(d, s.data)
			s.data = d
		}
	}
}

func (s *memSparseFile) Write(buf []byte) (n int, err error) {
	s.ensureSize(s.offset + int64(len(buf)))
	n = copy(s.data[s.offset:], buf)
	if n < len(buf) {
		err = io.ErrShortWrite
	}
	s.offset += int64(n)
	return
}

func (s *memSparseFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		s.offset = offset
		return s.offset, nil
	case io.SeekCurrent:
		s.offset += offset
		return s.offset, nil
	case io.SeekEnd:
		s.offset = int64(len(s.data)) + offset
		return s.offset, nil
	}
	return s.offset, errors.New("invalid whence")
}

func (s *memSparseFile) Truncate(size int64) error {
	if size > int64(len(s.data)) {
		if size <= int64(cap(s.data)) {
			l := len(s.data)
			s.data = s.data[:size]
			for i := l; i < len(s.data); i++ {
				s.data[i] = 0
			}
		} else {
			d := make([]byte, size)
			copy(d, s.data)
			s.data = d
		}
	} else if size < int64(len(s.data)) {
		s.data = s.data[:size]
	}
	return nil
}

func (s *memSparseFile) PunchHole(offset, size int64) error {
	if offset < int64(len(s.data)) {
		d := offset + size - int64(len(s.data))
		if d > 0 {
			size -= d
		}
		for i := offset; i < offset+size; i++ {
			s.data[i] = 0
		}
	}
	return nil
}

func (s *memSparseFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off < int64(len(s.data)) {
		n = copy(p, s.data[off:])
	}
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (s *memSparseFile) WriteAt(p []byte, off int64) (n int, err error) {
	s.ensureSize(off + int64(len(p)))
	n = copy(s.data[off:], p)
	return
}

func (s *memSparseFile) Close() error {
	return nil
}

func (s *memSparseFile) Sync() error {
	return nil
}

func (s *memSparseFile) Bytes() []byte {
	return s.data
}

type cmpTest struct {
	t      *testing.T
	f, ref memSparseFile
	sf     *SpgzFile
}

func (ct *cmpTest) fatal(args ...interface{}) {
	ct.t.Log(args...)
	ct.t.Log(string(debug.Stack()))
	ct.t.FailNow()
}

func (ct *cmpTest) open(blockSize int64, readOnly bool) {
	var flag int
	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_RDWR | os.O_CREATE
	}
	var err error
	if blockSize > 0 {
		ct.sf, err = NewFromSparseFileSize(&ct.f, flag, blockSize)
	} else {
		ct.sf, err = NewFromSparseFile(&ct.f, flag)
	}
	if err != nil {
		ct.fatal(err)
	}
}

func (ct *cmpTest) readFull(buf []byte) {
	_, err := io.ReadFull(ct.sf, buf)
	if err != nil {
		ct.fatal(err)
	}
}

func (ct *cmpTest) readAt(buf []byte, offset int64) {
	_, err := ct.sf.ReadAt(buf, offset)
	if err != nil {
		ct.fatal(err)
	}
}

func (ct *cmpTest) writeAt(block []byte, offset int64) {
	_, err := ct.sf.WriteAt(block, offset)
	if err != nil {
		ct.fatal(err)
	}
	_, _ = ct.ref.WriteAt(block, offset)
}

func (ct *cmpTest) writeUniformBlock(offset int64, size int, filler byte) {
	ct.writeAt(bytes.Repeat([]byte{filler}, size), offset)
}

func (ct *cmpTest) writeRandomBlock(offset int64, size int) {
	buf := make([]byte, size)
	rand.Read(buf)
	ct.writeAt(buf, offset)
}

func (ct *cmpTest) write(buf []byte) {
	_, err := ct.sf.Write(buf)
	if err != nil {
		ct.fatal(err)
	}
	_, _ = ct.ref.Write(buf)
}

func (ct *cmpTest) readFrom(buf []byte) {
	rd := bytes.NewBuffer(buf)
	_, err := ct.sf.ReadFrom(rd)
	if err != nil {
		ct.fatal(err)
	}
	_, _ = io.Copy(&ct.ref, bytes.NewBuffer(buf))
}

func (ct *cmpTest) seek(offset int64, whence int) int64 {
	o, err := ct.sf.Seek(offset, whence)
	if err != nil {
		ct.fatal(err)
	}
	_, _ = ct.ref.Seek(offset, whence)
	return o
}

func (ct *cmpTest) truncate(size int64) {
	err := ct.sf.Truncate(size)
	if err != nil {
		ct.fatal(err)
	}
	_ = ct.ref.Truncate(size)
}

func (ct *cmpTest) punchHole(offset, size int64) {
	err := ct.sf.PunchHole(offset, size)
	if err != nil {
		ct.fatal(err)
	}
	_ = ct.ref.PunchHole(offset, size)
}

func (ct *cmpTest) sync() {
	err := ct.sf.Sync()
	if err != nil {
		ct.fatal(err)
	}
}

func (ct *cmpTest) reopen(readOnly bool) {
	err := ct.sf.Close()
	if err != nil {
		ct.fatal(err)
	}
	_, _ = ct.f.Seek(0, io.SeekStart)
	ct.open(0, readOnly)
}

func (ct *cmpTest) checkSize(size int64) {
	actual, err := ct.sf.Size()
	if err != nil {
		ct.fatal(err)
	}
	if actual != size {
		ct.fatal(actual)
	}
}

func (ct *cmpTest) check() {
	ct.checkNoSync()
	ct.sync()
	ct.checkNoSync()
}

func (ct *cmpTest) checkNoSync() {
	ct.seek(0, io.SeekStart)
	buf, err := ioutil.ReadAll(ct.sf)
	if err != nil {
		ct.fatal(err)
	}
	if !bytes.Equal(buf, ct.ref.Bytes()) {
		ct.fatal("consistency check failed")
	}
}

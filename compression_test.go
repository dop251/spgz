package spgz

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"testing"
)

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
			for i := l; i < s.offset; i++ {
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

func TestCompressedWrite(t *testing.T) {
	var sf memSparseFile

	f, err := NewFromSparseFile(&sf, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Truncate(0)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1*1024*1024)
	_, err = f.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}
	_, err = f.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	o, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}

	if o != 2*1024*1024 {
		t.Fatalf("Unexpected size: %d", o)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestCompressedWrite1(t *testing.T) {
	var sf memSparseFile
	f, err := NewFromSparseFile(&sf, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Truncate(0)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 2*1024*1024)

	for i := 0; i < len(buf); i++ {
		buf[i] = byte(rand.Int31n(256))
	}

	_, err = f.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, _ = sf.Seek(0, io.SeekStart)

	f, err = NewFromSparseFile(&sf, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}

	buf1 := make([]byte, len(buf))

	_, err = io.ReadFull(f, buf1)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf, buf1) {
		t.Fatal("Blocks differ")
	}
}

func TestCompressedTruncate(t *testing.T) {
	var sf memSparseFile
	f, err := NewFromSparseFile(&sf, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1*1024*1024)

	for i := 0; i < len(buf); i++ {
		buf[i] = byte(rand.Int31n(256))
	}

	_, err = f.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Truncate(163999)
	if err != nil {
		t.Fatal(err)
	}

	sz, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if sz != 163999 {
		t.Fatalf("Unexpected size; %d", sz)
	}

	err = f.Truncate(63999)
	if err != nil {
		t.Fatal(err)
	}

	sz, err = f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if sz != 63999 {
		t.Fatalf("Unexpected size; %d", sz)
	}

	err = f.Truncate(f.blockSize)
	if err != nil {
		t.Fatal(err)
	}

	sz, err = f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if sz != f.blockSize {
		t.Fatalf("Unexpected size; %d", sz)
	}

}

func TestTruncateExpandWithDirtyLastBlock(t *testing.T) {
	var sf memSparseFile

	f, err := NewFromSparseFileSize(&sf, os.O_RDWR|os.O_CREATE, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	block := make([]byte, MinBlockSize+1000)
	rand.Read(block)

	_, err = f.Write(block)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Truncate(MinBlockSize*2 + 1000)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Read(block[:MinBlockSize]) // flush the 2nd block
	if err != nil {
		t.Fatal(err)
	}

	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != MinBlockSize*2+1000 {
		t.Fatalf("size: %d", size)
	}
}

func TestTruncateExpandWithinBlock(t *testing.T) {
	var sf memSparseFile

	f, err := NewFromSparseFileSize(&sf, os.O_RDWR|os.O_CREATE, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	block := make([]byte, MinBlockSize+1000)
	rand.Read(block)

	_, err = f.Write(block)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Truncate(MinBlockSize + 2000)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1000)
	_, err = f.ReadAt(buf, MinBlockSize+1000)
	if err != nil {
		t.Fatal(err)
	}
	if !IsBlockZero(buf) {
		t.Fatal("not zero")
	}
}

func TestTruncateExactlyAtBlockBoundary(t *testing.T) {
	var sf memSparseFile

	f, err := NewFromSparseFileSize(&sf, os.O_RDWR|os.O_CREATE, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	block := make([]byte, MinBlockSize*2+1000)
	rand.Read(block)

	_, err = f.Write(block)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.ReadAt(block[:MinBlockSize], 0)
	if err != nil {
		t.Fatal(err)
	}
	err = f.Truncate(MinBlockSize * 2)
	if err != nil {
		t.Fatal(err)
	}

	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != MinBlockSize*2 {
		t.Fatal(size)
	}
}

func TestPunchHole(t *testing.T) {
	var sf memSparseFile
	f, err := NewFromSparseFile(&sf, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1*1024*1024)

	rand.Read(buf)

	_, err = f.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	err = f.PunchHole(100, 200)
	if err != nil {
		t.Fatal(err)
	}

	buf1 := append([]byte(nil), buf...)
	for i := 100; i < 300; i++ {
		buf1[i] = 0
	}

	_, _ = f.Seek(0, io.SeekStart)
	buf2, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf2, buf1) {
		t.Fatal("not equal")
	}

	err = f.PunchHole(3333, 777777)
	if err != nil {
		t.Fatal(err)
	}

	for i := 3333; i < 3333+777777; i++ {
		buf1[i] = 0
	}

	_, _ = f.Seek(0, io.SeekStart)
	buf2, err = io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf2, buf1) {
		t.Fatal("not equal")
	}
}

func TestReadFromDirtyBuffer(t *testing.T) {
	var sf memSparseFile
	f, err := NewFromSparseFileSize(&sf, os.O_RDWR|os.O_CREATE, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, MinBlockSize+1)
	for i := range buf {
		buf[i] = 'x'
	}

	_, err = f.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(MinBlockSize+1001, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	rd := bytes.NewBuffer(buf)
	_, err = f.ReadFrom(rd)
	if err != nil {
		t.Fatal(err)
	}

	f.Close()
	_, err = sf.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	f1, err := NewFromSparseFile(&sf, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}

	s, err := f1.Size()
	if err != nil {
		t.Fatal(err)
	}

	if s != 2*(MinBlockSize+1)+1000 {
		t.Fatal(s)
	}
	buf1 := make([]byte, s)

	_, err = io.ReadFull(f1, buf1)
	if err != nil {
		t.Fatal(err)
	}

	expectRange(buf1, 0, len(buf), 'x', t)

	expectRange(buf1, len(buf), 1000, 0, t)

	expectRange(buf1, len(buf)+1000, len(buf), 'x', t)

}

func TestCorruptedCompression(t *testing.T) {
	var sf memSparseFile

	f, err := NewFromSparseFileSize(&sf, os.O_RDWR|os.O_CREATE, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, MinBlockSize)
	for i := range buf {
		buf[i] = 'x'
	}
	_, err = f.WriteAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt(buf, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	// corrupting the first block
	zeroBuf := make([]byte, 1000)
	_, err = sf.WriteAt(zeroBuf, headerSize+16)
	if err != nil {
		t.Fatal(err)
	}

	rbuf := make([]byte, MinBlockSize)
	n, err := f.ReadAt(rbuf, 0)
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
	_, err = f.WriteAt(buf, 100)
	if err == nil {
		t.Fatal("err is nil")
	}

	// Attempting to re-write the whole corrupted block
	_, err = f.WriteAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCorruptedCompressionHeader(t *testing.T) {
	var sf memSparseFile

	f, err := NewFromSparseFileSize(&sf, os.O_RDWR|os.O_CREATE, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, MinBlockSize)
	for i := range buf {
		buf[i] = 'x'
	}
	_, err = f.WriteAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt(buf, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	if sf.data[4096] != 1 {
		t.Fatal("first block is not compressed")
	}

	// corrupting the first block gzip header
	sf.data[4098] = ^sf.data[4098]

	rbuf := make([]byte, MinBlockSize)
	n, err := f.ReadAt(rbuf, 0)
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
	var sf memSparseFile

	f, err := NewFromSparseFileSize(&sf, os.O_RDWR|os.O_CREATE, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, MinBlockSize)
	for i := range buf {
		buf[i] = 'x'
	}
	_, err = f.WriteAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt(buf[:MinBlockSize-200], MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	// corrupting the last block
	zeroBuf := make([]byte, 1000)
	_, err = sf.WriteAt(zeroBuf, headerSize+MinBlockSize+16)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = sf.Seek(0, io.SeekStart)

	f, err = NewFromSparseFileSize(&sf, os.O_RDWR, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	s, err := f.Size()
	if err == nil {
		t.Fatal("no error")
	}

	// Truncating to fix the error
	err = f.Truncate(MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	// Attempting to write after the end of file
	_, err = f.WriteAt(buf[:1], s)
	if err != nil {
		t.Fatal(err)
	}

}

func testShrinkLastBlock(t *testing.T, lastBlockSize int64, compressed bool) {
	var sf memSparseFile

	f, err := NewFromSparseFileSize(&sf, os.O_RDWR|os.O_CREATE, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, MinBlockSize)
	if compressed {
		for i := range buf {
			buf[i] = 'x'
		}
	} else {
		rand.Read(buf)
	}
	_, err = f.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Write(buf[:lastBlockSize])
	if err != nil {
		t.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = sf.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	f, err = NewFromSparseFileSize(&sf, os.O_RDWR, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	if s, err := f.Size(); err != nil {
		t.Fatal(err)
	} else {
		if s != MinBlockSize+lastBlockSize {
			t.Fatalf("size: %d", s)
		}
	}

	_, err = f.WriteAt(buf[:lastBlockSize/2], MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, err = sf.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	f, err = NewFromSparseFileSize(&sf, os.O_RDWR, MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	if s, err := f.Size(); err != nil {
		t.Fatal(err)
	} else {
		if s != MinBlockSize+lastBlockSize {
			t.Fatalf("size: %d", s)
		}
	}

	err = f.Truncate(MinBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	if s, err := f.Size(); err != nil {
		t.Fatal(err)
	} else {
		if s != MinBlockSize {
			t.Fatalf("size: %d", s)
		}
	}
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
	var sf memSparseFile

	f, err := NewFromSparseFile(&sf, os.O_RDWR|os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}

	s, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if s != 0 {
		t.Fatal(s)
	}

	buf := make([]byte, 4096)
	rand.Read(buf)
	_, err = f.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	s, err = f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if s != int64(len(buf)) {
		t.Fatal(s)
	}
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

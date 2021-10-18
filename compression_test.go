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
	case os.SEEK_SET:
		s.offset = offset
		return s.offset, nil
	case os.SEEK_CUR:
		s.offset += offset
		return s.offset, nil
	case os.SEEK_END:
		s.offset = int64(len(s.data)) + offset
		return s.offset, nil
	}
	return s.offset, errors.New("Invalid whence")
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

	o, err := f.Seek(0, os.SEEK_END)
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

	sf.Seek(0, os.SEEK_SET)

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
	f, err := NewFromSparseFileSize(&sf, os.O_RDWR|os.O_CREATE, 4096)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 4096)
	for i := range buf {
		buf[i] = 'x'
	}

	_, err = f.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(5000, os.SEEK_SET)
	if err != nil {
		t.Fatal(err)
	}

	rd := bytes.NewBuffer(buf)
	_, err = f.ReadFrom(rd)
	if err != nil {
		t.Fatal(err)
	}

	f.Close()
	_, err = sf.Seek(0, os.SEEK_SET)
	if err != nil {
		t.Fatal(err)
	}

	f1, err := NewFromSparseFile(&sf, os.O_RDONLY)
	if err != nil {
		t.Fatal(err)
	}

	buf1 := make([]byte, 9096)

	_, err = io.ReadFull(f1, buf1)
	if err != nil {
		t.Fatal(err)
	}

	expectRange(buf1, 0, 4096, 'x', t)

	expectRange(buf1, 4096, 5000-4096, 0, t)

	expectRange(buf1, 5000, 4096, 'x', t)

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

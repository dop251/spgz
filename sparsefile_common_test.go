package spgz

import (
	"io"
	"testing"
)

type memSparseFileNoSupport struct {
	memSparseFile
}

func (s *memSparseFileNoSupport) PunchHole(offset, size int64) error {
	return ErrPunchHoleNotSupported
}

func expectRange(buf []byte, offset, size int, value byte, t *testing.T) {
	for i := offset; i < offset+size; i++ {
		if buf[i] != value {
			t.Fatalf("Invalid byte at %d: %d", i, buf[i])
		}
	}
}

func TestSparseWriterNoHoles(t *testing.T) {
	f := &memSparseFileNoSupport{}
	_, err := f.Write([]byte("XXXXXXXX"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(4, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Write(make([]byte, 8))
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	var b [12]byte
	_, err = io.ReadFull(f, b[:])
	if err != nil {
		t.Fatal(err)
	}

	if s := string(b[:]); s != "XXXX\x00\x00\x00\x00\x00\x00\x00\x00" {
		t.Fatalf("Unexpected value: '%s'", s)
	}

	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}

	if off != 12 {
		t.Fatalf("Unexpected file size: %d", off)
	}
}

func TestNewSparseFileWithFallback(t *testing.T) {
	f := &memSparseFileNoSupport{}
	sf := &SparseFileWithFallback{SparseFile: f}

	buf := make([]byte, 128*1024)
	for i := range buf {
		buf[i] = 'x'
	}
	_, err := sf.Write(buf)
	if err != nil {
		t.Fatal(err)
	}

	err = sf.PunchHole(3333, 50*1024)
	if err != nil {
		t.Fatal(err)
	}

	_ = sf.Close()

	expectRange(f.data, 0, 3333, 'x', t)

	expectRange(f.data, 3333, 50*1024, 0, t)

	expectRange(f.data, 3333+50*1024, 128*1024-(3333+50*1024), 'x', t)
}

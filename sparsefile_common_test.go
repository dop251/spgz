package spgz

import (
	"errors"
	"testing"
	"os"
	"io"
)

type memSparseFileNoSupport struct {
	memSparseFile
}

func (s *memSparseFileNoSupport) PunchHole(offset, size int64) error {
	return errors.New("Punching holes is not supported")
}

func TestSparseWriterNoHoles(t *testing.T) {
	f := &memSparseFileNoSupport{}
	_, err := f.Write([]byte("XXXXXXXX"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(4, os.SEEK_SET)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Write(make([]byte, 8))
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Seek(0, os.SEEK_SET)
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

	off, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		t.Fatal(err)
	}

	if off != 12 {
		t.Fatalf("Unexpected file size: %d", off)
	}
}

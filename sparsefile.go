package spgz

import (
	"io"
	"os"
	"syscall"
)

const (
	FALLOC_FL_KEEP_SIZE  = 0x01 /* default is extend size */
	FALLOC_FL_PUNCH_HOLE = 0x02 /* de-allocates range */
)

type Truncatable interface {
	Truncate(size int64) error
}

type SparseFile interface {
	io.ReadWriteSeeker
	io.ReaderAt
	io.WriterAt
	io.Closer
	Truncatable

	PunchHole(offset, size int64) error
	Sync() error
}

type sparseFile struct {
	*os.File
}

func NewSparseFile(f *os.File) *sparseFile {
	return &sparseFile{
		File: f,
	}
}

func NewSparseWriter(f SparseFile) *SparseWriter {
	return &SparseWriter{
		SparseFile: f,
	}
}

func (f *sparseFile) PunchHole(offset, size int64) error {
	return syscall.Fallocate(int(f.File.Fd()), FALLOC_FL_KEEP_SIZE|FALLOC_FL_PUNCH_HOLE, offset, size)
}

type SparseWriter struct {
	SparseFile
}

func (w *SparseWriter) Write(p []byte) (int, error) {
	if IsBlockZero(p) {
		offset, err := w.SparseFile.Seek(0, os.SEEK_CUR)
		if err != nil {
			return 0, err
		}
		err = w.PunchHole(offset, int64(len(p)))
		if err != nil {
			return 0, err
		}
		end, err := w.Seek(0, os.SEEK_END)
		offset += int64(len(p))
		_, err = w.Seek(offset, os.SEEK_SET)
		if err != nil {
			return 0, err
		}
		if end < offset {
			err = w.Truncate(offset)
			if err != nil {
				return 0, err
			}
		}
		return len(p), nil
	} else {
		return w.SparseFile.Write(p)
	}
}

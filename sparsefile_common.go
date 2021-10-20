package spgz

import (
	"io"
	"os"
)

const (
	BUFSIZE = 32768
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

type SparseWriter struct {
	SparseFile
}

type SparseFileWithFallback struct {
	SparseFile
	fallback bool
}

// NewSparseFileWithFallback creates a sparse file that will fall back to writing zeros if the first call to
// SparseFile.PunchHole() returns ErrPunchHoleNotSupported.
func NewSparseFileWithFallback(f *os.File) *SparseFileWithFallback {
	return &SparseFileWithFallback{
		SparseFile: NewSparseFile(f),
	}
}

// NewSparseFileWithoutHolePunching creates a sparse file that will write zeros instead of calling SparseFile.PunchHole()
func NewSparseFileWithoutHolePunching(f *os.File) *SparseFileWithFallback {
	return &SparseFileWithFallback{
		SparseFile: NewSparseFile(f),
		fallback:   true,
	}
}

func NewSparseWriter(f SparseFile) *SparseWriter {
	return &SparseWriter{
		SparseFile: f,
	}
}

func (w *SparseWriter) Write(p []byte) (int, error) {
	if IsBlockZero(p) {
		offset, err := w.SparseFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		end, err := w.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		l := int64(len(p))
		if offset < end {
			if offset+l < end {
				err = w.PunchHole(offset, int64(len(p)))
				if err != nil {
					return 0, err
				}
			} else {
				err = w.Truncate(offset)
				if err != nil {
					return 0, err
				}
				end = offset
			}
		}
		offset += l
		_, err = w.Seek(offset, io.SeekStart)
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

func (f *SparseFileWithFallback) PunchHole(offset, size int64) error {
	if !f.fallback {
		err := f.SparseFile.PunchHole(offset, size)
		if err != nil {
			if err == ErrPunchHoleNotSupported {
				f.fallback = true
			} else {
				return err
			}
		} else {
			return nil
		}
	}

	var buf [BUFSIZE]byte
	for size > 0 {
		var s int64
		if size > BUFSIZE {
			s = BUFSIZE
		} else {
			s = size
		}
		_, err := f.WriteAt(buf[:s], offset)
		if err != nil {
			return err
		}
		offset += s
		size -= s
	}

	return nil
}

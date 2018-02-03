// +build !linux

package spgz

import (
	"os"
	"errors"
)

var ErrPunchHoleNotSupported = errors.New("Punching holes is not supported on this platform")

type sparseFile struct {
	*os.File
}

func (f *sparseFile) PunchHole(offset, size int64) error {
	return ErrPunchHoleNotSupported
}

func NewSparseFile(f *os.File) *sparseFile {
	return &sparseFile{
		File: f,
	}
}

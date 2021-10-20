//go:build linux
// +build linux

package spgz

type BuseDevice struct {
	*SpgzFile
}

func NewBuseDevice(f *SpgzFile) *BuseDevice {
	return &BuseDevice{f}
}

func (d *BuseDevice) Trim(off, length int64) error {
	return d.PunchHole(off, length)
}

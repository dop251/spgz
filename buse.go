// +build linux

package spgz

type BuseDevice struct {
	*compFile
}

func NewBuseDevice(f *compFile) *BuseDevice {
	return &BuseDevice{f}
}

func (d *BuseDevice) Trim(off, length int64) error {
	return d.PunchHole(off, length)
}

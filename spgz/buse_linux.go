package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"

	"github.com/dop251/buse"
	"github.com/dop251/spgz"

	log "github.com/sirupsen/logrus"
)

const buseAvailable = true

func getBuseFlags() (*string, *int, *bool) {
	return flag.String("b", "", "BUSE mode: connect the `file` to a local nbd device (specified by arg)"),
		flag.Int("buse-block-size", 512, "Block (sector) `size` for BUSE device"),
		flag.Bool("buse-readonly", false, "Open file in read-only mode")
}

func checkDev(dev string) error {
	_, err := os.Stat(dev)
	if err != nil {
		if os.IsNotExist(err) {
			// device does not exist, try running 'modprobe nbd'
			cmd := exec.Command("modprobe", "nbd")
			err = cmd.Run()
			if err != nil {
				return fmt.Errorf("%q does not exist and 'modprobe nbd' has failed: %w", dev, err)
			}
		} else {
			return err
		}
	}
	return nil
}

func doBuse(file, dev string, blockSize int, readOnly bool) {
	var mode int
	if readOnly {
		mode = os.O_RDONLY
	} else {
		mode = os.O_RDWR
	}
	f, err := spgz.OpenFile(file, mode, 0666)
	if err != nil {
		log.Fatalf("Could not open file: %v", err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		log.Fatalf("Could not get size: %v", err)
	}
	err = checkDev(dev)
	if err != nil {
		log.Fatal(err)
	}
	device, err := buse.NewDevice(dev, size, spgz.NewBuseDevice(f))
	if err != nil {
		log.Fatalf("Could not create a device: %v", err)
	}
	device.SetLogger(log.StandardLogger())
	if blockSize > 0 {
		device.SetBlockSize(blockSize)
	}
	device.SetReadOnly(readOnly)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	disc := make(chan error, 1)
	go func() {
		disc <- device.Run()
	}()
	select {
	case <-sig:
		// Received SIGTERM, cleanup
		log.Infoln("SIGINT, disconnecting...")
		device.Disconnect()
		err := <-disc
		if err != nil {
			log.Warnf("Disconnected, exiting. Err: %v\n", err)
		} else {
			log.Infoln("Disconnected, exiting")
		}
	case err := <-disc:
		log.Warnf("Disconnected, err: %v\n", err)
	}
}

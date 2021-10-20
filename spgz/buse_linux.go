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

func getBuseFlag() *string {
	return flag.String("b", "", "Connect to a local nbd device")
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

func doBuse(file, dev string) {
	f, err := spgz.OpenFile(file, os.O_RDWR, 0666)
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

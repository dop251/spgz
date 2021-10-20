package main

import (
	"flag"
	"os"
	"os/signal"

	"github.com/dop251/buse"
	"github.com/dop251/spgz"

	log "github.com/sirupsen/logrus"
)

func getBuseFlag() *string {
	return flag.String("b", "", "Connect to a local nbd device")
}

func doBuse(file, dev string) {
	f, err := spgz.OpenFile(file, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("Could not open file: %v", err)
	}
	size, err := f.Size()
	if err != nil {
		log.Fatalf("Could not get size: %v", err)
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

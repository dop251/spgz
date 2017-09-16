package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"

	"github.com/dop251/spgz"
	"github.com/dop251/buse"
)

type fileType int
const (
	_FTYPE_FILE    fileType = iota
	_FTYPE_BLKDEV
	_FTYPE_STREAM
)

func usage() {
	s := "Compress:\n    %[1]s -c <compressed_file> <source>\n\nExtract:\n    %[1]s -x <compressed_file> [--no-sparse] <target>\n\n"+
		"Get original size:\n    %[1]s -s <compressed_file>\n\nConnect to a local nbd device:\n    %[1]s -b <compressed_file> /dev/nbd...\n"

	fmt.Fprintf(os.Stderr, s, os.Args[0])
	os.Exit(1)
}

func getFileType(f *os.File) (fileType, error) {
	info, err := f.Stat()
	if err != nil {
		return _FTYPE_FILE, err
	}

	mode := info.Mode()
	if !mode.IsRegular() {
		if mode & (os.ModeCharDevice | os.ModeNamedPipe | os.ModeSocket) != 0 {
			return _FTYPE_STREAM, nil
		}
		if mode & os.ModeDevice != 0 {
			return _FTYPE_BLKDEV, nil
		}
	}

	return _FTYPE_FILE, nil
}

func failOptions() {
	fmt.Fprint(os.Stderr, "-c, -s, and -x are mutually exclusive\n\n")
	usage()
}

func main() {
	var buse = flag.String("b", "", "Connect to a local nbd device")
	var create = flag.String("c", "", "Create compressed file")
	var extract = flag.String("x", "", "Extract compressed file")
	var size = flag.String("s", "", "Get original size in bytes")
	var noSparse = flag.Bool("no-sparse", false, "Disable sparse file")
	var debug = flag.Bool("debug", false, "Enable debug logging")


	flag.Parse()

	name := flag.Arg(0)

	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	if *extract != "" {
		if *create != "" || *size != "" {
			failOptions()
		}
		f, err := spgz.OpenFile(*extract, os.O_RDONLY, 0666)
		if err != nil {
			log.Fatalf("Could not open compressed file: %v", err)
		}
		defer f.Close()

		var (
			out   *os.File
			ftype fileType
		)

		if name == "-" {
			out = os.Stdout
		} else {
			out, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0640)
			if err != nil {
				log.Fatalf("Could not open output file: %v", err)
			}
		}

		ftype, err = getFileType(out)
		if err != nil {
			out.Close()
			log.Fatalf("Could not determine the target file type: %v", err)
		}

		var w io.WriteCloser
		if ftype == _FTYPE_BLKDEV {
			size, err := out.Seek(0, os.SEEK_END)
			if err != nil {
				log.Fatalf("Could not determine target device size: %v", err)
			}
			srcSize, err := f.Size()
			if err != nil {
				log.Fatalf("Could not determine source size: %v", err)
			}
			if size != srcSize {
				log.Fatalf("Target device size (%d) does not match source size (%d)", size, srcSize)
			}
			_, err = out.Seek(0, os.SEEK_SET)
			if err != nil {
				log.Fatalf("Seek failed: %v", err)
			}
			w = out
		} else {
			if ftype == _FTYPE_FILE {
				err = out.Truncate(0)
				if err != nil {
					log.Printf("Truncate() failed: %v", err)
				}
			}
			if *noSparse || ftype != _FTYPE_FILE {
				w = out
			} else {
				w = spgz.NewSparseWriter(spgz.NewSparseFile(out))
			}
		}

		defer w.Close()

		_, err = io.Copy(w, f)
		if err != nil {
			log.Fatalf("Copy failed: %v", err)
		}
	} else if *create != "" {
		if *size != "" {
			failOptions()
		}

		var in io.Reader
		if name != "-" {
			f, err := os.Open(name)
			if err != nil {
				log.Fatalf("Could not open source file ('%s'): %v", name, err)
			}
			in = f
		} else {
			in = os.Stdin
		}

		f, err := spgz.OpenFile(*create, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			log.Fatalf("Could not open file: %v", err)
		}

		_, err = io.Copy(f, in)
		if err != nil {
			log.Fatalf("Copy failed: %v", err)
		}
		err = f.Close()
		if err != nil {
			log.Fatalf("Close failed: %v", err)
		}
	} else if *buse != "" {
		doBuse(*buse, name)
	} else if *size != "" {
		f, err := spgz.OpenFile(*size, os.O_RDONLY, 0666)
		if err != nil {
			log.Fatalf("Could not open file: %v", err)
		}
		s, err := f.Size()
		if err != nil {
			log.Fatalf("Could not get file size: %v", err)
		}
		fmt.Printf("%d\n", s)
	} else {
		usage()
	}
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

	sig := make(chan os.Signal)
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

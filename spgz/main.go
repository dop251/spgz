package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/dop251/spgz"

	log "github.com/sirupsen/logrus"
)

type fileType int

const (
	fileTypeRegular fileType = iota
	fileTypeBlockDev
	fileTypeStream
)

func usage() {
	buseFlag := ""
	if buseAvailable {
		buseFlag = "b"
	}
	fmt.Printf("Usage: %s [-cxs%s] file [options] arg\n\n", os.Args[0], buseFlag)
	flag.PrintDefaults()
	os.Exit(1)
}

func getFileType(f *os.File) (fileType, error) {
	info, err := f.Stat()
	if err != nil {
		return fileTypeRegular, err
	}

	mode := info.Mode()
	if !mode.IsRegular() {
		if mode&(os.ModeCharDevice|os.ModeNamedPipe|os.ModeSocket) != 0 {
			return fileTypeStream, nil
		}
		if mode&os.ModeDevice != 0 {
			return fileTypeBlockDev, nil
		}
	}

	return fileTypeRegular, nil
}

func failOptions() {
	fmt.Fprint(os.Stderr, "-c, -s, and -x are mutually exclusive\n\n")
	usage()
}

func main() {
	var buse, blockSize, readOnly = getBuseFlags()
	var create = flag.String("c", "", "Create compressed `file`")
	var extract = flag.String("x", "", "Extract compressed `file`")
	var size = flag.String("s", "", "Get original size of `file` in bytes")
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
		if ftype == fileTypeBlockDev {
			size, err := out.Seek(0, io.SeekEnd)
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
			_, err = out.Seek(0, io.SeekStart)
			if err != nil {
				log.Fatalf("Seek failed: %v", err)
			}
			w = out
		} else {
			if ftype == fileTypeRegular {
				err = out.Truncate(0)
				if err != nil {
					log.Printf("Truncate() failed: %v", err)
				}
			}
			if *noSparse || ftype != fileTypeRegular {
				w = out
			} else {
				w = spgz.NewSparseWriter(spgz.NewSparseFileWithFallback(out))
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
		doBuse(*buse, name, *blockSize, *readOnly)
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

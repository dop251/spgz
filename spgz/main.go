package main

import (
	"github.com/dop251/spgz"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

type fileType int
const (
	_FTYPE_FILE    fileType = iota
	_FTYPE_BLKDEV
	_FTYPE_STREAM
)

func usage() {
	fmt.Fprintf(os.Stderr, "Compress:\n    %[1]s -c <compressed_file> <source>\n\nExtract:\n    %[1]s -x <compressed_file> [--no-sparse] <target>\n", os.Args[0])
	os.Exit(2)
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

func main() {
	var create = flag.String("c", "", "Create compressed file")
	var extract = flag.String("x", "", "Extract compressed file")
	var noSparse = flag.Bool("no-sparse", false, "Disable sparse file")

	flag.Parse()

	name := flag.Arg(0)

	if *create == "" && *extract == "" {
		usage()
	}

	if *create != "" && *extract != "" {
		fmt.Fprintf(os.Stderr, "-c and -x are mutually exclusive")
		usage()
	}

	if *extract != "" {
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
	} else {
		f, err := spgz.OpenFile(*create, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			log.Fatalf("Could not open file: %v", err)
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

		_, err = io.Copy(f, in)
		if err != nil {
			log.Fatalf("Copy failed: %v", err)
		}
		err = f.Close()
		if err != nil {
			log.Fatalf("Close failed: %v", err)
		}

	}
}

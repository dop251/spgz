//go:build !linux
// +build !linux

package main

import log "github.com/sirupsen/logrus"

const buseAvailable = false

var noBuse = ""
var noBuseBlockSize int
var noBuseReadOnly bool

func getBuseFlags() (*string, *int, *bool) {
	return &noBuse, &noBuseBlockSize, &noBuseReadOnly
}

func doBuse(_, _ string, _ int, _ bool) {
	log.Fatal("BUSE only available in Linux")
}

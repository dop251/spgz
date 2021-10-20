//go:build !linux
// +build !linux

package main

import log "github.com/sirupsen/logrus"

var noBuse = ""

func getBuseFlag() *string {
	return &noBuse
}

func doBuse(_, _ string) {
	log.Fatal("Buse only available in Linux")
}

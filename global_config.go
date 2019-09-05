package dastard

import "time"

// Portnumbers structs can contain all TCP port numbers used by Dastard.
type Portnumbers struct {
	RPC            int
	Status         int
	Trigs          int
	SecondaryTrigs int
	Summaries      int
}

// Ports globally holds all TCP port numbers used by Dastard.
var Ports Portnumbers

func setPortnumbers(base int) {
	Ports.RPC = base
	Ports.Status = base + 1
	Ports.Trigs = base + 2
	Ports.SecondaryTrigs = base + 3
	Ports.Summaries = base + 4
}

// BuildInfo can contain compile-time information about the build
type BuildInfo struct {
	Version string
	Githash string
	Date    string
}

// Build is a global holding compile-time information about the build
var Build = BuildInfo{
	Version: "0.2.6",
	Githash: "no git hash computed",
	Date:    "no build date computed",
}

// DastardStartTime is a global holding the time init() was run
var DastardStartTime time.Time

func init() {
	setPortnumbers(5500)
	DastardStartTime = time.Now()
}

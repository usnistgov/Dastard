package dastard

import (
	"testing"
	"time"

	"github.com/usnistgov/dastard/lancero"
)

// TestChannelOrder checks the map from channel to readout numbering
func TestChannelOrder(t *testing.T) {
	ls, err := NewLanceroSource()
	if err != nil {
		t.Error("NewLanceroSource failed:", err)
	}
	d0 := LanceroDevice{devnum: 0, nrows: 4, ncols: 4}
	d1 := LanceroDevice{devnum: 0, nrows: 3, ncols: 3}
	ls.devices[0] = &d0
	ls.devices[1] = &d1
	config := LanceroSourceConfig{FiberMask: 0xbeef, ActiveCards: []int{0, 1, 2},
		CardDelay: []int{1, 1, 1}}
	ls.Configure(&config)

	if len(ls.active) > 2 {
		t.Errorf("ls.active contains %d cards, want 2", len(ls.active))
	}
	ls.updateChanOrderMap()
	expect := []int{
		0, 1, 8, 9, 16, 17, 24, 25,
		2, 3, 10, 11, 18, 19, 26, 27,
		4, 5, 12, 13, 20, 21, 28, 29,
		6, 7, 14, 15, 22, 23, 30, 31,
		32, 33, 38, 39, 44, 45,
		34, 35, 40, 41, 46, 47,
		36, 37, 42, 43, 48, 49,
	}
	for i, v := range ls.chan2readoutOrder {
		if v != expect[i] {
			t.Errorf("chan2readoutOrder[%d]=%d, want %d", i, v, expect[i])
		}
	}

	config.ActiveCards = []int{0}
	ls.Configure(&config)
	ls.updateChanOrderMap()
	for i, v := range ls.chan2readoutOrder {
		if v != expect[i] {
			t.Errorf("chan2readoutOrder[%d]=%d, want %d", i, v, expect[i])
		}
	}

	config.ActiveCards = []int{1}
	ls.Configure(&config)
	ls.updateChanOrderMap()
	for i, v := range ls.chan2readoutOrder {
		if v != expect[i+32]-32 {
			t.Errorf("chan2readoutOrder[%d]=%d, want %d", i, v, expect[i+32]-32)
		}
	}

	// test roundint
	fs := []float64{-1.4, -.6, -.4, 0, .4, .5, .6, 1.4}
	es := []int{-1, -1, 0, 0, 0, 1, 1, 1}
	for i, f := range fs {
		if es[i] != roundint(f) {
			t.Errorf("roundint(%f)=%d, want %d", f, roundint(f), es[i])
		}
	}
}

func TestNoHardwareSource(t *testing.T) {
	var ncolsSet, nrowsSet, linePeriodSet, nLancero int
	ncolsSet = 1
	nrowsSet = 4
	linePeriodSet = 1000 // don't make this too low, or test -race will fail
	nLancero = 3
	source := new(LanceroSource)
	source.readPeriod = 1 * time.Millisecond
	source.devices = make(map[int]*LanceroDevice, nLancero)
	source.buffersChan = make(chan BuffersChanType, 25)
	cardDelay := []int{0} // a single card delay value works for multiple cards
	activeCards := make([]int, nLancero)
	for i := 0; i < nLancero; i++ {
		lan, err := lancero.NewNoHardware(ncolsSet, nrowsSet, linePeriodSet)
		if err != nil {
			t.Error(err)
		}
		dev := LanceroDevice{card: lan, devnum: i}
		source.devices[i] = &dev
		activeCards[i] = i
		source.ncards++
	}
	// above is essentially NewLanceroSource

	config := LanceroSourceConfig{ClockMhz: 125, CardDelay: cardDelay,
		ActiveCards: make([]int, nLancero)}
	if err := source.Configure(&config); err == nil && nLancero > 1 {
		t.Error("expected error for re-using a device")
	}
	config = LanceroSourceConfig{ClockMhz: 125, CardDelay: cardDelay,
		ActiveCards: activeCards, Nsamp: 1}
	if err := source.Configure(&config); err != nil {
		t.Error(err)
	}
	for i := 0; i < nLancero; i++ {
		if config.ActiveCards[i] != config.AvailableCards[i] {
			t.Errorf("AvailableCards not populated correctly. AvailableCards %v", config.AvailableCards)
		}
	}
	config.Nsamp = 499

	if err := source.Configure(&config); err == nil {
		t.Error("LanceroSource.Configure should fail with Nsamp>16")
	}
	config.Nsamp = 4
	if err := source.Configure(&config); err != nil {
		t.Error("LanceroSource.Configure fails:", err)
	}

	if err := Start(source, nil, 256, 1024); err != nil {
		source.Stop()
		t.Fatal(err)
	}
	defer source.Stop()

	if source.chanNumbers[3] != 2 {
		t.Errorf("LanceroSource.chanNumbers[3] has %v, want 2", source.chanNumbers[3])
	}
	if source.chanNames[3] != "chan2" {
		t.Errorf("LanceroSource.chanNames[3] has %v, want chan2", source.chanNames[3])
	}
	if source.chanNames[2] != "err2" {
		t.Errorf("LanceroSource.chanNames[2] %v, want err2", source.chanNames[3])
	}
	mfo := MixFractionObject{ChannelIndices: []int{0}, MixFractions: []float64{1.0}}
	if _, err := source.ConfigureMixFraction(&mfo); err == nil {
		t.Error("expected error for mixing on even channel")
	}
	mfo.ChannelIndices[0] = 1
	mix, err := source.ConfigureMixFraction(&mfo)
	if err != nil {
		t.Error(err)
	} else if mix[1] != 1.0 {
		t.Errorf("source.ConfigureMixFraction returns [%f], want %f", mix[0], 1.0)
	}
	time.Sleep(20 * time.Millisecond) // wait long enough for some data to be processed
	// these tests get lsync right at first, but while data is being processed
	// the lsync is not right. about half the time I run the test there are 3 blocking reads
	// at which point the lancero source shuts down due to it seeing lsync change
	// then Stop() will error but the err value is not checked so it doesn't cause a test failure
}

func TestMix(t *testing.T) {
	data := make([]RawType, 10)
	errData := make([]RawType, len(data))
	for i := range errData {
		errData[i] = RawType(i * 4)

	}
	mix := Mix{errorScale: 0}
	mix.MixRetardFb(&data, &errData)
	passed := true
	for i := range data {
		if data[i] != 0 {
			passed = false
		}
	}
	if !passed {
		t.Errorf("want all zeros, have\n%v", data)
	}
	mix = Mix{errorScale: 1}
	mix.MixRetardFb(&data, &errData)
	passed = true
	for i := range data {
		if data[i] != errData[i] {
			passed = false
		}
	}
	if !passed {
		t.Errorf("want %v\n have\n%v", errData, data)
	}
	if mix.lastFb != 0 {
		t.Errorf("want 0, have %v", mix.lastFb)
	}
	mix.lastFb = 100
	mix.MixRetardFb(&data, &errData)
	passed = true
	expect := []RawType{100 + 0, (0 + 1) * 4, (1 + 2) * 4, (2 + 3) * 4,
		(3 + 4) * 4, (4 + 5) * 4, (5 + 6) * 4, (6 + 7) * 4, (7 + 8) * 4, (8 + 9) * 4}
	for i := range data {
		if data[i] != expect[i] {
			passed = false
		}
	}
	if !passed {
		t.Errorf("want %v\nhave\n%v", expect, data)
	}
	if mix.lastFb != 36 {
		t.Errorf("want 36, have %v", mix.lastFb)
	}
	// test 2 LSBs of feedback are masked off
	for i := range data {
		data[i] = RawType(i % 4)
	}
	mix = Mix{errorScale: 0}
	mix.MixRetardFb(&data, &errData)
	passed = true
	for i := range data {
		if data[i] != 0 {
			passed = false
		}
	}
	if !passed {
		t.Errorf("have %v, want all zeros", data)
	}

	// Check that overflows are clamped to proper range
	err1 := []RawType{100, 0, 65436} // {100, 0, -100} as signed ints
	mix = Mix{errorScale: 1.0}
	mix.lastFb = 65530
	fb := []RawType{0, 50, 50}
	expect = []RawType{65535, 0, 0}
	mix.MixRetardFb(&fb, &err1)
	for i, e := range expect {
		if fb[i] != e {
			t.Errorf("mix with overflows fb[%d]=%d, want %d", i, fb[i], e)
		}
	}
}

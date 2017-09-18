package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/usnistgov/dastard/lancero"
)

type acquireOptions struct {
	period, delay     int
	length, verbosity int
	threshold         int
	nSamples          int
	mask              uint32
	output            string
	simulate          bool
}

var opt acquireOptions

func parseOptions() error {
	imask := 0
	flag.IntVar(&opt.period, "p", 32, "line sync period, in clock cycles")
	flag.IntVar(&opt.delay, "d", 0, "data delay, in clock cycles")
	flag.IntVar(&opt.length, "l", 32, "frame length")
	flag.IntVar(&opt.verbosity, "v", 0, "verbosity level")
	flag.IntVar(&opt.threshold, "t", 1024, "threshold (in frames), fill level interrupt")
	flag.IntVar(&opt.nSamples, "n", 0, "number of samples to acquire (<=0 means run indenfinitely)")
	flag.IntVar(&imask, "m", 0xffff, "channel mask for each of 16 channels")
	flag.StringVar(&opt.output, "o", "", "output filename")
	flag.BoolVar(&opt.simulate, "s", false, "simulate data (if false, read from fibers)")
	flag.Parse()
	opt.mask = uint32(imask)

	switch {
	case opt.period < 16:
		return fmt.Errorf("Line sync period (%d) must be at least 16", opt.period)
	case opt.period >= 1024:
		return fmt.Errorf("Line sync period (%d) must be < 1024", opt.period)
	case opt.mask > 0xffff:
		return fmt.Errorf("Line sync period (0x%x) must be < 0xffff", opt.mask)
	case opt.delay < 0 || opt.delay >= 32:
		return fmt.Errorf("Line delay (%d) must be in [0,31]", opt.delay)
	case opt.threshold < 1:
		return fmt.Errorf("Threshold (%d) must be at least 1", opt.threshold)
	case opt.threshold < 1024:
		fmt.Printf("WARNING: Threshold (%d) is recommended to be at least 1024", opt.threshold)
	}
	return nil
}

type verifier struct {
	nRows, nColumns    uint32
	row, column, error uint32
	columns            []uint32
	mask               uint32
}

func newVerifier(frameLength uint32, mask uint32) *verifier {
	v := &verifier{nRows: frameLength, mask: mask}
	for i := 0; i < 16; i++ {
		if mask&1 != 0 {
			v.columns = append(v.columns, uint32(i))
			v.nColumns++
		}
		mask = mask >> 1
	}
	return v
}

func (v *verifier) checkWord(data uint32) bool {
	ok := true
	channel := (data >> 28) & 0xf
	row := (data >> 18) & 0x3ff
	overRange := data&0x20000 != 0
	frame := data&0x10000 != 0
	errval := data & 0xffff

	frameExpected := (v.row == 0)
	if frame != frameExpected {
		ok = false
		fmt.Printf("verify(): The frame bit was %v, expected %v.\n", frame, frameExpected)
	}
	if overRange {
		ok = false
		fmt.Println("verify(): The over-range bit was 1, expected 0.")
	}
	if channel != v.columns[v.column] {
		ok = false
		fmt.Printf("verify(): Saw channel %d, expected %d.\n", channel, v.columns[v.column])
	}
	if row != v.row {
		ok = false
		fmt.Printf("verify(): Saw row %d, expected %d.\n", row, v.row)
	}
	if errval != v.error {
		ok = false
		fmt.Printf("verify(): Saw error val 0x%x, expected 0x%x.\n", errval, v.error)
	}

	// Update: 1 column each time; 1 row each time column wraps; and 1 "error" value
	// each time that the row wraps (i.e., per frame).
	v.column = (v.column + 1) % v.nColumns
	if v.column == 0 {
		v.row = (v.row + 1) % v.nRows
		if v.row == 0 {
			v.error = (v.error + 1) % 0x8000
		}
	}
	return ok
}

func (v *verifier) checkBuffer(b []byte) bool {
	ok := true
	buf := bytes.NewReader(b)
	var val uint32
	for {
		err := binary.Read(buf, binary.LittleEndian, &val)
		if err != nil {
			break
		}
		ok = v.checkWord(val) && ok
	}
	return ok
}

func acquire(lan *lancero.Lancero) (bytesRead int, err error) {
	var NROWS uint32 = 32
	verifier := newVerifier(NROWS, opt.mask)

	// Store output?
	var fd *os.File
	output := len(opt.output) > 0
	if output {
		fd, err = os.Create(opt.output)
		if err != nil {
			return
		}
		defer fd.Close()
	} else {
		fd = nil
	}
	fmt.Println(fd)

	// Start the adapter
	err = lan.StartAdapter(2)
	if err != nil {
		fmt.Println("Could not start adapter: ", err)
		return
	}
	defer lan.StopAdapter()

	// Configure and start the collector
	err = lan.CollectorConfigure(opt.period, opt.delay, opt.mask, opt.length)
	if err != nil {
		return
	}
	err = lan.StartCollector(opt.simulate)
	if err != nil {
		return
	}
	defer lan.StopCollector()
	defer lan.InspectAdapter()

	var buffers [][]byte
	var totalBytes int
	interruptCatcher := make(chan os.Signal, 1)
	signal.Notify(interruptCatcher, os.Interrupt)
	for {
		select {
		case <-interruptCatcher:
			return
		default:
			err = lan.Wait()
			if err != nil {
				return
			}
			buffers, totalBytes, err = lan.AvailableBuffers()
			if err != nil {
				return
			}
			fmt.Printf("Found %d buffers with %d total bytes\n", len(buffers), totalBytes)
			bytesRead += totalBytes
			if opt.nSamples > 0 && opt.nSamples <= bytesRead/4 {
				return
			}

			if output {
				for _, b := range buffers {
					if len(b) > 0 {
						var n int
						n, err = fd.Write(b)
						if err != nil {
							return
						}
						if n != len(b) {
							err = fmt.Errorf("Wrote %d bytes, expected %d", n, len(b))
							return
						}
					}
				}
			}

			// Verify the simulated data, if simulated.
			if opt.simulate {
				for _, b := range buffers {
					if ok := verifier.checkBuffer(b); !ok {
						fmt.Println("Buffer did not verify.")
						return
					}
				}
			}
			lan.ReleaseBytes(totalBytes)
		}
	}
}

func main() {
	err := parseOptions()
	if err != nil {
		fmt.Println("ERROR: ", err)
		return
	}

	lan, err := lancero.NewLancero(0)
	if err != nil {
		fmt.Println("ERROR: ", err)
		return
	}
	defer lan.Close()

	bytesRead, _ := acquire(lan)
	fmt.Printf("Read %d bytes.\n", bytesRead)
}

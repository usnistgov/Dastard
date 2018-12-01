package dastard

import (
	"fmt"
	"math/rand"
	"time"
)

// TriangleSource is a DataSource that synthesizes triangle waves.
type TriangleSource struct {
	minval     RawType
	maxval     RawType
	timeperbuf time.Duration
	onecycle   []RawType
	cycleLen   int
	AnySource
}

// NewTriangleSource creates a new TriangleSource.
func NewTriangleSource() *TriangleSource {
	ts := new(TriangleSource)
	ts.name = "Triangle"
	return ts
}

// TriangleSourceConfig holds the arguments needed to call TriangleSource.Configure by RPC
type TriangleSourceConfig struct {
	Nchan      int
	SampleRate float64
	Min, Max   RawType
}

// Configure sets up the internal buffers with given size, speed, and min/max.
func (ts *TriangleSource) Configure(config *TriangleSourceConfig) error {
	// Check configuration for acceptability
	if config.Nchan < 1 {
		return fmt.Errorf("TriangleSource.Configure() asked for %d channels, should be > 0", config.Nchan)
	}
	if config.Min > config.Max {
		return fmt.Errorf("have config.Min=%v > config.Max=%v, want Min<Max", config.Min, config.Max)
	}

	ts.sourceStateLock.Lock()
	defer ts.sourceStateLock.Unlock()
	if ts.sourceState != Inactive {
		return fmt.Errorf("cannot Configure a TriangleSource if it's not Inactive")
	}
	ts.nchan = config.Nchan
	ts.sampleRate = config.SampleRate
	ts.samplePeriod = time.Duration(roundint(1e9 / ts.sampleRate))
	nrise := config.Max - config.Min
	if nrise > 0 {
		ts.cycleLen = 2 * int(nrise)
		ts.onecycle = make([]RawType, ts.cycleLen)
		var i RawType
		for i = 0; i < nrise; i++ {
			ts.onecycle[i] = config.Min + i
			ts.onecycle[int(i)+int(nrise)] = config.Max - i
		}
	} else if nrise == 0 {
		ts.cycleLen = roundint(ts.sampleRate/10) + 1 // aim for 10 cycles/second
		ts.onecycle = make([]RawType, ts.cycleLen)
		for i := range ts.onecycle {
			ts.onecycle[i] = config.Max
		}
	}

	ts.minval = config.Min
	ts.maxval = config.Max
	cycleTime := float64(ts.cycleLen) / ts.sampleRate
	ts.timeperbuf = time.Duration(float64(time.Second) * cycleTime)
	if ts.timeperbuf > 4*time.Second {
		return fmt.Errorf("timeperbuf is %v, should be less than 4 seconds", ts.timeperbuf)
	}
	return nil
}

// Sample determines key data facts by sampling some initial data.
// It's a no-op for simulated (software) sources
func (ts *TriangleSource) Sample() error {
	ts.chanNames = make([]string, ts.nchan)
	ts.chanNumbers = make([]int, ts.nchan)
	ts.signed = make([]bool, ts.nchan)
	ts.rowColCodes = make([]RowColCode, ts.nchan)
	for i := 0; i < ts.nchan; i++ {
		ts.chanNames[i] = fmt.Sprintf("chan%d", i+1)
		ts.chanNumbers[i] = i + 1
		ts.rowColCodes[i] = rcCode(0, i, 1, ts.nchan)
	}
	return nil
}

// StartRun launches the repeated loop that generates Triangle data.
func (ts *TriangleSource) StartRun() error {
	go func() {
		for {
			nextread := ts.lastread.Add(ts.timeperbuf)
			waittime := time.Until(nextread)
			var now time.Time
			select {
			case <-ts.abortSelf:
				close(ts.nextBlock)
				return
			case <-time.After(waittime):
				now = time.Now()
				if ts.heartbeats != nil {
					dt := now.Sub(ts.lastread).Seconds()
					mb := float64(ts.cycleLen*2*ts.nchan) / 1e6
					ts.heartbeats <- Heartbeat{Running: true, Time: dt, DataMB: mb}
				}
				ts.lastread = nextread // ensure average cycle time is correct, using now would allow error to build up
			}

			// Backtrack to find the time associated with the first sample.
			firstTime := now.Add(-ts.timeperbuf) // use now here; should correspond to the time the data was read
			block := new(dataBlock)
			block.segments = make([]DataSegment, ts.nchan)
			for channelIndex := 0; channelIndex < ts.nchan; channelIndex++ {
				datacopy := make([]RawType, ts.cycleLen)
				copy(datacopy, ts.onecycle)
				seg := DataSegment{
					rawData:         datacopy,
					framesPerSample: 1,
					framePeriod:     ts.samplePeriod,
					firstFramenum:   ts.nextFrameNum,
					firstTime:       firstTime,
				}
				block.segments[channelIndex] = seg
			}
			ts.nextFrameNum += FrameIndex(ts.cycleLen)
			ts.nextBlock <- block
		}
	}()
	return nil
}

// SimPulseSource simulates simple pulsed sources
type SimPulseSource struct {
	timeperbuf time.Duration
	onecycle   []RawType
	cycleLen   int
	AnySource

	// regular bool // whether pulses are regular or Poisson-distributed
}

// NewSimPulseSource creates a new SimPulseSource with given size, speed.
func NewSimPulseSource() *SimPulseSource {
	ps := new(SimPulseSource)
	ps.name = "SimPulse"
	return ps
}

// SimPulseSourceConfig holds the arguments needed to call SimPulseSource.Configure by RPC
type SimPulseSourceConfig struct {
	Nchan      int
	SampleRate float64
	Pedestal   float64
	Amplitudes []float64
	Nsamp      int
}

// Configure sets up the internal buffers with given size, speed, and pedestal and amplitude.
func (sps *SimPulseSource) Configure(config *SimPulseSourceConfig) error {
	if config.Nchan < 1 {
		return fmt.Errorf("SimPulseSource.Configure() asked for %d channels, should be > 0", config.Nchan)
	}

	sps.sourceStateLock.Lock()
	defer sps.sourceStateLock.Unlock()
	if sps.sourceState != Inactive {
		return fmt.Errorf("cannot Configure a SimPulseSource if it's not Inactive")
	}
	sps.nchan = config.Nchan
	sps.sampleRate = config.SampleRate
	sps.samplePeriod = time.Duration(roundint(1e9 / sps.sampleRate))

	nsizes := len(config.Amplitudes)
	sps.cycleLen = nsizes * config.Nsamp
	firstIdx := 5
	sps.onecycle = make([]RawType, sps.cycleLen)

	ampl := []float64{0, 0}
	exprate := []float64{.99, .96}
	var value float64
	for i := 0; i < sps.cycleLen; i++ {
		if i%config.Nsamp == firstIdx {
			j := i / config.Nsamp
			ampl[0] = config.Amplitudes[j]
			ampl[1] = -config.Amplitudes[j]
		}
		value = config.Pedestal + ampl[0] + ampl[1]
		ampl[0] *= exprate[0]
		ampl[1] *= exprate[1]
		sps.onecycle[i] = RawType(value + 0.5)
	}

	cycleTime := float64(sps.cycleLen) / sps.sampleRate
	sps.timeperbuf = time.Duration(float64(time.Second) * cycleTime)
	sps.timeperbuf = time.Duration(float64(time.Second) * cycleTime)
	if sps.timeperbuf > 4*time.Second {
		return fmt.Errorf("timeperbuf is %v, should be less than 4 seconds", sps.timeperbuf)
	}
	return nil
}

// Sample determines key data facts by sampling some initial data.
// It's a no-op for simulated (software) sources
func (sps *SimPulseSource) Sample() error {
	sps.chanNames = make([]string, sps.nchan)
	sps.chanNumbers = make([]int, sps.nchan)
	sps.signed = make([]bool, sps.nchan)
	sps.rowColCodes = make([]RowColCode, sps.nchan)
	for i := 0; i < sps.nchan; i++ {
		sps.chanNames[i] = fmt.Sprintf("chan%d", i+1)
		sps.chanNumbers[i] = i + 1
		sps.rowColCodes[i] = rcCode(0, i, 1, sps.nchan)
	}
	return nil
}

// StartRun launches the repeated loop that generates Triangle data.
func (sps *SimPulseSource) StartRun() error {
	go func() {
		defer close(sps.nextBlock)
		for {
			nextread := sps.lastread.Add(sps.timeperbuf)
			waittime := time.Until(nextread)
			var now time.Time
			select {
			case <-sps.abortSelf:
				return
			case <-time.After(waittime):
				now = time.Now()
				if sps.heartbeats != nil {
					dt := now.Sub(sps.lastread).Seconds()
					mb := float64(sps.cycleLen*2*sps.nchan) / 1e6
					sps.heartbeats <- Heartbeat{Running: true, Time: dt, DataMB: mb}
				}
				sps.lastread = nextread // ensure average cycle time is correct, using now would allow error to build up
			}

			// Backtrack to find the time associated with the first sample.
			firstTime := now.Add(-sps.timeperbuf) // use now for accurate sample time
			block := new(dataBlock)
			block.segments = make([]DataSegment, sps.nchan)
			for channelIndex := 0; channelIndex < sps.nchan; channelIndex++ {
				datacopy := make([]RawType, sps.cycleLen)
				copy(datacopy, sps.onecycle)
				for i := 0; i < sps.cycleLen; i++ {
					datacopy[i] += RawType(rand.Intn(21) - 10)
				}
				seg := DataSegment{
					rawData:         datacopy,
					framesPerSample: 1,
					framePeriod:     sps.samplePeriod,
					firstFramenum:   sps.nextFrameNum,
					firstTime:       firstTime,
				}
				block.segments[channelIndex] = seg
			}
			sps.nextFrameNum += FrameIndex(sps.cycleLen)
			sps.nextBlock <- block
		}
	}()
	return nil
}

// ErroringSource  is used to test the behavior of errors in blockingRead
// an error in blocking read should change the state of the source such that
// the next call to blocking read will return io.EOF
// then source.Stop() will be called.
// the source should be able to be restatrted after this
// ErroringSource needs to exist in dastard (not just in tests) so that
// it can be exposed through the RPC server to test that the RPC server
// recovers properly as well
type ErroringSource struct {
	AnySource
	nStarts int
}

// NewErroringSource returns a new ErroringSource, requires no configuration
func NewErroringSource() *ErroringSource {
	es := new(ErroringSource)
	es.nchan = 1
	return es
}

// Sample is part of the required interface.
func (es *ErroringSource) Sample() error {
	return nil
}

// StartRun is part of the required interface and ensures future failure
func (es *ErroringSource) StartRun() error {
	es.nStarts++
	go func() {
		block := new(dataBlock)
		block.err = fmt.Errorf("ErroringSource always errors on first call")
		es.nextBlock <- block
	}()
	return nil
}

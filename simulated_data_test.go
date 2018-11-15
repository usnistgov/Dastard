package dastard

import (
	"fmt"
	"testing"
	"time"

	"gonum.org/v1/gonum/mat"
)

// TestTriangle checks that TriangleSource works as expected
func TestTriangle(t *testing.T) {
	ts := NewTriangleSource()
	config := TriangleSourceConfig{
		Nchan:      4,
		SampleRate: 10000.0,
		Min:        100,
		Max:        200,
	}
	ts.Configure(&config)
	ts.noProcess = true
	ds := DataSource(ts)
	if ds.Running() {
		t.Errorf("TriangleSource.Running() says true before first start.")
	}

	if err := Start(ds, nil, 256, 1024); err != nil {
		t.Fatalf("TriangleSource could not be started")
	}
	if len(ts.processors) != config.Nchan {
		t.Errorf("TriangleSource.Ouputs() returns %d channels, want %d", len(ts.processors), config.Nchan)
	}

	// Check first segment per source.
	// n := int(config.Max - config.Min)
	// for i, ch := range ts.output {
	// 	segment := <-ch
	// 	data := segment.rawData
	// 	if len(data) != 2*n {
	// 		t.Errorf("TriangleSource output %d is length %d, expect %d", i, len(data), 2*n)
	// 	}
	// 	for j := 0; j < n; j++ {
	// 		if data[j] != config.Min+RawType(j) {
	// 			t.Errorf("TriangleSource output %d has [%d]=%d, expect %d", i, j, data[j], int(config.Min)+j)
	// 		}
	// 		if data[j+n] != config.Max-RawType(j) {
	// 			t.Errorf("TriangleSource output %d has [%d]=%d, expect %d", i, j+n, data[j+n], int(config.Max)-j)
	// 		}
	// 	}
	// 	if segment.firstFramenum != 0 {
	// 		t.Errorf("TriangleSource first segment, output %d gives firstFramenum %d, want 0", i, segment.firstFramenum)
	// 	}
	// }
	// Check second segment per source.
	// for i, ch := range ts.output {
	// 	segment := <-ch
	// 	data := segment.rawData
	// 	if len(data) != 2*n {
	// 		t.Errorf("TriangleSource output %d is length %d, expect %d", i, len(data), 2*n)
	// 	}
	// 	for j := 0; j < n; j++ {
	// 		if data[j] != config.Min+RawType(j) {
	// 			t.Errorf("TriangleSource output %d has [%d]=%d, expect %d", i, j, data[j], int(config.Min)+j)
	// 		}
	// 		if data[j+n] != config.Max-RawType(j) {
	// 			t.Errorf("TriangleSource output %d has [%d]=%d, expect %d", i, j+n, data[j+n], int(config.Max)-j)
	// 		}
	// 	}
	// 	if segment.firstFramenum != FrameIndex(2*n) {
	// 		t.Errorf("TriangleSource second segment, ouput %d gives firstFramenum %d, want %d", i, segment.firstFramenum, 2*n)
	// 	}
	// }

	// Stop, then check that Running() is correct
	ds.Stop()
	if ds.Running() {
		t.Errorf("TriangleSource.Running() says true after stopped.")
	}

	// Start again
	if err := Start(ds, nil, 256, 1024); err != nil {
		t.Fatalf("TriangleSource could not be started, %s", err.Error())
	}
	if !ds.Running() {
		t.Errorf("TriangleSource.Running() says false after started.")
	}
	if err := ts.Configure(&config); err == nil {
		t.Errorf("TriangleSource can be configured with even though it's running, want error.")
	}
	if err := Start(ds, nil, 256, 1024); err == nil {
		t.Errorf("Start(TriangleSource) was allowed when source was running, want error.")
	}
	time.Sleep(100 * time.Millisecond)
	ds.Stop()
	if ds.Running() {
		t.Errorf("TriangleSource.Running() says true after stopped.")
	}

	// Start a third time
	if err := Start(ds, nil, 256, 1024); err != nil {
		t.Fatalf("TriangleSource could not be started")
	}
	// Check that we can alter the record length
	ds.ConfigurePulseLengths(0, 0)
	nsamp, npre := 500, 250
	ds.ConfigurePulseLengths(nsamp, npre)
	time.Sleep(5 * time.Millisecond)
	dsp := ts.processors[0]
	if dsp.NSamples != nsamp || dsp.NPresamples != npre {
		t.Errorf("TriangleSource has (nsamp, npre)=(%d,%d), want (%d,%d)",
			dsp.NSamples, dsp.NPresamples, nsamp, npre)
	}
	rows := 5
	cols := 500
	projectors := mat.NewDense(rows, cols, make([]float64, rows*cols))
	basis := mat.NewDense(cols, rows, make([]float64, rows*cols))
	if err := dsp.SetProjectorsBasis(*projectors, *basis, "test model"); err != nil {
		t.Error(err)
	}
	if err := ts.ConfigureProjectorsBases(1, *projectors, *basis, "test model"); err != nil {
		t.Error(err)
	}
	ds.Stop()

	// Now configure a 0-channel source and make sure it fails
	config.Nchan = 0
	if err := ts.Configure(&config); err == nil {
		t.Errorf("TriangleSource can be configured with 0 channels, want error.")
	}

	// Make sure that maxval < minval errors
	config = TriangleSourceConfig{
		Nchan:      4,
		SampleRate: 10000.0,
		Min:        300,
		Max:        200,
	}
	if err := ts.Configure(&config); err == nil {
		t.Error("TriangleSource can be configured with min>max, want error")
	}
	config = TriangleSourceConfig{
		Nchan:      4,
		SampleRate: 1.0,
		Min:        100,
		Max:        200,
	}
	if err := ts.Configure(&config); err == nil {
		t.Errorf("TriangleSource can be configured with too-slow buffer %v, want error", ts.timeperbuf)
	}
	// Make sure that maxval == minval does not error and cycleLen is >>1.
	config = TriangleSourceConfig{
		Nchan:      4,
		SampleRate: 10000.0,
		Min:        200,
		Max:        200,
	}
	if err := ts.Configure(&config); err != nil {
		t.Error(err)
	}
	if ts.cycleLen != 1001 {
		t.Errorf("ts.cycleLen is %v, want 1001", ts.cycleLen)
	}
}

func TestSimPulse(t *testing.T) {
	ps := NewSimPulseSource()
	config := SimPulseSourceConfig{
		Nchan:      5,
		SampleRate: 150000.0,
		Pedestal:   1000.0,
		Amplitudes: []float64{10000.0},
		Nsamp:      16000,
	}
	ps.Configure(&config)
	ps.noProcess = true // for testing
	ds := DataSource(ps)
	if ds.Running() {
		t.Errorf("SimPulseSource.Running() says true before first start.")
	}

	if err := Start(ds, nil, 123, 321); err != nil {
		t.Fatalf("SimPulseSource could not be started")
	}
	Npresamp, Nsamples, err1 := ds.getPulseLengths()
	if err1 != nil {
		t.Error(err1)
	}
	if Npresamp != 123 || Nsamples != 321 {
		t.Errorf("Npresample want 123, have %v. Nsamples want 321, have %v", Npresamp, Nsamples)
	}
	if len(ps.processors) != config.Nchan {
		t.Errorf("SimPulseSource.Ouputs() returns %d channels, want %d", len(ps.processors), config.Nchan)
	}
	// // Check first segment per source.
	// for i, ch := range ps.output {
	// 	segment := <-ch
	// 	data := segment.rawData
	// 	if len(data) != config.Nsamp {
	// 		t.Errorf("SimPulseSource output %d is length %d, expect %d", i, len(data), config.Nsamp)
	// 	}
	// 	min, max := RawType(65535), RawType(0)
	// 	for j := 0; j < config.Nsamp; j++ {
	// 		if data[j] < min {
	// 			min = data[j]
	// 		}
	// 		if data[j] > max {
	// 			max = data[j]
	// 		}
	// 	}
	// 	if min != RawType(config.Pedestal+0.5-10) {
	// 		t.Errorf("SimPulseSource minimum value is %d, expect %d", min, RawType(config.Pedestal+0.5))
	// 	}
	// 	if max <= RawType(config.Pedestal+config.Amplitude*0.4) {
	// 		t.Errorf("SimPulseSource minimum value is %d, expect > %d", max, RawType(config.Pedestal+config.Amplitude*0.4))
	// 	}
	// 	if segment.firstFramenum != 0 {
	// 		t.Errorf("SimPulseSource first segment, output %d gives firstFramenum %d, want 0", i, segment.firstFramenum)
	// 	}
	// }
	// // Check second segment per source.
	// for i, ch := range ps.output {
	// 	segment := <-ch
	// 	data := segment.rawData
	// 	if len(data) != config.Nsamp {
	// 		t.Errorf("SimPulseSource output %d is length %d, expect %d", i, len(data), config.Nsamp)
	// 	}
	// 	if segment.firstFramenum <= 0 {
	// 		t.Errorf("SimPulseSource second segment gives firstFramenum %d, want %d", segment.firstFramenum, config.Nsamp)
	// 	}
	// }
	ds.Stop()

	// Check that Running() is correct
	if ds.Running() {
		t.Errorf("SimPulseSource.Running() says true before started.")
	}
	if err := Start(ds, nil, 256, 1024); err != nil {
		t.Fatalf("SimPulseSource could not be started")
	}
	Npresamp, Nsamples, err2 := ds.getPulseLengths()
	if err2 != nil {
		t.Error(err2)
	}
	if Npresamp != 256 || Nsamples != 1024 {
		t.Errorf("Npresample want 123, have %v. Nsamples want 321, have %v", Npresamp, Nsamples)
	}
	if !ds.Running() {
		t.Errorf("SimPulseSource.Running() says false after started.")
	}
	if err := ps.Configure(&config); err == nil {
		t.Errorf("SimPulseSource can be configured with even though it's running, want error.")
	}
	ds.Stop()
	if ds.Running() {
		t.Errorf("SimPulseSource.Running() says true after stopped.")
	}

	// Now configure a 0-channel source and make sure it fails
	config.Nchan = 0
	if err := ps.Configure(&config); err == nil {
		t.Errorf("SimPulseSource can be configured with 0 channels.")
	}
}

func TestErroringSource(t *testing.T) {
	es := NewErroringSource()
	ds := DataSource(es)
	for i := 0; i < 5; i++ {
		// 	// start the source, wait for it to end due to error, repeat
		if err := Start(ds, nil, 256, 1024); err != nil {
			t.Fatalf(fmt.Sprintf("Could not start ErroringSource: i=%v, err=%v", i, err))
		}
		es.RunDoneWait()
		if ds.Running() {
			t.Error("ErroringSource is running, want not running")
		}
		if es.nStarts != (i + 1) {
			t.Errorf("have %v, want %v", es.nStarts, (i + 1))
		}
	}
}

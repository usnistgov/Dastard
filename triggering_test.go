package dastard

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// TestBrokerConnections checks that we can connect/disconnect group triggers
// from the broker and the coupling of err and FB into each other for LanceroSources.
func TestBrokerConnections(t *testing.T) {
	N := 4
	broker := NewTriggerBroker(N)

	// First be sure there are no connections, initially.
	for i := 0; i < N+1; i++ {
		for j := 0; j < N+1; j++ {
			if broker.isConnected(i, j) {
				t.Errorf("New TriggerBroker.isConnected(%d,%d)==true, want false", i, j)
			}
		}
	}

	// Add 2 connections and make sure they are completed, but others aren't.
	broker.AddConnection(0, 2)
	broker.AddConnection(2, 0)
	if !broker.isConnected(0, 2) {
		t.Errorf("TriggerBroker.isConnected(0,2)==false, want true")
	}
	if !broker.isConnected(2, 0) {
		t.Errorf("TriggerBroker.isConnected(2,0)==false, want true")
	}
	i := 1
	for j := 0; j < N+1; j++ {
		if broker.isConnected(i, j) {
			t.Errorf("TriggerBroker.isConnected(%d,%d)==true, want false after connecting 0->2", i, j)
		}
	}

	// Now break the connections and check that they are disconnected
	broker.DeleteConnection(0, 2)
	broker.DeleteConnection(2, 0)
	for i := 0; i < N+1; i++ {
		for j := 0; j < N+1; j++ {
			if broker.isConnected(i, j) {
				t.Errorf("TriggerBroker.isConnected(%d,%d)==true, want false after disconnecting all", i, j)
			}
		}
	}

	// Try Add/Delete/check on channel numbers that should fail
	if err := broker.AddConnection(0, N); err == nil {
		t.Errorf("TriggerBroker.AddConnection(%d,0) should fail but didn't", N)
	}
	if err := broker.DeleteConnection(0, N); err == nil {
		t.Errorf("TriggerBroker.DeleteConnection(%d,0) should fail but didn't", N)
	}

	// Check the Connections method
	for i := -1; i < 1; i++ {
		con := broker.Connections(i)
		if len(con) > 0 {
			t.Errorf("TriggerBroker.Connections(%d)) has length %d, want 0", i, len(con))
		}
	}
	broker.AddConnection(1, 0)
	broker.AddConnection(2, 0)
	broker.AddConnection(3, 0)
	broker.AddConnection(2, 0)
	broker.AddConnection(3, 0)
	con := broker.Connections(0)
	if len(con) != 3 {
		t.Errorf("TriggerBroker.Connections(0) has length %d, want 3", len(con))
	}
	if con[0] {
		t.Errorf("TriggerBroker.Connections(0)[0]==true, want false")
	}
	for i := 1; i < 4; i++ {
		if !con[i] {
			t.Errorf("TriggerBroker.Connections(0)[%d]==false, want true", i)
		}
	}

	// Now test FB <-> err coupling. This works when broker is embedded in a
	// LanceroSource.
	broker = NewTriggerBroker(N)
	var ls LanceroSource
	ls.nchan = N
	ls.broker = broker

	// FBToErr
	if err := ls.SetCoupling(FBToErr); err != nil {
		t.Errorf("SetCoupling(FBToErr) failed: %v", err)
	} else {
		for src := 0; src < N; src++ {
			for rx := 0; rx < N; rx++ {
				expect := (src-rx) == 1 && src%2 == 1
				c := broker.isConnected(src, rx)
				if c != expect {
					t.Errorf("After FB->Error isConnected(src=%d, rx=%d) is %t, want %t",
						src, rx, c, expect)
				}
			}
		}
	}

	// ErrToFB
	if err := ls.SetCoupling(ErrToFB); err != nil {
		t.Errorf("SetCoupling(ErrToFB) failed: %v", err)
	} else {
		for src := 0; src < N; src++ {
			for rx := 0; rx < N; rx++ {
				expect := (rx-src) == 1 && src%2 == 0
				c := broker.isConnected(src, rx)
				if c != expect {
					t.Errorf("After Error->Fb isConnected(src=%d, rx=%d) is %t, want %t",
						src, rx, c, expect)
				}
			}
		}
	}

	// None
	if err := ls.SetCoupling(NoCoupling); err != nil {
		t.Errorf("SetCoupling(NoCoupling) failed: %v", err)
	} else {
		for src := 0; src < N; src++ {
			for rx := 0; rx < N; rx++ {
				expect := false
				c := broker.isConnected(src, rx)
				if c != expect {
					t.Errorf("After NoCoupling isConnected(src=%d, rx=%d) is %t, want %t",
						src, rx, c, expect)
				}
			}
		}
	}
}

// TestBrokering checks the group trigger brokering operations.
func TestBrokering(t *testing.T) {
	N := 4
	broker := NewTriggerBroker(N)
	abort := make(chan struct{})
	go broker.Run()
	defer broker.Stop()
	broker.AddConnection(0, 3)
	broker.AddConnection(2, 3)

	for iter := 0; iter < 3; iter++ {
		for i := 0; i < N; i++ {
			trigs := triggerList{channelIndex: i, frames: []FrameIndex{FrameIndex(i) + 10, FrameIndex(i) + 20, 30}}
			broker.PrimaryTrigs <- trigs
		}
		t0 := <-broker.SecondaryTrigs[0]
		t1 := <-broker.SecondaryTrigs[1]
		t2 := <-broker.SecondaryTrigs[2]
		t3 := <-broker.SecondaryTrigs[3]
		for i, tn := range [][]FrameIndex{t0, t1, t2} {
			if len(tn) > 0 {
				t.Errorf("TriggerBroker chan %d received %d secondary triggers, want 0", i, len(tn))
			}
		}
		expected := []FrameIndex{10, 12, 20, 22, 30, 30}
		if len(t3) != len(expected) {
			t.Errorf("TriggerBroker chan %d received %d secondary triggers, want %d", 3, len(t3), len(expected))
		}
		for i := 0; i < len(expected); i++ {
			if t3[i] != expected[i] {
				t.Errorf("TriggerBroker chan %d secondary trig[%d]=%d, want %d", 3, i, t2[i], expected[i])
			}
		}
		if iter == 2 {
			close(abort)
		}
	}
}

// TestLongRecords ensures that we can generate triggers longer than 1 unit of
// data supply.
func TestLongRecords(t *testing.T) {
	const nchan = 1

	broker := NewTriggerBroker(nchan)
	go broker.Run()
	defer broker.Stop()
	var tests = []struct {
		npre   int
		nsamp  int
		nchunk int
	}{
		{9600, 10000, 999},
		{600, 10000, 999},
		{100, 10000, 999},
		{100, 10000, 1000},
		{100, 10000, 1001},
		{9100, 10000, 999},
		{9100, 10000, 1000},
		{9100, 10000, 1001},
		{1000, 10000, 9999},
		{1000, 10000, 10000},
		{1000, 10000, 10001},
	}
	for _, test := range tests {
		NPresamples := 256
		NSamples := 1024
		dsp := NewDataStreamProcessor(0, broker, NPresamples, NSamples)
		dsp.NPresamples = test.npre
		dsp.NSamples = test.nsamp
		dsp.SampleRate = 100000.0
		dsp.AutoTrigger = true
		dsp.AutoDelay = 500 * time.Millisecond
		expectedFrames := []FrameIndex{FrameIndex(dsp.NPresamples)}
		trigname := "Long Records auto"

		raw := make([]RawType, test.nchunk)
		dsp.LastTrigger = math.MinInt64 / 4 // far in the past, but not so far we can't subtract from it.
		sampleTime := time.Duration(float64(time.Second) / dsp.SampleRate)
		segment := NewDataSegment(raw, 1, 0, time.Now(), sampleTime)
		for i := 0; i <= dsp.NSamples; i += test.nchunk {
			primaries, secondaries := dsp.TriggerData()
			if (len(primaries) != 0) || (len(secondaries) != 0) {
				t.Errorf("%s trigger found triggers after %d chunks added, want none", trigname, i)
			}
			dsp.stream.AppendSegment(segment)
			segment.firstFramenum += FrameIndex(test.nchunk)
		}
		primaries, secondaries := dsp.TriggerData()
		if len(primaries) != len(expectedFrames) {
			t.Errorf("%s trigger (test=%v) found %d triggers, want %d", trigname, test, len(primaries), len(expectedFrames))
		}
		if len(secondaries) != 0 {
			t.Errorf("%s trigger found %d secondary (group) triggers, want 0", trigname, len(secondaries))
		}
		for i, pt := range primaries {
			if pt.trigFrame != expectedFrames[i] {
				t.Errorf("%s trigger at frame %d, want %d", trigname, pt.trigFrame, expectedFrames[i])
			}
		}
	}
}

// TestSingles tests that single edge, level, or auto triggers happen where expected.
func TestSingles(t *testing.T) {
	const nchan = 1

	broker := NewTriggerBroker(nchan)
	go broker.Run()
	defer broker.Stop()
	NPresamples := 256
	NSamples := 1024
	dsp := NewDataStreamProcessor(0, broker, NPresamples, NSamples)
	nRepeat := 1

	const bigval = 8000
	const tframe = 1000
	raw := make([]RawType, 10000)
	for i := tframe; i < tframe+10; i++ {
		raw[i] = bigval
	}
	const smallval = 1
	const tframe2 = 6000
	for i := tframe2; i < tframe2+10; i++ {
		raw[i] = smallval
	}

	dsp.NPresamples = 100
	dsp.NSamples = 1000
	dsp.SampleRate = 10000.0

	dsp.EdgeTrigger = true
	dsp.EdgeRising = true
	dsp.EdgeLevel = 100
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Edge", []FrameIndex{1000})

	dsp.EdgeTrigger = false
	dsp.LevelTrigger = true
	dsp.LevelRising = true
	dsp.LevelLevel = 100
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Level", []FrameIndex{1000})

	dsp.LevelTrigger = false
	dsp.AutoTrigger = true
	dsp.AutoDelay = 0 * time.Millisecond
	// Zero Delay results in records that are spaced by 1000 samples (dsp.NSamples)
	// starting at 100 (dsp.NPreSamples)
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Auto_0Millisecond", []FrameIndex{100, 1100, 2100, 3100, 4100, 5100, 6100, 7100, 8100})

	dsp.LevelTrigger = false
	dsp.AutoTrigger = true
	dsp.AutoDelay = 500 * time.Millisecond
	// first trigger is at NPreSamples=100
	// AutoDelay corresponds to 5000 samples, so we add that to 1100 to get 5100
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Auto_500Millisecond", []FrameIndex{100, 5100})

	dsp.LevelTrigger = true
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Level+Auto_500Millisecond", []FrameIndex{1000, 6000})

	dsp.LevelLevel = 1
	dsp.AutoTrigger = false
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Level_SmallThresh", []FrameIndex{1000, 6000})

	dsp.AutoDelay = 200 * time.Millisecond
	dsp.AutoTrigger = true
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Level+Auto_200Millisecond", []FrameIndex{1000, 3000, 5000, 6000, 8000})

	// Check that auto triggers are correct, particularly for multiple segments (issue #16)
	nRepeat = 4 // 4 seconds of data
	var expected []FrameIndex
	dsp.LevelTrigger = false
	dsp.NSamples = 1234
	dsp.NPresamples = 456
	expected = make([]FrameIndex, 0)
	for i := dsp.NPresamples; i < nRepeat*int(dsp.SampleRate); i += 2000 {
		expected = append(expected, FrameIndex(i))
	}
	testTriggerSubroutine(t, raw, nRepeat, dsp, "AutoMultipleSegmentsA", expected)

	dsp.AutoDelay = 1200 * time.Millisecond
	expected = make([]FrameIndex, 0)
	for i := dsp.NPresamples; i < nRepeat*int(dsp.SampleRate); i += 12000 {
		expected = append(expected, FrameIndex(i))
	}
	testTriggerSubroutine(t, raw, nRepeat, dsp, "AutoMultipleSegmentsB", expected)

	// Test signed signals
	for i := 0; i < len(raw); i++ {
		raw[i] = 65530
	}
	for i := tframe; i < tframe+10; i++ {
		raw[i] = bigval
	}
	for i := tframe2; i < tframe2+10; i++ {
		raw[i] = smallval
	}
	nRepeat = 1
	dsp.stream.signed = true
	dsp.LevelTrigger = false
	dsp.AutoTrigger = false
	dsp.EdgeTrigger = true
	dsp.EdgeRising = true
	dsp.EdgeLevel = 100
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Edge signed", []FrameIndex{1000})

	dsp.EdgeTrigger = false
	dsp.LevelTrigger = true
	dsp.LevelRising = true
	dsp.LevelLevel = 100
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Level signed", []FrameIndex{1000})
}

func testTriggerSubroutine(t *testing.T, raw []RawType, nRepeat int, dsp *DataStreamProcessor,
	trigname string, expectedFrames []FrameIndex) ([]*DataRecord, []*DataRecord) {
	// fmt.Println(trigname, len(dsp.stream.rawData))
	dsp.LastTrigger = math.MinInt64 / 4 // far in the past, but not so far we can't subtract from it.
	sampleTime := time.Duration(float64(time.Second) / dsp.SampleRate)
	segment := NewDataSegment(raw, 1, 0, time.Now(), sampleTime)
	dsp.stream.samplesSeen = 0
	var primaries, secondaries []*DataRecord
	for i := 0; i < nRepeat; i++ {
		dsp.stream.AppendSegment(segment)
		segment.firstFramenum += FrameIndex(len(raw))
		p, s := dsp.TriggerData()
		primaries = append(primaries, p...)
		secondaries = append(secondaries, s...)
	}
	if len(primaries) != len(expectedFrames) {
		t.Errorf("%s: have %v triggers, want %v triggers", trigname, len(primaries), len(expectedFrames))
		fmt.Print("have ")
		for _, p := range primaries {
			fmt.Printf("%v,", p.trigFrame)
		}
		fmt.Println()
		fmt.Print("want ")
		for _, v := range expectedFrames {
			fmt.Printf("%v,", v)
		}
		fmt.Println()
	}
	if len(secondaries) != 0 {
		t.Errorf("%s: trigger found %d secondary (group) triggers, want 0", trigname, len(secondaries))
	}
	for i, pt := range primaries {
		if i < len(expectedFrames) {
			if pt.trigFrame != expectedFrames[i] {
				t.Errorf("%s: trigger[%d] at frame %d, want %d", trigname, i, pt.trigFrame, expectedFrames[i])
			}
		}
	}

	// Check the data samples for the first trigger match raw, for samples where raw is long enough
	if len(primaries) != 0 && len(expectedFrames) != 0 {
		pt := primaries[0]
		offset := int(expectedFrames[0]) - dsp.NPresamples
		for i := 0; i < len(pt.data) && i+offset < len(raw) && offset >= 0; i++ {
			// fmt.Printf("i %v, offset %v, i+offset %v, len(raw) %v\n", i, offset, i+offset, len(raw))
			expect := raw[i+offset]
			if pt.data[i] != expect {
				t.Errorf("%s trigger[0] found data[%d]=%d, want %d", trigname, i,
					pt.data[i], expect)
			}
		}
	}
	dsp.stream.TrimKeepingN(0)
	return primaries, secondaries
}

// TestEdgeLevelInteraction tests that a single edge trigger happens where expected, even if
// there's also a level trigger.
func TestEdgeLevelInteraction(t *testing.T) {
	const nchan = 1

	broker := NewTriggerBroker(nchan)
	go broker.Run()
	defer broker.Stop()
	NPresamples := 256
	NSamples := 1024
	dsp := NewDataStreamProcessor(0, broker, NPresamples, NSamples)
	nRepeat := 1

	const bigval = 8000
	const tframe = 1000
	raw := make([]RawType, 10000)
	for i := tframe; i < tframe+10; i++ {
		raw[i] = bigval
	}
	const smallval = 1
	const tframe2 = 6000
	for i := tframe2; i < tframe2+10; i++ {
		raw[i] = smallval
	}
	dsp.NPresamples = 100
	dsp.NSamples = 1000

	dsp.EdgeTrigger = true
	dsp.EdgeRising = true
	dsp.EdgeLevel = 100
	dsp.LevelTrigger = true
	dsp.LevelRising = true
	dsp.LevelLevel = 100
	// should yield a single edge trigger
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Edge+Level 1", []FrameIndex{1000})
	dsp.LevelLevel = 10000
	// should yield a single edge trigger
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Edge+Level 2", []FrameIndex{1000})
	dsp.EdgeLevel = 20000
	dsp.LevelLevel = 100
	// should yield a single level trigger
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Edge + Level 3", []FrameIndex{1000})
	dsp.EdgeLevel = 1
	// should yield 2 edge triggers
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Edge + Level 4", []FrameIndex{1000, 6000})
	dsp.LevelLevel = 1
	dsp.EdgeLevel = 20000
	// should yield 2 level triggers
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Edge + Level 5", []FrameIndex{1000, 6000})
	dsp.LevelLevel = 1
	dsp.EdgeTrigger = false
	dsp.EdgeLevel = 1
	// should yield 2 level triggers
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Edge + Level 5", []FrameIndex{1000, 6000})
	// now exercise issue 25: when the 2nd edge trigger is too close to end-of-frame
	for i := 9050; i < 9060; i++ {
		raw[i] = bigval
	}
	dsp.EdgeTrigger = true
	testTriggerSubroutine(t, raw, nRepeat, dsp, "Edge + Level 6", []FrameIndex{1000, 6000, 9050})
}

func TestEdgeMulti(t *testing.T) {
	const nchan = 1

	broker := NewTriggerBroker(nchan)
	go broker.Run()
	defer broker.Stop()
	NPresamples := 256
	NSamples := 1024
	dsp := NewDataStreamProcessor(0, broker, NPresamples, NSamples)

	//kink model parameters
	var a, b, c float64
	a = 0
	b = 0
	c = 10
	raw := make([]RawType, 1000)
	kinkList := []float64{100, 200.1, 300.5, 400.9, 460, 500, 540, 700}
	kinkListFrameIndex := make([]FrameIndex, len(kinkList))
	for i := 0; i < len(kinkList); i++ {
		k := kinkList[i]
		kint := int(math.Ceil(k))
		for j := kint - 6; j < kint+20; j++ {
			raw[j] = RawType(math.Ceil(kinkModel(k, float64(j), a, b, c)))
			if j == kint+19 {
				raw[j] = RawType(kint) // make it easier to figure out which trigger you are looking at if you print raw
			}
		}
		kinkListFrameIndex[i] = FrameIndex(kint)
	}
	// fmt.Println(raw)
	dsp.NPresamples = 50
	dsp.NSamples = 100

	dsp.EdgeMulti = true
	dsp.EdgeLevel = 10000
	dsp.EdgeMultiVerifyNMonotone = 5
	nRepeat := 1
	testTriggerSubroutine(t, raw, nRepeat, dsp, "EdgeMulti A: level too high", []FrameIndex{})
	dsp.EdgeLevel = 1
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	// here we will find all triggers in trigInds, but triggers that are too short are not recordized
	// the kinks that occur at fractional samples will end up the rounded value
	// ideally 200.1 should trigger at 201, but since we only check k values in 0.5 step increments, we miss it
	// my tests suggest testing at 0.5 step increments is ok on real data (eg a set of data from the Raven backup array test in 2018)
	testTriggerSubroutine(t, raw, nRepeat, dsp, "EdgeMulti B: make only full length records", []FrameIndex{100, 200, 301, 401, 700})
	dsp.EdgeMultiMakeContaminatedRecords = true
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test

	// here we will find all triggers in trigInds, and contaminated records will be created
	// we expect the last two triggers to be not made because there will be limited by the rate limiting algorithm
	primaries, _ := testTriggerSubroutine(t, raw, nRepeat, dsp, "EdgeMulti C: MakeContaminatedRecords", []FrameIndex{100, 200, 301, 401, 460, 500})
	for _, record := range primaries {
		if len(record.data) != dsp.NSamples {
			t.Errorf("EdgeMulti C record has wrong number of samples %v", record)
		}
	}
	dsp.EdgeMultiMakeContaminatedRecords = false
	dsp.EdgeMultiMakeShortRecords = true
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	// here we will find all triggers in trigInds, and short records will be created
	primaries, _ = testTriggerSubroutine(t, raw, nRepeat, dsp, "EdgeMulti D: MakeShortRecords", []FrameIndex{100, 200, 301, 401, 460, 500, 540, 700})
	///                                                                 lengths   100, 100, 100, 100, 49,  40,  50,  100
	expectLengths := []int{100, 100, 100, 100, 49, 40, 50, 100}
	for i, record := range primaries {
		if len(record.data) != expectLengths[i] {
			//if true {
			t.Errorf("EdgeMulti D record %v: expect len %v, have len %v, presamples %v, trigFrame %v, %v:%v", i, expectLengths[i],
				len(record.data), record.presamples, record.trigFrame, int(record.trigFrame)-record.presamples, int(record.trigFrame)-record.presamples+len(record.data)-1)
		}
	}

	// edgeMulti searches within a given segment from dsp.NPresamples to ndata + dsp.NPresamples - dsp.NSamples
	// for these values that is from 50 to 950
	// so we want to test triggering on an event that starts before 950, and continues rising past 950
	rawE := make([]RawType, 1000)
	kinkListE := []float64{945}
	kinkListFrameIndexE := make([]FrameIndex, len(kinkListE))
	for i := 0; i < len(kinkListE); i++ {
		k := kinkListE[i]
		kint := int(math.Ceil(k))
		for j := kint - 6; j < kint+20; j++ {
			rawE[j] = RawType(math.Ceil(kinkModel(k, float64(j), a, b, c)))
			if j == kint+19 {
				rawE[j] = RawType(kint) // make it easier to figure out which trigger you are looking at if you print rawE
			}
		}
		kinkListFrameIndexE[i] = FrameIndex(kint)
	}
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	nRepeatE := 3
	// here we attempt to trigger around a segment boundary
	_, _ = testTriggerSubroutine(t, rawE, nRepeatE, dsp, "EdgeMulti E: handling segment boundary", []FrameIndex{945, 1945})

	dsp.NSamples = 15
	dsp.NPresamples = 6
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	_, _ = testTriggerSubroutine(t, rawE, nRepeat, dsp, "EdgeMulti F: dont make records when it is monotone for >= dsp.NSamples", []FrameIndex{})

	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	dsp.NSamples = 30
	dsp.NPresamples = 25
	dsp.EdgeMultiMakeContaminatedRecords = true
	dsp.EdgeLevel = 0
	dsp.EdgeMultiVerifyNMonotone = 0

	rawG := make([]RawType, 10)
	for i := 0; i < len(rawG); i++ {
		rawG[i] = RawType(i % 2)
	}
	nRepeatG := 20
	expectG := make([]FrameIndex, 0)
	for i := dsp.NPresamples + 2; i < len(rawG)*nRepeatG-(dsp.NSamples-dsp.NPresamples); i += 2 {
		expectG = append(expectG, FrameIndex(i))
	}
	_, _ = testTriggerSubroutine(t, rawG, nRepeatG, dsp, "EdgeMulti G: make lots of contaminated records", expectG)

	dsp.EdgeMulti = true
	dsp.EdgeMultiNoise = true
	dsp.EdgeLevel = math.MaxInt32 // don't ever add to TriggerInds
	dsp.NSamples = 100
	dsp.NPresamples = 50
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	dsp.LastEdgeMultiTrigger = 100 // make first noise trigger a round number
	nRepeatH := 2
	rawH := make([]RawType, 500)
	_, _ = testTriggerSubroutine(t, rawH, nRepeatH, dsp, "EdgeMulti H: EdgeMultiNoise basic", []FrameIndex{200, 300, 400, 500, 600, 700, 800, 900})

	nRepeatI := 25
	rawI := make([]RawType, 40)
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	dsp.LastEdgeMultiTrigger = 100 // make first noise trigger a round number
	_, _ = testTriggerSubroutine(t, rawI, nRepeatI, dsp, "EdgeMulti I: EdgeMultiNoise basic, segments shorter than records",
		[]FrameIndex{200, 300, 400, 500, 600, 700, 800, 900})

	dsp.NSamples = 10
	dsp.NPresamples = 5
	dsp.EdgeLevel = 1
	nRepeatJ := 2
	rawJ := make([]RawType, 100)
	rawJ[50] = 1
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	dsp.LastEdgeMultiTrigger = 20  // make first noise trigger a round number
	_, _ = testTriggerSubroutine(t, rawJ, nRepeatJ, dsp, "EdgeMulti J: EdgeMultiNoise avoiding edge triggers",
		[]FrameIndex{30, 40, 60, 70, 80, 90, 100, 110, 120, 130, 140, 160, 170, 180, 190})

	//kink model parameters
	var aFalling, bFalling, cFalling float64
	aFalling = 1000
	bFalling = 0
	cFalling = -10
	rawK := make([]RawType, 1000)
	for i := range rawK {
		rawK[i] = RawType(aFalling)
	}
	for i := 0; i < len(kinkList); i++ {
		k := kinkList[i]
		kint := int(math.Ceil(k))
		for j := kint - 6; j < kint+20; j++ {
			rawK[j] = RawType(math.Ceil(kinkModel(k, float64(j), aFalling, bFalling, cFalling)))
			if j == kint+19 {
				rawK[j] = RawType(kint) + RawType(aFalling) // make it easier to figure out which trigger you are looking at if you print raw
			}
		}
		kinkListFrameIndex[i] = FrameIndex(kint)
	}
	dsp.NPresamples = 50
	dsp.NSamples = 100
	dsp.EdgeMulti = true
	dsp.EdgeMultiNoise = false
	dsp.EdgeLevel = -10000
	dsp.EdgeMultiMakeContaminatedRecords = true
	dsp.EdgeMultiMakeShortRecords = false
	dsp.EdgeMultiVerifyNMonotone = 5
	nRepeatK := 1
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	testTriggerSubroutine(t, rawK, nRepeatK, dsp, "EdgeMulti K: level too large (negative)", []FrameIndex{})
	dsp.EdgeLevel = -1
	dsp.edgeMultiSetInitialState() // call this between each edgeMulti test
	testTriggerSubroutine(t, rawK, nRepeatK, dsp, "EdgeMulti L: negative trigger level", []FrameIndex{100, 200, 301, 401, 460, 500})
}

// TestEdgeVetosLevel tests that an edge trigger vetoes a level trigger as needed.
func TestEdgeVetosLevel(t *testing.T) {
	const nchan = 1

	broker := NewTriggerBroker(nchan)
	go broker.Run()
	defer broker.Stop()
	NPresamples := 256
	NSamples := 1024
	dsp := NewDataStreamProcessor(0, broker, NPresamples, NSamples)
	dsp.NPresamples = 20
	dsp.NSamples = 100

	dsp.EdgeTrigger = true
	dsp.EdgeLevel = 290
	dsp.EdgeRising = true
	dsp.LevelTrigger = true
	dsp.LevelRising = true
	dsp.LevelLevel = 99

	levelChangeAt := []int{50, 199, 200, 201, 299, 300, 301, 399, 400, 401, 500}
	edgeChangeAt := 300
	const rawLength = 1000
	expectNT := []int{2, 2, 2, 1, 1, 1, 1, 1, 1, 2, 2}
	for j, lca := range levelChangeAt {
		want := expectNT[j]

		raw := make([]RawType, rawLength)
		for i := lca; i < rawLength; i++ {
			raw[i] = 100
		}
		for i := edgeChangeAt; i < edgeChangeAt+100; i++ {
			raw[i] = 400
		}

		segment := NewDataSegment(raw, 1, 0, time.Now(), time.Millisecond)
		dsp.stream.AppendSegment(segment)
		primaries, _ := dsp.TriggerData()
		if len(primaries) != want {
			t.Errorf("EdgeVetosLevel problem with LCA=%d: saw %d triggers, want %d", lca, len(primaries), want)
		}
	}
}

func BenchmarkAutoTriggerOpsAre100SampleTriggers(b *testing.B) {
	const nchan = 1
	broker := NewTriggerBroker(nchan)
	go broker.Run()
	defer broker.Stop()
	NPresamples := 256
	NSamples := 1024
	dsp := NewDataStreamProcessor(0, broker, NPresamples, NSamples)
	dsp.NPresamples = 20
	dsp.NSamples = 100
	dsp.AutoTrigger = true
	dsp.SampleRate = 10000.0
	dsp.AutoDelay = 10 * time.Millisecond
	dsp.LastTrigger = math.MinInt64 / 4 // far in the past, but not so far we can't subtract from it.

	raw := make([]RawType, (b.N+1)*dsp.NSamples)
	sampleTime := time.Duration(float64(time.Second) / dsp.SampleRate)
	segment := NewDataSegment(raw, 1, 0, time.Now(), sampleTime)
	dsp.stream.AppendSegment(segment)
	b.ResetTimer()
	primaries, _ := dsp.TriggerData()
	if len(primaries) != b.N {
		fmt.Println("wrong number", len(primaries), b.N)
	}
}

func BenchmarkEdgeTrigger0TriggersOpsAreSamples(b *testing.B) {
	const nchan = 1
	broker := NewTriggerBroker(nchan)
	go broker.Run()
	defer broker.Stop()
	NPresamples := 256
	NSamples := 1024
	dsp := NewDataStreamProcessor(0, broker, NPresamples, NSamples)
	dsp.NPresamples = 20
	dsp.NSamples = 100

	dsp.EdgeTrigger = true
	dsp.EdgeLevel = 290
	dsp.EdgeRising = true
	dsp.LevelTrigger = true
	dsp.LevelRising = true
	dsp.LevelLevel = 99
	dsp.AutoTrigger = true

	raw := make([]RawType, b.N)
	for i := 0; i < b.N; i++ {
		raw[i] = 0
	}
	segment := NewDataSegment(raw, 1, 0, time.Now(), time.Millisecond)
	dsp.stream.AppendSegment(segment)
	records := make([]*DataRecord, 0)
	b.ResetTimer()

	records = dsp.edgeTriggerComputeAppend(records)
	if len(records) != 0 {
		b.Fatal("no records")
	}

}

func BenchmarkLevelTrigger0TriggersOpsAreSamples(b *testing.B) {
	const nchan = 1
	broker := NewTriggerBroker(nchan)
	go broker.Run()
	defer broker.Stop()
	NPresamples := 256
	NSamples := 1024
	dsp := NewDataStreamProcessor(0, broker, NPresamples, NSamples)
	dsp.NPresamples = 20
	dsp.NSamples = 100

	dsp.EdgeTrigger = true
	dsp.EdgeLevel = 290
	dsp.EdgeRising = true
	dsp.LevelTrigger = true
	dsp.LevelRising = true
	dsp.LevelLevel = 99
	dsp.AutoTrigger = true

	raw := make([]RawType, b.N)
	for i := 0; i < b.N; i++ {
		raw[i] = 0
	}
	segment := NewDataSegment(raw, 1, 0, time.Now(), time.Millisecond)
	dsp.stream.AppendSegment(segment)
	records := make([]*DataRecord, 0)
	b.ResetTimer()

	records = dsp.levelTriggerComputeAppend(records)
	if len(records) != 0 {
		b.Fatal("no records")
	}

}

func TestKinkModel(t *testing.T) {
	xdata := []float64{0, 1, 2, 3, 4, 5, 6, 7}
	ydata := []float64{0, 0, 0, 0, 1, 2, 3, 4}
	ymodel, a, b, c, X2, err := kinkModelResult(3, xdata, ydata)
	if a != 0 || b != 0 || c != 1 || X2 != 0 || err != nil {
		t.Errorf("a %v, b %v, c %v, X2 %v, err %v, ymodel %v", a, b, c, X2, err, ymodel)
	}
	ymodel, a, b, c, X2, err = kinkModelResult(4, xdata, ydata)
	if a != 0.6818181818181821 || b != 0.22727272727272738 ||
		c != 1.1363636363636362 || X2 != 0.45454545454545453 || err != nil {
		t.Errorf("a %v, b %v, c %v, X2 %v, err %v, ymodel %v", a, b, c, X2, err, ymodel)
	}
	kbest, X2min, err := kinkModelFit(xdata, ydata, []float64{1, 2, 2.5, 3, 3.5, 4, 5})
	if kbest != 3 || X2min != 0 || err != nil {
		t.Errorf("kbest %v, X2min %v, err %v", kbest, X2min, err)
	}
}

func TestTriggerCounter(t *testing.T) {
	now := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	tc := NewTriggerCounter(0, time.Second)
	tList := triggerList{channelIndex: 0, frames: []FrameIndex{}, keyFrame: 0,
		keyTime: now, sampleRate: 1000, lastFrameThatWillNeverTrigger: 0}
	if err := tc.observeTriggerList(&tList); err != nil {
		t.Error(err)
	}
	if tc.hi != 0 {
		t.Errorf("have %v, want %v", tc.hi, 0)
	}
	if tc.lo != -999 {
		t.Errorf("have %v, want %v", tc.lo, -999)
	}
	if tc.countsSeen != len(tList.frames) {
		t.Errorf("want %v, have %v", len(tList.frames), tc.countsSeen)
	}
	tList = triggerList{channelIndex: 0, frames: []FrameIndex{1, 2, 3, 4, 5}, keyFrame: 100,
		keyTime: now.Add(100 * time.Millisecond), sampleRate: 1000, lastFrameThatWillNeverTrigger: 0}
	if err := tc.observeTriggerList(&tList); err != nil {
		t.Error(err)
	}
	if tc.countsSeen != len(tList.frames) {
		t.Errorf("want %v, have %v", len(tList.frames), tc.countsSeen)
	}
	if tc.hi != 1000 {
		t.Errorf("have %v, want %v", tc.hi, 1000)
	}
	if tc.lo != 1 {
		t.Errorf("have %v, want %v", tc.lo, 1)
	}
	tList = triggerList{channelIndex: 0, frames: []FrameIndex{1007, 1008, 1009, 2000, 2001}, keyFrame: 1900,
		keyTime: now.Add(1900 * time.Millisecond), sampleRate: 1000, lastFrameThatWillNeverTrigger: 0}
	if err := tc.observeTriggerList(&tList); err != nil {
		t.Error(err)
	}
	if tc.hi != 3000 {
		t.Errorf("have %v, want %v", tc.hi, 3000)
	}
	if tc.lo != 2001 {
		t.Errorf("have %v, want %v", tc.lo, 2001)
	}
	if tc.countsSeen != 1 { // 2001
		t.Errorf("want %v, have %v", 1, tc.countsSeen)
	}
	tList = triggerList{channelIndex: 0, frames: []FrameIndex{}, keyFrame: 1900,
		keyTime: now.Add(1900 * time.Millisecond), sampleRate: 1000, lastFrameThatWillNeverTrigger: 3001}
	if err := tc.observeTriggerList(&tList); err != nil {
		t.Error(err)
	}
	if tc.hi != 4000 {
		t.Errorf("have %v, want %v", tc.hi, 4000)
	}
	if tc.lo != 3001 {
		t.Errorf("have %v, want %v", tc.lo, 3001)
	}
	if len(tc.messages) != 4 {
		t.Errorf("have %v, expect %v", len(tc.messages), 4)
	}
	expectCounts := []int{0, 5, 4, 1}
	for i, m := range tc.messages {
		if expectCounts[i] != m.countsSeen {
			t.Errorf("message %v has %v counts, want %v", i, m.countsSeen, expectCounts[i])
		}

	}
}

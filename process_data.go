package dastard

import (
	"fmt"
	"math"
	"time"

	"gonum.org/v1/gonum/mat"
)

// DataStreamProcessor contains all the state needed to decimate, trigger, write, and publish data.
type DataStreamProcessor struct {
	channelIndex         int
	Name                 string
	Broker               *TriggerBroker
	NSamples             int
	NPresamples          int
	SampleRate           float64
	LastTrigger          FrameIndex
	LastEdgeMultiTrigger FrameIndex
	stream               DataStream
	projectors           mat.Dense
	modelDescription     string
	// realtime analysis is disable if projectors .IsZero
	// otherwise projectors must be size (nbases,NSamples)
	// such that projectors*data (data as a column vector) = modelCoefs
	basis mat.Dense
	// if not projectors.IsZero basis must be size
	// (NSamples, nbases) such that basis*modelCoefs = modeled_data
	DecimateState
	TriggerState
	DataPublisher
}

// RemoveProjectorsBasis calls .Reset on projectors and basis, which disables projections in analysis
// Lock dsp.changeMutex before calling this function, it will not lock on its own.
func (dsp *DataStreamProcessor) removeProjectorsBasis() {
	dsp.projectors.Reset()
	dsp.basis.Reset()
	var s string
	dsp.modelDescription = s
}

// SetProjectorsBasis sets .projectors and .basis to the arguments, returns an error if the sizes are not right
func (dsp *DataStreamProcessor) SetProjectorsBasis(projectors mat.Dense, basis mat.Dense, modelDescription string) error {
	rows, cols := projectors.Dims()
	nbases := rows
	if dsp.NSamples != cols {
		return fmt.Errorf("projectors has wrong size, rows: %v, cols: %v, want cols: %v", rows, cols, dsp.NSamples)
	}
	brows, bcols := basis.Dims()
	if bcols != nbases {
		return fmt.Errorf("basis has wrong size, has cols: %v, want: %v", bcols, nbases)
	}
	if brows != dsp.NSamples {
		return fmt.Errorf("basis has wrong size, has rows: %v, want: %v", brows, dsp.NSamples)
	}
	dsp.projectors = projectors
	dsp.basis = basis
	dsp.modelDescription = modelDescription
	return nil
}

// HasProjectors return true if projectors are loaded
func (dsp *DataStreamProcessor) HasProjectors() bool {
	return !dsp.projectors.IsZero()
}

// NewDataStreamProcessor creates and initializes a new DataStreamProcessor.
func NewDataStreamProcessor(channelIndex int, broker *TriggerBroker, NPresamples int, NSamples int) *DataStreamProcessor {
	data := make([]RawType, 0, 1024)
	framesPerSample := 1
	firstFrame := FrameIndex(0)
	firstTime := time.Now()
	period := time.Duration(1 * time.Millisecond) // TODO: figure out what this ought to be, or make an argument
	stream := NewDataStream(data, framesPerSample, firstFrame, firstTime, period)
	dsp := DataStreamProcessor{channelIndex: channelIndex, Broker: broker,
		stream: *stream, NSamples: NSamples, NPresamples: NPresamples,
	}
	dsp.LastTrigger = math.MinInt64 / 4 // far in the past, but not so far we can't subtract from it
	dsp.projectors.Reset()              // dsp.projectors is set to zero value
	dsp.basis.Reset()                   // dsp.basis is set to zero value
	dsp.edgeMultiSetInitialState()      // set up edgeMulti in known state
	return &dsp
}

// DecimateState contains all the state needed to decimate data.
type DecimateState struct {
	DecimateLevel   int
	Decimate        bool
	DecimateAvgMode bool
}

// ConfigurePulseLengths sets this stream's pulse length and # of presamples.
// Also removes any existing projectors and basis.
func (dsp *DataStreamProcessor) ConfigurePulseLengths(nsamp, npre int) {
	// if nsamp or npre is invalid, panic, do not silently ignore
	if dsp.NSamples != nsamp || dsp.NPresamples != npre {
		dsp.removeProjectorsBasis()
		dsp.edgeMultiSetInitialState()
	}
	dsp.NSamples = nsamp
	dsp.NPresamples = npre
}

// ConfigureTrigger sets this stream's trigger state.
func (dsp *DataStreamProcessor) ConfigureTrigger(state TriggerState) {
	dsp.TriggerState = state
	dsp.edgeMultiSetInitialState()
}

func (dsp *DataStreamProcessor) processSegment(segment *DataSegment) {
	dsp.DecimateData(segment)
	dsp.stream.AppendSegment(segment)
	records, _ := dsp.TriggerData()
	dsp.AnalyzeData(records)                                       // add analysis results to records in-place
	if err := dsp.DataPublisher.PublishData(records); err != nil { // publish and save data, when enabled
		panic(err)
	}
	segment.processed = true
}

// DecimateData decimates data in-place.
func (dsp *DataStreamProcessor) DecimateData(segment *DataSegment) {
	if !dsp.Decimate || dsp.DecimateLevel <= 1 {
		return
	}
	data := segment.rawData
	Nin := len(data)
	Nout := (Nin - 1 + dsp.DecimateLevel) / dsp.DecimateLevel
	if dsp.DecimateAvgMode {
		level := dsp.DecimateLevel
		cdata := make([]float64, Nout)
		if segment.signed {
			for i := 0; i < Nin; i++ {
				j := i / level
				cdata[j] += float64(int16(data[i]))
			}
		} else {
			for i := 0; i < Nin; i++ {
				j := i / level
				cdata[j] += float64(data[i])
			}
		}
		if Nout*dsp.DecimateLevel < Nin {
			extra := Nin % level
			cdata[Nout-1] *= float64(level) / float64(extra)
		}

		if segment.signed {
			for i := 0; i < Nout; i++ {
				// Trick for rounding to int16: don't let any numbers be negative
				// because float->int is a truncation operation. If we remove the
				// +65536 below, then 0 will be a "rounding attractor".
				data[i] = RawType(int16(cdata[i]/float64(level) + 65536 + 0.5))
			}

		} else {
			for i := 0; i < Nout; i++ {
				data[i] = RawType(cdata[i]/float64(level) + 0.5)
			}
		}

	} else {
		// Decimate by dropping data
		for i := 0; i < Nout; i++ {
			data[i] = data[i*dsp.DecimateLevel]
		}
	}
	segment.rawData = data[:Nout]
	segment.framesPerSample *= dsp.DecimateLevel
	return
}

// AnalyzeData computes pulse-analysis values in-place for all elements of a
// slice of DataRecord values.
func (dsp *DataStreamProcessor) AnalyzeData(records []*DataRecord) {
	var modelCoefs mat.VecDense
	var modelFull mat.VecDense
	var residual mat.VecDense
	for _, rec := range records {
		dataVec := *mat.NewVecDense(len(rec.data), make([]float64, len(rec.data)))
		if rec.signed {
			for i, v := range rec.data {
				dataVec.SetVec(i, float64(int16(v)))
			}
		} else {
			for i, v := range rec.data {
				dataVec.SetVec(i, float64(v))
			}
		}

		var val float64
		for i := 0; i < rec.presamples; i++ {
			val += dataVec.AtVec(i)
		}
		ptm := val / float64(rec.presamples)
		rec.pretrigMean = ptm

		max := ptm
		var sum, sum2 float64
		for i := rec.presamples; i < len(rec.data); i++ {
			val = dataVec.AtVec(i)
			sum += val
			sum2 += val * val
			if val > max {
				max = val
			}
		}
		rec.peakValue = max - ptm

		N := float64(len(rec.data) - rec.presamples)
		rec.pulseAverage = sum/N - ptm
		meanSquare := sum2/N - 2*ptm*(sum/N) + ptm*ptm
		rec.pulseRMS = math.Sqrt(meanSquare)
		if dsp.HasProjectors() {
			rows, cols := dsp.projectors.Dims()
			nbases := rows
			if cols != len(rec.data) {
				panic("projections for variable length records not implemented")
			}
			projectors := &dsp.projectors
			basis := &dsp.basis

			modelCoefs.MulVec(projectors, &dataVec)
			modelFull.MulVec(basis, &modelCoefs)
			residual.SubVec(&dataVec, &modelFull)

			// copy modelCoefs into rec.modelCoefs
			rec.modelCoefs = make([]float64, nbases)
			mat.Col(rec.modelCoefs, 0, &modelCoefs)

			// calculate and asign StdDev
			residualSlice := make([]float64, len(rec.data))
			mat.Col(residualSlice, 0, &residual)
			rec.residualStdDev = stdDev(residualSlice)
		}
	}
}

// return the uncorrected std deviation of a float slice
func stdDev(a []float64) float64 {
	if len(a) == 0 {
		return math.NaN()
	}
	s, s2 := 0.0, 0.0
	for _, v := range a {
		s += v
	}
	mean := s / float64(len(a))
	for _, v := range a {
		x := v - mean
		s2 += x * x
	}
	return math.Sqrt(s2 / float64(len(a)))
}

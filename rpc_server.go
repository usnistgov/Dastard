package dastard

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/spf13/viper"
	"gonum.org/v1/gonum/mat"
)

// SourceControl is the sub-server that handles configuration and operation of
// the Dastard data sources.
// TODO: consider renaming -> DastardControl (5/11/18)
type SourceControl struct {
	simPulses *SimPulseSource
	triangle  *TriangleSource
	lancero   *LanceroSource
	erroring  *ErroringSource
	// TODO: Add sources for ROACH, Abaco
	ActiveSource   DataSource
	isSourceActive bool

	status        ServerStatus
	clientUpdates chan<- ClientUpdate
	totalData     Heartbeat
	heartbeats    chan Heartbeat

	// For queueing up RPC requests for later execution and getting the result
	queuedRequests chan func()
	queuedResults  chan error
}

// NewSourceControl creates a new SourceControl object with correctly initialized
// contents.
func NewSourceControl() *SourceControl {
	sc := new(SourceControl)
	sc.heartbeats = make(chan Heartbeat)
	sc.queuedRequests = make(chan func())
	sc.queuedResults = make(chan error)

	sc.simPulses = NewSimPulseSource()
	sc.triangle = NewTriangleSource()
	sc.erroring = NewErroringSource()
	lan, _ := NewLanceroSource()
	sc.lancero = lan

	sc.simPulses.heartbeats = sc.heartbeats
	sc.triangle.heartbeats = sc.heartbeats
	sc.erroring.heartbeats = sc.heartbeats
	sc.lancero.heartbeats = sc.heartbeats

	sc.status.Ncol = make([]int, 0)
	sc.status.Nrow = make([]int, 0)
	return sc
}

// ServerStatus the status that SourceControl reports to clients.
type ServerStatus struct {
	Running                bool
	SourceName             string
	Nchannels              int
	Nsamples               int
	Npresamp               int
	Ncol                   []int
	Nrow                   []int
	ChannelsWithProjectors []int // move this to something than reports mix also? and experimentStateLabel
	// TODO: maybe bytes/sec data rate...?
}

// Heartbeat is the info sent in the regular heartbeat to clients
type Heartbeat struct {
	Running bool
	Time    float64
	DataMB  float64
}

// FactorArgs holds the arguments to a Multiply operation
type FactorArgs struct {
	A, B int
}

// Multiply is a silly RPC service that multiplies its two arguments.
func (s *SourceControl) Multiply(args *FactorArgs, reply *int) error {
	*reply = args.A * args.B
	return nil
}

// ConfigureTriangleSource configures the source of simulated pulses.
func (s *SourceControl) ConfigureTriangleSource(args *TriangleSourceConfig, reply *bool) error {
	log.Printf("ConfigureTriangleSource: %d chan, rate=%.3f\n", args.Nchan, args.SampleRate)
	err := s.triangle.Configure(args)
	s.clientUpdates <- ClientUpdate{"TRIANGLE", args}
	*reply = (err == nil)
	log.Printf("Result is okay=%t and state={%d chan, rate=%.3f}\n", *reply, s.triangle.nchan, s.triangle.sampleRate)
	return err
}

// ConfigureSimPulseSource configures the source of simulated pulses.
func (s *SourceControl) ConfigureSimPulseSource(args *SimPulseSourceConfig, reply *bool) error {
	log.Printf("ConfigureSimPulseSource: %d chan, rate=%.3f\n", args.Nchan, args.SampleRate)
	err := s.simPulses.Configure(args)
	s.clientUpdates <- ClientUpdate{"SIMPULSE", args}
	*reply = (err == nil)
	log.Printf("Result is okay=%t and state={%d chan, rate=%.3f}\n", *reply, s.simPulses.nchan, s.simPulses.sampleRate)
	return err
}

// ConfigureLanceroSource configures the lancero cards.
func (s *SourceControl) ConfigureLanceroSource(args *LanceroSourceConfig, reply *bool) error {
	log.Printf("ConfigureLanceroSource: mask 0x%4.4x  active cards: %v\n", args.FiberMask, args.ActiveCards)
	err := s.lancero.Configure(args)
	s.clientUpdates <- ClientUpdate{"LANCERO", args}
	*reply = (err == nil)
	log.Printf("Result is okay=%t and state={%d MHz clock, %d cards}\n", *reply, s.lancero.clockMhz, s.lancero.ncards)
	return err
}

// runLaterIfActive will return error if source is Inactive; otherwise it will
// run the closure f at an appropriate point in the data handling cycle
// and return any error sent on s.queuedRequests.
func (s *SourceControl) runLaterIfActive(f func()) error {
	if !s.isSourceActive {
		return fmt.Errorf("No source is active")
	}
	s.queuedRequests <- f
	return <-s.queuedResults
}

// MixFractionObject is the RPC-usable structure for ConfigureMixFraction
type MixFractionObject struct {
	ChannelIndices []int
	MixFractions   []float64
}

// ConfigureMixFraction sets the MixFraction for the channel associated with ChannelIndex
// mix = fb + mixFraction*err/Nsamp
// This MixFractionObject contains mix fractions as reported by autotune, where error/Nsamp
// is used. Thus, we will internally store not MixFraction, but errorScale := MixFraction/Nsamp.
// Supported by LanceroSource only.
//
// This does not need to be sent on the queuedRequests channel, because internally
// LanceroSource.ConfigureMixFraction will queue these requests. The reason is that
// queuedRequests is for keeping RPC requests separate from the data-*processing* step.
// But changes to the mix settings need to be kept separate from LanceroSource.distrubuteData,
// which is part of the data-*production* step, not the data-processing step.
func (s *SourceControl) ConfigureMixFraction(mfo *MixFractionObject, reply *bool) error {
	currentMix, err := s.ActiveSource.ConfigureMixFraction(mfo)
	*reply = (err == nil)
	s.broadcastMixState(currentMix)
	return err
}

// ConfigureTriggers configures the trigger state for 1 or more channels.
func (s *SourceControl) ConfigureTriggers(state *FullTriggerState, reply *bool) error {
	log.Printf("Got ConfigureTriggers: %v", spew.Sdump(state))
	f := func() {
		err := s.ActiveSource.ChangeTriggerState(state)
		s.broadcastTriggerState()
		s.queuedResults <- err
	}
	err := s.runLaterIfActive(f)
	*reply = (err == nil)
	return err
}

// ProjectorsBasisObject is the RPC-usable structure for ConfigureProjectorsBases
type ProjectorsBasisObject struct {
	ChannelIndex     int
	ProjectorsBase64 string
	BasisBase64      string
	ModelDescription string
}

// ConfigureProjectorsBasis takes ProjectorsBase64 which must a base64 encoded string with binary data matching that from mat.Dense.MarshalBinary
func (s *SourceControl) ConfigureProjectorsBasis(pbo *ProjectorsBasisObject, reply *bool) error {
	*reply = false
	projectorsBytes, err := base64.StdEncoding.DecodeString(pbo.ProjectorsBase64)
	if err != nil {
		return err
	}
	basisBytes, err := base64.StdEncoding.DecodeString(pbo.BasisBase64)
	if err != nil {
		return err
	}
	var projectors, basis mat.Dense
	if err = projectors.UnmarshalBinary(projectorsBytes); err != nil {
		return err
	}
	if err = basis.UnmarshalBinary(basisBytes); err != nil {
		return err
	}
	f := func() {
		err := s.ActiveSource.ConfigureProjectorsBases(pbo.ChannelIndex, projectors, basis, pbo.ModelDescription)
		if err == nil {
			s.status.ChannelsWithProjectors = s.ActiveSource.ChannelsWithProjectors()
		}
		s.queuedResults <- err
	}
	err = s.runLaterIfActive(f)
	*reply = (err == nil)
	return err
}

// SizeObject is the RPC-usable structure for ConfigurePulseLengths to change pulse record sizes.
type SizeObject struct {
	Nsamp int
	Npre  int
}

// ConfigurePulseLengths is the RPC-callable service to change pulse record sizes.
func (s *SourceControl) ConfigurePulseLengths(sizes SizeObject, reply *bool) error {
	*reply = false // handle the case that sizes fails the validation tests and we return early
	log.Printf("ConfigurePulseLengths: %d samples (%d pre)\n", sizes.Nsamp, sizes.Npre)
	if !s.isSourceActive {
		return fmt.Errorf("No source is active")
	}
	if s.status.Npresamp == sizes.Npre && s.status.Nsamples == sizes.Nsamp {
		return nil // no change requested
	}
	if s.ActiveSource.ComputeWritingState().Active {
		return fmt.Errorf("Stop writing before changing record lengths")
	}

	f := func() {
		err := s.ActiveSource.ConfigurePulseLengths(sizes.Nsamp, sizes.Npre)
		if err == nil {
			s.status.Npresamp = sizes.Npre
			s.status.Nsamples = sizes.Nsamp
		}
		s.broadcastStatus()
		s.queuedResults <- err
	}
	err := s.runLaterIfActive(f)
	*reply = (err == nil)
	return err
}

// Start will identify the source given by sourceName and Sample then Start it.
func (s *SourceControl) Start(sourceName *string, reply *bool) error {
	*reply = false
	if s.isSourceActive {
		return fmt.Errorf("already have active source, do not start")
	}
	name := strings.ToUpper(*sourceName)
	switch name {
	case "SIMPULSESOURCE":
		s.ActiveSource = DataSource(s.simPulses)
		s.status.SourceName = "SimPulses"

	case "TRIANGLESOURCE":
		s.ActiveSource = DataSource(s.triangle)
		s.status.SourceName = "Triangles"

	case "LANCEROSOURCE":
		s.ActiveSource = DataSource(s.lancero)
		s.status.SourceName = "Lancero"

	case "ERRORINGSOURCE":
		s.ActiveSource = DataSource(s.erroring)
		s.status.SourceName = "Erroring"

	// TODO: Add cases here for ROACH, ABACO, etc.

	default:
		return fmt.Errorf("Data Source \"%s\" is not recognized", *sourceName)
	}

	log.Printf("Starting data source named %s\n", *sourceName)
	s.status.Running = true
	if err := Start(s.ActiveSource, s.queuedRequests, s.status.Npresamp, s.status.Nsamples); err != nil {
		s.status.Running = false
		s.isSourceActive = false
		return err
	}
	s.isSourceActive = true
	s.status.Nchannels = s.ActiveSource.Nchan()
	if ls, ok := s.ActiveSource.(*LanceroSource); ok {
		s.status.Ncol = make([]int, ls.ncards)
		s.status.Nrow = make([]int, ls.ncards)
		for i, device := range ls.active {
			s.status.Ncol[i] = device.ncols
			s.status.Nrow[i] = device.nrows
		}
	} else {
		s.status.Ncol = make([]int, 0)
		s.status.Nrow = make([]int, 0)
	}
	s.broadcastStatus()
	s.broadcastTriggerState()
	s.broadcastChannelNames()
	*reply = true
	return nil
}

// Stop stops the running data source, if any
func (s *SourceControl) Stop(dummy *string, reply *bool) error {
	if !s.isSourceActive {
		return fmt.Errorf("No source is active")
	}
	log.Printf("Stopping data source\n")
	s.ActiveSource.Stop()
	s.handlePossibleStoppedSource()
	s.broadcastStatus()
	*reply = true
	return nil
}

// handlePossibleStoppedSource checks for a stopped source and modifies
// s to be correct after a source has stopped.
// It should called in Stop() plus any method that would be incorrect if it didn't
// know the source was stopped
func (s *SourceControl) handlePossibleStoppedSource() {
	if s.isSourceActive && !s.ActiveSource.Running() {
		s.status.Running = false
		s.isSourceActive = false
		s.clientUpdates <- ClientUpdate{"STATUS", s.status}
		if s.ActiveSource.ShouldAutoRestart() {
			log.Println("dastard is aware it should AutoRestart, but it's not implemented yet")
		}
	}
}

// WaitForStopTestingOnly will block until the running data source is finished and
// thus sets s.isSourceActive to false
func (s *SourceControl) WaitForStopTestingOnly(dummy *string, reply *bool) error {
	for s.isSourceActive {
		s.handlePossibleStoppedSource()
		time.Sleep(1 * time.Millisecond)
	}
	return nil
}

// WriteControlConfig object to control start/stop/pause of data writing
// Path and FileType are ignored for any request other than Start
type WriteControlConfig struct {
	Request         string // "Start", "Stop", "Pause", or "Unpause", or "Unpause label"
	Path            string // write in a new directory under this path
	FilenamePattern string // only Jamie should use this, just to make his transition easier
	// overrides path and the default behavior of choosing filenames automatically
	WriteLJH22 bool // turn on one or more file formats
	WriteOFF   bool
	WriteLJH3  bool
}

// WriteControl requests start/stop/pause/unpause data writing
func (s *SourceControl) WriteControl(config *WriteControlConfig, reply *bool) error {
	f := func() {
		err := s.ActiveSource.WriteControl(config)
		if err == nil {
			s.broadcastWritingState()
		}
		s.queuedResults <- err
	}
	err := s.runLaterIfActive(f)
	*reply = (err == nil)
	return err
}

// StateLabelConfig is the argument type of SetExperimentStateLabel
type StateLabelConfig struct {
	Label string
}

// SetExperimentStateLabel sets the experiment state label in the _experiment_state file
func (s *SourceControl) SetExperimentStateLabel(config *StateLabelConfig, reply *bool) error {
	f := func() {
		s.queuedResults <- s.ActiveSource.SetExperimentStateLabel(config.Label)
	}
	err := s.runLaterIfActive(f)
	*reply = (err == nil)
	return err
}

// WriteComment writes the comment to comment.txt
func (s *SourceControl) WriteComment(comment *string, reply *bool) error {
	*reply = false
	if len(*comment) == 0 {
		return fmt.Errorf("can't write zero-length comment, sourceActive %v, len(*comment) %v",
			s.isSourceActive, len(*comment))
	}
	f := func() {
		ws := s.ActiveSource.ComputeWritingState()
		if ws.Active {
			commentFilename := path.Join(filepath.Dir(ws.FilenamePattern), "comment.txt")
			fp, err := os.Create(commentFilename)
			if err != nil {
				s.queuedResults <- err
			}
			defer fp.Close()
			fp.WriteString(*comment)
			// Always end the comment file with a newline.
			if !strings.HasSuffix(*comment, "\n") {
				fp.WriteString("\n")
			}
		}
		s.queuedResults <- nil
	}
	err := s.runLaterIfActive(f)
	*reply = (err == nil)
	return err
}

// ReadComment reads the contents of comment.txt if it exists, otherwise returns err
func (s *SourceControl) ReadComment(zero *int, reply *string) error {
	if !s.isSourceActive {
		return fmt.Errorf("cant read comment with no active source")
	} else if *zero != 0 {
		return fmt.Errorf("please pass an the value 0, as it will be ignored, you passed %v", zero)
	}
	ws := s.ActiveSource.ComputeWritingState()
	if !ws.Active {
		return fmt.Errorf("cant read comment when not actively writing")
	}
	commentFilename := path.Join(filepath.Dir(ws.FilenamePattern), "comment.txt")
	b, err := ioutil.ReadFile(commentFilename)
	if err != nil {
		return err
	}
	*reply = string(b)
	return nil
}

// CouplingStatus describes the status of FB / error coupling
type CouplingStatus int

// Specific allowed values for status of FB / error coupling
const (
	NoCoupling CouplingStatus = iota + 1 // NoCoupling turns off FB + error coupling
	FBToErr                              // FB triggers cause secondary triggers in error channels
	ErrToFB                              // Error triggers cause secondary triggers in FB channels
)

// CoupleErrToFB turns on or off coupling of Error -> FB
func (s *SourceControl) CoupleErrToFB(couple *bool, reply *bool) error {
	f := func() {
		c := NoCoupling
		if *couple {
			c = ErrToFB
		}
		err := s.ActiveSource.SetCoupling(c)
		s.clientUpdates <- ClientUpdate{"TRIGCOUPLING", c}
		s.queuedResults <- err
	}
	err := s.runLaterIfActive(f)
	*reply = (err == nil)
	return err
}

// CoupleFBToErr turns on or off coupling of FB -> Error
func (s *SourceControl) CoupleFBToErr(couple *bool, reply *bool) error {
	f := func() {
		c := NoCoupling
		if *couple {
			c = FBToErr
		}
		err := s.ActiveSource.SetCoupling(c)
		s.clientUpdates <- ClientUpdate{"TRIGCOUPLING", c}
		s.queuedResults <- err
	}
	err := s.runLaterIfActive(f)
	*reply = (err == nil)
	return err
}

func (s *SourceControl) broadcastHeartbeat() {
	s.clientUpdates <- ClientUpdate{"ALIVE", s.totalData}
	s.totalData.DataMB = 0
	s.totalData.Time = 0
}

func (s *SourceControl) broadcastStatus() {
	s.handlePossibleStoppedSource()
	s.clientUpdates <- ClientUpdate{"STATUS", s.status}
}

func (s *SourceControl) broadcastWritingState() {
	if s.isSourceActive && s.status.Running {
		state := s.ActiveSource.ComputeWritingState()
		s.clientUpdates <- ClientUpdate{"WRITING", state}
	}
}

func (s *SourceControl) broadcastTriggerState() {
	if s.isSourceActive && s.status.Running {
		state := s.ActiveSource.ComputeFullTriggerState()
		// log.Printf("TriggerState: %v\n", state)
		s.clientUpdates <- ClientUpdate{"TRIGGER", state}
	}
}

func (s *SourceControl) broadcastMixState(mix []float64) {
	s.clientUpdates <- ClientUpdate{"MIX", mix}
}

func (s *SourceControl) broadcastChannelNames() {
	if s.isSourceActive && s.status.Running {
		configs := s.ActiveSource.ChannelNames()
		// log.Printf("chanNames: %v\n", configs)
		s.clientUpdates <- ClientUpdate{"CHANNELNAMES", configs}
	}
}

// SendAllStatus causes a broadcast to clients containing all broadcastable status info
func (s *SourceControl) SendAllStatus(dummy *string, reply *bool) error {
	s.broadcastStatus()
	s.clientUpdates <- ClientUpdate{"SENDALL", 0}
	return nil
}

// RunRPCServer sets up and run a permanent JSON-RPC server.
// If block, it will block until Ctrl-C and gracefully shut down.
// (The intention is that block=true in normal operation, but false for tests.)
func RunRPCServer(portrpc int, block bool) {

	// Set up objects to handle remote calls
	sourceControl := NewSourceControl()
	defer sourceControl.lancero.Delete()
	sourceControl.clientUpdates = clientMessageChan

	mapServer := newMapServer()
	mapServer.clientUpdates = clientMessageChan

	// Signal clients that there's a new Dastard running
	sourceControl.clientUpdates <- ClientUpdate{"NEWDASTARD", "new Dastard is running"}

	// Load stored settings, and transfer saved configuration
	// from Viper to relevant objects.
	var okay bool
	var spc SimPulseSourceConfig
	log.Printf("Dastard is using config file %s\n", viper.ConfigFileUsed())
	err := viper.UnmarshalKey("simpulse", &spc)
	if err == nil {
		sourceControl.ConfigureSimPulseSource(&spc, &okay)
	}
	var tsc TriangleSourceConfig
	err = viper.UnmarshalKey("triangle", &tsc)
	if err == nil {
		sourceControl.ConfigureTriangleSource(&tsc, &okay)
	}
	var lsc LanceroSourceConfig
	err = viper.UnmarshalKey("lancero", &lsc)
	if err == nil {
		sourceControl.ConfigureLanceroSource(&lsc, &okay)
	}
	err = viper.UnmarshalKey("status", &sourceControl.status)
	sourceControl.status.Running = false
	sourceControl.ActiveSource = sourceControl.triangle
	sourceControl.isSourceActive = false
	if err == nil {
		sourceControl.broadcastStatus()
	}
	var ws WritingState
	err = viper.UnmarshalKey("writing", &ws)
	if err == nil {
		wsSend := WritingState{BasePath: ws.BasePath} // only send the BasePath to clients
		// other info like Active: true could be wrong, and is not useful
		sourceControl.clientUpdates <- ClientUpdate{"WRITING", wsSend}
	}

	// Regularly broadcast a "heartbeat" containing data rate to all clients
	go func() {
		ticker := time.Tick(2 * time.Second)
		for {
			select {
			case <-ticker:
				sourceControl.broadcastHeartbeat()
			case h := <-sourceControl.heartbeats:
				sourceControl.totalData.DataMB += h.DataMB
				sourceControl.totalData.Time += h.Time
				sourceControl.totalData.Running = h.Running
			}
		}
	}()

	// Now launch the connection handler and accept connections.
	go func() {
		server := rpc.NewServer()
		if err := server.Register(sourceControl); err != nil {
			panic(err)
		}
		if err := server.Register(mapServer); err != nil {
			log.Fatal(err)
		}
		server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
		port := fmt.Sprintf(":%d", portrpc)
		listener, err := net.Listen("tcp", port)
		if err != nil {
			panic(fmt.Sprint("listen error:", err))
		}
		for {
			if conn, err := listener.Accept(); err != nil {
				panic("accept error: " + err.Error())
			} else {
				log.Printf("new connection established\n")
				go func() { // this is equivalent to ServeCodec, except all requests from a single connection
					// are handled SYNCHRONOUSLY, so sourceControl doesn't need a lock
					// requests from multiple connections are still asynchronous, but we could add slice of
					// connections and loop over it instead of launch a goroutine per connection
					codec := jsonrpc.NewServerCodec(conn)
					for {
						err := server.ServeRequest(codec)
						if err != nil {
							log.Printf("server stopped: %v", err)
							break
						}
					}
				}()
			}
		}
	}()

	if !block {
		return
	}

	// Handle ctrl-C gracefully, by stopping the active source.
	interruptCatcher := make(chan os.Signal, 1)
	signal.Notify(interruptCatcher, os.Interrupt)
	<-interruptCatcher
	dummy := "dummy"
	sourceControl.Stop(&dummy, &okay)
}

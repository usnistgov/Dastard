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
	simPulses      *SimPulseSource
	triangle       *TriangleSource
	lancero        *LanceroSource
	roach          *RoachSource
	abaco          *AbacoSource
	erroring       *ErroringSource
	ActiveSource   DataSource
	isSourceActive bool
	mapServer      *MapServer

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
	sc.roach, _ = NewRoachSource()
	sc.abaco, _ = NewAbacoSource()

	sc.simPulses.heartbeats = sc.heartbeats
	sc.triangle.heartbeats = sc.heartbeats
	sc.erroring.heartbeats = sc.heartbeats
	sc.lancero.heartbeats = sc.heartbeats
	sc.roach.heartbeats = sc.heartbeats
	sc.abaco.heartbeats = sc.heartbeats

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
	// Remember any errors for later, when we try to start the source.
	s.lancero.configError = err
	s.clientUpdates <- ClientUpdate{"LANCERO", args}
	*reply = (err == nil)
	log.Printf("Result is okay=%t and state={%d MHz clock, %d cards}\n", *reply, s.lancero.clockMhz, s.lancero.ncards)
	return err
}

// ConfigureAbacoSource configures the Abaco cards.
func (s *SourceControl) ConfigureAbacoSource(args *AbacoSourceConfig, reply *bool) error {
	log.Printf("ConfigureAbacoSource: \n")
	err := s.abaco.Configure(args)
	s.clientUpdates <- ClientUpdate{"ABACO", args}
	*reply = (err == nil)
	log.Printf("Result is okay=%t\n", *reply)
	return err
}

// ConfigureRoachSource configures the abaco cards.
func (s *SourceControl) ConfigureRoachSource(args *RoachSourceConfig, reply *bool) error {
	log.Printf("ConfigureRoachSource: \n")
	err := s.roach.Configure(args)
	s.clientUpdates <- ClientUpdate{"ROACH", args}
	*reply = (err == nil)
	log.Printf("Result is okay=%t\n", *reply)
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
		errcpb := s.ActiveSource.ConfigureProjectorsBases(pbo.ChannelIndex, &projectors, &basis, pbo.ModelDescription)
		if errcpb == nil {
			s.status.ChannelsWithProjectors = s.ActiveSource.ChannelsWithProjectors()
		}
		s.queuedResults <- errcpb
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
	if s.ActiveSource.WritingIsActive() {
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

	case "ROACHSOURCE":
		s.ActiveSource = DataSource(s.roach)
		s.status.SourceName = "Roach"

	case "ABACOSOURCE":
		s.ActiveSource = DataSource(s.abaco)
		s.status.SourceName = "Abaco"

	case "ERRORINGSOURCE":
		s.ActiveSource = DataSource(s.erroring)
		s.status.SourceName = "Erroring"

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
	WriteLJH22      bool   // turn on one or more file formats
	WriteOFF        bool
	WriteLJH3       bool
	MapInternalOnly *Map // for dastard internal use only, used to pass map info to DataStreamProcessors
}

// mapError is used in WriteControl to indicate a map related error
type mapError struct {
	msg string
}

func (m mapError) Error() string {
	return m.msg
}

// WriteControl requests start/stop/pause/unpause data writing
func (s *SourceControl) WriteControl(config *WriteControlConfig, reply *bool) error {

	config.MapInternalOnly = s.mapServer.Map
	f := func() {
		err := s.ActiveSource.WriteControl(config)
		if err == nil {
			s.broadcastWritingState()
		}
		s.queuedResults <- err
	}
	err := s.runLaterIfActive(f)
	//check if we have a map error, if so, invalidate the map
	switch err.(type) {
	case mapError:
		var zero *int
		s.mapServer.Unload(zero, reply)
		*reply = err == nil
		return fmt.Errorf("map file invalidated: %v", err)
	default:
	}
	*reply = err == nil
	return err
}

// StateLabelConfig is the argument type of SetExperimentStateLabel
type StateLabelConfig struct {
	Label        string
	WaitForError bool // False (the default) will return ASAP and panic if there is an error
	// True will wait for a response and return any error, but will be slower (~50 ms typical, slower possible)
}

// SetExperimentStateLabel sets the experiment state label in the _experiment_state file
// The timestamp is fixed as soon as the RPC command is received
func (s *SourceControl) SetExperimentStateLabel(config *StateLabelConfig, reply *bool) error {
	timestamp := time.Now()
	if config.WaitForError {
		f := func() {
			s.queuedResults <- s.ActiveSource.SetExperimentStateLabel(timestamp, config.Label)
		}
		err := s.runLaterIfActive(f)
		*reply = (err == nil)
		return err
	} else {
		f := func() {
			s.queuedResults <- s.ActiveSource.SetExperimentStateLabel(timestamp, config.Label)
		}
		f2 := func() {
			err := s.runLaterIfActive(f)
			if err != nil {
				// panic here since this error could never be returned
				panic(fmt.Sprintf("error with WaitForError==false in SetExperimentStateLabel. %s", spew.Sdump(err)))
			}
		}
		go f2()
		return nil
	}
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
		return fmt.Errorf("cannot read comment with no active source")
	} else if *zero != 0 {
		return fmt.Errorf("please pass in the value 0, as it will be ignored, you passed %v", zero)
	}
	ws := s.ActiveSource.ComputeWritingState()
	if !ws.Active {
		return fmt.Errorf("cannot read comment when not actively writing")
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
	// from Viper to relevant objects. Note that these items are saved
	// in client_updater.go
	var okay bool
	var spc SimPulseSourceConfig
	log.Printf("Dastard is using config file %s\n", viper.ConfigFileUsed())
	err := viper.UnmarshalKey("simpulse", &spc)
	if spc.Nchan == 0 { // default to a valid Nchan value to avoid ConfigureSimPulseSource throwing an error
		spc.Nchan = 1
	}
	if err == nil {
		err0 := sourceControl.ConfigureSimPulseSource(&spc, &okay)
		if err0 != nil {
			panic(err0)
		}
	}
	var tsc TriangleSourceConfig
	err = viper.UnmarshalKey("triangle", &tsc)
	// Default to a valid Nchan value to avoid ConfigureTriangleSource throwing an error
	if tsc.Nchan == 0 {
		tsc.Nchan = 1
	}
	if err == nil {
		err0 := sourceControl.ConfigureTriangleSource(&tsc, &okay)
		if err0 != nil {
			panic(err0)
		}
	}
	var lsc LanceroSourceConfig
	err = viper.UnmarshalKey("lancero", &lsc)
	if err == nil {
		_ = sourceControl.ConfigureLanceroSource(&lsc, &okay)
		// Don't panic on config errors: they are expected on any system w/o Lancero cards.
		// That is, we are intentionally NOT checking any error returned by ConfigureLanceroSource.
	}
	var asc AbacoSourceConfig
	err = viper.UnmarshalKey("abaco", &asc)
	if err == nil {
		_ = sourceControl.ConfigureAbacoSource(&asc, &okay)
		// intentionally not chcecking for configure errors since it might fail on non abaco systems
	}
	var rsc RoachSourceConfig
	err = viper.UnmarshalKey("roach", &rsc)
	if err == nil {
		_ = sourceControl.ConfigureRoachSource(&rsc, &okay)
		// intentionally not chcecking for configure errors since it might fail on non roach systems
	}
	err = viper.UnmarshalKey("status", &sourceControl.status)
	sourceControl.status.Running = false
	sourceControl.ActiveSource = sourceControl.triangle
	sourceControl.isSourceActive = false
	sourceControl.mapServer = mapServer
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

	var mapFileName string
	err = viper.UnmarshalKey("tesmapfile", &mapFileName)
	if err == nil {
		_ = mapServer.Load(&mapFileName, &okay)
		// intentially not checking for error, it ok if we fail to load a map file
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
							//spew.Dump(codec)
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

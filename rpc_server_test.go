package dastard

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"os/user"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/usnistgov/dastard/lancero"
	"gonum.org/v1/gonum/mat"
)

const harrypotter int = 7

func simpleClient() (*rpc.Client, error) {
	serverAddress := fmt.Sprintf("localhost:%d", Ports.RPC)
	retries := 5
	wait := 10 * time.Millisecond
	tries := 1
	for {
		// One command to dial AND set up jsonrpc client:
		client, err := jsonrpc.Dial("tcp", serverAddress)
		tries++
		if err == nil || tries > retries {
			return client, err
		}
		time.Sleep(wait)
		wait = wait * 2
	}
}

func TestServer(t *testing.T) {
	client, err := simpleClient()
	defer client.Close()
	if err != nil {
		t.Fatalf("Could not connect simpleClient() to RPC server")
	}

	// Test the silly multiply feature
	type Args struct {
		A, B int
	}
	args := &Args{33, 0}
	var reply int
	for b := 10; b < 11; b++ {
		args.B = b
		err = client.Call("SourceControl.Multiply", args, &reply)
		if err != nil {
			t.Errorf("SourceControl.Multiply error on call: %s", err.Error())
		}
		if reply != args.A*args.B {
			t.Errorf("SourceControl.Multiply: %d * %d = %d, want %d\n", args.A, args.B, reply, args.A*args.B)
		}
	}

	// Test the viper config
	if hp := viper.GetInt("harrypotter"); hp != harrypotter {
		t.Errorf("viper.GetInt(%q) returns %d, want %d", "harrypotter", hp, harrypotter)
	}

	var okay bool

	// Test Map service
	fname := "maps/ar14_30rows_map.cfg"
	err = client.Call("MapServer.Load", &fname, &okay)
	if err != nil {
		t.Errorf("Error calling MapServer.Load(): %s", err.Error())
	}

	// Test a basic configuration
	simConfig := SimPulseSourceConfig{
		Nchan: 4, SampleRate: 10000.0, Pedestal: 3000.0,
		Amplitudes: []float64{10000., 8000., 6000.}, Nsamp: 1000,
	}
	err = client.Call("SourceControl.ConfigureSimPulseSource", &simConfig, &okay)
	if !okay {
		t.Errorf("Error on server with SourceControl.ConfigureSimPulseSource()")
	}
	if err != nil {
		t.Errorf("Error calling SourceControl.ConfigureSimPulseSource(): %s", err.Error())
	}

	// Try to start and stop with a wrong name
	sourceName := "harrypotter"
	err = client.Call("SourceControl.Start", &sourceName, &okay)
	if err == nil {
		t.Errorf("Expected error calling SourceControl.Start(\"%s\") with wrong name, saw none", sourceName)
	}
	err = client.Call("SourceControl.Stop", sourceName, &okay)
	if err == nil {
		t.Errorf("expected error on Stopping when there is no active source")
	}

	// Try to start and stop with a sensible name
	sourceName = "SimPulseSource"
	err = client.Call("SourceControl.Start", &sourceName, &okay)
	if err != nil {
		t.Errorf("Error calling SourceControl.Start(%s): %s", sourceName, err.Error())
	}
	if !okay {
		t.Errorf("SourceControl.Start(\"%s\") returns !okay, want okay", sourceName)
	}
	err = client.Call("SourceControl.Start", &sourceName, &okay)
	if err == nil {
		t.Errorf("expected error when starting Source while a source is active")
	}
	dummy := ""
	err = client.Call("SourceControl.SendAllStatus", &dummy, &okay)
	if err != nil {
		t.Error("Error calling SourceControl.SendAllStatus():", err)
	}
	time.Sleep(time.Millisecond * 400)
	sizes := SizeObject{Nsamp: 800, Npre: 200}
	err = client.Call("SourceControl.ConfigurePulseLengths", &sizes, &okay)
	if err != nil {
		t.Logf(err.Error())
		t.Errorf("Error calling SourceControl.ConfigurePulseLengths(%v)", sizes)
	}
	if !okay {
		t.Errorf("SourceControl.ConfigurePulseLengths(%v) returns !okay, want okay", sizes)
	}
	err = client.Call("SourceControl.Stop", sourceName, &okay)
	if err != nil {
		t.Logf(err.Error())
		t.Errorf("Error calling SourceControl.Stop(%s)", sourceName)
	}
	if !okay {
		t.Errorf("SourceControl.Stop(\"%s\") returns !okay, want okay", sourceName)
	}
	err = client.Call("SourceControl.ConfigurePulseLengths", &sizes, &okay)
	if err == nil {
		t.Errorf("Expected error calling SourceControl.ConfigurePulseLengths(%v) when source stopped, saw none", sizes)
	}

	// Configure, start, and stop a triangle server
	tconfig := TriangleSourceConfig{
		Nchan:      4,
		SampleRate: 10000.0,
		Min:        10,
		Max:        1510,
	}
	err = client.Call("SourceControl.ConfigureTriangleSource", &tconfig, &okay)
	if !okay {
		t.Errorf("Error on server with SourceControl.ConfigureTriangleSource()")
	}
	if err != nil {
		t.Errorf("Error calling SourceControl.ConfigureTriangleSource(): %s", err.Error())
	}

	sourceName = "TriangleSource"
	err = client.Call("SourceControl.Start", &sourceName, &okay)
	if err != nil {
		t.Errorf("Error calling SourceControl.Start(%s): %s", sourceName, err.Error())
	}
	if !okay {
		t.Errorf("SourceControl.Start(\"%s\") returns !okay, want okay", sourceName)
	}
	time.Sleep(time.Millisecond * 400)
	rows := 5
	cols := 1000
	size := SizeObject{Nsamp: cols, Npre: cols / 4}
	err = client.Call("SourceControl.ConfigurePulseLengths", &size, &okay)
	if err != nil {
		t.Error(err)
	}
	projectors := mat.NewDense(rows, cols, make([]float64, rows*cols))
	basis := mat.NewDense(cols, rows, make([]float64, rows*cols))
	projectorsBytes, err := projectors.MarshalBinary()
	if err != nil {
		t.Error(err)
	}
	basisBytes, err := basis.MarshalBinary()
	if err != nil {
		t.Error(err)
	}
	pbo := ProjectorsBasisObject{ChannelIndex: 0,
		ProjectorsBase64: base64.StdEncoding.EncodeToString(projectorsBytes),
		BasisBase64:      base64.StdEncoding.EncodeToString(basisBytes)}

	err = client.Call("SourceControl.ConfigureProjectorsBasis", &pbo, &okay)
	if err != nil {
		t.Error(err)
	}
	if !okay {
		t.Errorf("SourceControl.ConfigureProjectorsBasis(\"%s\") returns !okay, want okay", sourceName)
	}
	mfo := MixFractionObject{[]int{0}, []float64{1.0}}
	if err1 := client.Call("SourceControl.ConfigureMixFraction", &mfo, &okay); err1 == nil {
		t.Error("error on ConfigureMixFraction expected for non-mixable source")
	}
	tstate := FullTriggerState{ChannelIndicies: []int{0, 1, 2}}
	if err1 := client.Call("SourceControl.ConfigureTriggers", &tstate, &okay); err1 != nil {
		t.Error("error on ConfigureTriggers:", err)
	}
	for _, state := range []bool{false, true} {
		if err1 := client.Call("SourceControl.CoupleFBToErr", &state, &okay); err1 == nil {
			t.Error("expected error on CoupleFBToErr when non-Lancero source is active")
		}
		if err1 := client.Call("SourceControl.CoupleErrToFB", &state, &okay); err1 == nil {
			t.Error("expected error on CoupleErrToFB when non-Lancero source is active")
		}
	}
	path, err := ioutil.TempDir("", "dastard_test")
	if err != nil {
		t.Fatal("Could not open temporary directory")
	}
	defer os.RemoveAll(path) // clean up test files
	wconfig := WriteControlConfig{Request: "Start", Path: path, WriteLJH22: true}
	// we currently have a 240 channel map loaded, but only have 4 channels, so this should error and invalidate the map file
	if err1 := client.Call("SourceControl.WriteControl", &wconfig, &okay); err1 == nil {
		t.Error("expected an error because we should have a 240 channel map loaded instead of a 4 channel map", err1)
	}
	if err1 := client.Call("SourceControl.WriteControl", &wconfig, &okay); err1 != nil {
		t.Error("map should have been invalidated, so this should work")
	}
	if err1 := client.Call("SourceControl.ConfigurePulseLengths", &sizes, &okay); err1 == nil {
		t.Errorf("Expected error calling SourceControl.ConfigurePulseLengths(%v) when writing active, saw none", sizes)
	}
	time.Sleep(150 * time.Millisecond)
	comment := "hello"
	if err1 := client.Call("SourceControl.WriteComment", &comment, &okay); err1 != nil {
		t.Error("SourceControl.WriteComment error while writing:", err1)
	}
	if true { // prevent variables from persisting
		var zero *int
		var reply *string
		if err1 := client.Call("SourceControl.ReadComment", &zero, &reply); err1 != nil {
			t.Error("SourceControl.ReadComment error:", err1)
		}
		if *reply != "hello\n" {
			t.Errorf("want %q, have %q", "hello\n", *reply)
		}
	}
	stateLabelArg := StateLabelConfig{Label: "testlabel", WaitForError: true}
	if err1 := client.Call("SourceControl.SetExperimentStateLabel", &stateLabelArg, &okay); err1 != nil {
		t.Error(err1)
	}
	wconfig.Request = "Stop"
	if err1 := client.Call("SourceControl.WriteControl", &wconfig, &okay); err1 != nil {
		t.Error("SourceControl.WriteControl STOP error:", err1)
	}
	// Check that comment.txt file exists and has a newline appended
	if true { // prevent variables from persisting
		date := time.Now().Format("20060102")
		fname := fmt.Sprintf("%s/%s/0000/comment.txt", path, date)
		file, err0 := os.Open(fname)
		defer file.Close()
		if err0 != nil {
			t.Errorf("Could not open comment file %q", fname)
		} else {
			b := make([]byte, 1+len(comment))
			_, err2 := file.Read(b)
			if err2 != nil {
				t.Error("file.Read failed on comment file", err2)
			} else if string(b) != "hello\n" {
				t.Errorf("comment.txt file contains %q, want %q", b, "hello\n")
			}
		}
	}
	// Check that experiment_state file exists
	if true { // prevent variables from persisting
		date := time.Now().Format("20060102")
		fname := fmt.Sprintf("%s/%s/0000/%s_run0000_experiment_state.txt", path, date, date)
		file, err0 := os.Open(fname)
		defer file.Close()
		if err0 != nil {
			t.Error(err0)
		}
	}
	if err1 := client.Call("SourceControl.WriteComment", &comment, &okay); err1 != nil {
		t.Error("SourceControl.WriteComment error after source stoped:", err1)
	}
	if err1 := client.Call("SourceControl.SetExperimentStateLabel", &stateLabelArg, &okay); err1 == nil {
		t.Error("expected error after STOP")
	}
	if err = client.Call("SourceControl.Stop", sourceName, &okay); err != nil {
		t.Errorf("Error calling SourceControl.Stop(%s)\n%v", sourceName, err)
	}
	if !okay {
		t.Errorf("SourceControl.Stop(\"%s\") returns !okay, want okay", sourceName)
	}

	// Make sure Nchan = 0 raises error when we try to configure
	simConfig.Nchan = 0
	err = client.Call("SourceControl.ConfigureSimPulseSource", &simConfig, &okay)
	if err == nil {
		t.Errorf("Expected error on server with SourceControl.ConfigureSimPulseSource() when Nchan<1, %t %v", okay, err)
	}
	tconfig.Nchan = 0
	err = client.Call("SourceControl.ConfigureTriangleSource", &tconfig, &okay)
	if err == nil {
		t.Errorf("Expected error on server with SourceControl.ConfigureTriangleSource() when Nchan<1")
	}

	// here test all methods that expect an active source to make sure they error appropriatley
	// otherwise you will get incomprehensible stack traces when they error unexpectedly
	if err1 := client.Call("SourceControl.ConfigureProjectorsBasis", &pbo, &okay); err1 == nil {
		t.Error("expected error on ConfigureProjectorsBasiswhen no source is active")
	}
	if err1 := client.Call("SourceControl.Stop", sourceName, &okay); err1 == nil {
		t.Errorf("expected error stopping source when no source is active")
	}
	if err1 := client.Call("SourceControl.ConfigureMixFraction", &mfo, &okay); err1 == nil {
		t.Error("expected error on ConfigureMixFraction when no source is active")
	}
	if err1 := client.Call("SourceControl.ConfigureTriggers", &tstate, &okay); err1 == nil {
		t.Error("expected error on ConfigureTriggers when no source is active")
	}
	for _, state := range []bool{false, true} {
		if err1 := client.Call("SourceControl.CoupleFBToErr", &state, &okay); err1 == nil {
			t.Error("expected error on CoupleFBToErr when no source is active")
		}
		if err1 := client.Call("SourceControl.CoupleErrToFB", &state, &okay); err1 == nil {
			t.Error("expected error on CoupleErrToFB when no source is active")
		}
	}

	// lancero source should fail
	sourceName = "LanceroSource"
	if err := client.Call("SourceControl.Start", &sourceName, &okay); err == nil {
		t.Error("expect PrepareRun could not run with 0 channels (expect > 0)")
	}
	if !okay {
		t.Errorf("SourceControl.Start(\"%s\") returns !okay, want okay", sourceName)
	}
}

// verifyConfigFile checks that path/filename exists, and creates the directory
// and file if it doesn't.
func verifyConfigFile(path, filename string) error {
	u, err := user.Current()
	if err != nil {
		return err
	}
	path = strings.Replace(path, "$HOME", u.HomeDir, 1)

	// Create directory <path>, if needed
	_, err = os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		err = os.MkdirAll(path, 0775)
		if err != nil {
			return err
		}
	}

	// Create an empty file path/filename, if it doesn't exist.
	fullname := fmt.Sprintf("%s/%s", path, filename)
	_, err = os.Stat(fullname)
	if os.IsNotExist(err) {
		f, err := os.OpenFile(fullname, os.O_WRONLY|os.O_CREATE, 0664)
		if err != nil {
			return err
		}
		f.Close()
	}
	return nil
}

// setupViper sets up the viper configuration manager: says where to find config
// files and the filename and suffix. Sets some defaults.
func setupViper() error {
	viper.SetDefault("Verbose", false)

	const path string = "$HOME/.dastard"
	const filename string = "testconfig"
	const suffix string = ".yaml"
	if err := verifyConfigFile(path, filename+suffix); err != nil {
		return err
	}

	viper.SetConfigName(filename)
	viper.AddConfigPath(path)
	viper.AddConfigPath(".")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		return fmt.Errorf("error reading config file: %s", err)
	}

	// Set up different ports for testing than you'd use otherwise
	setPortnumbers(33300)

	// Write output files in a temporary file
	ws := WritingState{BasePath: "/tmp"}
	viper.Set("writing", ws)

	// Check config saving.
	msg := make(map[string]interface{})
	msg["HarryPotter"] = harrypotter
	saveState(msg)
	return nil
}

func TestErroringSourceRPC(t *testing.T) {
	client, errClient := simpleClient()
	defer client.Close()
	if errClient != nil {
		t.Fatal(errClient)
	}
	sourceName := "ERRORINGSOURCE"
	dummy := ""
	okay := false
	for i := 0; i < 2; i++ {
		if err := client.Call("SourceControl.Start", &sourceName, &okay); err != nil {
			t.Error(err)
		}
		if err := client.Call("SourceControl.WaitForStopTestingOnly", &dummy, &okay); err != nil {
			t.Error(err)
		}
		if err := client.Call("SourceControl.Stop", &dummy, &okay); err == nil {
			t.Error("ErroringSource.Stop: expected error for stop because already waited for source to end")
		}
	}
}

func TestMain(m *testing.M) {
	// set log to write to a file
	f, err := os.Create("dastardtestlogfile")
	if err != nil {
		panic(fmt.Sprintf("error opening file: %v", err))
	}
	defer f.Close()
	log.SetOutput(f)
	lancero.SetLogOutput(f)

	// set global cringeGlobalsPath to point to test file
	cringeGlobalsPath = "lancero/test_data/cringeGlobals.json"

	// Find config file, creating it if needed, and read it.
	if err := setupViper(); err != nil {
		panic(err)
	}

	abort := make(chan struct{})
	go RunClientUpdater(Ports.Status, abort)
	RunRPCServer(Ports.RPC, false)

	// run tests and wrap up
	result := m.Run()

	// Prevent zmq "dangling 'PUB' socket..." errors by closing the channels that
	// will cause the zmq sockets to be destroyed.
	if PubRecordsChan != nil {
		close(PubRecordsChan)
	}
	if PubSummariesChan != nil {
		close(PubSummariesChan)
	}
	close(abort)
	os.Exit(result)
}

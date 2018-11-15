package dastard

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"gonum.org/v1/gonum/mat"
)

func TestChannelNames(t *testing.T) {
	ds := AnySource{nchan: 4}
	ds.setDefaultChannelNames()
	expect := []string{"chan0", "chan1", "chan2", "chan3"}
	if len(ds.chanNames) != 4 {
		t.Errorf("ds.chanNames length = %d, want 4", len(ds.chanNames))
	} else {
		for i, n := range ds.chanNames {
			if n != expect[i] {
				t.Errorf("ds.chanNames[%d]=%q, want %q", i, n, expect[i])
			}
		}
	}

	row := 3
	col := 4
	nrows := 5
	ncols := 10
	code := rcCode(row, col, nrows, ncols)
	if code.row() != row {
		t.Errorf("rcCode(%d,%d,%d,%d).row() = %d, want %d", row, col, nrows, ncols, code.row(), row)
	}
	if code.col() != col {
		t.Errorf("rcCode(%d,%d,%d,%d).col() = %d, want %d", row, col, nrows, ncols, code.col(), col)
	}
	if code.rows() != nrows {
		t.Errorf("rcCode(%d,%d,%d,%d).rows() = %d, want %d", row, col, nrows, ncols, code.rows(), nrows)
	}
	if code.cols() != ncols {
		t.Errorf("rcCode(%d,%d,%d,%d).cols() = %d, want %d", row, col, nrows, ncols, code.cols(), ncols)
	}
}

func TestWritingFiles(t *testing.T) {
	tmp, err1 := ioutil.TempDir("", "dastardTest")
	if err1 != nil {
		t.Errorf("could not make TempDir")
		return
	}
	defer os.RemoveAll(tmp)

	dir, err2 := makeDirectory(tmp)
	if err2 != nil {
		t.Error(err2)
	} else if !strings.HasPrefix(dir, tmp) {
		t.Errorf("Writing in path %s, which should be a prefix of %s", tmp, dir)
	}
	dir2, err2 := makeDirectory(tmp)
	if err2 != nil {
		t.Error(err2)
	} else if !strings.HasPrefix(dir2, tmp) {
		t.Errorf("Writing in path %s, which should be a prefix of %s", tmp, dir2)
	} else if !strings.HasSuffix(dir2, "run0001_%s.%s") {
		t.Errorf("makeDirectory produces %s, of which %q should be a suffix", dir2, "run0001_%s.%s")
	}

	if _, err := makeDirectory("/notallowed"); err == nil {
		t.Errorf("makeDirectory(%s) should have failed", "/notallowed")
	}

	ds := AnySource{nchan: 4}
	ds.rowColCodes = make([]RowColCode, ds.nchan)
	ds.PrepareRun(256, 1024)
	defer ds.Stop()
	config := &WriteControlConfig{Request: "Pause", Path: tmp, WriteLJH22: true}
	for _, request := range []string{"Pause", "Unpause", "Stop"} {
		config.Request = request
		if err := ds.WriteControl(config); err != nil {
			t.Errorf("WriteControl request %s failed on a non-writing file: %v", request, err)
		}
	}
	config.Request = "notvalid"
	if err := ds.WriteControl(config); err == nil {
		t.Errorf("WriteControl request %s should fail, but didn't", config.Request)
	}
	config.Request = "Start"
	config.WriteLJH22 = false
	if err := ds.WriteControl(config); err == nil {
		t.Errorf("WriteControl request Start with no valid filetype should fail, but didn't")
	}
	config.WriteLJH22 = true
	config.Path = "/notvalid/because/permissions"
	if err := ds.WriteControl(config); err == nil {
		t.Errorf("WriteControl request Start with nonvalid path should fail, but didn't")
	}

	config.Path = tmp
	if err := ds.WriteControl(config); err != nil {
		t.Errorf("WriteControl request %s failed: %v", config.Request, err)
	}
	for _, request := range []string{"Pause", "Unpause", "Stop"} {
		config.Request = request
		if err := ds.WriteControl(config); err != nil {
			t.Errorf("WriteControl request %s failed on a writing file: %v", request, err)
		}
	}
	// set projectors so that we can use WriterOFF = true
	config.WriteOFF = true
	config.Request = "start"
	if err := ds.WriteControl(config); err == nil {
		t.Errorf("expected error for asking to WriteOFF with no projectors set\n%v", config.Request)
	}
	nbases := 1
	nsamples := 1024
	projectors := mat.NewDense(nbases, nsamples, make([]float64, nbases*nsamples))
	basis := mat.NewDense(nsamples, nbases, make([]float64, nbases*nsamples))
	if err1 := ds.processors[0].SetProjectorsBasis(*projectors, *basis, "test model"); err1 != nil {
		t.Error(err1)
	}
	config.Request = "Start"
	config.WriteLJH22 = true
	config.WriteOFF = true
	config.WriteLJH3 = true
	if err := ds.WriteControl(config); err != nil {
		t.Errorf("%v\n%v", err, config.Request)
	}
	if !ds.processors[0].DataPublisher.HasLJH22() {
		t.Error("WriteLJH22 did not result in HasLJH22")
	}
	if !ds.processors[0].DataPublisher.HasOFF() {
		t.Error("WriteOFF did not result in HasOFF")
	}
	if ds.processors[1].DataPublisher.HasOFF() {
		t.Error("WriteOFF resulting in HasOFF for a channel without projectors")
	}
	if !ds.processors[0].DataPublisher.HasLJH3() {
		t.Error("WriteLJH3 did not result in HasLJH3")
	}
	config.Request = "PAUSE"
	if err := ds.WriteControl(config); err != nil {
		t.Errorf("%v\n%v", err, config.Request)
	}
	config.Request = "UnPAUSE "
	if err := ds.WriteControl(config); err == nil {
		t.Errorf("expected error for length==8, %v", config.Request)
	}
	config.Request = "UnPAUSEZZZZ"
	if err := ds.WriteControl(config); err == nil {
		t.Errorf("expected error for 8th character not equal to a space, %v", config.Request)
	}
	config.Request = "UnPAUSE AQ7"
	if err := ds.WriteControl(config); err != nil {
		t.Error(err)
	}
	experimentStateFilename := ds.writingState.ExperimentStateFilename
	config.Request = "Stop"
	if err := ds.WriteControl(config); err != nil {
		t.Errorf("%v\n%v", err, config.Request)
	}
	if ds.processors[0].DataPublisher.HasLJH22() {
		t.Error("Stop did not result in !HasLJH22")
	}
	if ds.processors[0].DataPublisher.HasOFF() {
		t.Error("Stop did not result in !HasOFF")
	}
	if ds.processors[0].DataPublisher.HasLJH3() {
		t.Error("Stop did not result in !HasLJH3")
	}
	if true { // prevent variable from persisting
		fileContents, err2 := ioutil.ReadFile(experimentStateFilename)
		fileContentsStr := string(fileContents)
		if err2 != nil {
			t.Error(err2)
		}
		expectFileContentsStr := "# unix time in nanoseconds, state label\n1538424162462127037, START\n1538174046828690465, AQ7\n1538424428433771969, STOP\n"
		if !strings.HasPrefix(fileContentsStr, "# unix time in nanoseconds, state label\n") ||
			!strings.Contains(fileContentsStr, ", START\n") ||
			!strings.Contains(fileContentsStr, ", AQ7\n") ||
			!strings.HasSuffix(fileContentsStr, ", STOP\n") ||
			len(expectFileContentsStr) != len(fileContentsStr) {
			t.Errorf("have\n%v\nwant (except timestamps should disagree)\n%v\n", fileContentsStr, expectFileContentsStr)
		}
	}
}

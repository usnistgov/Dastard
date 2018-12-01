package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"time"
)

const flushWithinBlock = true
const flushAfterBlocks = true

type Writer struct {
	FileName      string
	headerWritten bool
	writer        *bufio.Writer
}

func (w *Writer) writeHeader() error {
	file, err := os.Create(w.FileName)
	if err != nil {
		return err
	}
	w.writer = bufio.NewWriterSize(file, 32768)
	w.writer.WriteString("HEADER\n")
	w.headerWritten = true
	return nil
}

func (w *Writer) writeRecord(nBytes int) error {
	data := make([]byte, nBytes)
	nWritten, err := w.writer.Write(data)
	if nWritten != nBytes {
		return fmt.Errorf("wrong number of bytes written")
	}
	return err
}

func main() {
	dirname, err0 := ioutil.TempDir("", "")
	if err0 != nil {
		panic(err0)
	}
	fmt.Println(dirname)
	recordLength := 500
	numberOfChannels := 240
	recordsPerChanPerTick := 5
	writers := make([]*Writer, numberOfChannels)
	abortChan := make(chan struct{})
	for i := range writers {
		writers[i] = &Writer{FileName: fmt.Sprintf("%v/%v.ljh", dirname, i)}
	}
	go func() {
		signalChan := make(chan os.Signal)
		signal.Notify(signalChan, os.Interrupt)
		<-signalChan
		close(abortChan)
	}()

	tickDuration := 50 * time.Millisecond
	ticker := time.NewTicker(tickDuration)
	z := 0
	tLast := time.Now()
	fmt.Printf("recordsPerChanPerTick %v, Chans %v, tickDuration %v\n", recordsPerChanPerTick, numberOfChannels, tickDuration)
	fmt.Printf("records/second/chan %v, records/second total %v\n", float64(recordsPerChanPerTick)/tickDuration.Seconds(), float64(recordsPerChanPerTick*numberOfChannels)/tickDuration.Seconds())
	fmt.Printf("megabytes/second total %v\n", float64(recordLength)*float64(recordsPerChanPerTick*numberOfChannels)/tickDuration.Seconds()*1e-6)
	fmt.Printf("flushWithinBlock %v, flushAfterBlocks %v\n", flushWithinBlock, flushAfterBlocks)
	for {
		z++
		// 1. here we would get data from data source
		select {
		case <-abortChan:
			fmt.Println("clean exit")
			return
		case <-ticker.C:
			var wg sync.WaitGroup
			writeDurations := make([]time.Duration, numberOfChannels)
			flushDurations := make([]time.Duration, numberOfChannels)
			for i, w := range writers {
				wg.Add(1)
				go func(w *Writer, i int) {
					// 2. here we would process data, we launch one goroutine per channel to parallelize this processing
					tStart := time.Now()
					defer wg.Done()
					// 3. here we write data to disk, still within the same goroutines that did the processing
					for j := 0; j < recordsPerChanPerTick; j++ {
						if !w.headerWritten {
							err := w.writeHeader()
							if err != nil {
								panic(fmt.Sprintf("failed create file and write header: %v\n", err))
							}
						}
						w.writeRecord(recordLength)
					}
					tWrite := time.Now()
					if flushWithinBlock {
						w.writer.Flush()
					}
					writeDurations[i] = tWrite.Sub(tStart)
					flushDurations[i] = time.Now().Sub(tWrite)
				}(w, i)
			}
			wg.Wait()
			for _, w := range writers {
				if flushAfterBlocks {
					w.writer.Flush()
				}
			}
			var writeSum time.Duration
			var flushSum time.Duration
			var writeMax time.Duration
			var flushMax time.Duration
			for i := range writeDurations {
				writeSum += writeDurations[i]
				flushSum += flushDurations[i]
				if writeDurations[i] > writeMax {
					writeMax = writeDurations[i]
				}
				if flushDurations[i] > flushMax {
					flushMax = flushDurations[i]
				}
			}
			if z%100 == 0 || time.Now().Sub(tLast) > 75*time.Millisecond {
				fmt.Printf("z %v, time.Now().Sub(tLast) %v\n", z, time.Now().Sub(tLast))
				fmt.Printf("writeMean %v, flushMean %v, writeMax %v, flushMax %v\n", writeSum/time.Duration(numberOfChannels), flushSum/time.Duration(numberOfChannels), writeMax, flushMax)
			}
			tLast = time.Now()
		}
	}

}

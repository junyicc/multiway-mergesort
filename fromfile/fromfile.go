package fromfile

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"sort"
	"time"

	"github.com/junyicc/multiway-mergesort/model"
	"github.com/junyicc/multiway-mergesort/pkg/gen"
)

const (
	MaxRecordSize = 16000000
	Partitions    = 20
)

var BlockSize = MaxRecordSize / Partitions

func MultiMergeSortFromFile() {
	// generate random records in one file
	fpath := `/home/junyi/workspace/data/multiway-mergesort/source.txt`
	outpath := `/home/junyi/workspace/data/multiway-mergesort/result.txt`

	genStartTime := time.Now()
	err := gen.RandomFile(fpath, MaxRecordSize)
	if err != nil {
		log.Panicf("failed to generate source file, err: %v\n", err)
	}
	log.Printf("generating random file takes %d ms", time.Since(genStartTime).Milliseconds())

	// phase 1
	phase1StartTime := time.Now()
	sortedFiles, err := phase1(fpath)
	if err != nil {
		log.Panicf("failed at phase 1: %v\n", err)
	}
	log.Printf("phase1 takes %d ms", time.Since(phase1StartTime).Milliseconds())

	// phase 2
	phase2StartTime := time.Now()
	err = phase2(sortedFiles, outpath)
	if err != nil {
		log.Panicf("failed at phase 1: %v\n", err)
	}
	log.Printf("phase2 takes %d ms", time.Since(phase2StartTime).Milliseconds())

	log.Printf("successfully sorted big file: %s -> %s\n", fpath, outpath)
}

// phase1 separates one unsorted big file into several sorted pieces
// and return sorted file path
func phase1(fpath string) ([]string, error) {
	// open file
	f, err := os.Open(fpath)
	if err != nil {
		log.Println("failed to open records file in phase 1")
		return nil, err
	}
	// read file with block size
	scanner := bufio.NewScanner(f)
	sortedFiles := make([]string, 0, Partitions)
	var k int
	for ; k < Partitions; k++ {
		records := make(model.Records, 0, BlockSize)
		var i int
		for scanner.Scan() {
			b := scanner.Bytes()
			var record model.Record
			_ = json.Unmarshal(b, &record)
			records = append(records, record)
			i++

			if i >= BlockSize {
				break
			}
		}
		if len(records) > 0 {
			// sort block-size records
			sort.Sort(&records)

			// write sorted records to disk
			subFilename := gen.GenSubFilename(fpath, k)
			sortedFiles = append(sortedFiles, subFilename)
			if err := records.FlushToDisk(subFilename); err != nil {
				log.Printf("failed to flush sub records %d", k)
				return nil, err
			}
		}
	}
	// remaining records
	remainingRecords := make(model.Records, 0, BlockSize)
	for scanner.Scan() {
		b := scanner.Bytes()
		var record model.Record
		_ = json.Unmarshal(b, &record)
		remainingRecords = append(remainingRecords, record)
	}
	if len(remainingRecords) > 0 {
		// sort block-size records
		sort.Sort(&remainingRecords)
		// write sorted records to disk
		subFilename := gen.GenSubFilename(fpath, k)
		sortedFiles = append(sortedFiles, subFilename)
		if err := remainingRecords.FlushToDisk(subFilename); err != nil {
			log.Printf("failed to flush sub records %d", k)
			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("scan record files error")
		return nil, err
	}

	return sortedFiles, nil
}

type sortedInput struct {
	fpath string
	ch    chan []byte
}

func phase2(sortedFiles []string, outpath string) error {
	if len(sortedFiles) < 1 {
		return fmt.Errorf("empty sorted files")
	}
	// create output file
	outF, err := os.Create(outpath)
	if err != nil {
		log.Printf("failed to create result file: %s\n", outpath)
		return err
	}
	defer outF.Close()

	n := len(sortedFiles)
	outWriter := bufio.NewWriterSize(outF, n)
	// bind every sub-record file with chan
	sortedInputs := make([]sortedInput, 0, n)
	for _, fpath := range sortedFiles {
		input := sortedInput{
			fpath: fpath,
			ch:    make(chan []byte),
		}
		sortedInputs = append(sortedInputs, input)
	}
	// open every chan
	for _, input := range sortedInputs {
		go func(input sortedInput) {
			f, err := os.Open(input.fpath)
			if err != nil {
				log.Printf("failed to open %s\n", err)
				return
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				a := scanner.Bytes()
				// copy before transfer
				b := append(a[:0:0], a...)
				input.ch <- b
			}
			close(input.ch)
		}(input)
	}
	// read the first record from every sub-record file
	inputBuffer := make(model.Records, n)
	// count of the chan finish reading
	finishedRead := 0
	// invalid record with max int64 key
	invalidRecord := model.Record{
		Key:   math.MaxInt64,
		Value: nil,
	}
	for i := 0; i < n; i++ {
		b, ok := <-sortedInputs[i].ch
		if !ok {
			// one input chan finished reading and closed
			finishedRead++
			inputBuffer[i] = invalidRecord
			continue
		}

		// parse record
		var record model.Record
		err := json.Unmarshal(b, &record)
		if err != nil {
			log.Printf("fail to parse %d sorted input: %s\n", i, b)
			inputBuffer[i] = invalidRecord
			continue
		}
		// put sorted input into input buffer
		inputBuffer[i] = record
	}

	outputBuffer := make(model.Records, 0, n)
	// count of total output records
	totalOutput := 0
	for finishedRead < n {
		// if output buffer is full, flush to disk
		if len(outputBuffer) >= n {
			// flush to disk
			if err := flushOutputBuffer(outWriter, outputBuffer); err != nil {
				log.Printf("fail to flush output buffer: %v\n", err)
			}
			totalOutput += len(outputBuffer)
			// renew outputBuffer
			outputBuffer = make(model.Records, 0, n)
		}

		// select min record
		minIndex := selectMinRecord(inputBuffer)

		// write the minimum record to output buffer, record i chan
		outputBuffer = append(outputBuffer, inputBuffer[minIndex])

		// get the new record from minIndex chan
		b, ok := <-sortedInputs[minIndex].ch
		if !ok {
			// one input chan finished reading and closed
			finishedRead++
			inputBuffer[minIndex] = invalidRecord
		} else {
			// parse record
			var record model.Record
			err := json.Unmarshal(b, &record)
			if err != nil {
				log.Printf("fail to parse %d sorted input: %s\n", minIndex, b)
				inputBuffer[minIndex] = invalidRecord
			} else {
				// put sorted input into input buffer
				inputBuffer[minIndex] = record
			}
		}
	}
	// process the remaining records
	sort.Sort(inputBuffer)
	for _, record := range inputBuffer {
		if reflect.DeepEqual(record, invalidRecord) {
			continue
		}
		outputBuffer = append(outputBuffer, record)
	}
	// flush output buffer
	err = flushOutputBuffer(outWriter, outputBuffer)
	if err != nil {
		log.Printf("fail to flush the last output buffer: %v\n", err)
		return err
	}
	totalOutput += len(outputBuffer)

	if totalOutput != MaxRecordSize {
		return fmt.Errorf("fail to sort all records, sorted records: %d, all records: %d\n", totalOutput, MaxRecordSize)
	}
	return nil
}

func selectMinRecord(records model.Records) int {
	min := int64(math.MaxInt64)
	minIndex := 0
	for i, record := range records {
		if record.Key < min {
			min = record.Key
			minIndex = i
		}
	}
	return minIndex
}

func flushOutputBuffer(w *bufio.Writer, records model.Records) error {
	for _, record := range records {
		b, _ := json.Marshal(record)
		b = append(b, '\n')
		_, err := w.Write(b)
		if err != nil {
			return err
		}
	}
	return w.Flush()
}

package inmemory

import (
	"log"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/junyicc/multiway-mergesort/model"
	"github.com/junyicc/multiway-mergesort/pkg/gen"
)

const (
	MaxRecordSize = 16000000
	Partitions    = 40
)

var BlockSize = MaxRecordSize / Partitions

func MultiMergeSortInMemory() {
	startTime := time.Now()
	// generate random records
	records := gen.RandomRecords(MaxRecordSize)

	// phase1 splits records into pieces
	chans := phase1(records)

	res := phase2(chans)

	if len(records) == len(res) {
		log.Println("successfully sort")
	} else {
		log.Panicln("failed to sort")
	}
	log.Printf("takes %d ms\n", time.Since(startTime).Milliseconds())
}

func phase1(records model.Records) []chan model.Record {
	if len(records) < 1 {
		return nil
	}

	chans := make([]chan model.Record, 0, Partitions)
	var wg sync.WaitGroup
	for i := 0; i < Partitions; i++ {
		wg.Add(1)
		go func(i int) {
			ch := make(chan model.Record)
			defer close(ch)

			// add channel
			chans = append(chans, ch)
			wg.Done()

			var piece model.Records
			if i == Partitions-1 {
				piece = records[i*BlockSize:]
			} else {
				piece = records[i*BlockSize : (i+1)*BlockSize]
			}
			if len(piece) > 0 {
				sort.Sort(piece)
				for _, record := range piece {
					ch <- record
				}
			}
		}(i)
	}
	wg.Wait()
	return chans
}

func phase2(chans []chan model.Record) model.Records {
	if len(chans) < 1 {
		return nil
	}
	n := len(chans)
	buffer := make(model.Records, n, n)
	res := make(model.Records, 0, MaxRecordSize)
	finishedChCnt := 0
	// invalid record with max int64 key
	invalidRecord := model.Record{
		Key:   math.MaxInt64,
		Value: nil,
	}
	// read records from chans, and put into buffer
	for i := 0; i < n; i++ {
		record, ok := <-chans[i]
		if !ok {
			finishedChCnt++
			buffer[i] = invalidRecord
		} else {
			buffer[i] = record
		}
	}

	for finishedChCnt < n {
		// select index of min record
		minIndex := selectMinRecord(buffer)
		// append to result
		res = append(res, buffer[minIndex])
		// renew buffer
		record, ok := <-chans[minIndex]
		if !ok {
			finishedChCnt++
			buffer[minIndex] = invalidRecord
		} else {
			buffer[minIndex] = record
		}
	}
	sort.Sort(buffer)
	for _, record := range buffer {
		if reflect.DeepEqual(record, invalidRecord) {
			continue
		}
		res = append(res, record)
	}
	return res
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

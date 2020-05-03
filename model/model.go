package model

import (
	"bufio"
	"encoding/json"
	"os"
	"path"

	"github.com/junyicc/multiway-mergesort/pkg/file"
)

type Record struct {
	Key   int64       `json:"key"`
	Value interface{} `json:"value"`
}

type Records []Record

func (rs Records) Len() int {
	return len(rs)
}

func (rs Records) Less(i, j int) bool {
	return rs[i].Key < rs[j].Key
}

func (rs Records) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

func (rs Records) FlushToDisk(fpath string) error {
	fdir := path.Dir(fpath)
	if !file.Exists(fdir) {
		if err := os.MkdirAll(fdir, os.ModePerm); err != nil {
			return err
		}
	}
	f, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer f.Close()
	// write to file
	w := bufio.NewWriterSize(f, 1024*10)
	for i := 0; i < len(rs); i++ {
		b, _ := json.Marshal(rs[i])
		b = append(b, '\n')
		_, err = w.Write(b)
		if err != nil {
			return err
		}
	}
	return w.Flush()
}

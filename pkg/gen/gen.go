package gen

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/junyicc/multiway-mergesort/model"

	"github.com/junyicc/multiway-mergesort/pkg/file"
)

var src = rand.NewSource(time.Now().UnixNano())

func GenRandomFile(fpath string, size int) error {
	fdir := filepath.Dir(fpath)
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
	w := bufio.NewWriterSize(f, 1024*100)
	for i := 0; i < size; i++ {
		record := genRandomRecord()
		b, _ := json.Marshal(record)
		b = append(b, '\n')
		_, err = w.Write(b)
		if err != nil {
			return err
		}
	}
	return w.Flush()
}

func genRandomRecord() model.Record {
	return model.Record{
		Key:   genRandomNum(),
		Value: "0",
	}
}

func genRandomNum() int64 {
	return src.Int63()
}

func GenSubFilename(fpath string, i int) string {
	fdir := filepath.Dir(fpath)
	return filepath.Join(fdir, fmt.Sprintf("sub-%d.txt", i))
}

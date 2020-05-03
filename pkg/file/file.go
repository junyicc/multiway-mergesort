package file

import "os"

func Exists(fpath string) bool {
	_, err := os.Stat(fpath)
	if err == nil {
		return true
	}
	return os.IsExist(err)
}

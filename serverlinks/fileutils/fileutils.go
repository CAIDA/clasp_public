package fileutils

import (
	"path/filepath"
	"sort"
)

type ParseFileTsFunc func(string) int64

func SortResultFiles(dir string, tsfunc ParseFileTsFunc, asc int) ([]string, error) {
	//	resultfiles, err := ioutil.ReadDir(dir)
	resultfiles, err := filepath.Glob(filepath.Join(dir, "*.tar.bz2"))
	if err != nil {
		return nil, err
	}
	if len(resultfiles) == 0 {
		return resultfiles, nil
	}
	sort.Slice(resultfiles, func(i, j int) bool {
		its := tsfunc(filepath.Base(resultfiles[i]))
		jts := tsfunc(filepath.Base(resultfiles[j]))
		if asc == 0 {
			return jts > its
		} else {
			return its > jts
		}
	})
	return resultfiles, nil
}

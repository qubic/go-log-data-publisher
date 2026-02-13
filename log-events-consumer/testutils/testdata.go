package testutils

import (
	"embed"
	"io/fs"
)

//go:embed testdata/kafka/*.json testdata/elastic/*.json testdata/filtered/*.json
var testData embed.FS

func ReadTestFile(path string) ([]byte, error) {
	return testData.ReadFile(path)
}

func ReadDir(path string) ([]fs.DirEntry, error) {
	return testData.ReadDir(path)
}

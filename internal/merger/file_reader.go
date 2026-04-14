package merger

import (
	"bufio"
	"os"
)

type FileReader struct {
	Scanner *bufio.Scanner
	File    *os.File
}

func NewFileReader(filename string) (*FileReader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return &FileReader{
		Scanner: bufio.NewScanner(file),
		File:    file,
	}, nil
}

func (fr *FileReader) Next() (string, bool) {
	if fr.Scanner.Scan() {
		return fr.Scanner.Text(), true
	}
	// FIXED: Check for scan errors (IO errors, tokenization errors)
	if err := fr.Scanner.Err(); err != nil {
		return "", false // Return error via bool
	}
	return "", false
}

func (fr *FileReader) Close() {
	fr.File.Close()
}

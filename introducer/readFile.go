package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type FileReadService struct {
}
type ReadFileArgs struct {
	HyDFSFileName string
	LocalFileName string
	Addr          string
}

// Response for checking and writing file
type CheckAndWriteResponse struct {
	Success bool   // Whether the operation was successful
	Error   string // Error message, if any
}

// Response for reading file
type CheckAndReadFileResponse struct {
	Success bool   // Whether the operation was successful
	Error   string // Error message, if any
}

func getAllChunkNames(fileName string) []string {
	files, err := os.ReadDir("./hydfs/")
	results := make([]string, 0)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return nil
	}
	filenameWithoutExt := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	for _, file := range files {
		if strings.HasPrefix("./hydfs/"+file.Name(), filenameWithoutExt) {
			results = append(results, "./hydfs/"+file.Name())
		}
	}
	return results
}

func (f *FileReadService) CheckAndReadFile(args ReadFileArgs, res *CheckAndReadFileResponse) error {
	sourceFileName := args.HyDFSFileName
	destFileName := args.LocalFileName
	addr := args.Addr

	// fmt.Printf("Reading file %s\n", sourceFileName)

	chunkFiles := getAllChunkNames(sourceFileName)

	if len(chunkFiles) == 0 {
		res.Success = false
		res.Error = "No chunks found for file"
		return nil
	}

	for _, chunkFile := range chunkFiles {
		err := writeInAppendModeOnServer(chunkFile, destFileName, addr, 0)
		if err != nil {
			res.Success = false
			res.Error = err.Error()
			return nil
		}
	}

	res.Success = true
	return nil
}

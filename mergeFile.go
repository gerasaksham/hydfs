package main

import (
	"bufio"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type FileMergeService struct {
	mergeMu sync.Mutex // Mutex to prevent concurrent writes
}

// Arguments for file writing
type FileMergeArgs struct {
	FileName string // The name of the file to be written
}

// Response for the write operation
type FileMergeResponse struct {
	Success bool   // Whether the write was successful
	Error   string // Error message, if any

}

func mergeFileOnServer(filename, server string) error {

	client, err := rpc.Dial("tcp", makeAddrPort(server, 8000))
	if err != nil {
		return err
	}
	defer client.Close()

	args := FileMergeArgs{
		FileName: filename,
	}

	var res FileMergeResponse
	err = client.Call("FileMergeService.MergeFile", args, &res)
	if err != nil {
		fmt.Printf("RPC error in mergeFileOnServer: %v\n", err)
		return err
	}

	if res.Success {
		fmt.Println("File successfully merged on server")
	} else {
		fmt.Println("Error merging file on server:", res.Error)
	}

	return nil
}

func (s *FileMergeService) MergeFile(args FileMergeArgs, res *FileMergeResponse) error {
	startTime := time.Now()
	fmt.Printf("Start time now: %v\n", startTime)

	baseFileName := strings.TrimSuffix(args.FileName, ".txt")

	files, err := filepath.Glob(baseFileName + "*")
	if err != nil {
		res.Success = false
		fmt.Printf("Error finding files: %v\n", err)
		res.Error = fmt.Sprintf("Error finding files: %v", err)
		return err
	}

	s.mergeMu.Lock()
	defer s.mergeMu.Unlock()

	// Sort the files in ascending order
	sort.Strings(files)

	// Create the output file in append mode
	outputFile, err := os.OpenFile(files[0], os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		res.Success = false
		fmt.Printf("Error opening output file: %v\n", err)
		res.Error = err.Error()
		return err
	}
	defer outputFile.Close()

	// Merge the sorted files into the output file
	for i := 1; i < len(files); i++ {
		err := appendFileContents(outputFile, files[i])
		if err != nil {
			res.Success = false
			fmt.Printf("Error merging file %s: %v\n", files[i], err)
			res.Error = fmt.Sprintf("Error merging file %s: %v", files[i], err)
			return err
		} else {
			// Remove the merged file
			err := os.Remove(files[i])
			if err != nil {
				res.Success = false
				fmt.Printf("Error removing file %s: %v\n", files[i], err)
				res.Error = fmt.Sprintf("Error removing file %s: %v", files[i], err)
				return err
			}
			delete(fileMap, files[i])
		}
	}

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	fmt.Printf("End time now: %v\n", endTime)
	fmt.Printf("Time taken to merge files: %v\n", elapsedTime)

	res.Success = true
	return nil
}

func appendFileContents(outputFile *os.File, inputFile string) error {
	inFile, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file %s: %w", inputFile, err)
	}
	defer inFile.Close()

	// Use buffered readers and writers for potentially better I/O performance
	bufferedIn := bufio.NewReader(inFile)
	bufferedOut := bufio.NewWriter(outputFile)
	defer bufferedOut.Flush() // Ensure the buffer is fully written out

	_, err = io.Copy(bufferedOut, bufferedIn)
	if err != nil {
		fmt.Printf("Error copying contents from file %s: %v\n", inputFile, err)
		return fmt.Errorf("failed to copy contents from file %s: %w", inputFile, err)
	}

	return nil
}

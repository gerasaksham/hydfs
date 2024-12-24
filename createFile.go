package main

import (
	"fmt"
	"net/rpc"
	"os"
	"sync"
)

// Define a struct for the service
type FileWriteService struct {
	writeMu sync.Mutex // Mutex to prevent concurrent writes
}

// Arguments for file writing
type FileWriteArgs struct {
	FileName      string // The name of the file to be written
	Data          []byte // Chunk of file data
	Offset        int64  // Offset to write at
	OperationFlag int    // 0 for reads, 1 for primary write, 2 for replica write
	isAppend      bool
}

// Response for the write operation
type FileWriteResponse struct {
	Success bool   // Whether the write was successful
	Error   string // Error message, if any
	File    string // File name
}

// func getFileNameFromPath(path string) string {
// 	// Get the last part of the path
// 	_, fileName := filepath.Split(path)
// 	return fileName
// }

// Operation flag is 0 for reads, 1 for primary write, 2 for replica write
func writeFileOnServer(sourceFileName string, destinationFileName string, destinationServer string, operationFlag int) error {
	client, err := rpc.Dial("tcp", makeAddrPort(destinationServer, 8000))
	// fmt.Printf("Dialing server %s\n", makeAddrPort(destinationServer, 8000))
	if err != nil {
		fmt.Printf("Error dialing server: %v\n", err)
		return err
	}
	defer client.Close()

	// fmt.Printf("Writing file %s to server %s\n", sourceFileName, destinationServer)

	inFile, err := os.Open(sourceFileName)
	if err != nil {
		fmt.Printf("Error opening source file: %v\n", err)
		return err
	}
	defer inFile.Close()

	const chunkSize = 1024
	var offset int64 = 0

	buff := make([]byte, chunkSize)
	var res FileWriteResponse

	for {

		n, err := inFile.Read(buff)
		if err != nil && err.Error() != "EOF" {
			fmt.Println("File read error:", err)
			break
		}
		if n == 0 {
			// fmt.Println("File read from local complete")
			break
		}
		isAppend := true
		if destinationFileName[len(destinationFileName)-6:] == "_0.txt" {
			isAppend = false
		}

		args := FileWriteArgs{
			FileName:      destinationFileName,
			Data:          buff[:n],
			Offset:        offset,
			OperationFlag: operationFlag,
			isAppend:      isAppend,
		}

		// fmt.Printf("Writing chunk of size %d at offset %d\n", n, offset)

		err = client.Call("FileWriteService.WriteFileChunk", args, &res)
		if err != nil {
			fmt.Println("RPC error:", err)
			return err
		}
		if !res.Success {
			fmt.Printf("Server error: %s\n", res.Error)
			return err
		}

		// Update the offset for the next chunk
		offset += int64(n)
	}
	fmt.Printf("File %s written to server %s\n", sourceFileName, destinationServer)

	return nil
}

// Method to write a chunk of file data
func (s *FileWriteService) WriteFileChunk(args FileWriteArgs, res *FileWriteResponse) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// fmt.Println("Writing to file", args.FileName, "at offset", args.Offset)

	// fmt.Println("File map:", fileMap)
	// fmt.Println("File name:", args.FileName)
	// fmt.Println("arg filename: ", args.FileName)
	if _, ok := fileMap[args.FileName]; !ok {
		if args.OperationFlag != 0 {
			fileMap[args.FileName] = args.OperationFlag
		}
	}
	filename := args.FileName
	// check if the filename contains the string "_0.txt"
	if filename[len(filename)-6:] == "_0.txt" && args.OperationFlag != 0 {
		fileCounter[filename] = 0
	}

	// Check if the file already exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		// fmt.Println("File does not exist, creating new file")
	} else if err != nil {
		res.Success = false
		res.Error = err.Error()
		return err
	} else if args.Offset == 0 && args.isAppend {
		res.Success = false
		res.Error = "File already exists"
		return fmt.Errorf("file already exists")
	}

	// Open the file in append mode (create if not exists)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		res.Success = false
		res.Error = err.Error()
		return err
	}
	defer file.Close()

	// Write the chunk at the specified offset
	_, err = file.WriteAt(args.Data, args.Offset)
	if err != nil {
		res.Success = false
		res.Error = err.Error()
		return err
	}

	// fmt.Printf("args data length: %d\n", len(args.Data))

	// Set success to true
	res.Success = true
	return nil
}

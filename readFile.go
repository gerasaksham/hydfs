package main

import (
	"fmt"
	"net/rpc"
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

func readFileFromServer(fileName string, localFileName string) error {

	fileHash := HashString(fileName)
	serverNode, err := findServerGivenFileHash(fileHash)
	// fmt.Printf("Server node: %v\n", serverNode)
	if err != nil {
		fmt.Println("Error finding server:", err)
		return err
	}
	client, err := rpc.Dial("tcp", makeAddrPort(serverNode.Name, 8000))
	if err != nil {
		fmt.Println("Error dialing server:", err)
		return err
	}
	defer client.Close()

	args := ReadFileArgs{HyDFSFileName: fileName, LocalFileName: localFileName, Addr: selfAddr}

	var res CheckAndReadFileResponse
	err = client.Call("FileReadService.CheckAndReadFile", args, &res)
	if err != nil {
		fmt.Println("RPC error:", err)
		return err
	}

	if res.Success {
		// fmt.Println("File successfully read from server")
	} else {
		fmt.Println("Error:", res.Error)
	}
	return nil
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

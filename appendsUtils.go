package main

import (
	"fmt"
	"net/rpc"
	"path/filepath"
	"strconv"
	"strings"
)

type CounterRequestArgs struct {
	HyDFSFileName string
}

type CounterResponse struct {
	Counter int
}

type AppendResponse struct {
	Success bool
	Error   string
}

func appendFileOnServer(localFilename string, hydfsFilename string) error {
	destNode, err := findServerGivenFileHash(HashString(hydfsFilename))
	if err != nil {
		return err
	}
	// fmt.Printf("congrats you are in appendFileOnServer method destNode: %s\n", destNode.Name)
	client, err := rpc.Dial("tcp", makeAddrPort(destNode.Name, AppendPort))
	if err != nil {
		fmt.Printf("Error dialing server: %v\n", err)
		return fmt.Errorf("error dialing server: %v", err)
	}
	defer client.Close()

	args := CounterRequestArgs{
		HyDFSFileName: hydfsFilename,
	}
	var reply CounterResponse
	err = client.Call("FileService.GetCounter", args, &reply)
	if err != nil {
		fmt.Printf("Error calling RPC method in appendFileOnServer: %v\n", err)
		return fmt.Errorf("error calling RPC method: %v", err)
	}

	hydfsFilenameWithCounter := addCounterToFilename(hydfsFilename, reply.Counter)
	// fmt.Printf("Appending counter to filename: %s\n", hydfsFilenameWithCounter)

	replicas := getReplicas(destNode.Name)
	// fmt.Printf("Replicas: %v\n", replicas)
	//ok := 0
	go func() {
		fileWriteError := writeFileOnServer(localFilename, hydfsFilenameWithCounter, destNode.Name, 1)
		if fileWriteError != nil {
			fmt.Printf("Error writing file on server: %v\n", fileWriteError)
		}
	}()

	for _, replica := range replicas {
		go func(replica string) {
			fileWriteError := writeFileOnServer(localFilename, hydfsFilenameWithCounter, replica, 2)
			if fileWriteError != nil {
				fmt.Printf("Error writing file on server: %v\n", fileWriteError)
			}
		}(replica)
	}
	// wait for all replicas to be written

	// if ok > 1 {
	// 	fmt.Printf("File %s written to server %s with name %s\n", localFilename, destNode.Name, hydfsFilenameWithCounter)
	// 	return nil
	// }
	// fmt.Printf("Could not write file to quorum\n")
	// return fmt.Errorf("could not write file to quorum")
	return nil

	// _, fileWriteError := writeFileOnServer(localFilename, hydfsFilename, destNode.Name, 1)
	// if fileWriteError != nil {
	// 	return fileWriteError
	// }
	// fmt.Printf("File %s written to server %s with name %s\n", localFilename, destNode.Name, hydfsFilename)
	// return nil
}

func addCounterToFilename(filename string, counter int) string {

	dir := filepath.Dir(filename)
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	newFileName := filepath.Join(dir, name+"_"+strconv.Itoa(counter)+ext)

	return "./" + newFileName
}

func multiAppendFileOnServer(args multiAppendArgs) error {
	destFile := args.FileName

	if len(args.VMFileMap) == 0 {
		return fmt.Errorf("no files to append")
	} else {

		for vm, file := range args.VMFileMap {
			go runAppendFromServer(file, destFile, vm)
		}
	}

	return nil
}

func runAppendFromServer(localFilename string, hydfsFilename string, server string) {
	client, err := rpc.Dial("tcp", makeAddrPort(server, AppendPort))
	if err != nil {
		fmt.Println("Error dialing server:", err)
		return
	}
	defer client.Close()

	args := AppendRequest{
		LocalFileName: "./local/" + localFilename,
		HyDFSFileName: "./hydfs/" + hydfsFilename,
	}

	var res AppendResponse
	err = client.Call("FileService.AppendFile", args, &res)
	if err != nil {
		fmt.Println("RPC error:", err)
		return
	}

}

func (f *FileService) AppendFile(args AppendRequest, res *AppendResponse) error {
	localFilename := args.LocalFileName
	hydfsFilename := args.HyDFSFileName

	err := appendFileOnServer(localFilename, hydfsFilename)
	if err != nil {
		res.Success = false
		res.Error = err.Error()
		return err
	}

	res.Success = true
	return nil
}

func (f *FileService) AppendNumberFiles(args AppendNumberRequest, res *AppendResponse) error {
	localFilename := args.LocalFileName
	hydfsFilename := args.HyDFSFileName

	for i := 0; i < args.NumberOfAppends; i++ {
		go appendFileOnServer(localFilename, hydfsFilename)
	}

	res.Success = true
	return nil
}

type AppendNumberRequest struct {
	LocalFileName   string
	HyDFSFileName   string
	NumberOfAppends int
}

func runNumberOfAppendsFromServer(localFilename string, hydfsFilename string, server string, numberOfAppends int) {
	client, err := rpc.Dial("tcp", makeAddrPort(server, AppendPort))
	if err != nil {
		fmt.Println("Error dialing server:", err)
		return
	}
	defer client.Close()

	args := AppendNumberRequest{
		LocalFileName:   "./local/" + localFilename,
		HyDFSFileName:   "./hydfs/" + hydfsFilename,
		NumberOfAppends: numberOfAppends,
	}

	fmt.Printf("Appending %d files to %s\n", numberOfAppends, hydfsFilename)
	fmt.Printf("local file name: %s\n", args.LocalFileName)

	var res AppendResponse
	err = client.Call("FileService.AppendNumberFiles", args, &res)
	if err != nil {
		fmt.Println("RPC error:", err)
		return
	}

}

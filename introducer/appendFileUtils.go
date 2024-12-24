package main

import (
	"fmt"
	"net/rpc"
	"os"
)

func writeInAppendModeOnServer(chunkFile string, destFileName string, addr string, operationFlag int) error {
	// Open the file in read mode
	file, err := os.Open(chunkFile)
	if err != nil {
		return err
	}
	defer file.Close()

	client, err := rpc.Dial("tcp", makeAddrPort(addr, 8000))
	if err != nil {
		fmt.Println("Error dialing server:", err)
		return err
	}
	defer client.Close()

	buff := make([]byte, 1024)
	var res FileWriteResponse

	for {
		n, err := file.Read(buff)
		if err != nil && err.Error() != "EOF" {
			fmt.Println("File read error:", err)
			break
		}
		if n == 0 {
			// fmt.Printf("File read from local complete\n")
			break
		}
		args := FileWriteArgs{
			FileName: destFileName,
			Data:     buff[:n],
		}

		err = client.Call("FileWriteService.AppendFileChunk", args, &res)

		if err != nil {
			fmt.Println("RPC error:", err)
			return err
		}
		if !res.Success {
			fmt.Printf("Server error: %s\n", res.Error)
			return err
		}

	}
	// fmt.Printf("File %s written to server %s\n", chunkFile, addr)
	return nil
}

func (s *FileWriteService) AppendFileChunk(args FileWriteArgs, res *FileWriteResponse) error {
	// Check if the file already exists
	if _, err := os.Stat(args.FileName); os.IsNotExist(err) {
		// do nothing
	} else if err != nil {
		res.Success = false
		res.Error = err.Error()
		return err
	}

	// Open the file in append mode (create if not exists)
	file, err := os.OpenFile(args.FileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		res.Success = false
		res.Error = err.Error()
		return err
	}
	defer file.Close()

	_, err = file.Write(args.Data)
	if err != nil {
		res.Success = false
		res.Error = err.Error()
		return err
	}

	// fmt.Printf("Appended data to file %s\n", args.FileName)
	// Set success to true
	res.Success = true
	return nil
}

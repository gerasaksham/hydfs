package main

import (
	"fmt"
	"net/rpc"
)

type ReplicateRequestArgs struct {
	FileName string
	Addr     string
}

type ReplicateResponse struct {
	Error string
}

func sendReplicateRequest(fileName string, addr string) error {
	client, err := rpc.Dial("tcp", makeAddrPort(addr, 8000))
	if err != nil {
		fmt.Printf("Error dialing server: %v\n", err)
		return err
	}
	defer client.Close()

	args := ReplicateRequestArgs{
		FileName: fileName,
		Addr:     addr,
	}

	var res ReplicateResponse
	err = client.Call("FileWriteService.ReplicateFiletoServers", args, &res)
	if err != nil {
		fmt.Printf("RPC error: %v\n", err)
		return err
	}

	return nil

}

func (s *FileWriteService) ReplicateFiletoServers(args ReplicateRequestArgs, res *ReplicateResponse) error {
	serverHash := HashString(args.Addr)
	fileName := args.FileName
	var successor1, successor2 ListNode

	if (len(listOfHashes)) == 0 {
		res.Error = "No servers available"
		return fmt.Errorf("no servers available")
	}

	for i := 0; i < len(listOfHashes); i++ {

		node := listOfHashes[i]
		if node.Hash == serverHash {
			// Access the next element if it exists
			successor1 = listOfHashes[(i+1)%len(listOfHashes)]
			successor2 = listOfHashes[(i+2)%len(listOfHashes)]
			break
		}
	}
	fmt.Printf("Found Successor 1: %v\n", successor1)
	fmt.Printf("Found Successor 2: %v\n", successor2)
	if successor1.Name == "" || successor2.Name == "" {
		res.Error = "Could not find successors"
		return fmt.Errorf("could not find successors")
	}
	go func() {
		if err := writeFileOnServer(fileName, fileName, successor1.Name, 2); err != nil {
			fmt.Printf("Error replicating to %s: %v\n", successor1.Name, err)
		}
	}()
	go func() {
		if err := writeFileOnServer(fileName, fileName, successor2.Name, 2); err != nil {
			fmt.Printf("Error replicating to %s: %v\n", successor2.Name, err)
		}
	}()

	return nil
}

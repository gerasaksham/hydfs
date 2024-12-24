package main

import (
	"fmt"
	"net/rpc"
	"sort"

	"github.com/spaolacci/murmur3"
)

func HashString(serverName string) uint32 {
	hashValue := murmur3.Sum32([]byte(serverName))
	return hashValue % 128
}

func sortListOfHashes() {
	sort.Slice(listOfHashes, func(i, j int) bool {
		return listOfHashes[i].Hash < listOfHashes[j].Hash
	})
}

// func initializeServerFileMap() {
// 	// iterate over membership list and add the server to the serverFileMap
// 	mutex.Lock()
// 	defer mutex.Unlock()
// 	for server := range membershipList {
// 		serverFileMap[HashString(server)] = ServerNode{server, []File{}}
// 	}
// }

func listHostNames(filepath string, filename string) error {
	fileHash := HashString(filepath)
	server, err := findServerGivenFileHash(fileHash)
	if err != nil {
		return err
	}

	replicas := getReplicas(server.Name)
	if len(replicas) < 2 {
		fmt.Println("Error: Not enough replicas found for file", filename)
		return fmt.Errorf("not enough replicas found for file %s", filename)
	}
	fmt.Println("File", filename, "Server:", server.Name, "Hash value:", server.Hash)
	fmt.Println("File", filename, "Server:", replicas[0], "Hash value:", HashString(replicas[0]))
	fmt.Println("File", filename, "Server:", replicas[1], "Hash value:", HashString(replicas[1]))

	return nil
}

func getReplicas(serverName string) []string {
	serverHash := HashString(serverName)
	replicas := []string{}

	for i := 0; i < len(listOfHashes); i++ {
		if listOfHashes[i].Hash == serverHash {
			replicas = append(replicas, listOfHashes[(i+1)%len(listOfHashes)].Name)
			replicas = append(replicas, listOfHashes[(i+2)%len(listOfHashes)].Name)
		}
	}
	return replicas
}

func findServerGivenFileHash(fileHash uint32) (ListNode, error) {
	if len(listOfHashes) == 0 {
		return ListNode{}, fmt.Errorf("no nodes in the list")
	}

	// Find the server hash that is just greater than the file hash
	for _, node := range listOfHashes {
		if node.Hash > fileHash {
			return node, nil
		}
	}

	// If no such server hash exists, return the smallest server hash (wrap-around case)
	return listOfHashes[0], nil
}

func removeNodeFromListHash(nodeName string) {
	for i, node := range listOfHashes {
		if node.Name == nodeName {
			// Remove the element by slicing
			listOfHashes = append(listOfHashes[:i], listOfHashes[i+1:]...)
			break
		}
	}
	sortListOfHashes()
}

func writeFileFromReplica(LocalFileName string, HyDFSFileName string, replica string) error {
	// fmt.Printf("Dialing server %s\n", makeAddrPort(replica, 8001))
	client, err := rpc.Dial("tcp", makeAddrPort(replica, 8001))
	if err != nil {
		fmt.Println("Error dialing server:", err)
		return err
	}
	defer client.Close()

	fmt.Println("Writing file from replica")

	args := WriteFileFromReplicaArgs{LocalFileName: LocalFileName, HyDFSFileName: HyDFSFileName, ServerName: selfAddr}
	var res WriteFileFromReplicaResponse
	err = client.Call("FileWriteFromReplicaService.WriteFileFromReplicaRPC", args, &res)
	if err != nil {
		fmt.Println("RPC error:", err)
		return err
	}

	if !res.Success {
		fmt.Println("Error writing file from replica:", res.Error)
		return fmt.Errorf("error writing file from replica: %s", res.Error)
	}

	fmt.Println("File successfully written from replica")
	return nil
}

type WriteFileFromReplicaArgs struct {
	LocalFileName string
	HyDFSFileName string
	ServerName    string
}

type WriteFileFromReplicaResponse struct {
	Success bool
	Error   string
}

type FileWriteFromReplicaService struct {
}

func (s *FileWriteFromReplicaService) WriteFileFromReplicaRPC(args WriteFileFromReplicaArgs, res *WriteFileFromReplicaResponse) error {
	localFilename := args.LocalFileName
	hydfsFilename := args.HyDFSFileName
	fmt.Printf("you are in WriteFileFromReplica method localFilename: %s, hydfsFilename: %s\n", localFilename, hydfsFilename)
	err := writeFileOnServer(hydfsFilename, localFilename, args.ServerName, 0)
	if err != nil {
		res.Success = false
		res.Error = err.Error()
		return err
	}

	res.Success = true
	return nil
}

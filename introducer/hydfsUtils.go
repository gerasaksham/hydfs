package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spaolacci/murmur3"
)

func HashString(serverName string) uint32 {
	hashValue := murmur3.Sum32([]byte(serverName))
	return hashValue % 128
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

func sortListOfHashes() {
	sort.Slice(listOfHashes, func(i, j int) bool {
		return listOfHashes[i].Hash < listOfHashes[j].Hash
	})
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

func makeAddrPort(addr string, port int) string {
	index := strings.IndexRune(addr, '|')
	if index == -1 {
		index = len(addr)
	}
	return fmt.Sprintf("%s:%d", addr[:index], port)
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

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
)

func Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// func getFileNameFromPath(path string) string {
// 	// Get the last part of the path
// 	_, fileName := filepath.Split(path)
// 	return fileName
// }

func prettyPrintMap(m map[string]int) {
	// Marshal the map into a JSON string with indentation
	jsonData, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling map:", err)
		return
	}

	// Print the JSON string
	fmt.Println(string(jsonData))
}

func getNextTarget() (string, error) {
	mutex.Lock()
	defer mutex.Unlock()
	if (len(listOfNodes)) == 0 {
		return "", fmt.Errorf("no nodes in the list")
	}

	nextTarget = (nextTarget + 1) % len(listOfNodes)
	if nextTarget == 0 {
		rand.Shuffle(len(listOfNodes), func(i, j int) {
			listOfNodes[i], listOfNodes[j] = listOfNodes[j], listOfNodes[i]
		})
	}
	return listOfNodes[nextTarget], nil
}

func addNewNode(newNode string) {
	log.Printf("Adding new node %s\n", newNode)
	mutex.Lock()
	defer mutex.Unlock()
	listOfNodes = append(listOfNodes, newNode)
	membershipList[newNode] = 0
	listOfHashes = append(listOfHashes, ListNode{Name: newNode, Hash: HashString(newNode), Alive: true})
	sortListOfHashes()
	rebalanceFilesAfterNodeJoin(newNode)
}

func shouldDropMessage() bool {
	return rand.Intn(100) < dropRate
}

// Deletes the node from the list of nodes and marks it as failed
func markNodeAsFailed(node string) {
	mutex.Lock()
	defer mutex.Unlock()
	for i, n := range listOfNodes {
		if n == node {
			listOfNodes = append(listOfNodes[:i], listOfNodes[i+1:]...)
			membershipList[node] = 2
			// delete(serverFileMap, HashString(node))
			break
		}
	}
	// find node in list of hashes and mark it as failed
	for i, n := range listOfHashes {
		if n.Name == node {
			listOfHashes[i].Alive = false
			break
		}
	}
	// fmt.Printf("alive value of %s is %t\n", node, listOfHashes[getRingIndex(node)].Alive)
	go rebalanceFilesAfterNodeFailure(node)

}

func makeAddrPort(addr string, port int) string {
	index := strings.IndexRune(addr, '|')
	if index == -1 {
		index = len(addr)
	}
	return fmt.Sprintf("%s:%d", addr[:index], port)
}

func getHostnameFromNodename(node string) string {
	parts := strings.Split(node, "|")
	return parts[0]
}

func printRingMembershipList() {
	fmt.Println("Membership List: Ring Id")

	// Create a slice of key-value pairs
	type kv struct {
		Key   string
		Value uint32
	}
	var sortedList []kv
	for _, node := range listOfHashes {
		sortedList = append(sortedList, kv{Key: node.Name, Value: node.Hash})
	}

	// Sort the slice based on the hash values
	sort.Slice(sortedList, func(i, j int) bool {
		return sortedList[i].Value < sortedList[j].Value
	})

	// Print the sorted list
	for _, kv := range sortedList {
		fmt.Printf("%s: %d\n", kv.Key, kv.Value)
	}
}

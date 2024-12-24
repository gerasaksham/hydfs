package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strings"
)

func rebalanceFilesAfterNodeFailure(node string) {
	startTime := getCurrentTime()
	fmt.Printf("rebalancing started at time %v\n", startTime)
	rebalanceReplicaFiles()
	rebalancePrimaryFiles()
	removeNodeFromListHash(node)
	endTime := getCurrentTime()
	fmt.Printf("rebalancing ended at time %v\n", endTime)
	fmt.Printf("time elapsed in seconds: %v\n", (endTime - startTime))
}

func cleanupFilenameForHash(filename string) string {
	// find index of _ in filename
	idx := strings.Index(filename, "_")
	if idx == -1 {
		return filename
	}
	// find extension of the file
	ext := filepath.Ext(filename)
	return filename[:idx] + ext
}

func findTwoaliveSuccessors(index int) (ListNode, ListNode) {
	selfHash := HashString(selfAddr)
	var newSuccessor1, newSuccessor2 ListNode
	var newSucessor1Index int
	for i := index + 1; i < len(listOfHashes); i++ {
		node := listOfHashes[i%len(listOfHashes)]
		if node.Alive && node.Hash != selfHash {
			// Access the next element if it exists
			newSuccessor1 = listOfHashes[i%len(listOfHashes)]
			newSucessor1Index = i % len(listOfHashes)
			break
		}
	}
	for i := newSucessor1Index + 1; i < len(listOfHashes); i++ {
		node := listOfHashes[i%len(listOfHashes)]
		if node.Alive {
			// Access the next element if it exists
			newSuccessor2 = listOfHashes[i%len(listOfHashes)]
			break
		}
	}
	fmt.Printf("nexttwoalive successors: %s, %s\n", newSuccessor1.Name, newSuccessor2.Name)
	return newSuccessor1, newSuccessor2
}

func getPrimaryFiles() []string {
	var primaryFiles []string
	for fileName, value := range fileMap {
		if value == 1 {
			primaryFiles = append(primaryFiles, fileName)
		}
	}
	return primaryFiles
}

func rebalanceReplicaFiles() {
	selfHash := HashString(selfAddr)
	var successor1, successor2 ListNode
	var successor2Index int

	primaryFiles := getPrimaryFiles()

	for i := 0; i < len(listOfHashes); i++ {
		node := listOfHashes[i]
		if node.Hash == selfHash {
			// Access the next element if it exists
			successor1 = listOfHashes[(i+1)%len(listOfHashes)]
			successor2 = listOfHashes[(i+2)%len(listOfHashes)]
			successor2Index = (i + 2) % len(listOfHashes)
			break
		}
	}
	// newSuccessor1, newSuccessor2 := findTwoaliveSuccessors(successor2Index)
	newSuccessor1idx := findAliveSuccessorOfNode(successor2Index)
	if newSuccessor1idx == -1 {
		fmt.Printf("No alive successor found\n")
		return
	}
	newSuccessor2idx := findAliveSuccessorOfNode(newSuccessor1idx)
	if newSuccessor2idx == -1 {
		fmt.Printf("No second alive successor found\n")
		return
	}
	newSuccessor1 := listOfHashes[newSuccessor1idx]
	newSuccessor2 := listOfHashes[newSuccessor2idx]
	newSuccessor := newSuccessor1
	if !successor1.Alive && successor2.Alive || successor1.Alive && !successor2.Alive {
		for _, fileName := range primaryFiles {
			err := writeFileOnServer(fileName, fileName, newSuccessor.Name, 2)
			if err != nil && newSuccessor != newSuccessor2 {
				var opErr *net.OpError
				if errors.As(err, &opErr) && opErr.Op == "dial" {
					err = writeFileOnServer(fileName, fileName, newSuccessor2.Name, 2)
					if err != nil {
						fmt.Printf("Error writing file %s to %s: %v\n", fileName, newSuccessor2.Name, err)
						return
					}
					newSuccessor = newSuccessor2
					continue
				}
			}
		}

	} else if !successor1.Alive && !successor2.Alive {

		for _, fileName := range primaryFiles {
			go writeFileOnServer(fileName, fileName, newSuccessor1.Name, 2)
			go writeFileOnServer(fileName, fileName, newSuccessor2.Name, 2)
		}

	} else {
		return
	}
}

func getLocalReplicaFiles() []string {
	var localReplicaFiles []string
	for filename, value := range fileMap {
		if value == 2 {
			localReplicaFiles = append(localReplicaFiles, filename)
		}
	}
	return localReplicaFiles
}

func getRingIndex(node string) int {
	for i, n := range listOfHashes {
		if n.Name == node {
			return i
		}
	}
	return -1
}

func findAliveSuccessorOfNode(idx int) int {
	for i := 1; i < len(listOfHashes); i++ {
		successor := listOfHashes[(idx+i)%len(listOfHashes)]
		if successor.Alive {
			return (idx + i) % len(listOfHashes)
		}
	}
	return -1
}

func rebalancePrimaryFiles() {
	// fmt.Printf("Rebalancing primary files\n")
	localReplicaFiles := getLocalReplicaFiles()
	currentIndex := getRingIndex(selfAddr)
	if currentIndex == -1 {
		fmt.Printf("Node %s not found in the list of hashes\n", selfAddr)
		return
	}

	first_predecessor := listOfHashes[(currentIndex+(len(listOfHashes)-1))%len(listOfHashes)]
	second_predecessor := listOfHashes[(currentIndex+(len(listOfHashes)-2))%len(listOfHashes)]
	// fmt.Printf("First predecessor: %s, Second predecessor: %s\n", first_predecessor.Name, second_predecessor.Name)

	if first_predecessor.Alive && second_predecessor.Alive {
		return
	} else {
		if !first_predecessor.Alive {
			for _, filename := range localReplicaFiles {
				hashOfFile := HashString(cleanupFilenameForHash(filename))
				ownerNode, err := findServerGivenFileHash(hashOfFile)
				log.Printf("Owner node: %s for file: %s\n", ownerNode.Name, filename)
				if err != nil {
					fmt.Printf("Error finding owner node for file %s\n", filename)
					continue
				}
				if ownerNode.Name == first_predecessor.Name {
					fileMap[filename] = 1
					firstAliveSuccessorIndex := findAliveSuccessorOfNode(currentIndex)
					if firstAliveSuccessorIndex == -1 {
						fmt.Printf("firstAliveSuccessorIndex is -1\n")
						continue
					}
					secondAliveSuccessorIndex := findAliveSuccessorOfNode(firstAliveSuccessorIndex)
					if secondAliveSuccessorIndex == -1 {
						fmt.Printf("secondAliveSuccessorIndex is -1\n")
						continue
					}
					firstAliveSuccessor := listOfHashes[firstAliveSuccessorIndex].Name
					secondAliveSuccessor := listOfHashes[secondAliveSuccessorIndex].Name
					log.Printf("First alive successor: %s, Second alive successor: %s\n", firstAliveSuccessor, secondAliveSuccessor)

					if err := writeFileOnServer(filename, filename, firstAliveSuccessor, 2); err != nil {
						fmt.Printf("Error replicating to %s: %v", firstAliveSuccessor, err)
						var opErr *net.OpError
						if errors.As(err, &opErr) && opErr.Op == "dial" {
							fmt.Printf("Failed to connect to %s\n", firstAliveSuccessor)
							fmt.Printf("Trying to connect to %s\n", secondAliveSuccessor)
							writeFileOnServer(filename, filename, secondAliveSuccessor, 2)
							nextSuccessorIndex := findAliveSuccessorOfNode(secondAliveSuccessorIndex)
							if nextSuccessorIndex == -1 {
								continue
							}
							nextSuccessor := listOfHashes[nextSuccessorIndex].Name
							writeFileOnServer(filename, filename, nextSuccessor, 2)
							continue
						}
					}
					if err := writeFileOnServer(filename, filename, secondAliveSuccessor, 2); err != nil {
						fmt.Printf("Error replicating to %s: %v", secondAliveSuccessor, err)
						var opErr *net.OpError
						if errors.As(err, &opErr) && opErr.Op == "dial" {
							fmt.Printf("Failed to connect to %s\n", secondAliveSuccessor)
							fmt.Printf("Trying to connect to %s\n", firstAliveSuccessor)
							nextSuccessorIndex := findAliveSuccessorOfNode(secondAliveSuccessorIndex)
							if nextSuccessorIndex == -1 {
								continue
							}
							nextSuccessor := listOfHashes[nextSuccessorIndex].Name
							writeFileOnServer(filename, filename, nextSuccessor, 2)
						}
					}
				}
			}
			if !second_predecessor.Alive {
				for _, filename := range localReplicaFiles {
					hashOfFile := HashString(cleanupFilenameForHash(filename))
					ownerNode, err := findServerGivenFileHash(hashOfFile)
					log.Printf("Owner node: %s for file: %s\n", ownerNode.Name, filename)
					if err != nil {
						fmt.Printf("Error finding owner node for file %s\n", filename)
						continue
					}
					if ownerNode.Name == second_predecessor.Name {
						firstAliveSuccessorIndex := findAliveSuccessorOfNode(currentIndex)
						if firstAliveSuccessorIndex == -1 {
							fmt.Printf("firstAliveSuccessorIndex is -1\n")
							continue
						}
						secondAliveSuccessorIndex := findAliveSuccessorOfNode(firstAliveSuccessorIndex)
						if secondAliveSuccessorIndex == -1 {
							fmt.Printf("secondAliveSuccessorIndex is -1\n")
							continue
						}
						firstAliveSuccessor := listOfHashes[firstAliveSuccessorIndex].Name
						secondAliveSuccessor := listOfHashes[secondAliveSuccessorIndex].Name
						// fmt.Printf("First alive successor: %s, Second alive successor: %s\n", firstAliveSuccessor, secondAliveSuccessor)
						if err := writeFileOnServer(filename, filename, firstAliveSuccessor, 2); err != nil {
							fmt.Printf("Error replicating to %s: %v", firstAliveSuccessor, err)
						}
						if err := writeFileOnServer(filename, filename, secondAliveSuccessor, 2); err != nil {
							fmt.Printf("Error replicating to %s: %v", secondAliveSuccessor, err)
						}
					}
				}
			}

		}
	}
}

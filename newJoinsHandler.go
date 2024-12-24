package main

import (
	"fmt"
)

func rebalanceFilesAfterNodeJoin(node string) {
	// Check if the new node that joined is the predecessor of the current node
	selfIndex := getRingIndex(selfAddr)
	predecessorIndex := (selfIndex + (len(listOfHashes) - 1)) % len(listOfHashes)
	predecessorNode := listOfHashes[predecessorIndex]
	if predecessorNode.Name == node && predecessorNode.Alive {
		fmt.Printf("rebalancing files for new node %s by %s\n", node, selfAddr)
		rebalaceFilesForNewNode(predecessorNode)
	}
}

func rebalaceFilesForNewNode(newNode ListNode) {
	localPrimaryFiles := getPrimaryFiles()

	for _, filename := range localPrimaryFiles {
		fileHash := HashString(filename)
		ownerNode, err := findServerGivenFileHash(fileHash)
		fmt.Printf("Owner node for file %s is %s\n", filename, ownerNode.Name)
		if err != nil {
			fmt.Printf("Error finding owner node for file %s\n", filename)
			continue
		}
		if ownerNode.Name == newNode.Name {
			if err := writeFileOnServer(filename, filename, newNode.Name, 1); err != nil {
				fmt.Printf("Error replicating to %s: %v", newNode.Name, err)
				continue
			}
			fileMap[filename] = 2
		}
	}
}

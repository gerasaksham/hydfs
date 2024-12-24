package main

import (
	"fmt"
)

func mergeFiles(filename string) error {
	// Merge files
	destNode, err := findServerGivenFileHash(HashString(filename))
	if err != nil {
		return err
	}
	replicas := getReplicas(destNode.Name)
	replicas = append(replicas, destNode.Name)

	for _, replica := range replicas {
		go func(replica string) {
			err := mergeFileOnServer(filename, replica)
			if err != nil {
				fmt.Printf("Error merging file on server: %v\n", err)
			}
		}(replica)
	}
	return nil
}

package main

import (
	"sync"
)

var (
	mu sync.Mutex
)

type FileService struct {
	counterRequests chan counterRequest
}
type counterRequest struct {
	args  CounterRequestArgs
	reply chan int
}

func NewFileService() *FileService {
	fs := &FileService{
		counterRequests: make(chan counterRequest, 1500),
	}
	go fs.processCounterRequests()
	return fs
}

func (fs *FileService) processCounterRequests() {
	for req := range fs.counterRequests {
		mu.Lock()
		fileCounter[req.args.HyDFSFileName]++
		counter := fileCounter[req.args.HyDFSFileName]
		mu.Unlock()

		req.reply <- counter // Send the incremented counter back
	}
}

func (fs *FileService) GetCounter(args CounterRequestArgs, reply *CounterResponse) error {
	replyChan := make(chan int)
	fs.counterRequests <- counterRequest{args: args, reply: replyChan}

	// Wait for the incremented counter to be returned from processCounterRequests
	reply.Counter = <-replyChan
	return nil
}

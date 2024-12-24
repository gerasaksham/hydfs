package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	listOfNodes        = []string{}
	listOfHashes       = []ListNode{}
	nextTarget         = 0
	mutex              sync.Mutex
	membershipList     = make(map[string]int)
	incarnationNumbers = make(map[string]int)
	fileMap            = make(map[string]int)
	fileCounter        = make(map[string]int)
	nodeAddr           string
	selfAddr           string
	susEnabled         = true
	stopChan           = make(chan struct{})
	udpConnPing        *net.UDPConn
	udpConnBroadcast   *net.UDPConn
	dropRate           = 0
	logFile            *os.File
	//numOfBytes         = 0
	bytesSyntax sync.Mutex
)

type ListNode struct {
	Name  string
	Hash  uint32
	Alive bool
}

type ServerNode struct {
	Name  string
	Files []File
}

type File struct {
	FileName string
	Owner    bool
}

type multiAppendArgs struct {
	FileName  string
	VMFileMap map[string]string
}

const (
	Ping           string = "ping"
	Ack            string = "ack"
	PingPort       int    = 8080
	AckPort        int    = 8081
	BroadPort      int    = 8082
	AppendPort     int    = 8000
	Introducer     string = "Introducer"
	IntroducerAddr string = "fa24-cs425-3101.cs.illinois.edu"
	Broadcast      string = "Broadcast"
	EnableSus      string = "EnableSus"
	DisableSus     string = "DisableSus"
	ChangeDropRate string = "ChangeDropRate"
)

func init() {
	// Create a log file
	var err error
	logFile, err = os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Error creating log file: %v\n", err)
		os.Exit(1)
	}

	// Set log output to the file
	log.SetOutput(logFile)
	rand.Seed(time.Now().UnixNano())
}

// Updates about nodes
type Update struct {
	Node              string
	Status            int // 0 for alive, 1 for suspected, 2 for dead
	DropRate          int
	IncarnationNumber int
}

type JoinRequest struct {
	Addr string
}

type Packet struct {
	Buffer []byte
}

type Message struct {
	Type   string
	Source string
	Update Update
}

type PingMessage struct {
	Source    string
	Suspected bool
}

type Introduction struct {
	MembershipList     map[string]int // include self and introducer
	IncarnationNumbers map[string]int
	SelfAddress        string
	ListOfNodes        []string // don't include self
	SusEnabled         bool
	DropRate           int
}

type AppendRequest struct {
	LocalFileName string
	HyDFSFileName string
}

func susConfirmation(timeoutTime time.Time, targetAddr string, selfAddr string) {
	for {
		if time.Now().After(timeoutTime) {
			if membershipList[targetAddr] == 1 {
				fmt.Printf("Suspected node %s did not respond; marking as expired at %s\n", targetAddr, time.Now())
				log.Printf("Suspected node %s did not respond; marking as expired\n", targetAddr)
				markNodeAsFailed(targetAddr)
				update := Update{Node: targetAddr, Status: 2}
				informAllNodes(update, selfAddr)
			}
			return
		}
	}
}

func sendPing(selfAddr string, targetAddr string, pingMessage PingMessage) {

	if shouldDropMessage() {
		log.Printf("Dropping ping message from %s to %s\n", selfAddr, targetAddr)
		return
	}

	// Remove the timestamps and attach the ports
	sourceAddr := makeAddrPort(selfAddr, AckPort)
	destAddr := makeAddrPort(targetAddr, PingPort)
	timeout := 5 * time.Second

	udpDestAddr, err := net.ResolveUDPAddr("udp", destAddr)
	if err != nil {
		log.Printf("error resolving UDP address: %v", err)
		return
	}

	// Bind the client to a specific port
	udpSourceAddr, err := net.ResolveUDPAddr("udp", sourceAddr)
	if err != nil {
		log.Printf("error resolving local UDP address: %v", err)
		return
	}

	// Create a UDP connection
	conn, err := net.DialUDP("udp", nil, udpDestAddr)

	if err != nil {
		log.Printf("error creating UDP connection: %v", err)
		return
	}
	defer conn.Close()
	// Marshal the ping message
	data, err := Marshal(pingMessage)
	if err != nil {
		log.Printf("error marshaling ping message: %v", err)
		return
	}

	_, err = conn.Write(data)
	if err != nil {
		log.Printf("Error sending ping message: %v\n", err)
		return
	}

	if getHostnameFromNodename(targetAddr) != IntroducerAddr { // Set the timeout for receiving acknowledgment
		deadline := time.Now().Add(timeout)
		conn1, err := net.ListenUDP("udp", udpSourceAddr)
		if err != nil {
			log.Printf("Error creating UDP listener: %v\n", err)
			return
		}
		defer conn1.Close()
		err = conn1.SetDeadline(deadline)
		if err != nil {
			log.Printf("Error setting deadline: %v\n", err)
			return
		}

		// Wait for acknowledgment
		buffer := make([]byte, 1024)
		n, _, err := conn1.ReadFromUDP(buffer)
		//bytesSyntax.Lock()
		//numOfBytes += n
		//bytesSyntax.Unlock()

		if err != nil || n == 0 {
			if netErr, ok := err.(net.Error); ok {
				if netErr.Timeout() {
					log.Printf("Timeout waiting for acknowledgment from %s\n", targetAddr)
				}
			} else {
				log.Printf("Error reading acknowledgment: %v\n", err)
			}
			if !susEnabled {
				markNodeAsFailed(targetAddr)
				update := Update{Node: targetAddr, Status: 2}
				informAllNodes(update, selfAddr)
				log.Printf("Failure Detected for %s at %s\n", targetAddr, time.Now())
				fmt.Printf("Failure Detected for %s at %s\n", targetAddr, time.Now())
				return
			} else {
				if !(pingMessage.Suspected) {
					mutex.Lock()
					if membershipList[targetAddr] == 0 {
						membershipList[targetAddr] = 1
					}
					mutex.Unlock()
					fmt.Printf("Marked %s as suspected\n", targetAddr)
					log.Printf("Marked %s as suspected\n", targetAddr)
					go informAllNodes(Update{Node: targetAddr, Status: 1, IncarnationNumber: incarnationNumbers[targetAddr]}, selfAddr)
					timeoutTime := time.Now().Add(10 * time.Second)
					go susConfirmation(timeoutTime, targetAddr, selfAddr)
				}
			}
			return
		}

		// Received acknowledgment
		if susEnabled {
			if pingMessage.Suspected {
				mutex.Lock()
				membershipList[targetAddr] = 0
				mutex.Unlock()
				informAllNodes(Update{Node: targetAddr, Status: 0, IncarnationNumber: incarnationNumbers[targetAddr]}, selfAddr)
			}
		}
	}
}

func broadcastListener() {
	addr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(BroadPort))
	if err != nil {
		log.Printf("Error resolving UDP address: %v\n", err)
		return
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("Error listening to udp updates: %v\n", err)
		return
	}
	udpConnBroadcast = conn // Store the connection in the global variable
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		select {
		case <-stopChan:
			return
		default:
			n, _, err := conn.ReadFromUDP(buf)
			bytesSyntax.Lock()
			//numOfBytes += n
			bytesSyntax.Unlock()
			if err != nil {
				log.Printf("Error reading udp packet: %v\n", err)
				continue
			}
			if n == 0 {
				continue
			}
			var message Message
			_ = Unmarshal(buf[:n], &message)
			if message.Type == Broadcast {
				update := message.Update
				log.Printf("Received broadcast message: %v\n", update)

				if update.Node == selfAddr {
					if susEnabled && update.Status == 1 {
						mutex.Lock()
						membershipList[update.Node] = 0
						incarnationNumbers[update.Node]++
						mutex.Unlock()
						informAllNodes(Update{Node: update.Node, Status: 0, IncarnationNumber: incarnationNumbers[update.Node]}, selfAddr)
						log.Printf("Updated membership list: %v\n", membershipList)
						log.Printf("Updated incarnation numbers: %v\n", incarnationNumbers)
					}
					continue
				}
				if update.Status == 2 {
					markNodeAsFailed(update.Node)
				} else if update.Status == 0 {
					if susEnabled && membershipList[update.Node] == 1 && update.IncarnationNumber > incarnationNumbers[update.Node] {
						mutex.Lock()
						membershipList[update.Node] = 0
						incarnationNumbers[update.Node] = update.IncarnationNumber
						mutex.Unlock()
						log.Printf("Updated membership list: %v\n", membershipList)
						log.Printf("Updated incarnation numbers: %v\n", incarnationNumbers)
					}
				} else if update.Status == 1 {
					if susEnabled && membershipList[update.Node] != 2 {
						if update.IncarnationNumber >= incarnationNumbers[update.Node] {
							mutex.Lock()
							membershipList[update.Node] = 1
							incarnationNumbers[update.Node] = update.IncarnationNumber
							mutex.Unlock()
							log.Printf("Updated membership list: %v\n", membershipList)
							log.Printf("Updated incarnation numbers: %v\n", incarnationNumbers)
						}
					}
				}
			} else if message.Type == Introducer {
				update := message.Update
				addNewNode(update.Node)
			} else if message.Type == EnableSus {
				susEnabled = true
			} else if message.Type == DisableSus {
				susEnabled = false
			} else if message.Type == ChangeDropRate {
				dropRate = message.Update.DropRate
			}
		}
	}
}

func sendAcknowledgment(conn *net.UDPConn, remoteAddr *net.UDPAddr, message Message) error {
	if shouldDropMessage() {
		log.Printf("Dropping acknowledgment message to %s\n", remoteAddr)
		return nil
	}

	data, err := Marshal(message)
	if err != nil {
		log.Printf("error marshaling acknowledgment message: %v", err)
		return fmt.Errorf("error marshaling acknowledgment message: %v", err)
	}
	_, err = conn.WriteToUDP(data, remoteAddr)
	if err != nil {
		log.Printf("error sending acknowledgment: %v", err)
		return fmt.Errorf("error sending acknowledgment: %v", err)
	}

	return nil
}

func startNode(nodeAddr string) {
	nodeAddr = makeAddrPort(nodeAddr, PingPort)
	udpAddr, err := net.ResolveUDPAddr("udp", nodeAddr)
	if err != nil {
		log.Printf("Error resolving UDP address: %v\n", err)
		return
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Printf("Error creating UDP listener: %v\n", err)
		return
	}
	udpConnPing = conn // Store the connection in the global variable

	// Channel to receive packets

	packetChannel := make(chan Packet, 100)

	go func() {
		defer conn.Close()
		for {
			select {
			case <-stopChan:
				return
			default:
				buffer := make([]byte, 1024)
				n, _, err := conn.ReadFromUDP(buffer)
				bytesSyntax.Lock()
				//numOfBytes += n
				bytesSyntax.Unlock()
				if err != nil {
					log.Printf("Error reading from UDP: %v\n", err)
					continue
				}
				packetChannel <- Packet{Buffer: buffer[:n]}
			}
		}
	}()

	// Goroutine to process packets from the channel
	go func() {
		for packet := range packetChannel {
			var message PingMessage
			err := Unmarshal(packet.Buffer, &message)
			if err != nil {
				log.Printf("message before error: %v\n", packet.Buffer)
				log.Printf("Error unmarshalling message: %v\n", err)
				continue
			}

			if susEnabled && message.Suspected {
				mutex.Lock()
				incarnationNumbers[selfAddr]++
				mutex.Unlock()
				informAllNodes(Update{Node: selfAddr, Status: 0, IncarnationNumber: incarnationNumbers[selfAddr]}, selfAddr)
			}
			var ackMessage Message
			ackMessage.Source = selfAddr
			ackMessage.Type = Ack
			targetAddr := makeAddrPort(message.Source, AckPort)
			destAddr, err := net.ResolveUDPAddr("udp", targetAddr)
			if err != nil {
				log.Printf("Error resolving UDP address: %v\n", err)
				continue
			}
			err = sendAcknowledgment(conn, destAddr, ackMessage)
			if err != nil {
				log.Printf("Error sending acknowledgment: %v\n", err)
			}
		}
	}()
}

func periodicPing() {
	for {
		select {
		case <-stopChan:
			return
		default:
			targetAddr, error := getNextTarget()
			if error != nil {
				log.Printf("No ping target defined for node %s\n", nodeAddr)
				time.Sleep(5 * time.Second)
				continue
			}
			suspected := false
			if susEnabled && membershipList[targetAddr] == 1 {
				suspected = true
			}
			if membershipList[targetAddr] == 2 {
				continue
			}
			pingMessage := PingMessage{Source: selfAddr, Suspected: suspected}

			sendPing(selfAddr, targetAddr, pingMessage)
			time.Sleep(600 * time.Millisecond)
		}
	}
}

func informAllNodes(update Update, selfHostName string, msgType ...string) {

	var messageType string
	if len(msgType) > 0 {
		messageType = msgType[0]
	} else {
		messageType = Broadcast // Set your default message type here
	}

	for _, node := range listOfNodes {
		targetAddress := getHostnameFromNodename(node) + ":" + strconv.Itoa(BroadPort)
		if shouldDropMessage() {
			log.Printf("Dropping broadcast message to %s\n", targetAddress)
			continue
		}
		resolveAddr, err := net.ResolveUDPAddr("udp", targetAddress)
		if err != nil {
			log.Printf("Error resolving udp address: %v\n", err)
			return
		}
		conn, err := net.DialUDP("udp", nil, resolveAddr)
		if err != nil {
			log.Printf("Error dialing udp connection: %v\n", err)
			return
		}
		message := Message{Type: messageType, Source: selfHostName, Update: update}
		data, err := Marshal(message)
		if err != nil {
			log.Printf("Error marshalling update: %v\n", err)
			return
		}
		defer conn.Close()
		conn.Write(data)
	}
}

func leaveCluster() {

	// Signal goroutines to stop
	close(stopChan)

	// Close the UDP listener connection
	if udpConnBroadcast != nil {
		udpConnBroadcast.Close()
	}
	if udpConnPing != nil {
		udpConnPing.Close()
	}

	// Clean up resources
	log.Printf("Node %s has left the cluster.\n", selfAddr)
}

// func logBandwidth() {
// 	for {
// 		timeTenSecondsFromNow := time.Now().Add(10 * time.Second)
// 		for time.Now().Before(timeTenSecondsFromNow) {
// 			continue
// 		}
// 		// Print float value of NumofBytes divied by 10
// 		// Reset NumofBytes to 0
// 		bytesSyntax.Lock()
// 		bandwidth := float64(numOfBytes) / 10
// 		numOfBytes = 0
// 		bytesSyntax.Unlock()
// 		fmt.Printf("Bandwidth: %f\n", bandwidth)
// 	}
// }

func main() {
	// Make channel for append requests
	appendRequests := make(chan AppendRequest, 1500)
	// Start a goroutine to handle append requests
	go func() {
		for req := range appendRequests {
			// fmt.Printf("Appending file %s to server %s\n", req.LocalFileName, req.HyDFSFileName)
			err := appendFileOnServer(req.LocalFileName, req.HyDFSFileName)
			if err != nil {
				log.Printf("Error appending file on server: %v\n", err)
			}
		}
	}()

	selfHostName, err := os.Hostname()
	if err != nil {
		log.Printf("Error getting hostname: %v\n", err)
		return
	}
	fileService := NewFileService()
	rpc.Register(fileService)
	fileMergeService := new(FileMergeService)
	rpc.Register(fileMergeService)
	// listenerAppend, err := net.Listen("tcp", ":8001")
	// if err != nil {
	// 	fmt.Println("Error starting server:", err)
	// 	return
	// }
	// defer listenerAppend.Close()

	// fmt.Println("Server started on port 8001")
	// rpc.Accept(listenerAppend)
	fileWriteService := new(FileWriteService)
	fileReadService := new(FileReadService)
	fileWriteFromReplicaService := new(FileWriteFromReplicaService)
	rpc.Register(fileWriteFromReplicaService)
	rpc.Register(fileWriteService)
	rpc.Register(fileReadService)

	listener, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal("Listener error:", err)
	}
	getListener, getErr := net.Listen("tcp", ":8001")
	if getErr != nil {
		fmt.Println("Error starting server:", getErr)
		log.Fatal("Listener error:", getErr)
	}
	log.Println("Serving RPC server on port 8000")
	go func() {
		for {
			conn, err := getListener.Accept()
			if err != nil {
				fmt.Println("Connection error:", err)
				log.Println("Connection error:", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
	// Accept connections
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println("Connection error:", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	// Delete contents of ./hydfs/ directory
	entries, err := os.ReadDir("./hydfs/")
	if err != nil {
		log.Printf("Error reading hydfs directory: %v\n", err)
		return
	}

	for _, entry := range entries {
		err = os.RemoveAll("./hydfs/" + entry.Name())
		if err != nil {
			log.Printf("Error removing %s: %v\n", entry.Name(), err)
		}
	}

	// clientCache := Cache{cacheMap: make(map[string]CacheValue), byteCapacity: 1024 * 1024 * 2, numberOfBytesUsed: 0}

	reader := bufio.NewReader(os.Stdin)
	for {
		userInput, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading input: %v\n", err)
			return
		}
		if userInput == "join\n" {
			stopChan = make(chan struct{}) // Reinitialize the stop channel
			conn, err := net.Dial("tcp", IntroducerAddr+":8000")
			if err != nil {
				log.Printf("Error connecting to introducer: %v\n", err)
				return
			}
			client := rpc.NewClient(conn)
			joinRequest := JoinRequest{Addr: selfHostName}
			introduction := Introduction{}
			err = client.Call("FileServer.Join", joinRequest, &introduction)
			if err != nil {
				log.Printf("Error joining cluster: %v\n", err)
				return
			}
			mutex.Lock()
			listOfNodes = introduction.ListOfNodes
			for _, node := range listOfNodes {
				listOfHashes = append(listOfHashes, ListNode{Name: node, Hash: HashString(node), Alive: true})
			}
			membershipList = introduction.MembershipList
			selfAddr = introduction.SelfAddress
			listOfHashes = append(listOfHashes, ListNode{Name: selfAddr, Hash: HashString(selfAddr)})
			incarnationNumbers = introduction.IncarnationNumbers
			susEnabled = introduction.SusEnabled
			dropRate = introduction.DropRate
			mutex.Unlock()
			sortListOfHashes()

			// // remove introducer from listofhashes
			// for i, node := range listOfHashes {
			// 	if strings.HasPrefix(node.Name, "fa24-cs425-3101.cs.illinois.edu") {
			// 		listOfHashes = append(listOfHashes[:i], listOfHashes[i+1:]...)
			// 		break
			// 	}
			// }

			go periodicPing()
			go startNode(selfHostName)
			go broadcastListener()
			// go logBandwidth()
		} else if userInput == "list_self\n" {
			fmt.Printf("Self address: %s\n", selfAddr)
		} else if userInput == "list_mem\n" {
			prettyPrintMap(membershipList)
		} else if userInput == "enable_sus\n" {
			susEnabled = true
			informAllNodes(Update{}, selfAddr, EnableSus)
		} else if userInput == "disable_sus\n" {
			susEnabled = false
			informAllNodes(Update{}, selfAddr, DisableSus)
		} else if userInput == "leave\n" {
			leaveCluster()
			fmt.Printf("Leaving at %s\n", time.Now())
		} else if userInput == "status_sus\n" {
			if susEnabled {
				fmt.Printf("Suspicion is enabled\n")
			} else {
				fmt.Printf("Suspicion is disabled\n")
			}
		} else if strings.HasPrefix(userInput, "change_drop_rate") {
			parts := strings.Split(userInput, " ")
			if len(parts) == 2 {
				newDropRate, err := strconv.Atoi(parts[1][:len(parts[1])-1])
				if err != nil {
					fmt.Printf("Invalid drop rate value: %v\n", err)
					continue
				}
				informAllNodes(Update{DropRate: newDropRate}, selfAddr, ChangeDropRate)
				fmt.Printf("Drop rate changed to %d\n", newDropRate)
			} else {
				fmt.Printf("Invalid command format. Use: change_drop_rate <value>\n")
			}
		} else if userInput == "print_listofhashes\n" {
			fmt.Printf("List of hashes: %v\n", listOfHashes)
		} else if userInput == "test_file_map\n" {
			filename := "test.txt"
			fileHash := HashString(filename)
			server, err := findServerGivenFileHash(fileHash)
			if err != nil {
				log.Printf("Error finding server hash: %v\n", err)
				continue
			}
			fmt.Printf("File hash: %d\n, Server Hash: %d\n", fileHash, server.Hash)
		} else if strings.HasPrefix(userInput, "create") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 3 {
				log.Println("Invalid input format. Expected: createfile srcfile destfile")
				continue
			}
			fmt.Printf("Creating file %s on server %s\n", inputParts[1], inputParts[2])

			srcFile := inputParts[1]
			srcFile = "./local/" + srcFile
			destFile := inputParts[2]
			destFile = "./hydfs/" + destFile
			// fmt.Printf("Hash of file: %d\n", HashString(destFile))
			destNode, err := findServerGivenFileHash(HashString(destFile))
			if err != nil {
				log.Printf("Error finding server given file hash: %v\n", err)
				continue
			}
			destNodeName := destNode.Name
			//fileCounter[destFile] = 0
			destFile = addCounterToFilename(destFile, 0)
			replicas := getReplicas(destNodeName)
			fmt.Printf("Primary node: %s\n", destNodeName)
			fmt.Printf("Replicas: %v\n", replicas)

			go func() {
				err = writeFileOnServer(srcFile, destFile, destNodeName, 1)
			}()
			if err != nil {
				log.Printf("Error writing file on server: %v\n", err)
			}

			for _, replica := range replicas {
				go func(replica string) {
					err = writeFileOnServer(srcFile, destFile, replica, 2)
					if err != nil {
						log.Printf("Error writing file on server: %v\n", err)
					}
				}(replica)
			}

			fmt.Printf("File %s created on server %s successfully\n", destFile, destNodeName)

			// Replicate the file on next two successors
			//sendReplicateRequest(destFile, destNodeName)
		} else if strings.HasPrefix(userInput, "getfromreplica") {
			fmt.Printf("Getting file from replica\n")
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 4 {
				fmt.Println("Invalid input format. Expected: getfromreplica servername srcfile destfile")
				log.Println("Invalid input format. Expected: getfromreplica srcfile destfile")
				continue
			}
			localFilename := "./local/" + inputParts[3]
			fmt.Printf("Local filename: %s\n", localFilename)
			hydfsFilename := "./hydfs/" + inputParts[2]
			fmt.Printf("Hydfs filename: %s\n", hydfsFilename)
			serverName := inputParts[1]
			fmt.Printf("Server name: %s\n", serverName)
			hydfsFilename = addCounterToFilename(hydfsFilename, 0)
			err := writeFileFromReplica(localFilename, hydfsFilename, serverName)
			if err != nil {
				fmt.Printf("Error writing file on server: %v\n", err)
				log.Printf("Error writing file on server: %v\n", err)
			}

		} else if strings.HasPrefix(userInput, "get") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 3 {
				log.Println("Invalid input format. Expected: readfile filename")
				continue
			}
			// fmt.Printf("Reading file %s\n", inputParts[1])
			hydfsFilename := "./hydfs/" + inputParts[1]
			localFilename := "./local/" + inputParts[2]

			// value, boo := clientCache.get(hydfsFilename)
			// if boo {
			// 	// fmt.Printf("File %s found in cache\n", hydfsFilename)
			// 	err := os.WriteFile(localFilename, []byte(value), 0644)
			// 	if err != nil {
			// 		log.Printf("Error writing file to OS: %v\n", err)
			// 	}
			// 	// fmt.Printf("File contents: \n%s\n", value)
			// 	continue
			// }

			err := readFileFromServer(hydfsFilename, localFilename)
			if err != nil {
				log.Printf("Error reading file from server: %v\n", err)
			}
			// // Read the directory to get all files
			// files, err := os.ReadDir("./local/")
			// if err != nil {
			// 	log.Printf("Error reading local directory: %v\n", err)
			// 	return
			// }

			localReadFile, err := os.Open(localFilename)
			if err != nil {
				log.Printf("Error reading file from OS: %v\n", err)
				continue
			}
			defer localReadFile.Close()

			// fileContents, err := io.ReadAll(localReadFile)
			// if err != nil {
			// 	log.Printf("Error reading file from OS: %v\n", err)
			// 	continue
			// }

			// clientCache.put(hydfsFilename, string(fileContents))
			// fmt.Printf("File %s read successfully\n", hydfsFilename)
			// fmt.Printf("Contents of file %s:\n%s\n", hydfsFilename, string(fileContents))

			// // Filter files based on the prefix
			// prefix := inputParts[2]
			// for _, file := range files {
			// 	if strings.HasPrefix(file.Name(), prefix) {
			// 		fileContent, err := os.ReadFile("./local/" + file.Name())
			// 		if err != nil {
			// 			log.Printf("Error reading file %s from OS: %v\n", file.Name(), err)
			// 			continue
			// 		}
			// 		fmt.Printf("Contents of %s:\n%s\n", file.Name(), string(fileContent))
			// 	}
			// }
			fmt.Printf("File %s read successfully\n", hydfsFilename)

		} else if userInput == "store\n" {
			// print list of files in hydfs folder
			files, err := os.ReadDir("./hydfs/")
			if err != nil {
				log.Printf("Error reading hydfs directory: %v\n", err)
				continue
			}
			fmt.Printf("Files stored in hydfs:\n")
			for _, file := range files {
				fmt.Printf("%s\n", file.Name())
			}
		} else if strings.HasPrefix(userInput, "append") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 3 {
				log.Println("Invalid input format. Expected: appendfile srcfile destfile")
				continue
			}
			localFilename := "./local/" + inputParts[1]
			hydfsFilename := "./hydfs/" + inputParts[2]
			// clientCache.remove(hydfsFilename)
			// Send append request to the channel
			appendRequests <- AppendRequest{
				LocalFileName: localFilename,
				HyDFSFileName: hydfsFilename,
			}
			fmt.Printf("Appended file %s to file %s\n", inputParts[1], inputParts[2])
		} else if strings.HasPrefix(userInput, "merge") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 2 {
				log.Println("Invalid input format. Expected: appendfile srcfile destfile")
				continue
			}
			filename := "./hydfs/" + inputParts[1]
			err := mergeFiles(filename)
			if err != nil {
				log.Printf("Error merging files: %v\n", err)
			}
			fmt.Printf("Merged files successfully\n")
		} else if userInput == "wait\n" {
			time.Sleep(5 * time.Second)
		} else if strings.HasPrefix(userInput, "ls") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 2 {
				log.Println("Invalid input format. Expected: appendfile srcfile destfile")
				continue
			}
			filename := "./hydfs/" + inputParts[1]
			err := listHostNames(filename, inputParts[1])
			if err != nil {
				log.Printf("Error merging files: %v\n", err)
			}
		} else if userInput == "list_mem_ids\n" {
			printRingMembershipList()
		} else if strings.HasPrefix(userInput, "multiappend") {
			inputParts := strings.Fields(userInput)
			numberOfArguments := len(inputParts)
			if (numberOfArguments)%2 != 0 {
				fmt.Printf("Wrong number of arguments\n")
				continue
			}
			if numberOfArguments < 4 {
				fmt.Printf("Invalid command - too few arguments\n")
				continue
			}

			targetFilename := inputParts[1]
			args := multiAppendArgs{FileName: targetFilename, VMFileMap: make(map[string]string)}
			for i := 2; i < numberOfArguments; i += 2 {
				vm := inputParts[i]
				appendFile := inputParts[i+1]
				args.VMFileMap[vm] = appendFile
			}

			err := multiAppendFileOnServer(args)
			if err != nil {
				fmt.Printf("Error appending file on server: %v\n", err)
				log.Printf("Error appending file on server: %v\n", err)
			}

		} else if strings.HasPrefix(userInput, "thousand") {
			// create the 10mb file in hydfs
			// run RPC to append concurrently to that file with a 10kb file
			// specify number of concurrent appends
			inputParts := strings.Fields(userInput)
			numberOfServers, _ := strconv.Atoi(string(inputParts[1]))

			// create the 10mb file
			// srcFile := "./local/business_49.txt"
			// destFile := "./hydfs/10mb.txt"
			dstFileWithoutCounter := "10mb.txt"
			// destNode, _ := findServerGivenFileHash(HashString(destFile))
			// destNodeName := destNode.Name
			// destFile = addCounterToFilename(destFile, 0)
			// replicas := getReplicas(destNodeName)
			// writeFileOnServer(srcFile, destFile, destNodeName, 1)
			// for _, replica := range replicas {
			// 	writeFileOnServer(srcFile, destFile, replica, 2)
			// }

			// rpc stuff
			fileThatIsAppended := "business_51.txt"
			numberOfAppendsPerServer := 1000 / numberOfServers
			for i := 0; i < numberOfServers; i++ {
				serverName := listOfHashes[i]
				if serverName.Alive {
					go runNumberOfAppendsFromServer(fileThatIsAppended, dstFileWithoutCounter, serverName.Name, numberOfAppendsPerServer)
				}
			}

		} else if userInput == "currenttime\n" {
			fmt.Printf("Current time: %s\n", time.Now())
		} else if strings.HasPrefix(userInput, "printLocalFile") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 2 {
				log.Println("Invalid input format. Expected: printLocalFile filename")
				continue
			}
			filename := "./local/" + inputParts[1]
			fileContent, err := os.ReadFile(filename)
			if err != nil {
				log.Printf("Error reading file from OS: %v\n", err)
				continue
			}
			fmt.Printf("Contents of %s:\n%s\n", filename, string(fileContent))
		} else if strings.HasPrefix(userInput, "head") {
			inputParts := strings.Fields(userInput)
			if len(inputParts) != 2 {
				log.Println("Invalid input format. Expected: head filename")
				continue
			}
			// read 10 lines of the local file
			filename := "./local/" + inputParts[1]
			file, err := os.Open(filename)
			if err != nil {
				log.Printf("Error opening file: %v\n", err)
				continue
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			lineCount := 0
			for scanner.Scan() {
				fmt.Println(scanner.Text())
				lineCount++
				if lineCount == 10 {
					break
				}
			}
		} else {
			fmt.Printf("Invalid command\n")
			continue
		}
	}
}

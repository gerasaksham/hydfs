package main

import (
	"bufio"
	"encoding/json"
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

// find self address

var (
	listOfNodes        = []string{}
	membershipList     = map[string]int{}
	incarnationNumbers = map[string]int{}
	mutex              sync.Mutex
	selfHostName       string
	selfAddr           string
	susEnabled         = true
	dropRate           = 0
	logFile            *os.File
	fileMap            = make(map[string]int)
	fileCounter        = make(map[string]int)
	listOfHashes       = []ListNode{}
)

type ListNode struct {
	Name  string
	Hash  uint32
	Alive bool
}

type Introduction struct {
	MembershipList     map[string]int // include self and introducer
	IncarnationNumbers map[string]int
	SelfAddress        string
	ListOfNodes        []string // don't include self
	SusEnabled         bool
	DropRate           int
}

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
}

type Update struct {
	Node              string
	Status            int // 0 for alive, 1 for suspected, 2 for dead
	DropRate          int
	IncarnationNumber int
}

type Message struct {
	Type   string
	Source string
	Update Update
}

type FileServer struct{}

type FileWriteFromReplicaService struct{}

func shouldDropMessage() bool {
	return rand.Intn(100) < dropRate
}

const (
	Ping           string = "ping"
	Ack            string = "ack"
	PingPort       int    = 8080
	AckPort        int    = 8081
	BroadPort      int    = 8082
	AppendPort     int    = 8000
	Introducer     string = "Introducer"
	Broadcast      string = "Broadcast"
	EnableSus      string = "EnableSus"
	DisableSus     string = "DisableSus"
	ChangeDropRate string = "ChangeDropRate"
)

func handleUpdate(update Update) {
	mutex.Lock()
	defer mutex.Unlock()
	if update.Status == 0 {
		if !susEnabled {
			if _, ok := membershipList[update.Node]; !ok {
				membershipList[update.Node] = 0
			}
		} else {
			if _, ok := membershipList[update.Node]; !ok {
				if update.IncarnationNumber > incarnationNumbers[update.Node] && update.IncarnationNumber != 2 {
					membershipList[update.Node] = 0
					incarnationNumbers[update.Node] = update.IncarnationNumber
				}
			}
		}
	} else if update.Status == 1 {
		if susEnabled {
			if _, ok := membershipList[update.Node]; !ok {
				if update.IncarnationNumber >= incarnationNumbers[update.Node] && update.IncarnationNumber != 2 {
					membershipList[update.Node] = 1
					incarnationNumbers[update.Node] = update.IncarnationNumber
				}
			}
		}
	} else if update.Status == 2 {
		// check if the node is in the membership list
		if _, ok := membershipList[update.Node]; !ok {
			return
		}
		delete(membershipList, update.Node)
		// find node in list of hashes and mark it as failed
		for i, n := range listOfHashes {
			if n.Name == update.Node {
				listOfHashes[i].Alive = false
				break
			}
		}

		fmt.Printf("listofhashes: %v\n", listOfHashes)

		go rebalanceFilesAfterNodeFailure(update.Node)

		for i, node := range listOfNodes {
			if node == update.Node {
				listOfNodes = append(listOfNodes[:i], listOfNodes[i+1:]...)
				break
			}
		}
		log.Printf("Updated membership list: %v\n", membershipList)
		log.Printf("Updated list of nodes: %v\n", listOfNodes)
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
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			log.Printf("Error reading udp packet: %v\n", err)
			continue
		}
		if n == 0 {
			continue
		}
		var message Message
		err = Unmarshal(buf[:n], &message)
		if err != nil {
			log.Printf("Error unmarshalling message: %v\n", err)
			continue
		}
		if message.Type == Broadcast {
			update := message.Update
			handleUpdate(update)
		} else if message.Type == EnableSus {
			susEnabled = true
		} else if message.Type == DisableSus {
			susEnabled = false
		} else if message.Type == ChangeDropRate {
			dropRate = message.Update.DropRate
		}
	}
}

func Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func getHostnameFromNodename(node string) string {
	parts := strings.Split(node, "|")
	return parts[0]
}

type JoinRequest struct {
	Addr string
}

func informAllNodes(update Update, selfHostName string, msgType ...string) {
	// fmt.Printf("List of nodes: %v\n", listOfNodes)
	var messageType string
	if len(msgType) > 0 {
		messageType = msgType[0]
	} else {
		messageType = Introducer // Set your default message type here
	}

	for _, node := range listOfNodes {
		if node == update.Node {
			continue
		}
		targetAddress := getHostnameFromNodename(node) + ":" + strconv.Itoa(BroadPort)
		if shouldDropMessage() {
			fmt.Printf("Dropping broadcast message to %s\n", targetAddress)
			continue
		}
		resolveAddr, err := net.ResolveUDPAddr("udp", targetAddress)
		if err != nil {
			fmt.Printf("Error resolving udp address: %v\n", err)
			return
		}
		conn, err := net.DialUDP("udp", nil, resolveAddr)
		if err != nil {
			fmt.Printf("Error dialing udp connection: %v\n", err)
			return
		}
		message := Message{Type: messageType, Source: selfHostName, Update: update}
		data, err := Marshal(message)
		if err != nil {
			fmt.Printf("Error marshalling update: %v\n", err)
			return
		}
		defer conn.Close()
		conn.Write(data)
	}
}

func prettyPrintMap(m map[string]int) {
	jsonData, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		log.Println("Error marshaling map:", err)
		return
	}
	fmt.Println(string(jsonData))
}

func (fs *FileServer) Join(joinRequest JoinRequest, introduction *Introduction) error {
	mutex.Lock()
	defer mutex.Unlock()
	fmt.Printf("Join request received from: %v\n", joinRequest.Addr)
	log.Printf("Join request received from: %v\n", joinRequest.Addr)
	newNodeAddress := joinRequest.Addr
	newNodeAddress += "|" + strconv.FormatInt(time.Now().Unix(), 10)
	// newListOfNodes := append(listOfNodes, selfHostName)
	newListOfNodes := []string{}
	for _, node := range listOfNodes {
		newListOfNodes = append(newListOfNodes, node)
	}
	newListOfNodes = append(newListOfNodes, selfHostName)
	membershipList[newNodeAddress] = 0
	listOfNodes = append(listOfNodes, newNodeAddress)
	incarnationNumbers[newNodeAddress] = 0
	listOfHashes = append(listOfHashes, ListNode{Name: newNodeAddress, Hash: HashString(newNodeAddress), Alive: true})
	sortListOfHashes()
	introductionObject := Introduction{MembershipList: membershipList, SelfAddress: newNodeAddress, ListOfNodes: newListOfNodes, IncarnationNumbers: incarnationNumbers, SusEnabled: susEnabled, DropRate: dropRate}
	*introduction = introductionObject
	go informAllNodes(Update{Node: newNodeAddress, Status: 0, IncarnationNumber: 0}, selfHostName)
	fmt.Printf("New node joined: %s\n", newNodeAddress)
	return nil
}

func main() {
	var fileServer FileServer
	rpc.Register(&fileServer)
	fileWriteService := new(FileWriteService)
	fileReadService := new(FileReadService)
	fileService := new(FileService)
	fileMergeService := new(FileMergeService)
	fileWriteFromReplicaService := new(FileWriteFromReplicaService)
	rpc.Register(fileWriteFromReplicaService)
	rpc.Register(fileWriteService)
	rpc.Register(fileReadService)
	rpc.Register(fileService)
	rpc.Register(fileMergeService)

	selfHostName, _ = os.Hostname()
	fmt.Println("Starting introducer on: ", selfHostName)
	selfHostName += "|" + strconv.FormatInt(time.Now().Unix(), 10)
	selfAddr = selfHostName
	mutex.Lock()
	membershipList[selfHostName] = 0
	listOfHashes = append(listOfHashes, ListNode{Name: selfHostName, Hash: HashString(selfHostName), Alive: true})
	mutex.Unlock()
	getListener, getErr := net.Listen("tcp", ":8001")
	if getErr != nil {
		log.Fatal("Listener error:", getErr)
	}
	log.Println("Serving RPC server on port 8000")
	go func() {
		for {
			conn, err := getListener.Accept()
			if err != nil {
				log.Println("Connection error:", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
	go func() {
		l, e := net.Listen("tcp", ":8000")
		if e != nil {
			log.Fatal("listen error:", e)
		}
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Fatal("accept error:", err)
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

	go broadcastListener()
	reader := bufio.NewReader(os.Stdin)
	for {
		userInput, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			return
		}
		if userInput == "list_self\n" {
			fmt.Printf("Self address: %s\n", selfHostName)
		} else if userInput == "list_mem\n" {
			prettyPrintMap(membershipList)
		} else if userInput == "enable_sus\n" {
			susEnabled = true
			informAllNodes(Update{}, selfHostName, EnableSus)
		} else if userInput == "disable_sus\n" {
			susEnabled = false
			informAllNodes(Update{}, selfHostName, DisableSus)
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
				informAllNodes(Update{DropRate: newDropRate}, selfHostName, ChangeDropRate)
				fmt.Printf("Drop rate changed to %d\n", newDropRate)
			} else {
				fmt.Printf("Invalid command format. Use: change_drop_rate <value>\n")
			}
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
		}
	}
}

package impl

import (
	"bytes"
	"crypto"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {

	peerAddress := conf.Socket.GetAddress()

	node := node{
		conf:                            conf,
		address:                         peerAddress,
		stop:                            make(chan struct{}, 3),
		catalog:                         Catalog{catalog: make(map[string]map[string]struct{})},
		routingTable:                    RoutingTable{table: map[string]string{peerAddress: peerAddress}},
		rumorLists:                      RumorLists{rumorLists: map[string][]types.Rumor{peerAddress: make([]types.Rumor, 0)}},
		ackWaitList:                     AckWaitList{list: make(map[string]chan struct{})},
		dataRequestWaitList:             DataRequestWaitList{list: make(map[string]chan []byte)},
		searchRequestWaitList:           SearchRequestWaitList{list: make(map[string]chan []types.FileInfo)},
		paxosCollectingPromisesWaitList: PaxosCollectingPromisesWaitList{promisesChannels: make(map[uint]chan struct{})},
		paxosCollectingAcceptsWaitList:  PaxosCollectingAcceptsWaitList{acceptsChannels: make(map[string]chan struct{})},
		processedSearchRequest:          make([]string, 0),
		nbRunningRoutines:               0,
		waitGroup:                       sync.WaitGroup{},
		TLCstep:                         0,
		paxosAcceptedId:                 0,
		receivedTLCMsgs:                 0,
		paxosMaxId:                      0,
		paxosPhase:                      1,
		upcomingTLCMsgs:                 make([]types.TLCMessage, 0),
		collectedPromises:               make([]types.PaxosPromiseMessage, 0),
		collectedAccepts:                make([]types.PaxosAcceptMessage, 0),
	}

	node.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, node.processAckMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, node.processRumorsMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, node.processChatMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, node.processStatusMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, node.processPrivateMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, node.processEmptyMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.DataReplyMessage{}, node.processDataReplyMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.DataRequestMessage{}, node.processDataRequestMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.SearchRequestMessage{}, node.processSearchRequestMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.SearchReplyMessage{}, node.processSearchReplyMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.PaxosPrepareMessage{}, node.processPaxosPrepareMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.PaxosProposeMessage{}, node.processPaxosProposeMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.PaxosAcceptMessage{}, node.processPaxosAcceptMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.PaxosPromiseMessage{}, node.processPaxosPromiseMessage())
	node.conf.MessageRegistry.RegisterMessageCallback(types.TLCMessage{}, node.processTLCMessage())

	return &node

}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf         peer.Configuration
	stop         chan struct{}
	catalog      Catalog
	routingTable RoutingTable
	address      string
	rumorLists   RumorLists

	ackWaitList            AckWaitList
	dataRequestWaitList    DataRequestWaitList
	searchRequestWaitList  SearchRequestWaitList
	processedSearchRequest []string

	nbRunningRoutines uint
	waitGroup         sync.WaitGroup

	upcomingTLCMsgs    []types.TLCMessage
	receivedTLCMsgs    uint
	TLCstep            uint
	paxosMaxId         uint
	paxosAcceptedId    uint
	paxosAcceptedValue *types.PaxosValue
	paxosPhase         uint
	collectedPromises  []types.PaxosPromiseMessage
	collectedAccepts   []types.PaxosAcceptMessage

	paxosCollectingPromisesWaitList PaxosCollectingPromisesWaitList
	paxosCollectingAcceptsWaitList  PaxosCollectingAcceptsWaitList
}

//====================================================HW0==========================================================================================

// launches the given func as a goroutine if the condition is respected
func (n *node) launchRoutineWithFun(f func(), cond bool) {
	if !cond {
		return
	}

	n.nbRunningRoutines += 1
	n.waitGroup.Add(1)
	go f()
}

func (n *node) Start() error {
	n.launchRoutineWithFun(n.ProcessTraffic, true)
	n.launchRoutineWithFun(n.AntiEntropy, n.conf.AntiEntropyInterval > 0)
	n.launchRoutineWithFun(n.HeartBeat, n.conf.HeartbeatInterval > 0)
	return nil
}

func (n *node) Stop() error {

	for i := 0; i < int(n.nbRunningRoutines); i += 1 {
		n.stop <- struct{}{}
	}
	close(n.stop)
	n.waitGroup.Wait()
	return nil
}

func (n *node) ProcessTraffic() {
	for {
		select {
		case <-n.stop:

			n.waitGroup.Done()
			return
		default:
			pkt, err := n.conf.Socket.Recv(time.Second * 1)
			if errors.Is(err, transport.TimeoutErr(0)) {
				continue
			}

			if err != nil {
				xerrors.Errorf("Failed to receive packet due to another error that an timeout: %v", err)
			}

			peerAddress := n.conf.Socket.GetAddress()
			pktDestAddress := pkt.Header.Destination

			//process the packet
			if peerAddress == pktDestAddress || pktDestAddress == "" {
				processErr := n.conf.MessageRegistry.ProcessPacket(pkt)

				if processErr != nil {
					xerrors.Errorf("Failed to process packet: %v", processErr)
				}

			}
			//relay the packet
			if pktDestAddress != peerAddress {
				//update the header and send the packet to its destination, or rebroadcast it
				pkt.Header.RelayedBy = peerAddress
				pkt.Header.TTL -= 1

				nextHopAddr := n.routingTable.getRelayAddr(pktDestAddress)

				if nextHopAddr != "" {
					sendErr := n.conf.Socket.Send(nextHopAddr, pkt, n.conf.AckTimeout)
					if sendErr != nil {
						xerrors.Errorf("Failed to forward: %v", sendErr)
					}
				}
			}
		}
	}
}

func (n *node) Unicast(dest string, msg transport.Message) error {

	if dest == "" {
		return xerrors.Errorf("Empty destination: %v", dest)
	}
	peerAddress := n.conf.Socket.GetAddress()
	neigh := n.routingTable.getRelayAddr(dest)
	if neigh == "" {
		return xerrors.Errorf("No entry in routing table to destination %v", neigh)
	}

	header := transport.NewHeader(peerAddress, peerAddress, dest, 0)
	packet := transport.Packet{Header: &header, Msg: &msg}
	sendErr := n.conf.Socket.Send(neigh, packet, n.conf.AckTimeout)

	if sendErr != nil {

		return sendErr
	}
	return nil
}

func (n *node) AddPeer(addr ...string) {

	if len(addr) == 0 {
		//nothing to add
		return
	}

	n.routingTable.lock.Lock()
	defer n.routingTable.lock.Unlock()

	for i := range addr {
		newAddress := addr[i]
		n.routingTable.table[newAddress] = newAddress
	}
}

func (n *node) GetRoutingTable() peer.RoutingTable {

	n.routingTable.lock.RLock()
	defer n.routingTable.lock.RUnlock()

	var copy peer.RoutingTable = make(peer.RoutingTable)
	for k, v := range n.routingTable.table {
		copy[k] = v
	}
	return copy
}

func (n *node) SetRoutingEntry(origin, relayAddr string) {
	n.routingTable.lock.Lock()
	defer n.routingTable.lock.Unlock()

	if relayAddr == "" {
		delete(n.routingTable.table, origin)
	} else {
		n.routingTable.table[origin] = relayAddr
	}
}

//====================================================HW1==========================================================================================

func (n *node) Broadcast(msg transport.Message) error {

	header := transport.NewHeader(n.address, n.address, n.address, 0)
	pkt := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	err := n.conf.MessageRegistry.ProcessPacket(pkt)
	if err != nil {
		return err
	}

	randomNeighAddr := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(n.address))

	inputMsg := msg.Copy()

	rumor := types.Rumor{
		Origin:   n.address,
		Sequence: n.GetOwnRumorSeq() + 1,
		Msg:      &inputMsg,
	}

	n.rumorLists.UpdateRumorSeqOf(n.address, rumor)

	rumorsArray := []types.Rumor{rumor}

	if randomNeighAddr == "" {
		log.Printf("Node has no neighbor")
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	ackID, sendErr := n.SendRumorMsg(randomNeighAddr, rumorsArray)

	if sendErr != nil {
		return sendErr
	}

	go n.waitForAck(ackID, rumorsArray, randomNeighAddr)
	return nil

}

func (n *node) AntiEntropy() {

	ticker := time.NewTicker(n.conf.AntiEntropyInterval)

	for {
		select {
		case <-n.stop:
			ticker.Stop()
			n.waitGroup.Done()
			return
		case <-ticker.C:
			rdmNeighor := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(n.address))
			n.SendStatusMessage(rdmNeighor)
		}
	}
}

func (n *node) HeartBeat() {

	n.Broadcast(n.CreateEmptyMessage())

	ticker := time.NewTicker(n.conf.HeartbeatInterval)
	for {
		select {
		case <-n.stop:
			n.waitGroup.Done()
			return
		case <-ticker.C:
			n.Broadcast(n.CreateEmptyMessage())
		}
	}
}

func (n *node) CreateEmptyMessage() transport.Message {
	emptyMsg := types.EmptyMessage{}
	trsptEmptyMsg, _ := n.conf.MessageRegistry.MarshalMessage(emptyMsg)
	return trsptEmptyMsg
}

func (n *node) CreateStatusMessage() types.StatusMessage {
	currentNodeStatus := n.rumorLists.ConvertRumorsToSeq()
	return currentNodeStatus
}

func (n *node) SendStatusMessage(address string) error {

	statusMsg := n.CreateStatusMessage()

	statusMsgHeader := transport.NewHeader(n.address, n.address, address, 0)

	trsptStatusMsg, statusErr := n.conf.MessageRegistry.MarshalMessage(statusMsg)
	if statusErr != nil {
		return statusErr
	}

	pkt := transport.Packet{
		Header: &statusMsgHeader,
		Msg:    &trsptStatusMsg,
	}

	sendErr := n.conf.Socket.Send(address, pkt, 0)
	if sendErr != nil {
		return sendErr
	}

	return nil

}

func (n *node) SendRumorMsg(address string, rumors []types.Rumor) (string, error) {
	header := transport.NewHeader(n.address, n.address, address, 0)
	msg := types.RumorsMessage{Rumors: rumors}

	trsprtMsg, marshErr := n.conf.MessageRegistry.MarshalMessage(&msg)
	if marshErr != nil {
		return "", marshErr
	}

	pkt := transport.Packet{
		Header: &header,
		Msg:    &trsprtMsg,
	}

	sendErr := n.conf.Socket.Send(address, pkt, n.conf.AckTimeout)
	return header.PacketID, sendErr

}

func (n *node) waitForAck(ackID string, rumors []types.Rumor, oldAddr string) {

	ackChannel := make(chan struct{})
	n.ackWaitList.addEntry(ackID, ackChannel)

	timer := &time.Timer{}

	if n.conf.AckTimeout != 0 {
		timer = time.NewTimer(n.conf.AckTimeout)
	}

	for {
		select {
		case <-ackChannel:
			timer.Stop()
			close(ackChannel)
			n.ackWaitList.removeEntry(ackID)
			return
		case <-timer.C:
			newAckID, newAddr := n.resendRumor(oldAddr, rumors)
			go n.waitForAck(newAckID, rumors, newAddr)
			return
		}
	}
}

func (n *node) resendRumor(oldAddr string, rumors []types.Rumor) (string, string) {

	rand.Seed(time.Now().UnixNano())

	rdmAddr := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(oldAddr, n.address))

	newAck, _ := n.SendRumorMsg(rdmAddr, rumors)
	return newAck, rdmAddr

}

func (n *node) isNeighbor(addr string) bool {
	n.routingTable.lock.Lock()
	defer n.routingTable.lock.Unlock()

	if relay, ok := n.routingTable.table[addr]; ok {
		return addr == relay
	} else {
		return false
	}
}

//Process a packet through the messageRegistry
func (n *node) ProcessPacket(header *transport.Header, msg *transport.Message) error {

	newPkt := transport.Packet{Header: header, Msg: msg}
	err := n.conf.MessageRegistry.ProcessPacket(newPkt)

	return err
}

//====================================================HW2==========================================================================================

//Clean up, if time
func (n *node) Upload(data io.Reader) (metahash string, err error) {

	bytesBuf := new(bytes.Buffer)
	bytesBuf.ReadFrom(data)
	buf := bytesBuf.Bytes()

	blobStore := n.conf.Storage.GetDataBlobStore()
	nbChunks := (len(buf) / int(n.conf.ChunkSize))

	var metaFileValue []byte
	metaFileValueSep := ""
	for i := 0; i < nbChunks; i++ {
		if i != 0 {
			metaFileValueSep += peer.MetafileSep
		}
		idx := i * int(n.conf.ChunkSize)
		chunk := buf[idx : idx+int(n.conf.ChunkSize)]
		h := crypto.SHA256.New()
		_, err = h.Write(chunk)
		if err != nil {
			return "", err
		}
		sha256Chunk := h.Sum(nil)
		metaFileValue = append(metaFileValue, sha256Chunk...)
		metaFileValueSep += hex.EncodeToString(sha256Chunk)
		metahashHex := hex.EncodeToString(sha256Chunk)
		blobStore.Set(metahashHex, chunk)
	}
	//compute last smaller chunk
	if len(buf)%int(n.conf.ChunkSize) != 0 {
		//metaFileValue = append(metaFileValue, []byte(peer.MetafileSep)...)
		chunk := buf[nbChunks*int(n.conf.ChunkSize):]
		metaFileValueSep += peer.MetafileSep
		h := crypto.SHA256.New()
		_, err = h.Write(chunk)
		if err != nil {
			return "", err
		}
		sha256Chunk := h.Sum(nil)
		metaFileValue = append(metaFileValue, sha256Chunk...)
		metaFileValueSep += hex.EncodeToString(sha256Chunk)
		metahashHex := hex.EncodeToString(sha256Chunk)
		blobStore.Set(metahashHex, chunk)
	}

	h := crypto.SHA256.New()
	_, err = h.Write(metaFileValue)
	if err != nil {
		return "", err
	}
	metaFileKey := hex.EncodeToString(h.Sum(nil))
	blobStore.Set(metaFileKey, []byte(metaFileValueSep))

	return metaFileKey, nil

}

func (n *node) GetCatalog() peer.Catalog {
	n.catalog.lock.RLock()
	defer n.catalog.lock.RUnlock()

	var copy peer.Catalog = make(peer.Catalog)
	for k, v := range n.catalog.catalog {
		copy[k] = v
	}
	return copy
}

func (n *node) UpdateCatalog(key string, peer string) {
	n.catalog.lock.Lock()
	defer n.catalog.lock.Unlock()

	_, ok := n.catalog.catalog[key]
	if !ok {
		n.catalog.catalog[key] = make(map[string]struct{})
	}
	(n.catalog.catalog[key])[peer] = struct{}{}
}

func (n *node) Download(metahash string) ([]byte, error) {

	//First, get the metaFile

	blobStore := n.conf.Storage.GetDataBlobStore()
	metaFileValue, err := n.downloadContentFromHash(metahash)
	if err != nil {
		return nil, err
	}
	//necessary to convert into string before splitting ? How about directly splitting the array ?
	splittedMetaFileValue := strings.Split(bytes.NewBuffer(metaFileValue).String(), peer.MetafileSep)

	var combinedChunks []byte
	for _, chunkHash := range splittedMetaFileValue {
		chunk, err := n.downloadContentFromHash(chunkHash)
		if err != nil {
			return nil, err
		}
		blobStore.Set(chunkHash, chunk)
		combinedChunks = append(combinedChunks, chunk...)
	}
	blobStore.Set(metahash, metaFileValue)
	return combinedChunks, nil
}

//Downloads a chunk for the corresponding metahash (does the exact same thing for the metaFile)
func (n *node) downloadContentFromHash(hash string) ([]byte, error) {
	blobStore := n.conf.Storage.GetDataBlobStore()
	content := blobStore.Get(hash)
	if content != nil {
		return content, nil
	}
	requestId := xid.New().String()
	//Else,send dataRequest to neighbor who has the file
	targetList := n.GetCatalog()[hash]
	for neighbor := range targetList {
		if n.routingTable.getRelayAddr(neighbor) == "" {
			//if not in routingTable, try another node in catalog
			continue
		}
		dataReqMsg := types.DataRequestMessage{RequestID: requestId, Key: hash}
		buf, err := json.Marshal(dataReqMsg)
		if err != nil {
			return nil, err
		}
		trsptMsg := transport.Message{Type: types.DataRequestMessage{}.Name(), Payload: buf}
		err = n.Unicast(neighbor, trsptMsg)
		if err != nil {
			return nil, err
		}
		errs := make(chan error)
		chunkChan := make(chan []byte)
		go n.waitForDataReply(requestId, neighbor, trsptMsg, errs, chunkChan, n.conf.BackoffDataRequest.Initial, n.conf.BackoffDataRequest.Retry)
		select {
		case chunk := <-chunkChan:
			close(chunkChan)
			close(errs)
			return chunk, nil
		case err := <-errs:
			close(chunkChan)
			close(errs)
			return nil, err
		}
	}

	return nil, xerrors.Errorf("could not download the chunk")
}

func (n *node) waitForDataReply(requestID string, neighbor string, trsptMsg transport.Message, errs chan error, chunkChan chan []byte, interval time.Duration, retransmissionsLeft uint) {

	requestChannel := make(chan []byte)
	n.dataRequestWaitList.addEntry(requestID, requestChannel)
	timer := time.NewTimer(interval)

	for {
		select {
		case chunk := <-requestChannel:
			timer.Stop()
			close(requestChannel)
			if len(chunk) == 0 {
				errs <- xerrors.Errorf("peer responded with empty value")
				return
			}
			n.dataRequestWaitList.removeEntry(requestID)
			chunkChan <- chunk
			return
		case <-timer.C:
			if retransmissionsLeft == 0 {
				errs <- xerrors.Errorf("no more retransmissions left")
				return
			}
			retransmissionsLeft--
			//how to redefine timer ?
			interval = interval * time.Duration(n.conf.BackoffDataRequest.Factor)
			//send again
			err := n.Unicast(neighbor, trsptMsg)
			if err != nil {
				errs <- err
				return
			}
			go n.waitForDataReply(requestID, neighbor, trsptMsg, errs, chunkChan, interval, retransmissionsLeft)
			//todo: something after ?
		}
	}

}

func (n *node) Resolve(name string) (metahash string) {
	return bytes.NewBuffer(n.conf.Storage.GetNamingStore().Get(name)).String()
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {
	requestId := xid.New().String()
	fileInfosChan := make(chan []types.FileInfo)
	n.searchRequestWaitList.addEntry(requestId, fileInfosChan)
	fileInfos, err := n.relaySearchRequestMessage([]string{n.address}, reg.String(), budget, requestId, n.address)
	if err != nil {
		return nil, err
	}
	for _, fileInfo := range fileInfos {
		namingStore := n.conf.Storage.GetNamingStore()
		namingStore.Set(fileInfo.Name, []byte(fileInfo.Metahash))
		names = n.appendIfDoesntAlreadyContains(names, fileInfo.Name)
	}

	timer := time.NewTimer(timeout)
	names, err = n.waitForFileInfos(*timer, fileInfosChan, requestId, names)
	return names, err
}

func (n *node) waitForFileInfos(timer time.Timer, fileInfosChan chan []types.FileInfo, requestId string, names []string) ([]string, error) {
	select {
	case <-timer.C:
		close(fileInfosChan)
		n.searchRequestWaitList.removeEntry(requestId)
		return names, nil
	case fileInfosList := <-n.searchRequestWaitList.list[requestId]:
		for _, fileInfo := range fileInfosList {
			namingStore := n.conf.Storage.GetNamingStore()
			namingStore.Set(fileInfo.Name, []byte(fileInfo.Metahash))
			names = n.appendIfDoesntAlreadyContains(names, fileInfo.Name)
		}
		return n.waitForFileInfos(timer, fileInfosChan, requestId, names)
	}
}

func (n *node) appendIfDoesntAlreadyContains(names []string, newName string) []string {
	for _, name := range names {
		if name == newName {
			return names
		}
	}
	return append(names, newName)
}

//returns only local fileInfos + sends a message to all neighbors considering budget
func (n *node) relaySearchRequestMessage(neighborsToAvoid []string, pattern string, budget uint, requestId string, origin string) ([]types.FileInfo, error) {
	var fileInfos []types.FileInfo
	namingStore := n.conf.Storage.GetNamingStore()
	blobStore := n.conf.Storage.GetDataBlobStore()
	namingStore.ForEach(func(name string, metahash []byte) bool {
		if regexp.MustCompile(pattern).MatchString(name) && (blobStore.Get(string(metahash)) != nil || n.address == origin) {
			splittedMetaFileValue := strings.Split(bytes.NewBuffer(blobStore.Get(string(metahash))).String(), peer.MetafileSep)
			var chunks [][]byte
			for _, chunkHash := range splittedMetaFileValue {
				if blobStore.Get(chunkHash) != nil {
					chunks = append(chunks, []byte(chunkHash))
				} else {
					chunks = append(chunks, nil)
				}
			}
			fileInfos = append(fileInfos, types.FileInfo{Name: name, Metahash: string(metahash), Chunks: chunks})
		}
		return true
	})

	if budget == 0 {
		return fileInfos, nil
	}

	if int(budget) <= len(n.GetRoutingTable())-1 {
		for budget != 0 {
			neighbor := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(neighborsToAvoid...))
			if neighbor == "" {
				return fileInfos, nil
			}
			err := n.sendSearchRequestMessage(1, neighbor, pattern, requestId, origin)
			if err != nil {
				return fileInfos, err
			}
			budget--
			neighborsToAvoid = append(neighborsToAvoid, neighbor)
		}
	} else {
		i := 1
		routingTableCopy := n.GetRoutingTable()
		for _, neighborToAvoid := range neighborsToAvoid {
			delete(routingTableCopy, neighborToAvoid)
		}
		if len(routingTableCopy) == 0 {
			return fileInfos, nil
		}
		for neighbor := range routingTableCopy {
			dividedBudget := math.Ceil(float64(budget) / float64(len(n.GetRoutingTable())-i))
			budget -= uint(dividedBudget)
			i++
			err := n.sendSearchRequestMessage(uint(dividedBudget), neighbor, pattern, requestId, origin)
			if err != nil {
				return fileInfos, err
			}
		}

	}
	return fileInfos, nil
}

func (n *node) sendSearchRequestMessage(budget uint, neighbor string, pattern string, requestID string, origin string) error {
	searchRequestMsg := types.SearchRequestMessage{RequestID: requestID, Origin: origin, Pattern: pattern, Budget: budget}
	buf, err := json.Marshal(searchRequestMsg)
	if err != nil {
		return err
	}
	trsptMsg := transport.Message{Type: types.SearchRequestMessage{}.Name(), Payload: buf}
	err = n.Unicast(neighbor, trsptMsg)
	if err != nil {
		return err
	}
	return nil
}

func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {

	filename := make(chan string)

	go func() {
		namingStore := n.conf.Storage.GetNamingStore()
		blobStore := n.conf.Storage.GetDataBlobStore()
		namingStore.ForEach(func(name string, metahash []byte) bool {
			if pattern.MatchString(name) && (blobStore.Get(string(metahash)) != nil) {
				potentialFullKnown := true
				splittedMetaFileValue := strings.Split(bytes.NewBuffer(blobStore.Get(string(metahash))).String(), peer.MetafileSep)
				for _, chunkHash := range splittedMetaFileValue {
					if blobStore.Get(chunkHash) == nil {
						potentialFullKnown = false
					}
				}
				if potentialFullKnown {
					filename <- name
					return true
				}
			}
			return true
		})
	}()

	go n.expandingRingLoop(xid.New().String(), filename, conf.Retry-1, conf, conf.Initial, pattern.String())
	return <-filename, nil

}

func (n *node) expandingRingLoop(requestId string, filename chan string, retransmissionsLeft uint, conf peer.ExpandingRing, budget uint, pattern string) {

	ticker := time.NewTicker(conf.Timeout)
	requestChannel := make(chan []types.FileInfo)
	n.searchRequestWaitList.addEntry(requestId, requestChannel)

	go n.relaySearchRequestMessage([]string{n.address}, pattern, budget, requestId, n.address)

	for {
		select {
		case fileInfos := <-n.searchRequestWaitList.list[requestId]:
			for _, fileInfo := range fileInfos {
				namingStore := n.conf.Storage.GetNamingStore()
				namingStore.Set(fileInfo.Name, []byte(fileInfo.Metahash))
				potentialFullKnown := true
				for _, chunk := range fileInfo.Chunks {
					if chunk == nil {
						potentialFullKnown = false
					}
				}
				if potentialFullKnown {
					close(n.searchRequestWaitList.list[requestId])
					n.searchRequestWaitList.removeEntry(requestId)
					filename <- fileInfo.Name
					return
				}
			}
		case <-ticker.C:
			if retransmissionsLeft == 0 {
				close(n.searchRequestWaitList.list[requestId])
				n.searchRequestWaitList.removeEntry(requestId)
				filename <- ""
				return
			}
			//retransmissionsLeft -= uint(1)
			budget *= conf.Factor

			otherfilename := make(chan string)
			go n.expandingRingLoop(xid.New().String(), otherfilename, retransmissionsLeft-1, conf, budget, pattern)
			filenameVar := <-otherfilename
			filename <- filenameVar
			return
		}
	}
}

//====================================================HW3==========================================================================================

func (n *node) Tag(name string, mh string) error {

	if n.conf.TotalPeers <= 1 {
		namingStore := n.conf.Storage.GetNamingStore()
		namingStore.Set(name, []byte(mh))
		return nil
	} else {
		if n.conf.Storage.GetNamingStore().Get(name) != nil {
			return xerrors.Errorf("name already exists in storage")
		}

		err := n.paxosAlgorithm(name, mh, 0, n.conf.PaxosID)
		if err != nil {
			return err
		}

	}

	return nil
}

func (n *node) paxosAlgorithm(name string, mh string, step uint, id uint) error {
	//phase 1
	n.sendPaxosPrepareMessage(step, id)

	//phase 2
	uniqId := xid.New().String()
	value := types.PaxosValue{UniqID: uniqId, Filename: name, Metahash: mh}
	highestId := uint(0)
	for _, promiseMsg := range n.collectedPromises {
		if promiseMsg.AcceptedValue != nil && promiseMsg.AcceptedID > highestId {
			highestId = promiseMsg.AcceptedID
			value = *promiseMsg.AcceptedValue
		}
	}
	n.sendPaxosProposeMessage(value, step, id, name, mh, uniqId)

	var hash []byte
	hash = append(hash, []byte(strconv.Itoa(int(step)))...)
	hash = append(hash, []byte(value.UniqID)...)
	hash = append(hash, []byte(value.Filename)...)
	hash = append(hash, []byte(value.Metahash)...)
	var prevhash []byte
	if step != 0 {
		blockChain := n.conf.Storage.GetBlockchainStore()
		buf := blockChain.Get(hex.EncodeToString(blockChain.Get(storage.LastBlockKey)))
		var block types.BlockchainBlock
		err := block.Unmarshal(buf)
		if err != nil {
			return err
		}
		prevhash = block.Hash
	} else {
		for i := 0; i < 4; i++ {
			prevhash = append(prevhash, 0)
		}
	}
	hash = append(hash, prevhash...)
	h := crypto.SHA256.New()
	_, err := h.Write(hash)
	if err != nil {
		return err
	}
	hash = h.Sum(nil)

	block := types.BlockchainBlock{Index: step, Hash: hash, Value: value, PrevHash: prevhash}
	tlcMsg := types.TLCMessage{Step: step, Block: block}
	buf, _ := json.Marshal(tlcMsg)
	trsptMsg := transport.Message{Type: types.TLCMessage{}.Name(), Payload: buf}
	n.Broadcast(trsptMsg)

	n.collectedAccepts = make([]types.PaxosAcceptMessage, 0)
	n.collectedPromises = make([]types.PaxosPromiseMessage, 0)

	return nil
}

func (n *node) sendPaxosPrepareMessage(step uint, id uint) error {
	paxosPrepareMsg := types.PaxosPrepareMessage{Step: step, ID: id, Source: n.address}
	buf, err := json.Marshal(paxosPrepareMsg)
	if err != nil {
		return err
	}
	trsptMsg := transport.Message{Type: types.PaxosPrepareMessage{}.Name(), Payload: buf}
	n.Broadcast(trsptMsg)

	//wait for promises
	finishPaxosPhaseChan := make(chan struct{})
	n.paxosCollectingPromisesWaitList.addEntry(id, finishPaxosPhaseChan)
	timer := time.NewTimer(n.conf.PaxosProposerRetry)
	for {
		select {
		case <-timer.C:
			print("time off")
			n.sendPaxosPrepareMessage(step, id+n.conf.TotalPeers)
			return nil
		case <-finishPaxosPhaseChan:
			close(finishPaxosPhaseChan)
			n.paxosCollectingPromisesWaitList.removeEntry(id)
			n.paxosPhase = 2
			return nil
		}
	}
}

func (n *node) sendPaxosProposeMessage(value types.PaxosValue, step uint, id uint, name string, mh string, uniqId string) error {
	paxosProposeMessage := types.PaxosProposeMessage{Step: step, ID: id, Value: value}
	buf, err := json.Marshal(paxosProposeMessage)
	if err != nil {
		return err
	}
	trsptMsg := transport.Message{Type: types.PaxosProposeMessage{}.Name(), Payload: buf}
	n.Broadcast(trsptMsg)

	//wait for accepts
	finishPaxosPhaseChan := make(chan struct{})
	n.paxosCollectingAcceptsWaitList.addEntry(uniqId, finishPaxosPhaseChan)
	timer := time.NewTimer(n.conf.PaxosProposerRetry)
	for {
		select {
		case <-timer.C:
			n.paxosPhase = 1
			err := n.paxosAlgorithm(name, mh, step, id+n.conf.TotalPeers)
			if err != nil {
				return err
			}
			return nil
		case <-finishPaxosPhaseChan:
			close(finishPaxosPhaseChan)
			n.paxosCollectingAcceptsWaitList.removeEntry(uniqId)
			n.paxosPhase = 1
			return nil
		}
	}
}

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
	"sync"
	"time"

	"github.com/rs/xid"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {

	peerAddress := conf.Socket.GetAddress()

	node := node{
		conf:                   conf,
		address:                peerAddress,
		stop:                   make(chan struct{}, 3),
		catalog:                Catalog{catalog: make(map[string]map[string]struct{})},
		routingTable:           RoutingTable{table: map[string]string{peerAddress: peerAddress}},
		rumorLists:             RumorLists{rumorLists: map[string][]types.Rumor{peerAddress: make([]types.Rumor, 0)}},
		ackWaitList:            AckWaitList{list: make(map[string]chan struct{})},
		dataRequestWaitList:    DataRequestWaitList{list: make(map[string]chan []byte)},
		searchRequestWaitList:  SearchRequestWaitList{list: make(map[string]chan []types.FileInfo)},
		processedSearchRequest: make([]string, 0),
		nbRunningRoutines:      0,
		waitGroup:              sync.WaitGroup{},
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

	return &node

}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf                   peer.Configuration
	stop                   chan struct{}
	catalog                Catalog
	routingTable           RoutingTable
	address                string
	rumorLists             RumorLists
	ackWaitList            AckWaitList
	dataRequestWaitList    DataRequestWaitList
	searchRequestWaitList  SearchRequestWaitList
	processedSearchRequest []string
	nbRunningRoutines      uint
	waitGroup              sync.WaitGroup
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
	//Why not launch goroutine ?
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

func (n *node) Upload(data io.Reader) (metahash string, err error) {

	bytesBuf := new(bytes.Buffer)
	bytesBuf.ReadFrom(data)
	buf := bytesBuf.Bytes()

	blobStore := n.conf.Storage.GetDataBlobStore()
	nbChunks := (len(buf) / int(n.conf.ChunkSize))

	var metaFileValue []byte
	for i := 0; i < nbChunks; i++ {
		idx := i * int(n.conf.ChunkSize)
		chunk := buf[idx : idx+int(n.conf.ChunkSize)]
		h := crypto.SHA256.New()
		_, err = h.Write(chunk)
		if err != nil {
			return "", err
		}
		sha256Chunk := h.Sum(nil)
		log.Print(sha256Chunk)
		metaFileValue = append(metaFileValue, sha256Chunk...)
		metahashHex := hex.EncodeToString(sha256Chunk)
		log.Print("storing")
		blobStore.Set(metahashHex, chunk)
	}
	//compute last smaller chunk
	if len(buf)%int(n.conf.ChunkSize) != 0 {
		chunk := buf[nbChunks*int(n.conf.ChunkSize):]
		h := crypto.SHA256.New()
		_, err = h.Write(chunk)
		if err != nil {
			return "", err
		}
		sha256Chunk := h.Sum(nil)
		log.Print(sha256Chunk)
		metaFileValue = append(metaFileValue, sha256Chunk...)
		metahashHex := hex.EncodeToString(sha256Chunk)
		log.Print("storing")
		blobStore.Set(metahashHex, chunk)
	}

	h := crypto.SHA256.New()
	_, err = h.Write(metaFileValue)
	if err != nil {
		return "", err
	}
	metaFileKey := hex.EncodeToString(h.Sum(nil))
	log.Print("storing")
	blobStore.Set(metaFileKey, metaFileValue)

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

	(n.catalog.catalog[key])[peer] = struct{}{}
	log.Print("added entry to catalog")
	log.Print(n.catalog.catalog[key])
	log.Print((n.catalog.catalog[key])[peer])

}

func (n *node) Download(metahash string) ([]byte, error) {

	//First, get the metaFile
	blobStore := n.conf.Storage.GetDataBlobStore()
	metaFileValue, err := n.downloadContentFromHash(metahash)
	if err != nil {
		return nil, err
	}

	var splittedChunks []string
	for i := 0; i < len(metaFileValue)/32; i++ {
		splittedChunks = append(splittedChunks, string(metaFileValue[i:i+32]))
	}

	var combinedChunks []byte
	for _, chunkHash := range splittedChunks {
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
	for neighbor := range n.GetCatalog() {
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
		go n.waitForDataReply(requestId, neighbor, trsptMsg, errs, chunkChan)
		if err := <-errs; err != nil {
			return nil, err
		}
		chunk := <-chunkChan
		close(chunkChan)
		close(errs)
		return chunk, nil
	}

	return nil, xerrors.Errorf("could not download the chunk")
}

func (n *node) waitForDataReply(requestID string, neighbor string, trsptMsg transport.Message, errs chan error, chunkChan chan []byte) {

	requestChannel := make(chan []byte)
	n.dataRequestWaitList.addEntry(requestID, requestChannel)

	interval := n.conf.BackoffDataRequest.Initial
	timer := time.NewTimer(interval)
	retransmissionsLeft := n.conf.BackoffDataRequest.Retry

	for {
		select {
		case <-requestChannel:
			timer.Stop()
			close(requestChannel)
			chunk := <-requestChannel
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
			timer = time.NewTimer(interval)
			//send again
			err := n.Unicast(neighbor, trsptMsg)
			if err != nil {
				errs <- err
				return
			}
			go n.waitForDataReply(requestID, neighbor, trsptMsg, errs, chunkChan)
			//todo: something after ?
		}
	}

}

func (n *node) Tag(name string, mh string) error {
	namingStore := n.conf.Storage.GetNamingStore()
	namingStore.Set(name, []byte(mh))
	return nil
}

func (n *node) Resolve(name string) (metahash string) {
	return bytes.NewBuffer(n.conf.Storage.GetNamingStore().Get(name)).String()
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {

	fileInfos, err := n.relaySearchRequestMessage([]string{n.address}, reg.String(), budget, xid.New().String(), n.address)
	if err != nil {
		return nil, err
	}

	for _, fileInfo := range fileInfos {
		namingStore := n.conf.Storage.GetNamingStore()
		namingStore.Set(fileInfo.Name, []byte(fileInfo.Metahash))
		names = append(names, fileInfo.Name)
	}

	return names, nil
}

func (n *node) relaySearchRequestMessage(neighborsToAvoid []string, pattern string, budget uint, requestId string, origin string) ([]types.FileInfo, error) {
	replyChan := make(chan []types.FileInfo)
	errs := make(chan error)

	//neighborsToAvoid := []string{n.address, pkt.Header.RelayedBy, pkt.Header.Source}
	var fileInfos []types.FileInfo
	namingStore := n.conf.Storage.GetNamingStore()
	blobStore := n.conf.Storage.GetDataBlobStore()
	namingStore.ForEach(func(name string, metahash []byte) bool {
		if regexp.MustCompile(pattern).MatchString(name) && blobStore.Get(string(metahash)) != nil {
			var splittedChunks []string
			for i := 0; i < len(metahash)/32; i++ {
				splittedChunks = append(splittedChunks, string(metahash[i:i+32]))
			}
			var chunks [][]byte
			for _, chunkHash := range splittedChunks {
				if blobStore.Get(chunkHash) != nil {
					chunks = append(chunks, []byte(chunkHash))
				}
			}
			fileInfos = append(fileInfos, types.FileInfo{Name: name, Metahash: string(metahash), Chunks: chunks})
		}
		return true
	})

	if int(budget) <= len(n.GetRoutingTable())-1 {
		for budget != 0 {
			neighbor := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(neighborsToAvoid...))

			n.sendSearchRequestMessage(1, neighbor, pattern, requestId, origin)

			n.waitGroup.Add(1)
			go n.waitForSearchReply(requestId, replyChan, errs)
			budget--
			neighborsToAvoid = append(neighborsToAvoid, neighbor)
			go n.collectFileInfos(replyChan, fileInfos, neighbor)

		}

	} else {
		i := 1
		routingTableCopy := n.GetRoutingTable()
		for _, neighborToAvoid := range neighborsToAvoid {
			delete(routingTableCopy, neighborToAvoid)
		}
		for neighbor := range routingTableCopy {
			dividedBudget := math.Ceil(float64(budget) / float64(len(n.GetRoutingTable())-i))
			budget -= uint(dividedBudget)
			i++
			n.sendSearchRequestMessage(budget, neighbor, pattern, requestId, origin)

			n.waitGroup.Add(1)
			go n.waitForSearchReply(requestId, replyChan, errs)
			go n.collectFileInfos(replyChan, fileInfos, neighbor)
		}

	}

	//block until timeout
	n.waitGroup.Wait()
	if err := <-errs; err != nil {
		return nil, err
	}
	close(replyChan)
	close(errs)
	return fileInfos, nil
}

func (n *node) collectFileInfos(replyChan chan []types.FileInfo, fileInfos []types.FileInfo, neighbor string) {
	for reply := range replyChan {
		for _, fileInfo := range reply {
			n.catalog.lock.Lock()
			n.catalog.catalog[fileInfo.Metahash][neighbor] = struct{}{}
			n.catalog.lock.Unlock()
			fileInfos = append(fileInfos, fileInfo)
			n.waitGroup.Done()
		}
	}
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

func (n *node) waitForSearchReply(requestID string, replyChan chan []types.FileInfo, errs chan error) {
	requestChannel := make(chan []types.FileInfo)
	n.searchRequestWaitList.addEntry(requestID, requestChannel)
	for range requestChannel {
		close(requestChannel)
		reply := <-requestChannel
		replyChan <- reply
		n.searchRequestWaitList.removeEntry(requestID)
	}
}

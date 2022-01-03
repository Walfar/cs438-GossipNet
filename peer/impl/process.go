package impl

import (
	"encoding/hex"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

func (n *node) processChatMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		log.Print(msg.String())
		return nil
	}
}

func (n *node) processRumorsMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		rumorMsg, castOk := msg.(*types.RumorsMessage)

		if !castOk {
			return xerrors.Errorf("message type is not rumor")
		}
		newData := false
		origin := ""
		relay := ""

		for _, rumor := range rumorMsg.Rumors {

			origin = ""
			relay = ""
			rumorSeq := rumor.Sequence

			originCounter := n.rumorLists.GetRumorSeqOf(rumor.Origin)

			if originCounter != -1 {
				//if peer exists in map
				if uint(originCounter)+1 == rumorSeq {
					//if it is the next expected rumor from this peer
					processErr := n.ProcessPacket(pkt.Header, rumor.Msg)
					if processErr != nil {
						return processErr
					}

					n.rumorLists.UpdateRumorSeqOf(rumor.Origin, rumor)
					origin = rumor.Origin
					relay = pkt.Header.RelayedBy

				}
			} else {
				if rumorSeq == 1 {
					//add peer to the map if first rumor received from it
					n.rumorLists.UpdateRumorSeqOf(rumor.Origin, rumor)
					origin = rumor.Origin
					relay = pkt.Header.RelayedBy

					newData = true

					newPkt := transport.Packet{Header: pkt.Header, Msg: rumor.Msg}
					processErr := n.conf.MessageRegistry.ProcessPacket(newPkt)

					if processErr != nil {
						return processErr
					}

				}
				//do nothing because it wasnt the first message sent by this peer
			}

			//update routing table
			if (origin != relay && !n.isNeighbor(origin)) || !n.routingTable.containsEntry(origin) {
				n.SetRoutingEntry(origin, relay)
			}
		}

		//--------------------------------------------------------------------------------//
		//Send Ack to the source
		ackHeader := transport.NewHeader(n.address, n.address, pkt.Header.Source, 0)
		ackMsg := types.AckMessage{
			AckedPacketID: pkt.Header.PacketID,
			Status:        n.CreateStatusMessage(),
		}

		trsptAckMsg, marshErr := n.conf.MessageRegistry.MarshalMessage(ackMsg)
		if marshErr != nil {
			return marshErr
		}

		sendErr := n.conf.Socket.Send(pkt.Header.Source, transport.Packet{Header: &ackHeader, Msg: &trsptAckMsg}, n.conf.AckTimeout)
		if sendErr != nil {
			return sendErr
		}

		//--------------------------------------------------------------------------------//

		//send rumorsMessage to rdm neighbors
		if newData {

			randomNeighAddr := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(n.address, pkt.Header.Source))

			if randomNeighAddr != "" {
				pkt.Header.Destination = randomNeighAddr
				//updated relay address to own
				pkt.Header.RelayedBy = n.address
				sendErr := n.conf.Socket.Send(randomNeighAddr, pkt, n.conf.AckTimeout)
				if sendErr != nil {
					return sendErr

				}
			}
		}
		return nil
	}
}

func (n *node) processAckMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {

		ackMsg, castOk := msg.(*types.AckMessage)
		if !castOk {
			return xerrors.Errorf("message type is not ack")
		}

		ackID := ackMsg.AckedPacketID

		n.ackWaitList.lock.RLock()
		defer n.ackWaitList.lock.RUnlock()

		if _, ok := n.ackWaitList.list[ackID]; ok {
			n.ackWaitList.list[ackID] <- struct{}{}
		} else {
			return xerrors.Errorf("no awaiting ack with this id")
		}

		trsptStatusMsg, statusErr := n.conf.MessageRegistry.MarshalMessage(ackMsg.Status)
		if statusErr != nil {
			return statusErr
		}

		statusPkt := transport.Packet{Header: pkt.Header, Msg: &trsptStatusMsg}

		processErr := n.conf.MessageRegistry.ProcessPacket(statusPkt)
		if processErr != nil {
			return processErr
		}

		return nil
	}
}

func (n *node) processStatusMessage() registry.Exec {

	return func(msg types.Message, pkt transport.Packet) error {

		remoteStatus, castOk := msg.(*types.StatusMessage)

		if !castOk {
			return xerrors.Errorf("message type is not rumor")
		}

		mustSend, missingRumors, diffErr := n.FindDifferences(n.rumorLists.ConvertRumorsToSeq(), *remoteStatus)
		if diffErr != nil {
			return diffErr
		}

		neighAddr := pkt.Header.Source

		//case 1
		if mustSend {
			n.SendStatusMessage(neighAddr)
		}

		//case 2
		if len(missingRumors) > 0 {
			//Ack ignored
			n.SendRumorMsg(neighAddr, missingRumors)
		}

		//case 4
		// if !mustSend && len(missingRumors) == 0 {
		// }

		//chose random neighbor

		rand.Seed(time.Now().UnixNano())
		rdmIndex := rand.Float64()
		if rdmIndex < n.conf.ContinueMongering {
			rdmAddr := n.routingTable.ChooseRDMNeighborAddr(MakeExceptionMap(neighAddr, n.address))

			if rdmAddr != "" {
				n.SendStatusMessage(rdmAddr)
			}

		}
		return nil
	}
}

func (n *node) processEmptyMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {

		_, castOk := msg.(*types.EmptyMessage)
		if !castOk {
			return xerrors.Errorf("message type is not ack")
		}

		origin := pkt.Header.Source
		relay := pkt.Header.RelayedBy

		if (origin != relay && !n.isNeighbor(origin)) || !n.routingTable.containsEntry(origin) {
			n.SetRoutingEntry(origin, relay)
		}

		return nil
	}
}

func (n *node) processPrivateMessage() registry.Exec {

	return func(msg types.Message, pkt transport.Packet) error {

		privateMsg, castOk := msg.(*types.PrivateMessage)
		if !castOk {
			return xerrors.Errorf("message type is not private")
		}

		if _, ok := privateMsg.Recipients[n.address]; ok {
			processErr := n.ProcessPacket(pkt.Header, privateMsg.Msg)
			return processErr
		}

		return nil
	}
}

func (n *node) FindDifferences(peerStatus types.StatusMessage, remoteStatus types.StatusMessage) (bool, []types.Rumor, error) {

	n.rumorLists.lock.Lock()
	defer n.rumorLists.lock.Unlock()

	notPresentNeighEntryCount := 0
	mustSendStatusMsg := false
	rumorsToSendToRemote := make([]types.Rumor, 0)

	for address, count := range peerStatus {
		//check remote has entry
		if neighCount, exists := remoteStatus[address]; !exists {
			//if not, "remove" entry from peer in remote
			notPresentNeighEntryCount += 1

			//add all rumors from that address to the rumors to send to remote
			completeEntry := make([]types.Rumor, count)
			nbElemCopied := copy(completeEntry, n.rumorLists.rumorLists[address][neighCount:count])
			if nbElemCopied != int(count) {
				return false, nil, nil
			}
			rumorsToSendToRemote = append(rumorsToSendToRemote, completeEntry...)

		} else {
			if neighCount > count {
				//missing remote rumors
				mustSendStatusMsg = true
			} else if neighCount < count {
				//get slice of missing rumors
				copiedRumors := make([]types.Rumor, count-neighCount)
				nbElemCopied := copy(copiedRumors, n.rumorLists.rumorLists[address][neighCount:count])

				rumorsToSendToRemote = append(rumorsToSendToRemote, copiedRumors...)

				if nbElemCopied != int(neighCount-count) {
					return false, nil, nil
				}
			}
		}
	}

	//check case where remote has more msg than own
	if len(remoteStatus) != len(peerStatus)-notPresentNeighEntryCount {
		mustSendStatusMsg = true
	}

	return mustSendStatusMsg, rumorsToSendToRemote, nil
}

func (n *node) processDataReplyMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {

		dataReplyMsg, castOk := msg.(*types.DataReplyMessage)
		if !castOk {
			return xerrors.Errorf("message type is not data reply")
		}
		n.dataRequestWaitList.lock.RLock()
		defer n.dataRequestWaitList.lock.RUnlock()

		if _, ok := n.dataRequestWaitList.list[dataReplyMsg.RequestID]; ok {
			n.dataRequestWaitList.list[dataReplyMsg.RequestID] <- dataReplyMsg.Value
		} else {
			return xerrors.Errorf("no awaiting request with this id")
		}
		return nil
	}
}

func (n *node) processDataRequestMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		dataRequestMsg, castOk := msg.(*types.DataRequestMessage)
		if !castOk {
			return xerrors.Errorf("message type is not data request")
		}
		blobStore := n.conf.Storage.GetDataBlobStore()
		content := blobStore.Get(dataRequestMsg.Key)
		dataReplyMsg := types.DataReplyMessage{RequestID: dataRequestMsg.RequestID, Key: dataRequestMsg.Key, Value: content}
		buf, err := json.Marshal(dataReplyMsg)
		if err != nil {
			return err
		}
		trsptMsg := transport.Message{Type: types.DataReplyMessage{}.Name(), Payload: buf}
		err = n.Unicast(pkt.Header.Source, trsptMsg)
		if err != nil {
			return err
		}
		return nil
	}
}

func (n *node) processSearchReplyMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		searchReplyMsg, castOk := msg.(*types.SearchReplyMessage)
		if !castOk {
			return xerrors.Errorf("message type is not search reply")
		}
		n.searchRequestWaitList.lock.RLock()
		defer n.searchRequestWaitList.lock.RUnlock()
		if _, ok := n.searchRequestWaitList.list[searchReplyMsg.RequestID]; ok {
			for _, fileInfo := range searchReplyMsg.Responses {
				n.UpdateCatalog(fileInfo.Metahash, pkt.Header.Source)
				for _, chunkMetahash := range fileInfo.Chunks {
					if chunkMetahash != nil {
						n.UpdateCatalog(string(chunkMetahash), pkt.Header.Source)
					}
				}
			}
			n.searchRequestWaitList.list[searchReplyMsg.RequestID] <- searchReplyMsg.Responses
		} else {
			return xerrors.Errorf("no awaiting search request with this id")
		}
		return nil
	}
}

func (n *node) processSearchRequestMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		searchRequestMsg, castOk := msg.(*types.SearchRequestMessage)
		if !castOk {
			return xerrors.Errorf("message type is not search request")
		}
		//avoid duplicates
		for _, requestId := range n.processedSearchRequest {
			if requestId == searchRequestMsg.RequestID {
				return nil
			}
		}
		n.processedSearchRequest = append(n.processedSearchRequest, searchRequestMsg.RequestID)
		pattern := searchRequestMsg.Pattern
		budget := searchRequestMsg.Budget - 1
		origin := searchRequestMsg.Origin
		requestId := searchRequestMsg.RequestID

		fileInfos, err := n.relaySearchRequestMessage([]string{n.address, pkt.Header.Source, pkt.Header.RelayedBy, searchRequestMsg.Origin}, pattern, budget, requestId, origin)
		if err != nil {
			return err
		}
		searchReplyMsg := types.SearchReplyMessage{RequestID: requestId, Responses: fileInfos}
		buf, err := json.Marshal(searchReplyMsg)
		if err != nil {
			return err
		}
		searchReplyTrsptMsg := transport.Message{Type: types.SearchReplyMessage{}.Name(), Payload: buf}

		peerAddress := n.conf.Socket.GetAddress()

		header := transport.NewHeader(peerAddress, peerAddress, searchRequestMsg.Origin, 0)
		packet := transport.Packet{Header: &header, Msg: &searchReplyTrsptMsg}
		sendErr := n.conf.Socket.Send(pkt.Header.RelayedBy, packet, n.conf.AckTimeout)
		if sendErr != nil {
			return sendErr
		}

		return nil
	}
}

func (n *node) processPaxosPrepareMessage() registry.Exec {

	return func(msg types.Message, pkt transport.Packet) error {

		paxosPrepareMsg, castOk := msg.(*types.PaxosPrepareMessage)
		if !castOk {
			return xerrors.Errorf("message type is not paxos prepare")
		}

		if paxosPrepareMsg.Step == n.TLCstep && paxosPrepareMsg.ID > n.paxosMaxId {
			paxosPromiseMsg := types.PaxosPromiseMessage{Step: paxosPrepareMsg.Step, ID: paxosPrepareMsg.ID}
			if n.paxosAcceptedValue != nil {
				paxosPromiseMsg.AcceptedValue = n.paxosAcceptedValue
				paxosPromiseMsg.AcceptedID = n.paxosAcceptedId
			}
			buf, _ := json.Marshal(paxosPromiseMsg)
			trsptMsg := transport.Message{Type: types.PaxosPromiseMessage{}.Name(), Payload: buf}
			privateMsg := types.PrivateMessage{Recipients: map[string]struct{}{paxosPrepareMsg.Source: {}}, Msg: &trsptMsg}

			buf, _ = json.Marshal(privateMsg)
			broadcastMsg := transport.Message{Type: types.PrivateMessage{}.Name(), Payload: buf}
			go n.Broadcast(broadcastMsg)
			n.paxosMaxId = paxosPrepareMsg.ID
		}
		return nil
	}
}

func (n *node) processPaxosProposeMessage() registry.Exec {

	return func(msg types.Message, pkt transport.Packet) error {

		paxosProposeMsg, castOk := msg.(*types.PaxosProposeMessage)
		if !castOk {
			return xerrors.Errorf("message type is not paxos propose")
		}

		if paxosProposeMsg.Step == n.TLCstep && paxosProposeMsg.ID == n.paxosMaxId {
			n.paxosAcceptedValue = &paxosProposeMsg.Value
			n.paxosAcceptedId = paxosProposeMsg.ID
			paxosAcceptMsg := types.PaxosAcceptMessage{Step: paxosProposeMsg.Step, ID: paxosProposeMsg.ID, Value: paxosProposeMsg.Value}
			buf, _ := json.Marshal(paxosAcceptMsg)
			trsptMsg := transport.Message{Type: types.PaxosAcceptMessage{}.Name(), Payload: buf}
			go n.Broadcast(trsptMsg)

		}
		return nil
	}
}

func (n *node) processPaxosPromiseMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {

		paxosPromiseMsg, castOk := msg.(*types.PaxosPromiseMessage)
		if !castOk {
			return xerrors.Errorf("message type is not paxos promise")
		}

		n.paxosCollectingPromisesWaitList.lock.RLock()
		defer n.paxosCollectingPromisesWaitList.lock.RUnlock()

		if paxosPromiseMsg.Step == n.TLCstep && n.paxosPhase == 1 {
			n.collectedPromises = append(n.collectedPromises, *paxosPromiseMsg)
			if len(n.collectedPromises) == n.conf.PaxosThreshold(n.conf.TotalPeers) {
				n.paxosCollectingPromisesWaitList.promisesChannels[paxosPromiseMsg.ID] <- struct{}{}
				return nil
			}
		}
		return nil
	}
}

func (n *node) processPaxosAcceptMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {

		paxosAcceptMsg, castOk := msg.(*types.PaxosAcceptMessage)
		if !castOk {
			return xerrors.Errorf("message type is not paxos accept")
		}
		n.paxosCollectingAcceptsWaitList.lock.RLock()
		defer n.paxosCollectingAcceptsWaitList.lock.RUnlock()

		if paxosAcceptMsg.Step == n.TLCstep && n.paxosPhase == 2 {
			n.collectedAccepts = append(n.collectedAccepts, *paxosAcceptMsg)
			if len(n.collectedAccepts) == n.conf.PaxosThreshold(n.conf.TotalPeers) {
				n.paxosCollectingAcceptsWaitList.acceptsChannels[paxosAcceptMsg.Value.UniqID] <- struct{}{}
				return nil
			}
		}
		return nil
	}
}

func (n *node) processTLCMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {

		tlcMsg, castOk := msg.(*types.TLCMessage)
		if !castOk {
			return xerrors.Errorf("message type is not tlc")
		}

		if tlcMsg.Step == n.TLCstep {
			n.receivedTLCMsgs += 1
			n.receivedTLCMsgs += uint(len(n.upcomingTLCMsgs))
		} else if tlcMsg.Step > n.TLCstep {
			n.upcomingTLCMsgs = append(n.upcomingTLCMsgs, *tlcMsg)
		}

		if n.conf.PaxosThreshold(n.conf.TotalPeers) <= int(n.receivedTLCMsgs) {
			buf, _ := tlcMsg.Block.Marshal()
			blockChain := n.conf.Storage.GetBlockchainStore()
			blockChain.Set(hex.EncodeToString(tlcMsg.Block.Hash), buf)
			blockChain.Set(storage.LastBlockKey, tlcMsg.Block.Hash)
			namingStore := n.conf.Storage.GetNamingStore()
			namingStore.Set(tlcMsg.Block.Value.Filename, []byte(tlcMsg.Block.Value.Metahash))

			maxStep := n.TLCstep
			for idx := range n.upcomingTLCMsgs {
				tlcMsg = &n.upcomingTLCMsgs[idx]
				buf, _ := tlcMsg.Block.Marshal()
				blockChain := n.conf.Storage.GetBlockchainStore()
				blockChain.Set(hex.EncodeToString(tlcMsg.Block.Hash), buf)
				if tlcMsg.Step >= maxStep {
					blockChain.Set(storage.LastBlockKey, tlcMsg.Block.Hash)
					maxStep = tlcMsg.Step
				}
				namingStore := n.conf.Storage.GetNamingStore()
				namingStore.Set(tlcMsg.Block.Value.Filename, []byte(tlcMsg.Block.Value.Metahash))
			}
			n.upcomingTLCMsgs = make([]types.TLCMessage, 0)

			n.TLCstep += 1
		}

		return nil
	}
}

//=============================================================================================================================

func (n *node) processFriendRequest() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		friendRequest, castOk := msg.(*types.FriendRequestMessage)
		if !castOk {
			return xerrors.Errorf("message type is not friendRequest")
		}
		n.friendRequestWaitList.add(pkt.Header.Source, friendRequest.PublicKey)
		return nil
	}
}

func (n *node) processPositiveResponse() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		positiveResponse, castOk := msg.(*types.PositiveResponse)
		if !castOk {
			return xerrors.Errorf("message type is not positiveResponse")
		}
		n.pendingFriendRequests.remove(pkt.Header.Source)
		n.friendsList.add(pkt.Header.Source, positiveResponse.PublicKey)
		return nil
	}
}

func (n *node) processNegativeResponse() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		_, castOk := msg.(*types.NegativeResponse)
		if !castOk {
			return xerrors.Errorf("message type is not negativeResponse")
		}
		n.pendingFriendRequests.remove(pkt.Header.Source)
		log.Print(pkt.Header.Source + " refused your friend request.")
		return nil
	}
}

func (n *node) processEncryptedMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		encryptedMsg, castOk := msg.(*types.EncryptedMessage)
		if !castOk {
			return xerrors.Errorf("message type is not encryptedMsg")
		}
		decryptedBytes, err := n.decryptBytes(encryptedMsg.EncryptedBytes)
		if err != nil {
			return err
		}
		log.Print(string(decryptedBytes))
		return nil
	}
}

package impl

import (
	"log"
	"math/rand"
	"time"

	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

func (n *node) processChatMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {
		log.Printf(msg.String())
		return nil
	}
}

func (n *node) processRumorsMessage() registry.Exec {
	return func(msg types.Message, pkt transport.Packet) error {

		rumorMsg, castOk := msg.(*types.RumorsMessage)

		if rumorMsg.Name() != "rumor" || !castOk {
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
				if uint(originCounter) == rumorSeq+1 {
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
		if !castOk || ackMsg.Name() != "ack" {
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

		if remoteStatus.Name() != "status" || !castOk {
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

		emptyMsg, castOk := msg.(*types.EmptyMessage)
		if !castOk || emptyMsg.Name() != "empty" {
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
		if privateMsg.Name() != "private" || !castOk {
			return xerrors.Errorf("message type is not rumor")
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

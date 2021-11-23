package impl

import (
	"math/rand"
	"sync"
	"time"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
)

type AckWaitList struct {
	lock sync.RWMutex
	list map[string]chan struct{}
}

func (a *AckWaitList) addEntry(ackID string, ackChannel chan struct{}) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.list[ackID] = ackChannel
}

func (a *AckWaitList) removeEntry(ackID string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	delete(a.list, ackID)
}

type SearchRequestWaitList struct {
	lock sync.RWMutex
	list map[string]chan []types.FileInfo
}

func (a *SearchRequestWaitList) addEntry(requestID string, requestChannel chan []types.FileInfo) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.list[requestID] = requestChannel
}

func (a *SearchRequestWaitList) removeEntry(requestID string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	delete(a.list, requestID)
}

type DataRequestWaitList struct {
	lock sync.RWMutex
	list map[string]chan []byte
}

func (a *DataRequestWaitList) addEntry(requestID string, requestChannel chan []byte) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.list[requestID] = requestChannel
}

func (a *DataRequestWaitList) removeEntry(requestID string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	delete(a.list, requestID)
}

type PaxosCollectingPromisesWaitList struct {
	lock             sync.RWMutex
	promisesChannels map[uint]chan struct{}
}

func (a *PaxosCollectingPromisesWaitList) addEntry(id uint, collectingChan chan struct{}) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.promisesChannels[id] = collectingChan
}

func (a *PaxosCollectingPromisesWaitList) removeEntry(id uint) {
	a.lock.Lock()
	defer a.lock.Unlock()
	delete(a.promisesChannels, id)
}

type PaxosCollectingAcceptsWaitList struct {
	lock            sync.RWMutex
	acceptsChannels map[uint]chan struct{}
}

func (a *PaxosCollectingAcceptsWaitList) addEntry(id uint, collectingChan chan struct{}) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.acceptsChannels[id] = collectingChan
}

func (a *PaxosCollectingAcceptsWaitList) removeEntry(id uint) {
	a.lock.Lock()
	defer a.lock.Unlock()
	delete(a.acceptsChannels, id)
}

type RumorLists struct {
	lock       sync.Mutex
	rumorLists map[string][]types.Rumor
}

func (n *node) GetOwnRumorSeq() uint {
	n.rumorLists.lock.Lock()
	defer n.rumorLists.lock.Unlock()
	return uint(len(n.rumorLists.rumorLists[n.address]))
}

func (r *RumorLists) UpdateRumorSeqOf(address string, rumor types.Rumor) {
	r.lock.Lock()
	defer r.lock.Unlock()

	_, ok := r.rumorLists[address]
	if !ok {
		r.rumorLists[address] = []types.Rumor{rumor}
	} else {
		r.rumorLists[address] = append(r.rumorLists[address], rumor)
	}
}

func (r *RumorLists) GetRumorSeqOf(address string) int {
	r.lock.Lock()
	defer r.lock.Unlock()

	rumors, ok := r.rumorLists[address]
	if ok {
		return len(rumors)
	} else {
		return -1
	}
}

func (r *RumorLists) ConvertRumorsToSeq() map[string]uint {

	r.lock.Lock()
	defer r.lock.Unlock()

	copy := make(map[string]uint, len(r.rumorLists))

	for k, v := range r.rumorLists {
		nbRumors := len(v)
		if nbRumors > 0 {
			copy[k] = uint(nbRumors)
		}
	}
	return copy
}

func (r *RumorLists) GetSlice(address string, start, end int) []types.Rumor {

	r.lock.Lock()
	defer r.lock.Unlock()

	copiedRumors := make([]types.Rumor, end-start)

	copy(copiedRumors, r.rumorLists[address][start:end])
	//if nbElemCopied != end-start {}

	return copiedRumors

}

type RoutingTable struct {
	lock  sync.RWMutex
	table peer.RoutingTable
}

func (rt *RoutingTable) getRelayAddr(dest string) string {
	rt.lock.RLock()
	defer rt.lock.RUnlock()

	neigh, ok := rt.table[dest]

	if !ok {
		return ""
	} else {
		return neigh
	}
}

func MakeExceptionMap(exceptions ...string) map[string]struct{} {

	exceptionsMap := make(map[string]struct{}, len(exceptions))

	for _, v := range exceptions {
		exceptionsMap[v] = struct{}{}
	}

	return exceptionsMap
}

func (rt *RoutingTable) containsEntry(dest string) bool {
	rt.lock.RLock()
	defer rt.lock.RUnlock()

	_, ok := rt.table[dest]
	return ok
}

//Choose random entry in routing table
func (rt *RoutingTable) ChooseRDMNeighborAddr(excepted map[string]struct{}) string {

	rt.lock.RLock()
	defer rt.lock.RUnlock()

	neighborsList := make([]string, 0)

	for k, v := range rt.table {
		//fmt.Println(k, v)
		//check that dest == next hop, meaning it is a direct neighbor
		if _, ok := excepted[k]; !ok && k == v {
			neighborsList = append(neighborsList, k)
		}
	}

	//fmt.Println("LEN RANDOM LIST", len(neighborsList))
	if len(neighborsList) == 0 {
		return ""
	}

	rand.Seed(time.Now().UnixNano())
	rdmIndex := rand.Intn(len(neighborsList))

	return neighborsList[rdmIndex]

}

type Catalog struct {
	lock    sync.RWMutex
	catalog peer.Catalog
}

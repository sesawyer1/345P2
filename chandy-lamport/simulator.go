package chandy_lamport

import (
	"log"
	"math/rand"
)

// ​ Max random delay added to packet delivery​​​
const maxDelay = 5

// Simulator is the entry point to the distributed snapshot application.​​
//
// It is a discrete time simulator, i.e. events that happen​ at time t + 1 come
// strictly after events that happen at time t. At each time step, the simulator
// examines messages queued up across all the links in the system and decides
// which ones to deliver to the destination.
//
// The simulator is responsible for starting the snapshot process, inducing servers
// to pass tokens to each other, and collecting the snapshot state after the process
// has terminated.
type Simulator struct {
	time           int
	nextSnapshotId int
	servers        map[string]*Server // key = server ID
	logger         *Logger
	// TODO: ADD MORE FIELDS HERE
	//status map[int]SnapStatus
	status             *SyncMap
	readyForCollection bool
	//readyForCollection bool
}

type SnapStatus struct {
	completed   bool
	doneServers map[string]bool
}

func NewSimulator() *Simulator {
	return &Simulator{
		0,
		0,
		make(map[string]*Server),
		NewLogger(),
		//make(map[int]SnapStatus),
		NewSyncMap(),
		false,
	}
}

// Return the receive time of a message after adding a random delay.
// Note: since we only deliver one message to a given server at each time step,
// the message may be received *after* the time step returned in this function.
func (sim *Simulator) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(5)
}

// Add a server to this simulator with the specified number of starting tokens
func (sim *Simulator) AddServer(id string, tokens int) {
	server := NewServer(id, tokens, sim)
	sim.servers[id] = server
}

// Add a unidirectional link between two servers
func (sim *Simulator) AddForwardLink(src string, dest string) {
	server1, ok1 := sim.servers[src]
	server2, ok2 := sim.servers[dest]
	if !ok1 {
		log.Fatalf("Server %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Server %v does not exist\n", dest)
	}
	server1.AddOutboundLink(server2)
}

// Run an event in the system
func (sim *Simulator) InjectEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.servers[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.serverId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step, handling all send message events
// that expire at the new time step, if any.
func (sim *Simulator) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the servers,
	// we must also iterate through the servers and the links in a deterministic way
	for _, serverId := range getSortedKeys(sim.servers) {
		server := sim.servers[serverId]
		for _, dest := range getSortedKeys(server.outboundLinks) {
			link := server.outboundLinks[dest]
			// Deliver at most one packet per server at each time step to
			// establish total ordering of packet delivery to each server
			if !link.events.Empty() {
				e := link.events.Peek().(SendMessageEvent)
				if e.receiveTime <= sim.time {
					link.events.Pop()
					sim.logger.RecordEvent(
						sim.servers[e.dest],
						ReceivedMessageEvent{e.src, e.dest, e.message})
					sim.servers[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Start a new snapshot process at the specified server
func (sim *Simulator) StartSnapshot(serverId string) {
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++
	sim.logger.RecordEvent(sim.servers[serverId], StartSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME

	sim.servers[serverId].StartSnapshot(snapshotId)

}

// Callback for servers to notify the simulator that the snapshot process has
// completed on a particular server
func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})
	// TODO: IMPLEMENT ME

	// don't want to call snapshot we want to update a field that signals the snapshot is done for this particular server
	// want a list of servers that have completed the snapshot
	// need snapshotId and serverId

	snapStat, _ := sim.status.Load(snapshotId)

	if snapStat == nil {
		snapStat = &SnapStatus{completed: false, doneServers: make(map[string]bool)}
	}

	stat := snapStat.(*SnapStatus)
	stat.doneServers[serverId] = true

	// check if all done
	if len(stat.doneServers) == len(sim.servers) {
		stat.completed = true
	}

	sim.status.Store(snapshotId, stat)

	// Check global completion
	allComplete := true
	sim.status.Range(func(key, value interface{}) bool {
		if snapStatus, ok := value.(*SnapStatus); ok && !snapStatus.completed {
			allComplete = false
			return false // Stop the iteration as soon as one incomplete snapshot is found
		}
		return true
	})

	// Store the readiness in the map to maintain atomicity with other related states
	if allComplete {
		sim.status.Store("readyForCollection", true)
		sim.readyForCollection = true
	} else {
		sim.status.Store("readyForCollection", false)
	}

}

// Collect and merge snapshot state from all the servers.
// This function blocks until the snapshot process has completed on all servers.
func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
	// TODO: IMPLEMENT ME
	snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}

	//stall
	for {
		if ready, ok := sim.status.Load("readyForCollection"); ok && ready.(bool) {
			log.Println(sim.readyForCollection)
			break
		}
	}

	for id := range sim.servers {
		if log, ok := sim.servers[id].SnapshotLog[snapshotId]; ok {
			snap.tokens[id] = log.currTokens // load in the snapshot tokens
			snap.messages = append(snap.messages, log.messages...)
		} else {
			continue
		}

	}
	return &snap
}

// // Callback for servers to notify the simulator that the snapshot process has
// // completed on a particular server
// func (sim *Simulator) NotifySnapshotComplete(serverId string, snapshotId int) {
// 	sim.logger.RecordEvent(sim.servers[serverId], EndSnapshot{serverId, snapshotId})
// 	// TODO: IMPLEMENT ME

// 	// don't want to call snapshot we want to update a field that signals the snapshot is done for this particular server
// 	// want a list of servers that have completed the snapshot
// 	// need snapshotId and serverId

// 	_, exist := sim.status[snapshotId]

// 	if !exist {
// 		sim.status[snapshotId] = SnapStatus{false, make(map[string]bool)}
// 		sim.status[snapshotId].doneServers[serverId] = true
// 	} else {
// 		sim.status[snapshotId].doneServers[serverId] = true
// 	}

// 	// check if all done
// 	if len(sim.status[snapshotId].doneServers) == len(sim.servers) {
// 		simStat := sim.status[snapshotId]
// 		simStat.completed = true

// 		sim.status[snapshotId] = simStat
// 	}

// 	for _, stat := range sim.status {
// 		if !stat.completed {
// 			return
// 		}
// 	}

// 	sim.readyForCollection = true

// }

// // Collect and merge snapshot state from all the servers.
// // This function blocks until the snapshot process has completed on all servers.
// func (sim *Simulator) CollectSnapshot(snapshotId int) *SnapshotState {
// 	// TODO: IMPLEMENT ME
// 	snap := SnapshotState{snapshotId, make(map[string]int), make([]*SnapshotMessage, 0)}

// 	//stall
// 	for !sim.readyForCollection {
// 	}
// 	log.Println(sim.readyForCollection)

// 	for id := range sim.servers {
// 		if log, ok := sim.servers[id].SnapshotLog[snapshotId]; ok {
// 			snap.tokens[id] = log.currTokens // load in the snapshot tokens
// 			snap.messages = append(snap.messages, log.messages...)
// 		} else {
// 			continue
// 		}

// 	}
// 	return &snap
// }

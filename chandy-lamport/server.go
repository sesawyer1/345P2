package chandy_lamport

import "log"

// The main participant of​ the distributed snapshot​ protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	snapshotStarted   bool
	SnapshotLog       map[int]SnapshotDetails
	receivedSnapshots map[int]bool
}

type SnapshotDetails struct {
	snapId       int
	openChannels map[string]bool
	messages     []*SnapshotMessage
	currTokens   int
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		false,
		make(map[int]SnapshotDetails),
		make(map[int]bool),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME

	switch msg := message.(type) {
	case TokenMessage:

		// updating the tokens is indpendent of the snapshot process
		server.Tokens += msg.numTokens

		// record message only if snapshot started and recording message from this channel (only takes one snapshot)
		if server.snapshotStarted {

			// loop through snapshot log to add to all snapshots happening
			for _, snap := range server.SnapshotLog {

				// only add to open channels
				channelOpen, exists := snap.openChannels[src]

				if exists && channelOpen {

					var snapLog = server.SnapshotLog[snap.snapId]
					snapLog.messages = append(server.SnapshotLog[snap.snapId].messages, &SnapshotMessage{src, server.Id, message})
					server.SnapshotLog[snap.snapId] = snapLog

				}
			}
		}

	case MarkerMessage:
		_, exists := server.receivedSnapshots[msg.snapshotId]
		if !exists {
			// case 1:
			server.receivedSnapshots[msg.snapshotId] = true
			server.snapshotStarted = true

			// Record its own state
			server.SnapshotLog[msg.snapshotId] = SnapshotDetails{msg.snapshotId, make(map[string]bool), make([]*SnapshotMessage, 0), server.Tokens}
			log.Printf("snapshot id %v server id %v tokens %v\n", msg.snapshotId, server.Id, server.Tokens)
			// Mark channel with src as empty (do not make it open)
			server.SnapshotLog[msg.snapshotId].openChannels[src] = false

			// send marker to all neighbors
			server.SendToNeighbors(message)

			// record from all incoming channels except src *** NOT DONE
			for _, link := range server.inboundLinks {
				if link.src != src {
					server.SnapshotLog[msg.snapshotId].openChannels[link.src] = true
				}
			}

		} else {

			// case 2: stop recording messages from src
			server.SnapshotLog[msg.snapshotId].openChannels[src] = false

		}

		// once have received markers from all inbound links, we are done
		counter := 0
		for _, open := range server.SnapshotLog[msg.snapshotId].openChannels {
			if !open {
				counter += 1
			}
		}
		if counter == len(server.inboundLinks) {
			server.sim.NotifySnapshotComplete(server.Id, msg.snapshotId)
		}
	}

}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME

	// Step 1:
	server.SnapshotLog[snapshotId] = SnapshotDetails{snapshotId, make(map[string]bool), make([]*SnapshotMessage, 0), server.Tokens}
	log.Printf("snapshot id %v server id %v tokens %v\n", snapshotId, server.Id, server.Tokens)
	server.snapshotStarted = true
	server.receivedSnapshots[snapshotId] = true

	// Step 2:
	server.SendToNeighbors(MarkerMessage{snapshotId})

	// Step 3:
	for _, link := range server.inboundLinks {
		server.SnapshotLog[snapshotId].openChannels[link.src] = true
	}

}

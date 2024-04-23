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
	snapshotStarted bool
	messages        []Message
	receivedMarker  bool
	currSnapshotId  int
}

type LocalState struct {
	Id         string
	snapshotId int
	tokens     int
}

type Message struct {
	content string
	src     string
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
		make([]Message, 0),
		false,
		0,
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

	// if not started snapshot, do start snapshot
	if !server.snapshotStarted {
		server.StartSnapshot(server.currSnapshotId)
	}
	// record message received on not this interface
	for _, link := range server.outboundLinks {
		// go through every message received by the link and add to the server's message list
		for !(link.events.Empty()) {
			event := link.events.Pop()
			switch event.(type) {
			case ReceivedMessageEvent:
				server.messages = append(server.messages, Message{event.(ReceivedMessageEvent).message.(TokenMessage).String(), event.(ReceivedMessageEvent).src})
			}
		}
	}

	server.sim.NotifySnapshotComplete(server.Id, server.currSnapshotId)

}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME
	if server.snapshotStarted {
		return
	} else {
		// record local state
		server.sim.logger.RecordEvent(server, LocalState{server.Id, snapshotId, server.Tokens})

		// send marker messages
		server.SendToNeighbors(MarkerMessage{snapshotId})

		server.snapshotStarted = true
		server.currSnapshotId = snapshotId
	}

}

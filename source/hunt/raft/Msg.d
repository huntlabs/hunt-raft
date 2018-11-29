module hunt.raft.Msg;

enum EntryType {
	EntryNormal     = 0,
	EntryConfChange = 1,
}

struct Entry {
	ulong     	Term;
	ulong     	Index;
	EntryType  	Type;
	string     	Data;
}

struct SnapshotMetadata {
	ConfState 	CS;
	ulong    	Index;
	ulong    	Term;
}

struct Snapshot {
	string    			Data;
	SnapshotMetadata 	Metadata;
}

enum MessageType {
	MsgHup             = 0,
	MsgBeat            = 1,
	MsgProp            = 2,
	MsgApp             = 3,
	MsgAppResp         = 4,
	MsgVote            = 5,
	MsgVoteResp        = 6,
	MsgSnap            = 7,
	MsgHeartbeat       = 8,
	MsgHeartbeatResp   = 9,
	MsgUnreachable     = 10,
	MsgSnapStatus      = 11,
	MsgCheckQuorum     = 12,
	MsgTransferLeader  = 13,
	MsgTimeoutNow      = 14,
	MsgReadIndex       = 15,
	MsgReadIndexResp   = 16,
	MsgPreVote         = 17,
	MsgPreVoteResp     = 18,
}

struct Message {
	MessageType 	Type;
	ulong      		To;
	ulong      		From;
	ulong      		Term;
	ulong      		LogTerm;
	ulong      		Index;
	Entry[]     	Entries;
	ulong     	 	Commit;
	Snapshot    	snap;
	bool        	Reject;
	ulong     	 	RejectHint;
	string      	Context;
}

struct HardState {
	ulong 		Term;
	ulong 		Vote;
	ulong 		Commit;
}

struct ConfState {
	ulong[] 	Nodes;
}

enum ConfChangeType {
	ConfChangeAddNode    = 0,
	ConfChangeRemoveNode = 1,
	ConfChangeUpdateNode = 2,
}

struct ConfChange{
	ulong          		ID;
	ConfChangeType  	Type;
	ulong          		NodeID ;
	string          	Context;
}


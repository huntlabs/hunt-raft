module hunt.raft.Node;

import hunt.raft.Msg;
import hunt.raft.Storage;
import hunt.raft.Raft;
import hunt.raft.Readonly;
import hunt.raft.Status;
import hunt.raft.Util;

import hunt.util.Serialize;
import hunt.logging;

import std.container;




alias Context = Object;
alias SnapshotStatus = int;

immutable SnapshotStatus SnapshotFinish = 1;
immutable SnapshotStatus SnapshotFailure = 2;

immutable HardState emptyState;
enum ErrStopped = "raft: stopped";

struct SoftState{
	ulong 		Lead;
	StateType 	RaftState;

	bool equals(SoftState *b)
	{
		return Lead == b.Lead && RaftState == b.RaftState;
	}
}

struct Ready
{
	SoftState* 		softstate;
	HardState		hs;

	ReadState[]		ReadStates;
	Entry[]			Entries;

	Snapshot		snap;
	Entry[]			CommittedEntries;

	Message[]		Messages;

	bool			mustSync;


	bool containsUpdates()
	{
		return  softstate != null || !IsEmptyHardState(hs) ||
			!IsEmptySnap(snap) || Entries.length > 0 ||
			CommittedEntries.length > 0 || Messages.length > 0 ||
				ReadStates.length != 0 ;

	}


	static bool MustSync( HardState prevst , HardState st , int entsnum)
	{
		return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term;
	}
	
	static Ready newReady(Raft r , SoftState* prevSortSt , HardState prevHardST)
	{
		Ready rd = {
		Entries:			r._raftLog.unstableEntries() ,
		CommittedEntries:	r._raftLog.nextEnts() ,
		Messages: 			r._msgs};
		
		auto softSt = r.softState();
		if(prevSortSt != null && !softSt.equals(prevSortSt))
			rd.softstate = softSt;
		
		auto hardSt = r.hardState();
		if(!isHardStateEqual(hardSt , prevHardST))
			rd.hs = hardSt;

		if(r._raftLog._unstable._snap != null)
			rd.snap = *r._raftLog._unstable._snap;

		if(r._readStates.length != 0)
			rd.ReadStates = r._readStates;
		
		rd.mustSync = Ready.MustSync(rd.hs , prevHardST , cast(int)rd.Entries.length);
		return rd;
	}
}

bool isHardStateEqual(HardState a ,HardState b)
{
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit;
}

bool IsEmptyHardState(HardState st)
{
	return isHardStateEqual(st , emptyState);
}

bool IsEmptySnap(Snapshot sp)
{
	return sp.Metadata.Index == 0;
}

struct Peer
{
	ulong 		ID;
	string 		Context;
}




version(NO_ORIGIN):

interface Node
{
	void Tick();
	ErrString Campaign(Context ctx);
	ErrString Propose(Context ctx , string data);
	ErrString ProposeConfChange(Context ctx , ConfChange cc);

	ErrString Step(Context ctx , Message msg);

	DList!Ready ready();

	void Advance();
	ConfState  ApplyConfChange(ConfChange cc);
	void TransferLeadership(Context ctx , ulong lead , ulong transferee);
	ErrString ReadIndex(Context ctx , string rctx);
	Status status();
	void ReportUnreachable(ulong id);
	void ReportSnapshot(ulong id , SnapshotStatus status);

	void Stop();

}




Node StartNode(Config c , Peer[] peers)
{
	auto r = new Raft(c);

	r.becomeFollower(1 , None);

	foreach( peer ; peers)
	{
		ConfChange cc = {Type:ConfChangeType.ConfChangeAddNode , NodeID:peer.ID , Context:peer.Context};
		byte[] ser = serialize(cc);
		Entry[] ents;
		Entry e =  {Type:EntryType.EntryConfChange , Term : 1 , Index : r._raftLog.lastIndex() + 1 , Data:cast(string)ser};
		ents ~= e;
		r._raftLog.append(ents);
	}

	r._raftLog._committed = r._raftLog.lastIndex();

	foreach(peer ; peers)
	{
		r.addNode(peer.ID);
	}

	auto n = new node();
	// go run.
	return n;
}

Node RestartNode(Config c)
{
	auto r = new Raft(c);

	auto n = new node();

	//go run

	return n;
}


class node : Node
{
	DList!Message 				_propc;
	DList!Message 				_recvc;
	DList!ConfChange			_confc;
	DList!ConfState				_confstatec;
	DList!Ready					_readyc;
	DList!Object				_advancec;
	DList!Object				_tickc;
	DList!Object				_done;
	DList!Object				_stop;
	DList!(DList!Status)		_status;


	this()
	{

	}




	//next.
	void Stop()
	{
		while(!_done.empty())
			break;
	}

	void run(Raft r)
	{
		DList!Message* 	propc ;
		DList!Ready*   	readyc;
		DList!Object*	advancec;
		ulong			prevLastUnstablei, prevLastUnstablet;
		bool			havePrevLastUnstablei;
		ulong 			prevSnapi;
		Ready			rd;

		ulong lead = None;
		SoftState* prevSoftSt = r.softState();
		HardState prevHardSt = emptyState;

		while(1)
		{
			if( advancec != null)
			{
				readyc = null;
			}
			else
			{
				//next.
			}

			if(lead != r._lead)
			{
				if(r.hasLeader())
				{
					if( lead == None)
					{
						logInfo(format("raft.node: %x elected leader %x at term %d",
								r._id, r._lead, r._Term));
					}
					else
					{
						logInfo(format("raft.node: %x changed leader from %x to %x at term %d",
								r._id, lead, r._lead, r._Term));
					}
					propc = &_propc;
				}
				else
				{
					logInfo("raft.node: %x lost leader %x at term %d" , r._id , lead , r._Term);
					propc = null;
				}

				lead = r._lead;
			}

			while(1)
			{
				if(!propc.empty())
				{
					Message m = propc.front();
					m.From = r._id;
					r.Step(m);
					propc.removeFront();
				}
				else if(!_recvc.empty())
				{
					Message m = _recvc.front();
					auto ok = m.From in r._prs;
					if( ok || !IsResponseMsg(m.Type)){
						r.Step(m);
					}
					_recvc.removeFront();
				}
				else if(!_confc.empty())
				{
					ConfChange cc = _confc.front();
					if( cc.NodeID == None)
					{
						r.resetPendingConf();
						//next
						ConfState cs = { Nodes:r.nodes()};
						_confstatec.insertBack(cs);
						_confc.removeFront();
						break;
					}
					switch(cc.Type)
					{
						case ConfChangeType.ConfChangeAddNode:
							r.addNode(cc.NodeID);
							break;
						case ConfChangeType.ConfChangeRemoveNode:
							if(cc.NodeID == r._id)
								propc = null;
							r.removeNode(cc.NodeID);
							break;
						case ConfChangeType.ConfChangeUpdateNode:
							r.resetPendingConf();
							break;
						default:
							logError("unexpected conf type");
					}
					//next
					ConfState cs = {Nodes:r.nodes()};
					_confstatec.insertBack(cs);
					_confc.removeFront();
				}
				else if(!_tickc.empty())
				{
					r._tick();
					_tickc.removeFront();
				}
				else if(readyc !is null)
				{
					//next
					//if(rd.softstate != null)
						prevSoftSt = rd.softstate;

					if(rd.Entries.length > 0 )
					{
						prevLastUnstablei = rd.Entries[$ - 1].Index;
						prevLastUnstablet = rd.Entries[$ - 1].Term;
						havePrevLastUnstablei = true;
					}

					if( !IsEmptyHardState(rd.hs))
					{
						prevHardSt = rd.hs;
					}

					r._msgs = null;
					r._readStates = null;
					advancec = &_advancec;
					readyc.insertBack(rd);
				}
				else if(!advancec.empty())
				{
					if(prevHardSt.Commit != 0)
						r._raftLog.appliedTo(prevHardSt.Commit);

					if(havePrevLastUnstablei)
					{
						r._raftLog.stableTo(prevLastUnstablei , prevLastUnstablet);
						havePrevLastUnstablei = false;
					}

					r._raftLog.stableSnapTo(prevSnapi);
					advancec.removeFront();
					advancec = null;
				}
				else if(!_status.empty())
				{
					auto c = _status.front();
					c.insertBack(getStatus(r));
					_status.removeFront();
				}
				else if(!_stop.empty())
				{
					//next
					//close(_done);
				}
			}


		}
	}


	void Tick()
	{
		//next
		_tickc.insertBack(new Object);
	}

	ErrString Campaign(Context ctx)
	{
		Message msg = {Type : MessageType.MsgHup};
		return step(ctx ,msg);
	}

	ErrString Propose(Context ctx , string data)
	{
		Message msg = {Type : MessageType.MsgProp , Entries :[{Data : data}]};
		return step(ctx , msg);
	}

	string ProposeConfChange(Context ctx , ConfChange cc)
	{
		return ErrNil;
	}


	ErrString Step(Context ctx , Message m)
	{
		if( IsLocalMsg(m.Type))
			return ErrNil;

		return step(ctx , m);
	}

	ErrString step(Context ctx , Message m)
	{
		//next
		if(m.Type == MessageType.MsgProp)
		{
			_propc.insertBack(m);
		}
		else
		{
			_recvc.insertBack(m);
		}

		return ErrNil;
	}

	DList!Ready ready()
	{
		return _readyc;
	}

	void Advance()
	{
		_advancec.insertBack(new Object);
	}

	ConfState ApplyConfChange(ConfChange cc)
	{
		//next
		_confc.insertBack(cc);

		while(!_confstatec.empty()){}

		ConfState cs = _confstatec.front();

		_confstatec.removeBack();


		return cs;
	}

	Status status()
	{
		//next
		//_status.
		Status st;

		return st;
	}

	void ReportUnreachable(ulong id)
	{

	}

	void ReportSnapshot(ulong id , SnapshotStatus status)
	{

	}

	void TransferLeadership(Context ctx , ulong lead , ulong transferee)
	{

	}

	ErrString ReadIndex(Context ctx , string rctx)
	{

		Entry[] ents = [{Data:rctx}];
		Message msg = {Type : MessageType.MsgReadIndex , Entries : ents};
		step(ctx , msg);
		return ErrNil;
	}

}
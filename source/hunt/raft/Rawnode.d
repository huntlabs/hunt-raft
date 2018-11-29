module hunt.raft.Rawnode;

import hunt.raft.Raft;
import hunt.raft.Node;
import hunt.raft.Msg;
import hunt.raft.Log;
import hunt.raft.Status;
import hunt.raft.Storage;
import hunt.raft.Util;

import hunt.logging;
import hunt.util.serialize;

enum ErrStepLocalMsg = "raft: cannot step raft local message";
enum ErrStepPeerNotFound = "raft: cannot step as peer not found";

class RawNode
{
	Raft		_raft;
	SoftState*	_preSoftSt;
	HardState	_prevHardSt;


	Ready newReady()
	{
		return Ready.newReady(_raft , _preSoftSt , _prevHardSt);
	}


	bool isLeader()
	{
		return _raft._lead == _raft._id;
	}

	void commitReady(Ready rd)
	{
		 if(rd.softstate != null)
		 {
			_preSoftSt = rd.softstate;
		 }

		if (!IsEmptyHardState(rd.hs))
		{
			_prevHardSt = rd.hs;
		}

		if(_prevHardSt.Commit != 0)
		{
			_raft._raftLog.appliedTo(_prevHardSt.Commit);
		}

		if( rd.Entries.length > 0)
		{
			auto e = rd.Entries[$ - 1];
			_raft._raftLog.stableTo(e.Index , e.Term);
		}

		if(!IsEmptySnap(rd.snap))
		{
			_raft._raftLog.stableSnapTo(rd.snap.Metadata.Index);
		}

		if(rd.ReadStates != null)
		{
			_raft._readStates = null;
		}
	}


	this(Config c)
	{
		_raft = new Raft(c);
	}



	this(Config c , Peer[] peers)
	{
		if( c._ID == 0)
		{
			logError("config.ID must not be zero");
		}
		auto r = new Raft(c);

		ulong lastIndex;
		auto err = c._storage.LastIndex(lastIndex);
		if( err != ErrNil)
		{
			logError(err);
		}

		if( lastIndex == 0)
		{
			r.becomeFollower(1 , None);
			Entry[] ents;
			foreach( i , peer ; peers )
			{
				ConfChange cc = { Type:ConfChangeType.ConfChangeAddNode ,
				NodeID : peer.ID , Context: peer.Context};
				auto data = cast(string)serialize(cc);
				Entry ent = {Type:EntryType.EntryConfChange , Term:1 , Index:i + 1 ,
				Data:data};
				ents ~= ent;
			}

			r._raftLog.append(ents);
			r._raftLog._committed = ents.length;
			foreach( peer ; peers)
			{
				r.addNode(peer.ID);
			}
		}

		_preSoftSt = r.softState();
		if( lastIndex == 0)
			_prevHardSt = emptyState;
		else
			_prevHardSt = r.hardState();

		_raft = r;
	}

	void Tick()
	{
		_raft._tick();
	}

	void TickQuiesced()
	{
		_raft._electionElapsed++;
	}

	ErrString Campaign()
	{
		Message msg = { Type : MessageType.MsgHup  };
		return _raft.Step(msg);

	}

	ErrString Propose(string data)
	{
		Message msg = {Type : MessageType.MsgProp , From : _raft._id ,
		Entries :[{Data : data}]};

		return _raft.Step(msg);
	}

	ErrString ProposeConfChange(ConfChange cc)
	{
		auto data = cast(string)serialize(cc);

		Message msg = {Type : MessageType.MsgProp , Entries:
		[{Type : EntryType.EntryConfChange , Data : data}]};

		return _raft.Step(msg);
	}

	ConfState ApplyConfChange(ConfChange cc)
	{
		if(cc.NodeID == None)
		{
			_raft.resetPendingConf();
			ConfState cs = {Nodes:_raft.nodes()};
			return cs;
		}

		switch(cc.Type)
		{
			case ConfChangeType.ConfChangeAddNode:
				_raft.addNode(cc.NodeID);
				break;
			case ConfChangeType.ConfChangeRemoveNode:
				_raft.removeNode(cc.NodeID);
				break;
			case ConfChangeType.ConfChangeUpdateNode:
				_raft.resetPendingConf();
				break;
			default:
				logError("unexpected conf type");
		}

		ConfState cs = {Nodes : _raft.nodes()};
		return cs;
	}

	ErrString Step(Message m)
	{
		if(  IsLocalMsg(m.Type))
			return ErrStepLocalMsg;

		auto prs = m.From in _raft._prs;

		if(prs != null || !IsResponseMsg(m.Type))
		{
			return _raft.Step(m);
		}

		return ErrStepPeerNotFound;
	}

	Ready ready()
	{
		auto rd = newReady();
		_raft._msgs = null;
		return rd;
	}

	bool HasReady()
	{
		if(_raft.softState().equals(_preSoftSt))
			return true;

		auto hardSt = _raft.hardState();

		if(!IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt , _prevHardSt))
			return true;

		if(_raft._raftLog._unstable._snap != null && !IsEmptySnap(*_raft._raftLog._unstable._snap))
			return true;
	
		if(_raft._readStates.length != 0)
			return true;

		return false;
	}


	void Advance(Ready rd)
	{
		commitReady(rd);
	}

	Status status()
	{
		return getStatus(_raft);
	}

	void ReportUnreachable(ulong id)
	{
		Message msg = {Type : MessageType.MsgUnreachable , From : id};
		_raft.Step(msg);
	}

	void ReportSnapshot(ulong id , SnapshotStatus status)
	{
		auto rej = (status == SnapshotFailure);

		Message msg = {Type : MessageType.MsgSnapStatus , From : id , Reject : rej};

		_raft.Step(msg);
	}

	void TransferLeader(ulong transferee)
	{
		Message msg = {Type:MessageType.MsgTransferLeader , From : transferee};
		_raft.Step(msg);
	}

	void ReadIndex(string rctx)
	{
		Message msg = {Type : MessageType.MsgReadIndex ,  Entries :[{Data : rctx}]};
		_raft.Step(msg);
	}
}




module raft.Raft;

import raft.Storage;
import raft.Read_only;
import raft.Log;
import raft.Progress;
import protocol.Msg;
import zhang2018.common.Log;
import raft.Node;
import std.algorithm;
import std.algorithm.sorting;
import raft.Util;
import core.stdc.stdlib;
import std.experimental.allocator;
import std.conv;

enum StateType{
	StateFollower = 0,
	StateCandidate,
	StateLeader,
	StatePreCandidate,
	numStates,
}

enum ReadOnlyOption
{
	ReadOnlySafe = 0,
	ReadOnlyLeaseBased,
}

alias CampaignType = string;

enum	campaignPreElection = "CampaignPreElection";
enum	campaignElection	= "CampaignElection";
enum	campaignTransfer 	= "CampaignTransfer";



class Config
{
	ulong 			_ID;
	ulong[]			_peers;
	int				_ElectionTick;
	int				_HeartbeatTick;

	Storage			_storage;
	ulong			_Applied;
	ulong 			_MaxSizePerMsg;
	int				_MaxInflightMsgs;
	bool			_CheckQuorum;
	bool			_PreVote;
	ReadOnlyOption	_ROOption;
	bool			_DisProForw;



	ErrString validate()
	{
		if (_ID == None)
			return "cannot use none as id";

		if(_HeartbeatTick <= 0)
			return "heartbeat tick must be greater than 0";

		if(_ElectionTick <= _HeartbeatTick)
			return "election tick must be greater than heartbeat tick";

		if(_storage is null)
			return "storage cannot be nil";

		if(_MaxInflightMsgs <= 0)
			return "max inflight messages must be greater than 0";

		return ErrNil;
	}
}


class Raft
{
	ulong			_id;
	ulong 			_Term;
	ulong			_Vote;
	ReadState[]		_readStates;
	raftLog			_raftLog;
	int				_maxInflight;
	ulong			_maxMsgSize;

	Progress[ulong]	_prs;
	StateType		_state;
	bool[ulong]		_votes;

	Message[]		_msgs;
	ulong 			_lead;
	ulong			_leadTransferee;
	bool			_pendingConf;
	readOnly		_readOnly;

	int				_electionElapsed;
	int				_heartbeatElapsed;
	bool			_checkQuorum;
	bool			_preVote;

	int				_heartbeatTimeout;
	int				_electionTimeout;

	int				_randomizedElectionTimeout;
	bool			_disProForw;
	void 	delegate(Message) _step;
	void	delegate() _tick;

	this(Config c)
	{
		auto err = c.validate();
		if(err != ErrNil)
			log_error(err);

		_raftLog = new raftLog(c._storage);
		HardState hs;
		ConfState cs;
		err =  c._storage.InitalState(hs , cs);
		if(err != ErrNil)
			log_error(err);

		auto peers = c._peers;
		if(cs.Nodes.length > 0)
		{
			if(peers.length > 0)
			{
				log_error("cannot specify both newRaft(peers) and ConfState.Nodes)");
			}
			peers = cs.Nodes;
		}

		_id = c._ID;
		_lead = None;
		_maxMsgSize = c._MaxSizePerMsg;
		_maxInflight = c._MaxInflightMsgs;
		_electionTimeout = c._ElectionTick;
		_heartbeatTimeout = c._HeartbeatTick;

		_checkQuorum	=	c._CheckQuorum;
		_preVote		=  c._PreVote;
		_readOnly 		= new readOnly(c._ROOption);
		_disProForw		= c._DisProForw;

		foreach(p ; peers)
		{
			auto prs = new Progress();
			prs._Next = 1;
			prs._ins = new inflights(_maxInflight);
			_prs[p] = prs;

		}

		if( !isHardStateEqual(hs ,emptyState)){
			loadState(hs);
		}

		if(c._Applied > 0 )
		{
			_raftLog.appliedTo(c._Applied);
		}

		becomeFollower(_Term , None);

		string nodestr;
		foreach( n ; nodes())
		{
			nodestr ~= to!string(n);
		}

		srand(cast(uint)(_id * _id));

		log_info(log_format("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
				_id, nodestr, _Term, _raftLog._committed, _raftLog._applied,
				_raftLog.lastIndex(), _raftLog.lastTerm()));

	}

	bool hasLeader()
	{
		return _lead != None;
	}

	SoftState* softState()
	{
		return theAllocator.make!SoftState(_lead , _state );
	}

	HardState hardState()
	{
		HardState hard = { Term : _Term , Vote : _Vote , Commit :_raftLog._committed};
		return hard;
	}

	int quorum()
	{
		return cast(int)_prs.length /2 + 1;
	}

	ulong[] nodes()
	{
		ulong[] ns = _prs.keys();
		ns.sort();
		return ns;
	}

	void send(Message m)
	{
		m.From = _id;
		if( m.Type == MessageType.MsgVote ||
			m.Type == MessageType.MsgVoteResp ||
			m.Type == MessageType.MsgPreVote ||
			m.Type == MessageType.MsgPreVoteResp)
		{
			if(m.Term == 0)
			{
				log_error(log_format("term should be set when sending %s", m.Type));
			}
		}
		else
		{
			if(m.Term != 0)
			{
				log_error(log_format("term should not be set when sending %s (was %d)", m.Type, m.Term));
			}

			if( m.Type != MessageType.MsgProp && m.Type != MessageType.MsgReadIndex)
			{
				m.Term = _Term;
			}
		}
		_msgs ~= m;
	}

	void sendAppend(ulong to)
	{
		auto pr = _prs[to];
		if(pr.IsPaused())
			return;

		Message m;
		m.To = to;

		ulong term;
		Entry[] ents;
		auto errt = _raftLog.term(pr._Next - 1 , term);
		auto erre = _raftLog.entries(pr._Next  , _maxMsgSize , ents);

		if( errt != ErrNil || erre != ErrNil )
		{
			if( !pr._RecentActive )
			{
				log_debug(log_format("ignore sending snapshot to %x since it is not recently active", to));
				return;
			}

			m.Type = MessageType.MsgSnap;
			Snapshot snap;
			auto err = _raftLog.GetSnap(snap);
			if( err != ErrNil)
			{
				if( err == ErrSnapshotTemporarilyUnavailable )
				{
					log_debug(log_format("%x failed to send snapshot to %x because snapshot is temporarily unavailable", _id, to));
					return ;
				}

				log_error(err);
			}

			if (IsEmptySnap(snap))
			{
				log_error("need non-empty snapshot");
			}

			m.snap = snap;
			auto sindex = snap.Metadata.Index;
			auto sterm = snap.Metadata.Term;

			log_debug(log_format("%d [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
					_id, _raftLog.firstIndex(), _raftLog._committed, sindex, sterm, to, pr));

			pr.becomeSnapshot(sindex);
			log_debug("%x paused sending replication messages to %x [%s]", _id, to, pr);
		}
		else{
			m.Type = MessageType.MsgApp;
			m.Index = pr._Next - 1;
			m.LogTerm = term;
			m.Entries = ents;
			m.Commit = _raftLog._committed;

			int n = cast(int)m.Entries.length;
			if( n != 0)
			{
				switch(pr._State)
				{
					case ProgressStateType.ProgressStateReplicate:
						auto last = m.Entries[n - 1].Index;
						pr.optimisticUpdate(last);
						pr._ins.add(last);
						break;
					case ProgressStateType.ProgressStateProbe:
						pr.pause();
						break;
					default:
						log_error(log_format("%x is sending append in unhandled state %s", _id, pr._State));
				}
			}


		}

		send(m);
	}

	void sendHeartbeat(ulong to , string ctx)
	{
		auto commit = min(_prs[to]._Match , _raftLog._committed);
		Message m = {To:to , Type:MessageType.MsgHeartbeat , Commit:commit , Context:ctx};
		send(m);
	}

	void bcastAppend()
	{
		foreach(  id ; _prs.keys())
		{
			if(id == _id)
				continue;
			sendAppend(id);	
		}
	}

	void bcastHeartbeat()
	{
		auto lastCtx = _readOnly.lastPendingRequestCtx();
		if(lastCtx.length == 0)
			bcastHeartbeatWithCtx(ErrNil);
		else
			bcastHeartbeatWithCtx(lastCtx);
	}



	void bcastHeartbeatWithCtx(string ctx)
	{
		foreach(  id ; _prs.keys())
		{
			if(id == _id)
				continue;
			sendHeartbeat(id , ctx);	
		}
	}

	bool maybeCommit()
	{
		ulong[] mis;
		foreach( id ; _prs.keys())
		{
			mis ~= _prs[id]._Match;
		}

		mis.reverse();
		mis.sort();
		auto mci = mis[quorum() - 1];
		return _raftLog.maybeCommit(mci , _Term);
	}

	void reset(ulong term)
	{
		if(_Term != term)
		{
			log_warning(_id , " reset term " , term);
			_Term = term;
			_Vote = None;
		}

		_lead = None;
		_electionElapsed = 0;
		_heartbeatElapsed = 0;
		resetRandomizedElectionTimeout();
		abortLeaderTransfer();
		_votes.clear();
		foreach( id ; _prs.keys())
		{
			auto prs = new Progress();
			prs._Next = _raftLog.lastIndex() + 1;
			prs._ins = new inflights(_maxInflight);
			_prs[id] = prs;
		
			if(id == _id)
			{
				prs._Match = _raftLog.lastIndex();
			}
		}

		_pendingConf = false;
		_readOnly = new readOnly(_readOnly._option);
	}

	void appendEntry(Entry []es)
	{
		auto li = _raftLog.lastIndex();
		for( auto i = 0; i < es.length ; i++)
		{
			es[i].Term = _Term;
			es[i].Index = li + 1 + i;
		}

		_raftLog.append(es);
		_prs[_id].maybeUpdate(_raftLog.lastIndex());
		maybeCommit();
	}

	void tickElection()
	{
		_electionElapsed++;

		if( promotable() && pastElectionTimeout())
		{
			_electionElapsed = 0;
			Message msg = {From:_id , Type:MessageType.MsgHup};
			Step(msg);
		}
	}

	void tickHeartbeat()
	{
		_heartbeatElapsed++;
		_electionElapsed++;

		if(_electionElapsed >= _electionTimeout)
		{
			_electionElapsed = 0;
			if( _checkQuorum)
			{
				Message msg = {From:_id , Type:MessageType.MsgCheckQuorum};
				Step(msg);
			}

			if(_state == StateType.StateLeader && _leadTransferee != None)
			{
				abortLeaderTransfer();
			}
		}

		if(_state != StateType.StateLeader)
			return;

		if(_heartbeatElapsed >= _heartbeatTimeout)
		{
			_heartbeatElapsed = 0;
			Message msg = {From:_id , Type:MessageType.MsgBeat};
			Step(msg);
		}
	}

	void becomeFollower(ulong term , ulong lead)
	{
		_step = &stepFollower;
		reset(term);
		_tick = &tickElection;
		_lead = lead;
		_state = StateType.StateFollower;
		log_info(log_format("%x became follower at term %d", _id, _Term));
	}

	void becomeCandidate()
	{
		if(_state == StateType.StateLeader)
		{
			log_error("invalid transition [leader -> candidate]");
		}

		_step = &stepCandidate;
		reset(_Term + 1);
		_tick = &tickElection;
		_Vote = _id;
		_state = StateType.StateCandidate;
		log_info(log_format("%x became candidate at term %d", _id, _Term));
	}

	void becomeLeader()
	{
		if(_state == StateType.StateFollower)
		{
			log_error("invalid transition [follower -> leader]");
		}

		_step = &stepLeader;
		reset(_Term);
		_tick = &tickHeartbeat;
		_lead = _id;
		_state = StateType.StateLeader;
		Entry[] ents;
		auto err = _raftLog.entries(_raftLog._committed + 1, noLimit , ents);
		if( err != ErrNil)
		{
			log_error("unexpected error getting uncommitted entries " , err);
		}

		auto nconf = numOfPendingConf(ents);
		if( nconf > 1)
		{
			log_error("unexpected multiple uncommitted config entry");
		}

		if(nconf == 1)
			_pendingConf = true;


		Entry[1] ent;
		appendEntry(ent);
		log_info(log_format("%x became leader at term %d", _id, _Term));
	}

	void campaign(CampaignType t)
	{
		ulong term;
		MessageType voteMsg;
		if(t == campaignPreElection)
		{
			becomeCandidate();
			voteMsg = MessageType.MsgPreVote;
			term = _Term + 1;
		}
		else 
		{
			becomeCandidate();
			voteMsg = MessageType.MsgVote;
			term = _Term;
		}

		if(quorum() == poll(_id , voteRespMsgType(voteMsg) , true))
		{
			if( t == campaignPreElection)
			{
				campaign(campaignElection);
			}
			else
			{
				becomeLeader();
			}
			return;
		}

		foreach( id ; _prs.keys)
		{
			if(id == _id)
				continue;

			log_info(log_format("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
					_id, _raftLog.lastTerm(), _raftLog.lastIndex(), voteMsg, id, _Term));

			string ctx;
			if(t == campaignTransfer)
				ctx = t;

			Message msg = {Term : term , To : id , Type : voteMsg , Index:_raftLog.lastIndex(),
			LogTerm:_raftLog.lastTerm() , Context:ctx};
			send(msg);
		}
	}

	int poll(ulong id , MessageType t , bool v)
	{
		if(v)
		{
			log_info(log_format("%x received %s from %x at term %d", _id, t, id, _Term));
		}
		else
		{
			log_info(log_format("%x received %s rejection from %x at term %d", _id, t, id, _Term));
		}

		auto exist = id in _votes;
		if( exist == null)
			_votes[id] = v;

		ulong granted = 0;
		foreach( key,value ; _votes)
		{
			if(value)
			{
				granted++;
			}
		}

		return cast(int)granted;
	}

	ErrString Step(Message m)
	{
		if(m.Term == 0){}
		else if(m.Term > _Term)
		{
			auto lead = m.From;
			if( m.Type == MessageType.MsgVote || m.Type == MessageType.MsgPreVote)
			{
				auto force = (m.Context == campaignTransfer);
				auto inLease = _checkQuorum && _lead != None && _electionElapsed < _electionTimeout;
				if( !force && inLease)
				{
					log_info(log_format("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
							_id, _raftLog.lastTerm(), _raftLog.lastIndex(), _Vote, m.Type, m.From, m.LogTerm, m.Index,
							_Term, _electionTimeout-_electionElapsed));
				}
				lead = None;
			}

			if(m.Type == MessageType.MsgPreVote){}
			else if(m.Type == MessageType.MsgPreVoteResp && !m.Reject){}
			else{
				log_info(log_format("%x [term: %d] received a %s message with higher term from %x [term: %d]",
							_id, _Term, m.Type, m.From, m.Term));
					becomeFollower(m.Term , lead);
			}

		}
		else if(m.Term < _Term)
		{
			if(_checkQuorum && (m.Type == MessageType.MsgHeartbeat || m.Type == MessageType.MsgApp))
			{
				Message msg = {To:m.From , Type:MessageType.MsgAppResp};
				send(msg);
			}
			else
			{
				log_info(log_format("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
						_id, _Term, m.Type, m.From, m.Term));
			}

			return ErrNil;
		}

		switch(m.Type)
		{
			case MessageType.MsgHup:
				if(_state != StateType.StateLeader)
				{
					Entry []ents;
					auto err = _raftLog.slice(_raftLog._applied + 1 , _raftLog._committed + 1, noLimit , ents);
					if( err != ErrNil)
					{
						log_error("unexpected error getting unapplied entries", err);
					}

					auto n = numOfPendingConf(ents);
					if( n != 0 && _raftLog._committed > _raftLog._applied)
					{
						log_warning(log_format("%x cannot campaign at term %d since there are still %d pending configuration changes to apply",
							_id, _Term, n));
						return ErrNil;
					}

					log_info(log_format("%x is starting a new election at term %d", _id, _Term));

					if(_preVote)
						campaign(campaignPreElection);
					else
						campaign(campaignElection);


				}
				else
				{
					log_debug("%x ignoring MsgHup because already leader", _id);
				}
			
				break;
			case MessageType.MsgVote , MessageType.MsgPreVote:
				if( (_Vote == None || m.Term > _Term || _Vote == m.From) && _raftLog.isUpToDate(m.Index ,
						m.LogTerm))
				{
					log_info(log_format("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
							_id, _raftLog.lastTerm(), _raftLog.lastIndex(), _Vote, m.Type, m.From, m.LogTerm, m.Index, _Term));
				
					Message msg = {To:m.From , Term:m.Term , Type : voteRespMsgType(m.Type)};
					send(msg);
					if(m.Type == MessageType.MsgVote)
					{
						_electionElapsed = 0;
						_Vote = m.From;
					}
				}
				else
				{
					log_info(log_format("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
							_id, _raftLog.lastTerm(), _raftLog.lastIndex(), _Vote, m.Type, m.From, m.LogTerm, m.Index, _Term));
				
					Message msg = {To: m.From ,
					Term : _Term , Type : voteRespMsgType(m.Type) , Reject:true};
					send(msg);
				}
				break;
			default:
				_step(m);
		}

		return ErrNil;
	}

	void stepLeader(Message m)
	{
		switch( m.Type)
		{
			case MessageType.MsgBeat:
				bcastHeartbeat();
				return;
			case MessageType.MsgCheckQuorum:
				if( !checkQuorumActive())
				{
					log_warning(_id , " stepped down to follower since quorum is not active");
					becomeFollower(_Term , None);
				}
				return;
			case MessageType.MsgProp:
				if(m.Entries.length == 0)
				{
					log_error(_id ," stepped empty MsgProp");
				}

				auto exist = _id in _prs;
				if(exist == null)
					return;

				log_info(m);
				if(_leadTransferee != None)
				{
					log_debug(log_format("%x [term %d] transfer leadership to %x is in progress; dropping proposal",
							_id, _Term, _leadTransferee));
					return;
				}

				foreach( i , e ; m.Entries)
				{
					if(e.Type == EntryType.EntryConfChange)
					{
						if(_pendingConf)
						{
							log_info(log_format("propose conf %s ignored since pending unapplied configuration", e));
							Entry en = {Type : EntryType.EntryNormal};
							m.Entries[i] = en;
						}
						_pendingConf = true;
					}
				}

				appendEntry(m.Entries);
				bcastAppend();
				return;
			case MessageType.MsgReadIndex:
				if( quorum() > 1)
				{	
					ulong term;
					auto err = _raftLog.term(_raftLog._committed , term);
					if(_raftLog.zeroTermOnErrCompacted(term , err) != _Term)
					{
						return;
					}

					switch(_readOnly._option)
					{
						case ReadOnlyOption.ReadOnlySafe:
							_readOnly.addRequest(_raftLog._committed , m);
							bcastHeartbeatWithCtx(m.Entries[0].Data);
							break;
						case ReadOnlyOption.ReadOnlyLeaseBased:
						{
							ulong ri = 0;
							if(_checkQuorum)
							{
								ri = _raftLog._committed;
							}

							if(m.From == None || m.From == _id)
							{
								ReadState rs = {Index:_raftLog._committed , RequestCtx: m.Entries[0].Data};
								_readStates ~= rs;
							}else
							{
								Message msg = {To : m.From ,  Type:MessageType.MsgReadIndexResp , Index:ri , Entries:m.Entries};

								send(msg);
							}
						}
						break;
						default:
							assert(0);
					}

				}
				else
				{
					ReadState rs = {Index:_raftLog._committed , RequestCtx:m.Entries[0].Data};
					_readStates ~= rs;
				}
				return;
			default:

		}

		auto pr = m.From in _prs;
		if(pr == null)
		{
			log_debug(log_format("%x no progress available for %x", _id, m.From));
		}

		switch(m.Type)
		{
			case MessageType.MsgAppResp:
				pr._RecentActive = true;

				if(m.Reject )
				{
					log_debug(log_format("%x received msgApp rejection(lastindex: %d) from %x for index %d",
						_id, m.RejectHint, m.From, m.Index));
					if(pr.maybeDecrTo(m.Index , m.RejectHint))
					{
						log_debug(log_format("%x decreased progress of %x to [%s]", _id, m.From, pr));
						if(pr._State == ProgressStateType.ProgressStateReplicate)
						{
							pr.becomeProbe();
						}
						sendAppend(m.From);
					}
				}
				else
				{
					auto oldPaused = pr.IsPaused();
					if( pr.maybeUpdate(m.Index))
					{
						if(pr._State == ProgressStateType.ProgressStateProbe)
							pr.becomeReplicate();
						else if(pr._State == ProgressStateType.ProgressStateSnapshot &&
							pr.needSnapshotAbort())
						{
							log_debug("%x snapshot aborted, resumed sending replication messages to %x [%s]", _id, m.From, pr);
							pr.becomeProbe();
						}
						else if(pr._State == ProgressStateType.ProgressStateReplicate)
							pr._ins.freeTo(m.Index);

						if(maybeCommit())
						{
							bcastAppend();
						}
						else if(oldPaused)
						{
							sendAppend(m.From);
						}

						if(m.From == _leadTransferee && pr._Match == _raftLog.lastIndex())
						{
							log_info(log_format("%x sent MsgTimeoutNow to %x after received MsgAppResp", _id, m.From));
							sendTimeoutNow(m.From);
						}
					}

				}
				break;
			case MessageType.MsgHeartbeatResp:
				pr._RecentActive = true;
				pr.resume();

				if( pr._State == ProgressStateType.ProgressStateReplicate && pr._ins.full())
				{
					pr._ins.freeFirstOne();
				}

				if(pr._Match < _raftLog.lastIndex())
				{
					sendAppend(m.From);
				}

				if(_readOnly._option != ReadOnlyOption.ReadOnlySafe || m.Context.length == 0)
					return ;

				auto ackCount = _readOnly.recvAck(m);
				if(ackCount < quorum())
				{
					return;
				}

				auto rss = _readOnly.advance(m);
				foreach( rs ; rss)
				{
					auto req = rs.req;
					if(req.From == None || req.From == _id)
					{
						ReadState rs0 = { Index:rs.index , RequestCtx:req.Entries[0].Data};
						_readStates ~= rs0;
					}
					else
					{
						Message msg = {To : req.From , Type : MessageType.MsgReadIndexResp ,
						Index:rs.index ,Entries:req.Entries};
						send(msg);
					}
				}
				break;
			case MessageType.MsgSnapStatus:
				if( pr._State != ProgressStateType.ProgressStateSnapshot)
					return;

				if (!m.Reject)
				{
					pr.becomeProbe();
					log_debug(log_format("%x snapshot succeeded, resumed sending replication messages to %x [%s]",
							_id, m.From, pr));
				}
				else
				{
					pr.snapshotFailure();
					pr.becomeProbe();
					log_debug(log_format("%x snapshot failed, resumed sending replication messages to %x [%s]", _id, m.From, pr));
				}

				pr.pause();
				break;
			case MessageType.MsgUnreachable:
				if( pr._State == ProgressStateType.ProgressStateReplicate)
				{
					pr.becomeProbe();
				}
				log_debug(log_format("%x failed to send message to %x because it is unreachable [%s]", 
						_id, m.From, pr));
				break;
			case MessageType.MsgTransferLeader:
				auto leadTransferee = m.From;
				auto lastLeadTransferee = _leadTransferee;
				if( lastLeadTransferee != None)
				{
					if( lastLeadTransferee == leadTransferee)
					{
						log_info(log_format("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
								_id, _Term, leadTransferee, leadTransferee));
						return;
					}
					abortLeaderTransfer();
					log_info(log_format("%x [term %d] abort previous transferring leadership to %x", _id, _Term, lastLeadTransferee));
				}

				if(leadTransferee == _id)
				{
					log_debug(_id , " is already leader. Ignored transferring leadership to self");
					return ;
				}

				log_info(log_format("%x [term %d] starts to transfer leadership to %x", _id, _Term, leadTransferee));
				_electionElapsed = 0;
				_leadTransferee = leadTransferee;
				if(pr._Match == _raftLog.lastIndex())
				{
					sendTimeoutNow(leadTransferee);
					log_info("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log",
						_id, leadTransferee, leadTransferee);
				}
				else
				{
					sendAppend(leadTransferee);
				}
				break;
			default:
			//	log_info(m.Type);
		}
	}

	void stepCandidate(Message m)
	{
		MessageType myVoteRespType;

		if(_state == StateType.StatePreCandidate)
			myVoteRespType = MessageType.MsgPreVoteResp;
		else
			myVoteRespType = MessageType.MsgVoteResp;

		switch(m.Type)
		{
			case MessageType.MsgProp:
				log_info("%x no leader at term %d; dropping proposal", _id, _Term);
				return;
			case MessageType.MsgApp:
				becomeFollower(_Term , m.From);
				handleAppendEntries(m);
				break;
			case MessageType.MsgHeartbeat:
				becomeFollower(_Term , m.From);
				handleHeartbeat(m);
				break;
			case MessageType.MsgSnap:
				becomeFollower(_Term , m.From);
				handleSnapshot(m);
				break;
			case MessageType.MsgTimeoutNow:
				log_debug(log_format("%x [term %d state %s] ignored MsgTimeoutNow from %x", _id, _Term, _state, m.From));
				break;
			default:
		}

		if( myVoteRespType == m.Type)
		{
			auto gr = poll(m.From , m.Type , !m.Reject);
			log_info(log_format("%x [quorum:%d] has received %d %s votes and %d vote rejections",
					_id, quorum(), gr, m.Type, _votes.length-gr));
			auto qr = quorum();
		
			if( gr == qr)
			{		
				if( _state == StateType.StatePreCandidate)
				{
					campaign(campaignElection);
				}
				else
				{
					becomeLeader();
					bcastAppend();
				}
			}
			else if( _votes.length - gr == qr)
			{
				becomeFollower(_Term , None);
			}
		}
	}	

	void stepFollower(Message m)
	{
		switch( m.Type)
		{
			case MessageType.MsgProp:
				if(_lead == None)
				{
					log_info(log_format("%x no leader at term %d; dropping proposal", _id, _Term));
					return;
				}
				else if(_disProForw)
				{
					log_info(log_format("%x not forwarding to leader %x at term %d; dropping proposal", _id, _lead, _Term));
				}

				m.To = _lead;
				send(m);
				break;
			case MessageType.MsgApp:
				_electionElapsed = 0;
				_lead = m.From;
				handleAppendEntries(m);
				break;
			case MessageType.MsgHeartbeat:
				_electionElapsed = 0;
				_lead = m.From;
				handleHeartbeat(m);
				break;
			case MessageType.MsgSnap:
				_electionElapsed = 0;
				_lead = m.From;
				handleSnapshot(m);
				break;
			case MessageType.MsgTransferLeader:
				if (_lead == None) {
					log_info(log_format("%x no leader at term %d; dropping leader transfer msg",
							_id, _Term));
					return;
				}
				m.To = _lead;
				send(m);
				break;
			case MessageType.MsgTimeoutNow:
				if (promotable()) {
					log_info(log_format("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.",
							_id, _Term, m.From));

					campaign(campaignTransfer);
				}
				else {

					log_info(log_format("%x received MsgTimeoutNow from %x but is not promotable", 
							_id, m.From));
				}
				break;
			case MessageType.MsgReadIndex:
				if (_lead == None) {
					log_info(log_format("%x no leader at term %d; dropping index reading msg",
							_id, _Term));
					return;
				}
				m.To = _lead;
				send(m);
				break;

			case MessageType.MsgReadIndexResp:
				if (m.Entries.length != 1) {
					log_error(log_format("%x invalid format of MsgReadIndexResp from %x, entries count: %d",
									_id, m.From, m.Entries.length));
					return;
				}
				ReadState rs = { Index:m.Index , RequestCtx:m.Entries[0].Data};
				_readStates ~= rs;
				break;
			default:

		}
	}


	void handleAppendEntries(Message m) {
		if (m.Index < _raftLog._committed) {
		
			Message msg = {To: m.From, Type: MessageType.MsgAppResp, Index: _raftLog._committed};
			send(msg);
			return;
		}

		ulong mlastIndex;
		auto ok = _raftLog.maybeAppend(m.Index , m.LogTerm , m.Commit , m.Entries , mlastIndex);

		if(ok){
			Message msg = { To:m.From , Type:MessageType.MsgAppResp , Index:mlastIndex};
			send(msg);
		} 
		else {
			ulong term;
			auto err = _raftLog.term(m.Index , term);

			log_debug(log_format("%s %x [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %x",
					err , _id, _raftLog.zeroTermOnErrCompacted(term , err), m.Index, m.LogTerm, m.Index, m.From));

			Message msg = {To: m.From, Type: MessageType.MsgAppResp,
			Index: m.Index, Reject: true, RejectHint: _raftLog.lastIndex()};

			send(msg);
		}
	}

	void handleHeartbeat(Message m) {
		_raftLog.commitTo(m.Commit);
		Message msg = {To: m.From, Type: MessageType.MsgHeartbeatResp, Context: m.Context};
		send(msg);
	}
	
	void handleSnapshot(Message m) {
		auto sindex= m.snap.Metadata.Index;
		auto sterm = m.snap.Metadata.Term;
		
		if (restore(m.snap)) {
			log_info(log_format("%x [commit: %d] restored snapshot [index: %d, term: %d]",
					_id, _raftLog._committed, sindex, sterm));
			Message msg = {To: m.From, Type: MessageType.MsgAppResp, Index: _raftLog.lastIndex()};
			send(msg);
		} else {
			log_info(log_format("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
					_id, _raftLog._committed, sindex, sterm));
			Message msg = {To: m.From, Type: MessageType.MsgAppResp, Index: _raftLog._committed};
			send(msg);
		}
	}

	bool restore(Snapshot s ) {
		if (s.Metadata.Index <= _raftLog._committed ){
			return false;
		}
		if (_raftLog.matchTerm(s.Metadata.Index, s.Metadata.Term)) {
			log_info(log_format("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
					_id, _raftLog._committed, _raftLog.lastIndex(), _raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term));
			_raftLog.commitTo(s.Metadata.Index);
			return false;
		}
		
		log_info(log_format("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
				_id, _raftLog._committed, _raftLog.lastIndex(), _raftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term));

		_raftLog.restore(s);

		foreach(  n ;s.Metadata.CS.Nodes) {
					
			ulong match = 0;
			auto next = _raftLog.lastIndex()+1;
			if (n == _id) {
				match = next - 1;
			}

			setProgress(n, match, next);
			log_info(log_format("%x restored progress of %x [%s]", _id, n, _prs[n]));
		}
		return true;
	}

	bool promotable()  {
		auto exist = _id in _prs;
		return exist != null;
	}
	
	void addNode(ulong id) {

		_pendingConf = false;
		if( id in _prs)
			return;
		setProgress(id, 0, _raftLog.lastIndex()+1);
		_prs[id]._RecentActive = true;
	}



	void removeNode(ulong id) {
		delProgress(id);
		_pendingConf = false;

		if (_prs.length == 0 ){
			return;
		}
		
	
		if (maybeCommit() ){
			bcastAppend();
		}

		if (_state == StateType.StateLeader && _leadTransferee == id) {
			abortLeaderTransfer();
		}
	}
	
	void resetPendingConf() { _pendingConf = false ;}
	
	void setProgress(ulong id, ulong match, ulong next) {
		auto prs = new Progress();
		prs._Next = next;
		prs._Match = match;
		prs._ins = new inflights(_maxInflight);
		_prs[id] = prs;
	}
	
	void delProgress(ulong id ) {
		_prs.remove(id);
	}
	
	void loadState( HardState state) {
		if( state.Commit < _raftLog._committed || state.Commit > _raftLog.lastIndex()) {

			log_error(log_format("%x state.commit %d is out of range [%d, %d]",
					_id, state.Commit, _raftLog._committed, _raftLog.lastIndex()));

		}
		_raftLog._committed = state.Commit;
		log_warning(_id , " loadState term " , _Term);
		_Term = state.Term;
		_Vote = state.Vote;
	}

	bool pastElectionTimeout()  {
		return _electionElapsed >= _randomizedElectionTimeout;
	}
	
	void resetRandomizedElectionTimeout() {
		_randomizedElectionTimeout = _electionTimeout + rand()%_electionTimeout;
		log_info(_id , " resetRandomizedElectionTimeout " , _randomizedElectionTimeout);
	}

	bool checkQuorumActive()  {
		int act;
			
		foreach( id ; _prs.keys()) 
		{
			if (id == _id) { // self is always active
				act++;
				continue;
			}
			
			if (_prs[id]._RecentActive) { 
				act++;
			}
			
			_prs[id]._RecentActive = false;
		}
		
		return act >= quorum();
	}

	void sendTimeoutNow(ulong to) {

		Message msg = {To: to, Type: MessageType.MsgTimeoutNow};
		send(msg);
	}
	
	void abortLeaderTransfer() {
		_leadTransferee = None;
	}
	
	int numOfPendingConf(Entry[] ents) {
		auto n = 0;
		foreach( i,v;ents) {
			if (ents[i].Type == EntryType.EntryConfChange) {
				n++;
			}
		}
		return n;
	}
}


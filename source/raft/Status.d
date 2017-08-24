module raft.Status;

import protocol.Msg;
import raft.Node;
import raft.Progress;
import raft.Raft;

import raft.Storage;

import zhang2018.common.Log;

struct Status
{
	ulong 				ID;
	HardState 			hs;
	SoftState 			ss;
	ulong 	  			Applied;
	Progress[ulong]		progress;
	ulong				LeadTransferee;

	string toString()
	{
		string format = `{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%s,"applied":%d,"progress":{`;
	    string data = log_format(format , ID, hs.Term, hs.Vote, hs.Commit, ss.Lead, ss.RaftState, Applied);
		if(progress.length == 0)
			data ~= "} , ";
		else
		{
			foreach( k , v ; progress)
			{
				format = `"%x":{"match":%d,"next":%d,"state":%q},`;
				data ~= log_format(format , k , v._Match , v._Next , v._State);
			}

			data.length = data.length - 1;
			data  ~= "},";
		}

		format = `"leadtransferee":"%x"}`;
		data ~= log_format(format , LeadTransferee);
		return data;
	}
}


Status getStatus(Raft r)
{
	Status s;
	s.ID 			 	= r._id;
	s.LeadTransferee 	= r._leadTransferee;
	s.hs = r.hardState();
	s.ss = *r.softState();

	s.Applied = r._raftLog._applied;

	if( s.ss.RaftState == StateType.StateLeader)
	{
		foreach(id , p ; r._prs)
			s.progress[id] = p;
	}
	return s;
}







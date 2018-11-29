module hunt.raft.Status;

import hunt.raft.Msg;
import hunt.raft.Node;
import hunt.raft.Progress;
import hunt.raft.Raft;
import hunt.raft.Storage;

import hunt.logging;

import std.format;

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
		string sformat = `{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%s,"applied":%d,"progress":{`;
	    string data = format(sformat , ID, hs.Term, hs.Vote, hs.Commit, ss.Lead, ss.RaftState, Applied);
		if(progress.length == 0)
			data ~= "} , ";
		else
		{
			foreach( k , v ; progress)
			{
				sformat = `"%x":{"match":%d,"next":%d,"state":%q},`;
				data ~= format(sformat , k , v._Match , v._Next , v._State);
			}

			data.length = data.length - 1;
			data  ~= "},";
		}

		sformat = `"leadtransferee":"%x"}`;
		data ~= format(sformat , LeadTransferee);
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







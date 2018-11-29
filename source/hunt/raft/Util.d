module hunt.raft.Util;

import hunt.raft.Msg;
import hunt.raft.Node;

import hunt.logging;
import hunt.util.serialize;

import std.format;

immutable ulong None = 0;
immutable noLimit = ulong.max;
bool IsLocalMsg(MessageType msgt)
{
	return msgt == MessageType.MsgHup ||
		msgt == MessageType.MsgBeat ||
		msgt == MessageType.MsgUnreachable ||
		msgt == MessageType.MsgSnapStatus ||
			msgt == MessageType.MsgCheckQuorum;
}

bool IsResponseMsg(MessageType msgt)
{
	return msgt == MessageType.MsgAppResp || 
		msgt == MessageType.MsgVoteResp ||
		msgt == MessageType.MsgHeartbeatResp ||
		msgt == MessageType.MsgUnreachable ||
			msgt == MessageType.MsgPreVoteResp;
}

MessageType voteRespMsgType(MessageType msgt)
{
	switch(msgt)
	{
		case MessageType.MsgVote:
			return MessageType.MsgVoteResp;
		case MessageType.MsgPreVote:
			return MessageType.MsgPreVoteResp;
		default:
			logError("not a vode message: " , msgt);
			return msgt;
	}

}



string DescribeMessage(Message m)
{
	string str = format("%x->%x %s Term:%d Log:%d/%d" ,
		m.From , m.To , m.Type , m.Term , m.LogTerm , m.Index);
	if( m.Reject )
	{
		str ~= " Rejected";
		if( m.Reject != 0)
		{
			str ~= format("(Hint:%d)", m.RejectHint);
		}
	}

	if ( m.Commit != 0)
	{
		str ~= format(" Commit:%d", m.Commit);
	}

	if( m.Entries.length > 0)
	{
		str ~=  "Entries:[";
		for( auto i = 0 ; i < m.Entries.length ; i++)
		{
			if( i != 0)
			{
				str ~= ", ";
			}

			str ~= DescribeEntry(m.Entries[i]);
		}
	}

  	if(!IsEmptySnap(m.snap))
	{
		str ~= " snapshot:V";
	}

	return str;
}


string DescribeEntry(Entry e)
{
	return format("%d/%d %s %s", e.Term, e.Index, e.Type , e.Data);
}


Entry[] limitSize(Entry[] ents , ulong maxSize)
{
	if(ents.length == 0)
		return ents;

	auto size = getsize!Entry(ents[0]);
	auto limit = 1;
	for(; limit < ents.length ; limit++)
	{	size += getsize(ents[limit]);
		if( size > maxSize)
			break;
	}

	return ents[0 .. limit];
}


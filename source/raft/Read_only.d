module raft.Read_only;


import raft.Raft;
import protocol.Msg;
import zhang2018.common.Log;

import std.experimental.allocator;

struct ReadState{
	ulong 		Index;
	string		RequestCtx;
}


struct readIndexStatus
{
	Message 		req;
	ulong 			index;
	Object[ulong] 	acks;
}

class readOnly
{
	ReadOnlyOption 					_option;
	readIndexStatus*[string]		_pendingReadIndex;
	string[]						_readIndexQueue;

	this(ReadOnlyOption option)
	{
		_option = option;
	}

	void addRequest(ulong index , Message m)
	{
		auto ctx = m.Entries[0].Data;
		if(ctx  in _pendingReadIndex)
			return;

		_pendingReadIndex[ctx] = theAllocator.make!readIndexStatus(m , index);
		_readIndexQueue ~= ctx;
	}

	int recvAck(Message m)
	{
		readIndexStatus **rs = (m.Context in _pendingReadIndex);
		if(rs == null)
			return 0;

		(*rs).acks[m.From] = new Object();
		return cast(int)(*rs).acks.length + 1;
	}

	readIndexStatus*[] advance(Message m)
	{
		int i = 0;
		bool found = false;

		auto ctx = m.Context;
		readIndexStatus*[] rss;

		foreach( v  ; _readIndexQueue)
		{
			i++;
			auto rs = v in _pendingReadIndex;
			if( rs == null)
			{
				log_error("cannot find corresponding read state from pending map");
			}
			rss ~=  *rs;
			if( v == ctx)
			{
				found = true;
				break;
			}		
		}

		if(found)
		{
			_readIndexQueue = _readIndexQueue[i .. $];
			foreach(rs ; rss)
			{
				_pendingReadIndex.remove(rs.req.Entries[0].Data);
			}

			return rss;
		}
		return null;
	}

	string lastPendingRequestCtx()
	{
		if(_readIndexQueue.length == 0)
			return "";

		return _readIndexQueue[$ - 1];
	}

}
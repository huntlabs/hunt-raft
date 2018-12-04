module network.node;


import wal.kvstore;
import network.server;
import network.client;
import network.base;
import network.http;

import hunt.raft;
import hunt.logging;
import hunt.util.serialize;
import hunt.util.timer;
import hunt.net;

import std.string;
import std.conv;
import std.format;

import core.thread;
import core.sync.mutex;


alias Server = network.server.Server;
alias Client = network.client.Client;
enum defaultSnapCount = 10000;
enum snapshotCatchUpEntriesN = 10000;

class node 
{
	__gshared node _gnode;

	this()
	{
		_mutex = new Mutex();
	}

	void publishSnapshot(Snapshot snap)
	{
		if(IsEmptySnap(snap))
			return;

		if(snap.Metadata.Index <= _appliedIndex)
		{
			logError(format("snapshot index [%d] should > progress.appliedIndex [%d] + 1", 
					snap.Metadata.Index, _appliedIndex));
		}

		_confState = snap.Metadata.CS;
		_snapshotIndex = snap.Metadata.Index;
		_appliedIndex = snap.Metadata.Index;
	}

	void saveSnap(Snapshot snap)
	{
		_kvs.savesnap(snap);
	}

	Entry[] entriesToApply(Entry[] ents)
	{
		if(ents.length == 0)
			return null;

		auto firstIdx = ents[0].Index;
		if(firstIdx > _appliedIndex + 1)
		{
			logError(format("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1",
					firstIdx, _appliedIndex));
		}

		if(_appliedIndex - firstIdx + 1 < ents.length)
			return ents[_appliedIndex - firstIdx + 1 .. $];
		
		return null;
	}

	bool publishEntries(Entry[] ents)
	{
		for(auto i = 0 ; i < ents.length ;i++)
		{
			switch(ents[i].Type)
			{
				case EntryType.EntryNormal:
					if(ents[i].Data.length == 0)
						break;

					RequestCommand command = unserialize!RequestCommand(cast(byte[])ents[i].Data);
					
					string value;
					if(command.Method == RequestMethod.METHOD_GET)
						value = _kvs.Lookup(command.Key);
					else
						_kvs.SetValue(command.Key , command.Value);
						
					auto http = (command.Hash in _request);
					if(http != null)
					{
						http.do_response(value ~ " action done");
						http.close();
					}



					break;
					//next
				case EntryType.EntryConfChange:
					ConfChange cc = unserialize!ConfChange(cast(byte[])ents[i].Data);
					_confState = _node.ApplyConfChange(cc);
					switch(cc.Type)
					{
						case ConfChangeType.ConfChangeAddNode:
							if( cc.Context.length > 0)
								addPeer(cc.NodeID , cc.Context);
							break;
						case ConfChangeType.ConfChangeRemoveNode:
							if(cc.NodeID == _ID)
							{
								logWarning(_ID , " I've been removed from the cluster! Shutting down.");
								return false;
							}
							logWarning(_ID , " del node " , cc.NodeID);
							delPeer(cc.NodeID);
							break;
						default:
							break;
					}
					break;
				default:

			}

			_appliedIndex = ents[i].Index;

		}

		return true;
	}

	 

	void maybeTriggerSnapshot()
	{
		if(_appliedIndex - _snapshotIndex <= defaultSnapCount)
			return;

		logInfo(format("start snapshot [applied index: %d | last snapshot index: %d]",
				_appliedIndex, _snapshotIndex));

		auto data = _kvs.getSnapshot();
		Snapshot snap;
		auto err = _storage.CreateSnapshot(_appliedIndex ,&_confState , cast(string)data , snap);
		if(err != ErrNil)
		{
			logError(err);
		}

		saveSnap(snap);

		long compactIndex = 1;
		if(_appliedIndex > snapshotCatchUpEntriesN)
			compactIndex = _appliedIndex - snapshotCatchUpEntriesN;

		_storage.Compact(compactIndex);
		logInfo("compacted log at index " , compactIndex);
		_snapshotIndex = _appliedIndex;
	}


	void Propose(RequestCommand command , HttpBase h)
	{
		auto err = _node.Propose(cast(string)serialize(command));
		if( err != ErrNil)
		{
			logError(err);
		}
		else
		{
			_request[command.Hash] = h;
		}
	}

	void ReadIndex(RequestCommand command , HttpBase h)
	{
		_node.ReadIndex(cast(string)serialize(command));
		_request[command.Hash] = h;
	}

	void delPropose(HttpBase h)
	{
		_request.remove(h.toHash);
	}

	void ProposeConfChange(ConfChange cc)
	{
		auto err = _node.ProposeConfChange(cc);
		if( err != ErrNil)
		{
			logError(err);
		}
	}

	void start(ulong ID ,string apiport , string cluster , bool join)
	{
		Config conf = new Config();

		_kvs = new kvstore();

		_storage = new MemoryStorage();

		Snapshot *shot = null;
		HardState hs;
		Entry[] ents;
	
		bool exist = _kvs.load("snap.log" ~ to!string(ID) , "entry.log" ~ to!string(ID), "hs.log" ~ to!string(ID),  shot , hs  , ents);
		if(shot != null)
		{
			_storage.ApplySnapshot(*shot);
			_confState = shot.Metadata.CS;
			_snapshotIndex = shot.Metadata.Index;
			_appliedIndex = shot.Metadata.Index;
		}

		_storage.setHadrdState(hs);
		_storage.Append(ents);
		if(ents.length > 0)
		{
			_lastIndex = ents[$ - 1].Index;
		}

		conf._ID 				= ID;
		conf._ElectionTick	 	= 10;
		conf._HeartbeatTick 	= 1;
		conf._storage 			= _storage;
		conf._MaxSizePerMsg		=	1024*1024;
		conf._MaxInflightMsgs	=	256;

	
		_ID	 			= ID;
		_buffer.length 	= 1024;

		string[] peerstr = split(cluster , ";");
		Peer[] peers;
		foreach(i , str ; peerstr)
		{
			Peer p = {ID:i + 1};
			peers ~= p;
		}

		if(exist)
		{
			_node = new RawNode(conf);
		}
		else
		{
			if(join)
			{
				peers.length = 0;
			}

			_node = new RawNode(conf , peers);
			logInfo(_ID , " " , peers);
		}

		_http = new Server!HttpBase(ID);
		_http.listen("0.0.0.0" , to!int(apiport));

		for(uint i = 0 ; i < peers.length ; i++)
		{
			//server
			if(i + 1 == ID)
			{
				_server = new Server!Base(ID);
				string[] hostport = split(peerstr[i] ,":");
				_server.listen(hostport[0] , to!int(hostport[1]));
				logInfo(ID , " server open " , hostport[0] , " " , hostport[1]);
			}
			//client
			else
			{
				addPeer(i + 1 , peerstr[i]);
			}
		}
		
		new Thread((){
			while(1)
			{
				ready();
				Thread.sleep(dur!"msecs"(1));
			}
		}).start();
		
		new Thread((){
			while(1){
				onTimer();
				Thread.sleep(dur!"msecs"(100));
			}
		}).start();
	
		NetUtil.startEventLoop(-1);

		/*_poll.addFunc(&ready);

		
		_poll.addTimer(&onTimer , 100 , WheelType.WHEEL_PERIODIC);

		_poll.start();*/

	}

	bool addPeer(ulong ID , string data)
	{
		if(ID in _clients)
			return false;
		
		auto client = new Client(_ID , ID);
		string[] hostport = split(data , ":");
		client.connect(hostport[0] , to!int(hostport[1]) , (Result!NetSocket result){
			if(result.failed()){
				addPeer(ID , data);
				return;
			}
			_clients[ID] = client;
			logInfo(_ID , " client connect " , hostport[0] , " " , hostport[1]);
		});
		
		return true;
	}

	bool delPeer(ulong ID)
	{
		if(ID !in _clients)
			return false;

		logInfo(_ID , " client disconnect " , ID);
		_clients[ID].close();
		_clients.remove(ID);
		
		return true;
	}

	void wait()
	{
//		_poll.wait();
	}

	void send(Message[] msg)
	{
		foreach(m ; msg)
			_clients[m.To].write(m);
	}

	void Step(Message msg)
	{
		_mutex.lock();
		_node.Step(msg);
		_mutex.unlock();
	}

	void onTimer()
	{
		_mutex.lock();
		_node.Tick();
		_mutex.unlock();
	}

	void ready()
	{
		_mutex.lock();
		scope(exit){
			_mutex.unlock();
		}
		Ready rd = _node.ready();
		if(!rd.containsUpdates())
		{
			return;
		}
		_kvs.save(rd.hs, rd.Entries);
		if( !IsEmptySnap(rd.snap))
		{
			saveSnap(rd.snap);
			_storage.ApplySnapshot(rd.snap);
			publishSnapshot(rd.snap);
		}
		_storage.Append(rd.Entries);
		send(rd.Messages);
		if(!publishEntries(entriesToApply(rd.CommittedEntries)))
		{
			logError("will stop");
//			_poll.stop();
			return;
		}

		//for readindex
		foreach( r ; rd.ReadStates)
		{
			if( r.Index >= _appliedIndex)
			{
				RequestCommand command =  unserialize!RequestCommand(cast(byte[])r.RequestCtx);
				auto h =  command.Hash in _request;
				if(h == null){
					continue;
				}
				string value;
				if(command.Method == RequestMethod.METHOD_GET)
				{	
					value = _kvs.Lookup(command.Key);
					h.do_response(value ~ "action done");
					h.close();
				}
			}
		}
		
		maybeTriggerSnapshot();
		_node.Advance(rd);
		
	}

	static node instance()
	{
		if(_gnode is null)
			_gnode = new node();
		return _gnode;
	}

	MemoryStorage							_storage;
	ulong									_ID;
	Server!Base								_server;
	Server!HttpBase							_http;
	Client[ulong]							_clients;
	RawNode									_node;
	byte[]									_buffer;

	kvstore									_kvs;
	bool									_join;
	ulong									_lastIndex;
	ConfState								_confState;
	ulong									_snapshotIndex;
	ulong									_appliedIndex;

	Mutex									_mutex;									
	HttpBase[ulong]							_request;
}


module app.groupraft;


import common.network;
import common.raft.node;

import hunt.raft;
import hunt.logging;
import hunt.util.Serialize;
import hunt.util.timer;
import hunt.net;

import std.string;
import std.conv;
import std.format;

import core.thread;
import core.sync.mutex;


import common.wal.storage;

alias Server = common.network.server.Server;
alias Client = common.network.client.Client;
alias Storage = common.wal.storage.Storage;


class ClusterClient
{
	ulong 				firstID;
	string				host;
	ushort				port;
	string				apihost;
	ushort				apiport;
}


class GroupRaft : MessageReceiver
{
	this()
	{
	
	}

	//node1 [1,2,3]
	//node2 [1,2,4]
	//node3 [2,3,4]
	//node4 [1,2,3]
	void addPeer(ulong ID , string data)
	{
		if(ID in _clients)
			return ;
		
		auto client = new Client(_ID , ID);
		string[] hostport = split(data , ":");
		client.connect(hostport[0] , to!int(hostport[1]) , (Result!NetSocket result){
			if(result.failed()){
				
                new Thread((){
                    Thread.sleep(dur!"seconds"(1));
                    addPeer(ID , data);
                }).start();
                return;
			}
			_clients[ID] = client;
			logInfo(_ID , " client connected " , hostport[0] , " " , hostport[1]);
		});
		
	}

	void start(ulong firstID , ulong[][ulong] regions , ClusterClient[] clients)
	{
		foreach(c ; clients)
		{
			//server
			if(firstID == c.firstID)
			{
				_server = new Server!(Base ,MessageReceiver)(firstID , this);
				_server.listen(c.host , c.port);
				logInfo(firstID ,  " server open " , c.host , " " , c.port);
			}
			//client
			else
			{
				addPeer(c.firstID , c.host ~ ":" ~ to!string(c.port));
			}
		}

		_ID = firstID;
		_mutex = new Mutex();

		foreach(k , v ; regions)
		{
			Config conf = new Config();
			auto kvs = new Storage();
			auto store = new MemoryStorage();
			auto ID = (firstID * 10 + k);
			Peer[] peers;
			foreach( id ; v)
			{
				Peer p = {ID: id * 10 +k};
				peers ~= p;
			}
			logInfo(ID ," " , peers , v);

			

			Snapshot *shot = null;

			ConfState confState;
			ulong snapshotIndex;
			ulong appliedIndex;
			ulong lastIndex;
			RawNode	node;


			HardState hs;
			Entry[] ents;
			bool exist = kvs.load("snap.log" ~ to!string(ID) , "entry.log" ~ to!string(ID) ,"hs.log" ~ to!string(ID), shot , hs , ents);
			if(shot != null)
			{
				store.ApplySnapshot(*shot);
				confState = shot.Metadata.CS;
				snapshotIndex = shot.Metadata.Index;
				appliedIndex = shot.Metadata.Index;
			}

			store.setHadrdState(hs);
			store.Append(ents);

			if(ents.length > 0)
			{
				lastIndex = ents[$ - 1].Index;
			}
			conf._ID 		   	= ID;
			conf._ElectionTick 	= 10;
			conf._HeartbeatTick = 1;
			conf._storage		= store;
			conf._MaxSizePerMsg = 1024 * 1024;
			conf._MaxInflightMsgs = 256;

			if(exist)
			{
				node = new RawNode(conf);
			}
			else
			{
				node = new RawNode(conf , peers);
			}

			_rafts[ID] = new Node(ID , store , node , kvs , lastIndex ,confState , snapshotIndex , appliedIndex);
		
		}

		logInfo(_clients , " " , _rafts);

		

	        /// ready
        new Thread((){
            while(1)
            {
                ready();
                Thread.sleep(dur!"msecs"(10));
            }
		}).start();
		
        /// tick
		new Thread((){
			while(1){
				tick();
				Thread.sleep(dur!"msecs"(100));
			}
		}).start();

        NetUtil.startEventLoop(-1);

	}


	void tick()
	{
		_mutex.lock();
		foreach( r ; _rafts)
		{
			r.node.Tick();
		}
		_mutex.unlock();
	}

	void ready()
	{
		_mutex.lock();
		foreach(r ; _rafts)
		{
			Ready rd = r.node.ready();
			if(!rd.containsUpdates())
			{
				continue;
			}

			r.store.save(rd.hs , rd.Entries);
			if(!IsEmptySnap(rd.snap))
			{
				r.store.saveSnap(rd.snap);
				r.storage.ApplySnapshot(rd.snap);
				r.publishSnapshot(rd.snap);
			}

			r.storage.Append(rd.Entries);
			send(rd.Messages);
			r.publishEntries(r.entriesToApply(rd.CommittedEntries));
			r.maybeTriggerSnapshot();
			r.node.Advance(rd);
		}
		_mutex.unlock();
	}

	void send(Message[] msg)
	{
		//logInfo(msg);
		foreach(m ; msg)
		{
			ulong ID = m.To / 10;
			if(ID in _clients)
				_clients[ID].write(m);
		}
	}

	void step(Message msg)
	{
		_mutex.lock();
		_rafts[msg.To].node.Step(msg);
		_mutex.unlock();
	}



	Server!(Base,MessageReceiver)			_server;
	MessageTransfer[ulong]					_clients;
	Node[ulong]								_rafts;
    Storage                                 _storage;
	Mutex									_mutex;		
	ulong									_ID;
}


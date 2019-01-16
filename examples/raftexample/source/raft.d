module app.raft;



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


import app.http;
import common.wal.storage;

alias Server = common.network.server.Server;
alias Client = common.network.client.Client;
alias Storage = common.wal.storage.Storage;

class Raft : Node,MessageReceiver
{
	void propose(RequestCommand command , HttpBase h)
	{
         _mutex.lock();
		auto err = node.Propose(cast(string)serialize(command));
		if( err != ErrNil)
		{
			logError(err);
		}
		else
		{
			_request[command.Hash] = h;
		}
          _mutex.unlock();
	}

	void readIndex(RequestCommand command , HttpBase h)
	{
        _mutex.lock();
		node.ReadIndex(cast(string)serialize(command));
		_request[command.Hash] = h;
        _mutex.unlock();
	}

	void delPropose(HttpBase h)
	{
        _mutex.lock();
		_request.remove(h.toHash);
        _mutex.unlock();
	}

	void proposeConfChange(ConfChange cc)
	{
        _mutex.lock();
		auto err = node.ProposeConfChange(cc);
		if( err != ErrNil)
		{
			logError(err);
		}
        _mutex.unlock();
	}





	this(ulong ID ,string apiport , string cluster , bool join)
	{
        auto conf = new Config(); 
        auto store = new MemoryStorage();

        _mutex = new Mutex();
        _storage = new Storage();
		Snapshot *shot = null;

        ConfState confState;
        ulong snapshotIndex;
        ulong appliedIndex;
        ulong lastIndex;
        RawNode	node;


        HardState hs;
        Entry[] ents;
        bool exist = _storage.load("snap.log" ~ to!string(ID) , "entry.log" ~ to!string(ID) , "hs.log" ~ to!string(ID) , shot , hs , ents);
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

        string[] peerstr = split(cluster , ";");
		Peer[] peers;
		foreach(i , str ; peerstr)
		{
			Peer p = {ID:i + 1};
			peers ~= p;
		}

    	if(exist)
		{
			node = new RawNode(conf);
		}
		else
		{
            /// next solve.
			if(join)
			{
				node = new RawNode(conf , []);
			}
            else
            {
                node = new RawNode(conf , peers);
            }
			    
		}
        super(ID , store , node , _storage , lastIndex ,confState , snapshotIndex , appliedIndex);	
		_http = new Server!(HttpBase,Raft)(ID , this);
		_http.listen("0.0.0.0" , to!int(apiport));

		for(uint i = 0 ; i < peers.length ; i++)
		{
			if(i + 1 == ID)
			{
				_server = new Server!(Base,MessageReceiver)(ID , this);
				string[] hostport = split(peerstr[i] ,":");
				_server.listen(hostport[0] , to!int(hostport[1]));
				logInfo(ID , " server open " , hostport[0] , " " , hostport[1]);
			}
            else
            {
                addPeer(i + 1 , peerstr[i]);    
            }
		}

      
  
        

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

    void ready()
	{
        _mutex.lock();
        scope(exit){
            _mutex.unlock();
        }
		Ready rd = node.ready();
		if(!rd.containsUpdates())
		{
			return;
		}
		_storage.save(rd.hs, rd.Entries);
		if( !IsEmptySnap(rd.snap))
		{
			_storage.saveSnap(rd.snap);
			storage.ApplySnapshot(rd.snap);
			publishSnapshot(rd.snap);
		}
		storage.Append(rd.Entries);
		send(rd.Messages);
		if(!publishEntries(entriesToApply(rd.CommittedEntries)))
		{
			logError("will be stop!");
			return;
		}

		
		foreach( r ; rd.ReadStates)
		{
			if( r.Index >= appliedIndex)
			{
				RequestCommand command =  unserialize!RequestCommand(cast(byte[])r.RequestCtx);
				auto h =  command.Hash in _request;
				if(h == null){
					continue;
				}
				string value;
				if(command.Method == RequestMethod.METHOD_GET)
				{	
					value = _storage.Lookup(command.Key);
					h.do_response(value ~ "action done");
					h.close();
				}
			}
		}
		
		maybeTriggerSnapshot();
		node.Advance(rd);
		
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
                    
                    logError(ents[i].Data.length);
					RequestCommand command = unserialize!RequestCommand(cast(byte[])ents[i].Data);
					
					string value;
					if(command.Method == RequestMethod.METHOD_GET)
						value = _storage.Lookup(command.Key);
					else
						_storage.SetValue(command.Key , command.Value);
						
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
					confstate = node.ApplyConfChange(cc);
					switch(cc.Type)
					{
						case ConfChangeType.ConfChangeAddNode:
                            if(cc.NodeID == ID)
                            {
                                break;
                            }

							if( cc.Context.length > 0)
							{
								addPeer(cc.NodeID , cc.Context);
								logInfo("add " , cc.NodeID);
							}
							break;
						case ConfChangeType.ConfChangeRemoveNode:
							if(cc.NodeID == ID)
							{
								logWarning(ID , " I've been removed from the cluster! Shutting down.");
								return false;
							}
							logWarning(ID , " del node " , cc.NodeID);
							delPeer(cc.NodeID);
							break;
						default:
							break;
					}
					break;
				default:

			}

			appliedIndex = ents[i].Index;

		}
		return true;
	}


    void addPeer(ulong ID , string data)
	{
		if(ID in _clients)
			return ;
		
		auto client = new Client(this.ID , ID);
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
			logInfo(this.ID , " client connected " , hostport[0] , " " , hostport[1]);
		});
		
	}
    
    void delPeer(ulong ID)
	{
		if(ID !in _clients)
			return ;

		logInfo(this.ID , " client disconnect " , ID);
		_clients[ID].close();
		_clients.remove(ID);
		
		return ;
	}


    void send(Message[] msg)
    {
        foreach(m ; msg)
            if(m.To in _clients)
			    _clients[m.To].write(m);
    }

    void tick()
    {
        _mutex.lock();
        node.Tick();
        _mutex.unlock();
    }
  

    void step(Message msg)
    {
         _mutex.lock();
        node.Step(msg);
        _mutex.unlock();
    }


	Server!(Base,MessageReceiver)			_server;
	Server!(HttpBase,Raft)					_http;
	MessageTransfer[ulong]					_clients;
	
    Storage                                 _storage;
	Mutex									_mutex;									
	HttpBase[ulong]							_request;
}


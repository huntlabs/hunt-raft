module common.raft.node;


import common.wal.api;


import hunt.raft;
import hunt.logging;
import hunt.util.Serialize;
import hunt.util.timer;
import hunt.net;

import std.string;
import std.conv;
import std.format;


enum defaultSnapCount = 10000;
enum snapshotCatchUpEntriesN = 10000;


class Node
{
	ulong 				ID;
	MemoryStorage 		storage;
	RawNode				node;
	bool				join;
	ulong				lastIndex;
	ConfState			confstate;
	ulong				snapshotIndex;
	ulong				appliedIndex;
	DataStorage			store;

	this(ulong ID , MemoryStorage storage , RawNode node , DataStorage store ,
		ulong lastIndex , ConfState confstate , ulong snapshotIndex , ulong appliedIndex)
	{
		this.ID = ID;
		this.storage = storage;
		this.node = node;
		this.store = store;
		this.lastIndex = lastIndex;
		this.confstate = confstate;
		this.snapshotIndex = snapshotIndex;
		this.appliedIndex = appliedIndex;
	}

	void publishSnapshot(Snapshot snap)
	{
		if(IsEmptySnap(snap))
			return;
		
		if(snap.Metadata.Index <= appliedIndex)
		{
			logError(format("snapshot index [%d] should > progress.appliedIndex [%d] + 1", 
					snap.Metadata.Index, appliedIndex));
		}
		
		confstate = snap.Metadata.CS;
		snapshotIndex = snap.Metadata.Index;
		appliedIndex = snap.Metadata.Index;
	}

	Entry[] entriesToApply(Entry[] ents)
	{
		if(ents.length == 0)
			return null;
		
		auto firstIdx = ents[0].Index;
		if(firstIdx > appliedIndex + 1)
		{
			logError(format("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1",
					firstIdx, appliedIndex));
		}
		
		if(appliedIndex - firstIdx + 1 < ents.length)
			return ents[appliedIndex - firstIdx + 1 .. $];
		
		return null;
	}



	void maybeTriggerSnapshot()
	{
		if(appliedIndex - snapshotIndex <= defaultSnapCount)
			return;
		
		logInfo(format("start snapshot [applied index: %d | last snapshot index: %d]",
				appliedIndex, snapshotIndex));
		
		auto data = store.getSnapData();
		Snapshot snap;
		auto err = storage.CreateSnapshot(appliedIndex ,&confstate , data , snap);
		if(err != ErrNil)
		{
			logError(err);
		}
		
		store.saveSnap(snap);

		long compactIndex = 1;
		if(appliedIndex > snapshotCatchUpEntriesN)
			compactIndex = appliedIndex - snapshotCatchUpEntriesN;
		
		storage.Compact(compactIndex);
		logInfo("compacted log at index " , compactIndex);
		snapshotIndex = appliedIndex;
	}

	bool publishEntries(Entry[] ents)
	{	
		for(auto i = 0 ; i < ents.length ;i++)
		{
			appliedIndex = ents[i].Index;
		}
		return true;
	}

	
	/*
	void send(Message[] msg)
	{

	}

	void addPeer(ulong ID , string data)
	{
		
	}

	void delPeer(ulong ID)
	{

	}


    void ready()
	{
		Ready rd = node.ready();
		if(!rd.containsUpdates())
		{
			return;
		}
		store.save(rd.hs, rd.Entries);
		if( !IsEmptySnap(rd.snap))
		{
			store.saveSnap(rd.snap);
			storage.ApplySnapshot(rd.snap);
			publishSnapshot(rd.snap);
		}
		storage.Append(rd.Entries);
		send(rd.Messages);
		if(!publishEntries(entriesToApply(rd.CommittedEntries)))
		{
			logWarning("will be stop!");
			return;
		}

		//for readindex
		foreach( r ; rd.ReadStates)
		{
			if( r.Index >= appliedIndex)
			{

			}
		}
		
		maybeTriggerSnapshot();
		node.Advance(rd);
		
	}*/
}


module raft.Storage;

import protocol.Msg;
import zhang2018.common.Log;

import raft.Util;

enum ErrCompacted = "requested index is unavailable due to compaction";
enum ErrSnapOutOfDate = "requested index is older than the existing snapshot";
enum ErrUnavailable = "requested entry at index is unavailable";
enum ErrSnapshotTemporarilyUnavailable = "snapshot is temporarily unavailable";
enum ErrNil = string.init;

alias ErrString = string;


interface Storage
{
	ErrString InitalState(out  HardState hs , out  ConfState cs);
	ErrString Entries(ulong lo , ulong hi , ulong maxSize ,out Entry []entries);
	ErrString Term(ulong i , out ulong term);
	ErrString LastIndex(out ulong index);
	ErrString FirstIndex(out ulong index);
	ErrString GetSnap(out Snapshot ss);
}




class MemoryStorage : Storage
{
	HardState 	_hs;
	Snapshot 	_ss;
	Entry[]		_ents;

	this()
	{
		_ents.length = 1;
	}

	ErrString InitalState(out HardState hs , out ConfState cs)
	{
		hs = _hs;
		cs = _ss.Metadata.CS;
		return ErrNil;
	}

	ErrString setHadrdState(in ref HardState hs)
	{
		_hs = hs;
		return ErrNil;
	}

	ErrString Entries(ulong lo , ulong hi , ulong maxSize , out Entry []entries)
	{
		ulong offset = _ents[0].Index;
		if( lo <= offset)
			return ErrCompacted;

		if( hi > lastIndex() + 1 )
			log_error("entries' hi(" , hi , ") is out of bound lastindex(" , lastIndex() , ")");
			
		if (_ents.length == 1)
			return ErrUnavailable;
	
		entries = _ents[ cast(uint)(lo - offset) .. cast(uint)(hi - offset) ];
		entries = limitSize(entries ,maxSize);
		return ErrNil;
	}

	ErrString Term(ulong i , out ulong term)
	{
		ulong offset = _ents[0].Index;
		term = 0;
		if ( i < offset)
			return ErrCompacted;
		if (i - offset >= _ents.length)
			return ErrUnavailable;

		term = _ents[ cast(uint)(i - offset)].Term;
		return ErrNil;
	}

	ErrString LastIndex(out ulong last)
	{
		last =  lastIndex();
		return ErrNil;
	}

	ulong lastIndex()
	{
		return _ents[0].Index + _ents.length - 1;
	}

	ErrString FirstIndex(out ulong first)
	{
		first = firstIndex();
		return ErrNil;
	}

	ulong firstIndex()
	{
		return _ents[0].Index + 1;
	}
	
	ErrString GetSnap(out Snapshot ss)
	{
		ss = _ss;
		return ErrNil;
	}

	ErrString ApplySnapshot( Snapshot snap)
	{
		auto Index = _ss.Metadata.Index;
		auto snapIndex = snap.Metadata.Index;
		if (Index >= snapIndex)
			return ErrSnapOutOfDate;

		_ss = snap;
		Entry ent = { Term: snap.Metadata.Term ,Index:snap.Metadata.Index};
		_ents.length = 0;
		_ents ~= ent;
		return ErrNil;
	}

	ErrString CreateSnapshot(ulong i , ConfState *cs , string data ,out Snapshot snap)
	{

		if( i <= _ss.Metadata.Index)
			return ErrSnapOutOfDate;

		auto offset = _ents[0].Index;
		if ( i > lastIndex())
			log_error("snapshot " , i , "is out of bound lastindex(" , lastIndex() , ")");

		_ss.Metadata.Index = i;
		_ss.Metadata.Term = _ents[cast(uint)(i - offset)].Term;
		if( cs != null)
			_ss.Metadata.CS = *cs;

		_ss.Data = data;
		snap = _ss;
		return ErrNil;
	}

	ErrString Compact(ulong compactIndex)
	{
		auto offset = _ents[0].Index;
		if(compactIndex <= offset)
			return ErrCompacted;

		if(compactIndex > lastIndex())
			log_error("compact ", compactIndex ," is out of bound lastindex(" , lastIndex() , ")");

		uint i = cast(uint)(compactIndex - offset);
		Entry[] ents;
		ents.length = 1;
		ents[0].Index = _ents[i].Index;
		ents[0].Term = _ents[i].Term;
		ents ~= _ents[i+1 .. $];
		_ents = ents;

		return ErrNil;
	}



	ErrString Append(Entry[] entries)
	{
		if (entries.length == 0)
			return ErrNil;

		auto first = firstIndex();
		auto last = entries[0].Index + entries.length -1;
		if( last < first)
			return ErrNil;

		if(first > entries[0].Index)
			entries = entries[cast(uint)(first - entries[0].Index) .. $];

		auto offset = entries[0].Index - _ents[0].Index;

		if(_ents.length > offset)
		{
			_ents = _ents[0 .. cast(uint)offset];
			_ents ~= entries;
		}
		else if(_ents.length == offset)
		{
			_ents ~= entries;
		}
		else{
			log_error("missing log entry [last:" ,lastIndex() ,
				", append at: " , entries[0].Index , "]");
		}

		return ErrNil;

	}
}

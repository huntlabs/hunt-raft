module hunt.raft.Logunstable;

import hunt.raft.Msg;

import hunt.logging;

import std.experimental.allocator;
import std.format;

class unstable
{
	Snapshot* 	_snap;
	Entry[]		_entries;
	ulong		_offset;

	bool maybeFirstIndex(out ulong index)
	{
		if(_snap != null)
		{
			index = _snap.Metadata.Index + 1;
			return true;
		}
		index = 0;
		return false;
	}

	bool maybeLastIndex(out ulong index)
	{
		if(_entries.length != 0)
		{
			index = _offset + _entries.length - 1;
			return true;
		}

		if(_snap != null)
		{
			index = _snap.Metadata.Index;
			return true;
		}

		index = 0;
		return false;
	}


	bool maybeTerm(ulong i , out ulong term)
	{
		if( i < _offset)
		{
			if(_snap == null)
			{	
				term = 0;
				return false;
			}

			if(_snap.Metadata.Index == i)
			{
				term = _snap.Metadata.Term;
				return true;
			}

			term = 0;
			return false;
		}

		ulong last = 0;
		if(!maybeLastIndex(last))
		{
			term = 0;
			return false;
		}

		if(i > last)
		{
			term = 0;
			return false;
		}

		term = _entries[cast(uint)(i - _offset)].Term;
		return true;
	}

	void stableTo(ulong i , ulong t)
	{
		ulong gt = 0;
		if(!maybeTerm(i , gt))
			return;

		if( gt == t && i >= _offset)
		{
			_entries = _entries[ cast(uint)(i + 1 - _offset) .. $ ];
			_offset = i + 1;
			shrinkEntriesArray();
		}
	}

	void shrinkEntriesArray()
	{
		if(_entries.length == 0)
		{	
			return;
		}
		else if( _entries.capacity > _entries.length)
		{
			_entries = _entries.dup;
		}
	}


	void stableSnapTo(ulong i) {
		if (_snap != null && _snap.Metadata.Index == i) {
			_snap = null;
		}
	}

	void restore(Snapshot s)
	{
		_offset = s.Metadata.Index + 1;
		_entries.length = 0;
		_snap = theAllocator.make!Snapshot(s);
	}

	void truncateAndAppend(Entry[] ents)
	{
		auto after = ents[0].Index;
		if(after == _offset + _entries.length)
		{
			_entries ~= ents;
		}
		else if(after <= _offset)
		{
			logInfo("replace the unstable entries from index " , after);
			_entries = ents;
			_offset = after;
		}
		else{
			logInfo("truncate the unstable entries before index " , after);
			_entries = slice(_offset , after);
			_entries ~= ents;
		}
	}


	Entry[] slice(ulong lo , ulong hi)
	{
		mustCheckOutOfBounds(lo , hi);
		return _entries[cast(uint)(lo - _offset) .. cast(uint)(hi - _offset)];
	}

	void mustCheckOutOfBounds(ulong lo , ulong hi)
	{
		if( lo > hi)
		{
			logError(format("invalid unstable.slice %d > %d", lo, hi));
		}

		auto upper = _offset + _entries.length;
		if( lo < _offset || hi > upper)
		{
			logError(format("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, 
					_offset, upper));
		}
	}
}
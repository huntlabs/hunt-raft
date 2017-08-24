module raft.Progress;

import zhang2018.common.Log;
import std.algorithm;

enum ProgressStateType
{
	ProgressStateProbe = 0,
	ProgressStateReplicate,
	ProgressStateSnapshot,
}

/*
immutable string[] prstmap = 
[
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot"
];

string toString(ProgressStateType st)
{
	return prstmap[st];
}
*/

class inflights
{
	int						_start;
	int						_count;
	int						_size;
	ulong[]					_bufffer;

	this(int size)
	{
		_size = size;
	}

	void add(ulong inflight)
	{
		if(full())
			log_error("cannot add into a full inflights");

		auto next = _start + _count;
		auto size = _size;
		if(next >= size)
			next -= size;

		if(next >= _bufffer.length)
			growBuf();

		_bufffer[next] = inflight;
		_count++;
	}

	void growBuf()
	{
		auto newSize = _bufffer.length * 2;
		if(newSize == 0)
			newSize = 1;
		else if( newSize > _size)
			newSize = _size;

		_bufffer.length = newSize;
	}

	void freeTo(ulong to)
	{
		if(_count == 0 || to < _bufffer[_start])
			return;

		auto i = 0;
		auto idx = _start;
		auto size = _size;

		for( ; i < _count ; i++)
		{	
			if( to < _bufffer[idx])
				break;
		
			idx++;
			if(idx >= size)
				idx -= size;
		}

		_count -= i;
		_start = idx;

		if(_count == 0)
			_start = 0;
	}


	void freeFirstOne()
	{
		freeTo(_bufffer[_start]);
	}


	bool full()
	{
		return _count == _size;
	}

	void reset()
	{
		_count = 0;
		_start = 0;
	}

}


class Progress
{
	ulong 					_Match;
	ulong 					_Next;

	ProgressStateType 		_State;
	bool					_Paused;
	ulong					_PendingSnapshot;

	bool					_RecentActive;

	inflights				_ins;


	void resetState(ProgressStateType state)
	{
		_Paused = false;
		_PendingSnapshot = 0;
		_State = state;
		_ins.reset();
	}

	void becomeProbe()
	{
		if(_State == ProgressStateType.ProgressStateSnapshot)
		{
			auto PendingSnapshot = _PendingSnapshot;
			resetState(ProgressStateType.ProgressStateProbe);
			_Next = max(_Match + 1 , PendingSnapshot + 1); 
		}
		else
		{
			resetState(ProgressStateType.ProgressStateProbe);
			_Next = _Match + 1;
		}
	}


	void becomeReplicate()
	{
		resetState(ProgressStateType.ProgressStateReplicate);
		_Next = _Match + 1;
	}

	void becomeSnapshot(ulong snapshoti)
	{
		resetState(ProgressStateType.ProgressStateSnapshot);
		_PendingSnapshot = snapshoti;
	}

	bool maybeUpdate(ulong n)
	{
		bool updated = false;
		if(_Match < n)
		{
			_Match = n;
			updated = true;
			resume();
		}
		if(_Next < n + 1)
		{
			_Next = n + 1;
		}

		return updated;
	}

	void optimisticUpdate(ulong n)
	{
		_Next = n + 1;
	}

	bool maybeDecrTo(ulong rejected , ulong last)
	{
		if(_State == ProgressStateType.ProgressStateReplicate)
		{
			if( rejected <= _Match)
				return false;

			_Next = _Match + 1;
			return true;
		}

		if(_Next - 1 != rejected)
		{
			return false;
		}

		_Next = min(rejected , last + 1);
		if(_Next < 1)
			_Next = 1;

		resume();

		return true;
	}

	void pause() { _Paused = true;}
	void resume(){ _Paused = false;}

	bool IsPaused()
	{
		switch(_State)
		{
			case ProgressStateType.ProgressStateProbe:
				return _Paused;
			case ProgressStateType.ProgressStateReplicate:
				return _ins.full();
			case ProgressStateType.ProgressStateSnapshot:
				return true;
			default:
				return false;
		}
	}

	void snapshotFailure()
	{
		_PendingSnapshot = 0;
	}

	bool needSnapshotAbort()
	{
		return _State == ProgressStateType.ProgressStateSnapshot && _Match >= _PendingSnapshot;
	}

	override string toString() {
		return log_format("next = %d, match = %d, state = %s, waiting = %d, pendingSnapshot = %d", _Next, _Match, _State, IsPaused(), _PendingSnapshot);
	}

}



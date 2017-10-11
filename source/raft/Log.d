module raft.Log;

import raft.Storage;
import raft.Log_unstable;
import zhang2018.common.Log;
import protocol.Msg;
import std.algorithm;
import raft.Util;

import std.conv;


class raftLog
{
	Storage 	_storage;
	unstable	_unstable;
	ulong		_committed;
	ulong		_applied;

	this(Storage storage)
	{
		if (storage is null)
		{
			log_error("storage must not be nil");
		}

		_storage = storage;
		_unstable = new unstable();
		ulong firstIndex;
		auto err = _storage.FirstIndex(firstIndex);
		if(err != ErrNil)
		{
			log_error(err);
		}

		ulong lastIndex;
		err = _storage.LastIndex(lastIndex);
		if(err != ErrNil)
		{
			log_error(err);
		}

		_unstable._offset = lastIndex + 1;
		_committed = firstIndex - 1;
		_applied = firstIndex -1;
	}

	override string toString()
	{
		return log_format("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d",
			_committed, _applied, _unstable._offset, _unstable._entries.length);
	}

	bool maybeAppend(ulong index , ulong logTerm , ulong committed , Entry[] ents , 
		out ulong lastnewi )
	{
		if(matchTerm(index , logTerm)){
			lastnewi = index + ents.length;
			auto ci = findConflict(ents);
			if( ci == 0){ }
			else if(ci <= _committed)
			{
				log_error(log_format("entry %d conflict with committed entry [committed(%d)]", ci, _committed));
			}
			else
			{
				auto offset = index + 1;
				append(ents[ cast(uint)(ci - offset) .. $ ]);
			}
			commitTo(min(committed , lastnewi));
			return true;

		}

		return false;
	}

	ulong append(Entry[] ents){
		if(ents.length == 0)
			return lastIndex();

		auto after = ents[0].Index - 1;
		if(after < _committed)
		{
			log_error(log_format("after(%d) is out of range [committed(%d)]", after, _committed));
		}

		_unstable.truncateAndAppend(ents);
		return lastIndex();
	}

	ulong findConflict(Entry[] ents)
	{
		foreach(ne ; ents){
			if(!matchTerm(ne.Index , ne.Term)){

				if(ne.Index <= lastIndex()){
					ulong t;
					auto err = term(ne.Index , t);
					log_info("found conflict at index %d [existing term: %d, conflicting term: %d]",
						ne.Index, zeroTermOnErrCompacted(t , err), ne.Term);
				}

				return ne.Index;
			}
		}
		return 0;
	}

	Entry[] unstableEntries()
	{
		return _unstable._entries;
	}

	Entry[] nextEnts()
	{
		auto off = max(_applied + 1 , firstIndex());
		if( _committed + 1 > off)
		{
			Entry[] ents;
			auto err = slice(off , _committed + 1, noLimit , ents);
			if(err != ErrNil)
			{
				log_error(log_format("unexpected error when getting unapplied entries (%s)", err));
			}
			return ents;
		}
		return null;
	}

	bool hasNextEnts()  {

		auto off = max(_applied + 1, firstIndex());
		return _committed + 1 > off;
	}

	ErrString GetSnap(out Snapshot ss) {
		if (_unstable._snap != null) {
			ss =  *_unstable._snap;
			return ErrNil;
		}
		return _storage.GetSnap(ss);
	}




	ulong firstIndex()  {
	
		ulong index;
		if(_unstable.maybeFirstIndex(index))
			return index;

		auto err = _storage.FirstIndex(index);
		if (err != ErrNil) {
			log_error(err);
		}
		return index;
	}


	ulong lastIndex()  {
		ulong index;
		if(_unstable.maybeLastIndex(index))
		{
			return index;
		}
		
		auto err = _storage.LastIndex(index);
		if(err != ErrNil){
			log_error(err);
		}
		return index;
	}

	void commitTo(ulong tocommit) {
	
		if (_committed < tocommit) {
			if (lastIndex() < tocommit) {
				log_error(log_format("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, lastIndex()));
			}
			_committed = tocommit;
		}
	}
	
	void appliedTo(ulong i) {
		if (i == 0) {
			return;
		}
		if (_committed < i || i < _applied) {
			log_error(log_format("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, _applied, _committed));
		}
		_applied = i;
	}
	
	void stableTo(ulong i , ulong t) { 
		_unstable.stableTo(i, t);
	}
	
	void stableSnapTo(ulong i) { 
		_unstable.stableSnapTo(i) ;
	}
	
	ulong lastTerm() {

		ulong t;
		auto err = term(lastIndex() , t);
		if( err != ErrNil) {
			log_error(log_format("unexpected error when getting the last term (%s)", err));
		}
		return t;
	}


	ErrString term(ulong i, out ulong term) {
	
		auto dummyIndex = firstIndex() - 1;
		if (i < dummyIndex || i > lastIndex()) {
			term = 0;
			return ErrNil;
		}


		if(_unstable.maybeTerm(i ,term)){

			return ErrNil;
		}

		auto err = _storage.Term(i , term);
		if (err == ErrNil) {

			return ErrNil;
		}
		if (err == ErrCompacted || err == ErrUnavailable) {
		
			term = 0;
			return err;
		}
		log_error(err);
		return ErrNil;
	}



	ErrString entries(ulong i,ulong maxsize , out Entry[] ents) {
		if (i > lastIndex()) {
			return ErrNil;
		}
		return slice(i, lastIndex()+1, maxsize , ents);
	}
	

	Entry[]  allEntries() {
		Entry[] ents;
		auto err = entries(firstIndex(), noLimit , ents);
		if (err == ErrNil) {
			return ents;
		}

		if (err == ErrCompacted) {
			return allEntries();
		}

		log_error(err);
		return null;
	}
	

	bool isUpToDate(ulong lasti, ulong term)
	{
		return term > lastTerm() || (term == lastTerm() && lasti >= lastIndex());
	}

	bool matchTerm(ulong i , ulong t)
	{
		ulong t0;
		auto err = term(i , t0);
		if(err != ErrNil)
			return false;
		return t0 == t;
	}

	bool maybeCommit(ulong maxIndex , ulong t)
	{
		ulong t0;
		auto err = term(maxIndex ,t0);
		if(maxIndex > _committed && zeroTermOnErrCompacted(t0 , err) == t)
		{
			commitTo(maxIndex);
			return true;
		}

		return false;
	}

	void restore(Snapshot ss)
	{
		log_info(log_format("log [%s] starts to restore snapshot [index: %d, term: %d]",
				to!string(this.toHash()), ss.Metadata.Index, ss.Metadata.Term));
		_committed = ss.Metadata.Index;
		_unstable.restore(ss);
	}

	ErrString slice(ulong lo , ulong hi , ulong maxSize , out Entry[] ents)
	{
		auto err = mustCheckOutOfBounds(lo , hi);
		if( err != ErrNil)
		{
			return err;
		}

		if(lo == hi)
		{
			return ErrNil;
		}


		if( lo < _unstable._offset)
		{
			Entry[] store;
			err = _storage.Entries(lo , min(hi , _unstable._offset) ,maxSize , store);
			if(err == ErrCompacted)
			{
				return err;
			}
			else if(err == ErrUnavailable)
			{	
				log_error(log_format("entries[%d:%d) is unavailable from storage", 
					lo, min(hi, _unstable._offset)));
			}
			else if(err != ErrNil)
			{
				log_error(err);
			}

			if(store.length < min(hi , _unstable._offset) - lo)
			{
				ents = store;
				return ErrNil;
			}

			ents = store;
		}

		if(hi > _unstable._offset)
		{
			Entry[] uns = _unstable.slice(max(lo , _unstable._offset) , hi);
			if( ents.length > 0)
				ents ~= uns;
			else
				ents = uns;
		}

		ents = limitSize(ents , maxSize);
		return ErrNil;
	}

	ErrString mustCheckOutOfBounds(ulong lo , ulong hi)
	{
		if(lo > hi)
		{
			log_error(log_format("invalid slice %d > %d", lo, hi));
		}

		ulong fi = firstIndex();
		if( lo < fi){
			return ErrCompacted;
		}

		auto length = lastIndex() + 1 - fi;
		if(lo < fi || hi > fi + length)
		{
			log_error(log_format("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, lastIndex()));
		}

		return ErrNil;
	}



	ulong zeroTermOnErrCompacted(ulong t, ErrString err){
		if(err == ErrNil)
		{
			return t;
		}
		if (err == ErrCompacted) {
			return 0;
		}

		log_error(log_format("unexpected error (%s)", err));
		return 0;
	}
}




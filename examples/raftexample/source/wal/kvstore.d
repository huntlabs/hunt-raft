module wal.kvstore;


import hunt.raft;

import hunt.util.serialize;
import hunt.logging;

import core.stdc.stdio;
import core.stdc.string;

import std.stdio;
import std.json;
import std.experimental.allocator;
import std.file;


struct kv
{
	string key;
	string val;
}



class kvstore
{


	byte[] getFileContent(string filename)
	{
		byte[] content;
		if(!exists(filename))
			return null;
		byte[] data = new byte[4096];
		auto ifs = File(filename, "rb");
		while(!ifs.eof())
		{
			content ~= ifs.rawRead(data);
		}
		ifs.close();
		return content;
	}

	void saveFileContent(string filename ,const byte[] buffer)
	{
		auto ifs = File(filename  , "wb");
		ifs.rawWrite(buffer);
		ifs.close();
	}
	



	bool load(string snapPath , string entryPath ,
		out Snapshot* snap , out HardState hs  , out Entry[] entries)
	{
		byte[] snapData = getFileContent(snapPath);
		if(snapData.length > 0)
		{
			snap = theAllocator.make!Snapshot(unserialize!Snapshot(snapData));
			logInfo(*snap);
			_json = parseJSON(snap.Data);
		}

		_snappath = snapPath;
		_entrypath = entryPath;

		byte[] entryData = getFileContent(entryPath);
		if(entryData.length > 0)
		{
			long parse_index;
			logInfo(_entrypath , " load " , entryData.length);
			hs = unserialize!HardState(entryData , parse_index);
			logInfo(_entrypath , " load " , hs , " " , parse_index , " " , entryData.length);
			if(parse_index < entryData.length)
			{
				entries = unserialize!(Entry[])(entryData[parse_index .. $]);	
				logInfo(_entrypath , " load " , entries );
			}
			return true;
		}
		return false;

	}

	void savesnap(Snapshot shot)
	{
		saveFileContent(_snappath , serialize(shot));
	}

	void save(HardState hs , Entry[] entries)
	{
		if(IsEmptyHardState(hs) && entries.length == 0)
			return;

		if(!_entryfd.isOpen())
		{
			if(exists(_entrypath))
			{
				_entryfd = File(_entrypath , "rb+");
			}
			else
			{
				_entryfd = File(_entrypath , "wb+");
			}
		}

		if(!IsEmptyHardState(hs))
		{	
			_entryfd.seek(0 , SEEK_SET);
			_entryfd.rawWrite(serialize(hs));
			_entryfd.flush();
			logInfo(_entrypath , " " , hs);
		}

		if(entries.length > 0)
		{
			_entryfd.seek(0 , SEEK_SET);
			byte[3] structHeader;
			byte[] header = _entryfd.rawRead(structHeader);
			ushort len;
			memcpy(&len , header.ptr + 1 , 2);
			_entryfd.seek(len  , SEEK_CUR);

			byte[7]	arrayHeader;
			header = _entryfd.rawRead(arrayHeader);

			logInfo(_entrypath , " " , entries);

			if(header.length == 0)	//first.
			{
				_entryfd.seek(0 , SEEK_END);
				_entryfd.rawWrite(serialize(entries));
				_entryfd.flush();
				return ;
			}

			ushort size;
			uint length;
			memcpy(&size , header.ptr + 1 , 2);
			memcpy(&length , header.ptr + 3 , 4);
			byte[] arrayData = serialize(entries);
			size += entries.length;
			length += arrayData.length;

			memcpy(header.ptr + 1 , &size , 2);
			memcpy(header.ptr + 3 , &length , 4);



			_entryfd.seek(-7 , SEEK_CUR);
			_entryfd.rawWrite(header);
			_entryfd.seek(0 , SEEK_END);
			_entryfd.rawWrite(arrayData[7 .. $]);
			_entryfd.flush();
		}



		
	}

	void SetValue(string key , string value)
	{
		_json[key] = value;
	}

	string Lookup(string key)
	{
		if(_json.isNull)
			return string.init;

		if(key in _json)
			return _json[key].str();
		return string.init;
	}

	byte[] getSnapshot()
	{
		return cast(byte[])_json.toString();
	}

	private JSONValue _json;
	private string  _entrypath;
	private string _snappath;
	private File	_entryfd;

}

unittest
{


}

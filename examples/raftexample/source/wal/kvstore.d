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

	struct HardEntry
	{
		HardState 	hs;
		Entry[] 	entries;
	}


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
	



	bool load(string snapPath , string entryPath , string hsPath,
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
		_hspath = hsPath;

		byte[] hsData = getFileContent(hsPath);
		if(hsData.length > 0)
		{	
			hs = unserialize!HardState(hsData);
			logInfo("load " , hs);
		}
		byte[] entryData = getFileContent(entryPath);
		if(entryData.length > 0)
		{	
			entries = unserialize!(Entry[])(entryData);
			logInfo("load " , entries);
		}

		if(entryData.length == 0)
			return false;
		else
			return true;

	}

	void savesnap(Snapshot shot)
	{
		saveFileContent(_snappath , serialize(shot));
	}

	void save(HardState hs , Entry[] entries)
	{
		if(IsEmptyHardState(hs) && entries.length == 0)
			return;

		if(!IsEmptyHardState(hs))
		{	
			logInfo("save " , hs);
			saveFileContent(_hspath , serialize(hs));
		}

		Entry[] newEntries;
		
		if(entries.length > 0)
		{	
			byte[] entryData = getFileContent(_entrypath);
			if(entryData.length > 0)
			{	
				newEntries ~= unserialize!(Entry[])(entryData);
			}
			newEntries ~= entries;
			logInfo("save " , newEntries);
			saveFileContent(_entrypath , serialize(newEntries));
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
	private string _hspath;
	private File	_entryfd;

}

unittest
{


}

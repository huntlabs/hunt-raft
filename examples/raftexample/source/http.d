module app.http;



import hunt.net;
import hunt.raft;

import app.raft;
import std.string;
import std.conv;
import std.format;



enum RequestMethod
{
	METHOD_GET = 0, 
	METHOD_SET = 1
};

struct RequestCommand
{
	RequestMethod Method;
	string		  Key;
	string		  Value;
	size_t		  Hash;
};


enum MAX_HTTP_REQUEST_BUFF = 4096;

alias Raft = app.raft.Raft;

class HttpBase 
{
	this(NetSocket sock, Raft raft )
	{
        this.sock = sock;
		this.raft = raft;
        sock.handler((in ubyte[] data){
			onRead(data);
		});
        sock.closeHandler((){
			onClose();
		});


	}

	bool is_request_finish(ref bool finish, ref string url , ref string strbody)
	{
		import std.typecons : No;
		
		string str = cast(string)buffer;
		long header_pos = indexOf(str , "\r\n\r\n");
		
		if( header_pos == -1)
		{
			finish = false;
			return true;
		}
		
		string strlength = "content-length: ";
		int intlength = 0;
		long pos = indexOf(str , strlength , 0 , No.caseSensitive);
		if( pos != -1)
		{
			long left = indexOf(str , "\r\n" , cast(size_t)pos);
			if(pos == -1)
				return false;
			
			strlength = cast(string)buffer[cast(size_t)(pos + strlength.length) .. cast(size_t)left];
			intlength = to!int(strlength);
		}
		
		
		if(header_pos + 4 + intlength == buffer.length)
		{
			finish = true;
		}
		else
		{
			finish = false;
			return true;
		}
		
		long pos_url = indexOf(str , "\r\n");
		if(pos_url == -1)
			return false;
		
		auto strs = split(cast(string)buffer[0 .. cast(size_t)pos_url]);
		if(strs.length < 3)
			return false;
		
		url = strs[1];
		strbody = cast(string)buffer[cast(size_t)(header_pos + 4) .. $];
		
		return true;
	}

	bool do_response(string strbody)
	{
		auto res = format("HTTP/1.1 200 OK\r\nServer: kiss\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s"
			, strbody.length , strbody);
	 	sock.write(res);
		return true;
	}

	bool process_request(string url , string strbody)
	{
		string path;
		long pos = indexOf(url , "?");
		string[string] params;
		if(pos == -1){
			path = url;
		}
		else{
			path = url[0 .. pos];
			auto keyvalues = split(url[pos + 1 .. $] , "&");
			foreach( k ; keyvalues)
			{
				auto kv = split(k , "=");
				if(kv.length == 2)
					params[kv[0]] = kv[1];
			}
		}

		if(path == "/get")
		{
			auto key = "key" in params;
			if(key == null || key.length == 0)
				return do_response("params key must not empty");

			RequestCommand command = { Method:RequestMethod.METHOD_GET , Key: *key , Hash:this.toHash()};
			raft.readIndex(command , this);
			return true;
		}
		else if(path == "/set")
		{

			auto key = "key" in params;
			auto value = "value" in params;
			if(key == null || value == null || key.length == 0)
				return do_response("params key  must not empty or value not exist");

			RequestCommand command = { Method:RequestMethod.METHOD_SET , Key: *key ,Value : *value , Hash:this.toHash()};
			raft.propose(command , this);
			return true;
		}
		else if(path == "/add")
		{
			

			auto nodeID = "ID" in params;
			auto Context = "Context" in params;
			if(nodeID == null || nodeID.length == 0 || Context.length == 0 || Context == null)
				return do_response("ID or Context must not empty");

			ConfChange cc = { NodeID : to!ulong(*nodeID) , Type : ConfChangeType.ConfChangeAddNode ,Context:*Context };
			raft.proposeConfChange(cc);
			return do_response("have request this add conf");
			
		}
		else if(path == "/del")
		{
			auto nodeID = "ID" in params;
			if(nodeID == null || nodeID.length == 0)
				return do_response("ID must not empty");
			ConfChange cc = { NodeID : to!ulong(*nodeID) , Type : ConfChangeType.ConfChangeRemoveNode };
			raft.proposeConfChange(cc);
			return do_response("have request this remove conf");
		}
		else
		{
			return do_response("can not sovle " ~ path);
		}

	}



	void onRead(in ubyte[] data)
	{
		buffer ~= data;
		bool finish;
		string strurl;
		string strbody;
		if(!is_request_finish(finish ,strurl,strbody ))
			return ;

		if(finish)
		{
			process_request(strurl , strbody);
		}
		else if(buffer.length >= MAX_HTTP_REQUEST_BUFF)
		{
			return ;
		}

	}

	void close()
	{
		sock.close();
	}

	void onClose() {
		raft.delPropose(this);
	}

    ubyte[]         buffer;
	NetSocket       sock;
	Raft			raft;
}


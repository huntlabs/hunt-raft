module network.client;

import hunt.logging;
import hunt.util.serialize;
import hunt.raft;
import hunt.net;
import core.stdc.string;
import network.node;
import core.thread;

import std.bitmanip;
import std.stdint;

class Client 
{
    ///
	this(ulong srcID , ulong dstID)
	{
		this.srcID = srcID;
		this.dstID = dstID;
        client = NetUtil.createNetClient();
	}

    ///
    void connect(string host , int port , ConnectHandler handler = null)
    {
        client.connect(port , host , 0 , ( Result!NetSocket result ){
            if(handler !is null)
                handler(result);
            if(!result.failed())
                sock = result.result();
        } );
    }

    ///
	void write(Message msg)
	{
        if(sock is null)
        {   
            logWarning(srcID ," not connect now. " , dstID); 
            return;
        }

	    //logDebug(srcID , " sendto " , dstID , " "  , msg);
		ubyte []data = cast(ubyte[])serialize(msg);
		int len = cast(int)data.length;
        ubyte[4] head = nativeToBigEndian(len);

        sock.write(head ~ data);
	}

    void close()
    {
        sock.close();
    }

private:
	ulong       srcID;
	ulong       dstID;
    NetClient   client;
    NetSocket   sock = null;
}


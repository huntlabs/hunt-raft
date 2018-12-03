module network.client;

import hunt.logging;
import hunt.util.serialize;
import hunt.raft;
import hunt.net;
import core.stdc.string;
import network.node;



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
    void connect(string host , int port)
    {
        client.connect(port , host , 0 , ( Result!NetSocket result ){
            if(result.failed())
                throw result.cause();
            sock = result.result();
        } );
    }

    ///
	void write(Message msg)
	{
	    logDebug(srcID , " sendto " , dstID , " "  , msg);
		ubyte []data = cast(ubyte[])serialize(msg);
		int len = cast(int)data.length;
		ubyte[4] head;
		memcpy(head.ptr , &len , 4);
        sock.write(head ~ data);
	}

private:
	ulong       srcID;
	ulong       dstID;
    NetClient   client;
    NetSocket   sock;
}


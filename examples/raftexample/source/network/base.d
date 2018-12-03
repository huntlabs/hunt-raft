module network.base;

import hunt.logging;
import hunt.util.serialize;

import hunt.net;
import hunt.raft;

import core.stdc.string;

import network.node;

import std.bitmanip;
import std.stdint;

class Base
{

    enum PACK_HEAD_LEN = 4;

    this(NetSocket sock)
    {
        this.sock = sock;
        sock.handler((in ubyte[] data){
            onRead(data);
        });
        sock.closeHandler((){
            onClose();
        });
    }


    void onRead(in ubyte[] data)
    {
        Message[] msgs; 
        int index = 0;
        int length = cast(int)data.length;
        int used;
		while(index < length)
		{
            int left = length - index;
            if( headLen < PACK_HEAD_LEN)
            {
                if(left >= PACK_HEAD_LEN - headLen)
                {
                    used = PACK_HEAD_LEN - headLen;
                    header[headLen .. headLen + used] = data[index .. index + used];
                    index += used;
                    headLen += used;
                    msgLen = bigEndianToNative!int32_t(header);
                    buffer.length = 0;
                    if(msgLen == 0)
                    {   
                        msgs ~= unserialize!Message(cast(byte[])buffer);
                        headLen = 0;
                    }
                }
                 else
                {
                    header[headLen .. headLen + left] = data[index .. index + left];
                    index += left;
                    headLen += left;
                }
            }
            else
            {
                if(left >= msgLen - cast(int)buffer.length)
                {
                    used = msgLen - cast(int)buffer.length;
                    buffer ~= data[index .. index + used];
                    index += used;
                    msgs ~= unserialize!Message(cast(byte[])buffer);
                    headLen = 0;
                }
                else
                {
                    buffer ~= data[index .. index + left];
                    index += left;
                }
            }

		}

        foreach(m ; msgs)
        {
            
        }


    }

    void onClose()
    {

    }

    NetSocket   sock;

    int                         msgLen;
    ubyte[]			            buffer;	
    int				            headLen = 0;
	ubyte[PACK_HEAD_LEN]		header;
}

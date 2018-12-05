module common.network.api;


import hunt.raft.Msg;

interface MessageTransfer
{
    void write(Message msg);
    void close();
}

interface MessageReceiver 
{
    void step(Message msg);
}

alias Finish = void delegate(string data);



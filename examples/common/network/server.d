module common.network.server;

import hunt.net;
import common.network.base;

import hunt.logging;
import common.network.api;

class Server(T,A...)
{
    this(ulong ID , A args)
    {
        this.ID = ID;
        this.args = args;
        server = NetUtil.createNetServer();
    }

    void listen(string host , int port)
    {
        alias Server = hunt.net.Server.Server;
        server.listen(host , port , (Result!Server result){
            if(result.failed())
                throw result.cause();
        });
        server.connectionHandler((NetSocket sock){
            auto context = new T(sock,args);
            auto tcp = cast(AsynchronousTcpSession)sock;
            tcp.attachObject(context);
            logInfo(ID , " have a connection");
        });

    }

    A				args;
    ulong           ID;
    NetServer       server;
}
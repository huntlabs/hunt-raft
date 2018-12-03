module network.server;

import hunt.net;
import network.base;

import hunt.logging;

class Server(T)
{
    this(ulong ID)
    {
        this.ID = ID;
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
            auto context = new T(sock);
            auto tcp = cast(AsynchronousTcpSession)sock;
            tcp.attachObject(context);
            logInfo(ID , " have a connection");
        });

    }


    ulong       ID;
    NetServer   server;
}
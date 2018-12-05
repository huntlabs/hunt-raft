
module app.app;

import app.groupraft;
import hunt.logging;
import std.conv;

import core.thread;



int main(string[] argv)
{
	if(argv.length < 2)
	{
		logInfo("raftexample ID");
		return -1;
	}

	ulong ID = to!ulong(argv[1]);

	LogConf conf;
	conf.fileName = "example.log" ~ argv[1];
	logLoadConf(conf);

	// node 1
	// port 3111
	// regions : [1,2,3]
	//
	// node 2
	// port 3112
	// regions : [1,2,4]
	//
	// node 3
	// port 3113
	// regions : [2,3,4]
	//
	// node 4
	// port 3114
	// regions : [1,3,4]


	//---->

	//
	//
	//
	//

	ClusterClient[] clients;
	clients ~= new ClusterClient();
	clients ~= new ClusterClient();
	clients ~= new ClusterClient();
	clients ~= new ClusterClient();


	clients[0].firstID = 1;
	clients[0].host = "127.0.0.1";
	clients[0].port = 3111;
	clients[1].firstID = 2;
	clients[1].host = "127.0.0.1";
	clients[1].port = 3112;

	clients[2].firstID = 3;
	clients[2].host = "127.0.0.1";
	clients[2].port = 3113;

	clients[3].firstID = 4;
	clients[3].host = "127.0.0.1";
	clients[3].port = 3114;

	auto groupraft = new GroupRaft();

	if(ID == 1)
	{
		ulong[][ulong] regions = [1:[1,2,4] , 2:[1,2,3] , 3:[1,3,4]];
		groupraft.start(ID , regions ,clients);
	}
	else if(ID == 2)
	{
		ulong[][ulong] regions = [1:[1,2,4] , 2:[1,2,3] , 4 :[2,3,4]];
		groupraft.start(ID , regions ,clients);
	}
	else if(ID == 3)
	{
		ulong[][ulong] regions =  [2:[1,2,3] , 3:[1,3,4] ,4 :[2,3,4]];
		groupraft.start(ID , regions ,clients);
	}
	else if(ID == 4)
	{
		ulong[][ulong] regions =  [1:[1,2,4] , 3:[1,3,4] ,4 :[2,3,4]];
		groupraft.start(ID , regions ,clients);
	}
	while(1)
		Thread.sleep(dur!"seconds"(1));
}





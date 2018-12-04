


import network.node;
import hunt.logging;
import std.conv;
import std.stdio;

import core.thread;

int main(string[] argv)
{
	if(argv.length < 5)
	{
		logInfo("raftexample ID apiport cluster join");
		logInfo("raftexample 1 2110 \"127.0.0.1:1110;127.0.0.1:1111;127.0.0.1:1112\" false ");
		return -1;
	}
	ulong ID = to!ulong(argv[1]);
	LogConf conf;
	conf.fileName = "example.log";
	logLoadConf(conf);
	node.instance.start(ID , argv[2] , argv[3], to!bool(argv[4]));
	while(1)
		Thread.sleep(dur!"seconds"(1));
}





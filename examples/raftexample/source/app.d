


import network.node;
import hunt.logging;
import std.conv;

int main(string[] argv)
{
	if(argv.length < 5)
	{
		logInfo("raftexample ID apiport cluster join");
		logInfo("raftexample 1 2110 \"127.0.0.1:1110;127.0.0.1:1111;127.0.0.1:1112\" false ");
		return -1;
	}
	ulong ID = to!ulong(argv[1]);
	node.instance.start(ID , argv[2] , argv[3], to!bool(argv[4]));
	node.instance.wait();
	return 0;
}





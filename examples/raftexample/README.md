# raftexample

etcd/contrib/raftexample implement by dlang .

show how to use raft lib @ https://github.com/huntlabs/raft .

dependencies network dreactor lib @ https://github.com/zhangyuchun/dreactor

./restart.sh  will delete snap & entry , recreate distributed server.

./start.sh    go on distributed server using entry & sanp.

./stop.sh     stop distributed server


./raftexample ID apiport cluster join
./raftexample 1 2110 "127.0.0.1:1110;127.0.0.1:1111;127.0.0.1:1112" false 





/set  set a kv  

http://127.0.0.1:2110/set?key=test&value=1234

/get  get a kv

http://127.0.0.1:2110/get?key=test

/add  add a node

http://127.0.0.1:2110/add?ID=4&Context=127.0.0.1:1113

/del  del a node

http://127.0.0.1:2110/del?ID=4



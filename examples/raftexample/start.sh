#!/bin/sh
ulimit -c unlimited
#rm -rf entry.log*
rm -f example.log
./raftexample 1 2110 "127.0.0.1:1110;127.0.0.1:1111;127.0.0.1:1112" false &
./raftexample 2 2111 "127.0.0.1:1110;127.0.0.1:1111;127.0.0.1:1112" false &
./raftexample 3 2112 "127.0.0.1:1110;127.0.0.1:1111;127.0.0.1:1112" false &

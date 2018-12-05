#!/bin/sh
ulimit -c unlimited
#rm -rf entry.log*
rm -f raftgroupexample.log
./raftgroupexample 1 &
./raftgroupexample 2 &
./raftgroupexample 3 &
./raftgroupexample 4 &

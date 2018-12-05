#!/bin/sh
ulimit -c unlimited
killall -9 raftgroupexample
rm -rf entry.log*
rm -rf snap.log*
rm -fr hs.log*
rm -rf example*
./raftgroupexample 1 &
./raftgroupexample 2 &
./raftgroupexample 3 &
./raftgroupexample 4 &

module common.wal.api;

import hunt.raft;

interface DataStorage
{
    void saveSnap(Snapshot shot);
    string getSnapData();
    void save(HardState hs , Entry[] entries);
}
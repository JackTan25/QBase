#include "lru_index_pointer.h"

template<const int N,const int M>
VectorRecord<N,M>* IndexPointerLruCache<N,M>::Get(const ItemPointer& key){
    // 1. try to get IndexPointer from cache and the get VectorRecord from RecordpagePool
    IndexPointer index_pointer = getCopy(key);
    if(index_pointer != InValidIndexPointer){
        // if we can get a valid index pointer, the record buffer must exists here.
        return pool.GetMemoryVectorRecord(index_pointer);
    }else{
        // 2. if we can't retrive from 1, we should do a DirectIORead from RocksDB, but we need
        // to evict a IndexPointer so we can get a free record memory.
        std::string record = pool.DirectIoRead(key);
        // 2.1 if cache is full, we should evict a index pointer here
        // 2.2 if cache is not full, we should try to New a IndexPointer from RecordCachePool
        if(is_full()){
            index_pointer = remove_front();
            // we should preserve the value in RecordPagePool
        }else{
            index_pointer = pool.New();
        }
        insert(key,index_pointer);
        pool.ReserveVectorRecord(record,index_pointer);
        return pool.GetMemoryVectorRecord(index_pointer);
    }
}

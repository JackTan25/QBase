#include "lru_index_pointer.h"

// pin feature needed:
// when a page request multi records, we need to pin the records needed by
// page. 
// We use the high 16bytes as pin counts, the low 16bytes as offset
// template<const int N,const int M>
// VectorRecord<N,M>* IndexPointerLruCache<N,M>::Get(ItemPointer key){
    
// }

// template<const int N,const int M>
// void IndexPointerLruCache<N,M>::UnPinItemPointer(const ItemPointer& k){

// }

HeapTid GetHeapTid(const ItemPointer& item_pointer){
	uint64_t tid = 0;
	tid |= item_pointer->ip_blkid.bi_hi<<SHIFT1;
	tid |= item_pointer->ip_blkid.bi_lo<<SHIFT2;
	tid |= item_pointer->ip_posid;
	return tid;
}
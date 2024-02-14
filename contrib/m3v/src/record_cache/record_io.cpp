#include "record_io.h"
#include "page_sort_index.h"

std::string ItemPointerToString(const ItemPointerData& key){
    return std::to_string(key.ip_blkid.bi_hi) + std::to_string(key.ip_blkid.bi_lo) + std::to_string(key.ip_posid);
}

void distanceRealVectorFunc(VectorRecord* record1,VectorRecord* record2,const std::vector<uint32_t>& offsets,std::vector<float> &distances){
	assert(record1!=nullptr);
	assert(record2!=nullptr);
	assert(record1->GetSize() == record2->GetSize());
	float* a = reinterpret_cast<float*>(record1->GetData());
	float* b = reinterpret_cast<float*>(record2->GetData());
    int offset = 0;
    for(int i = 0;i < offsets.size();i++){
        distances[i] = L2Distance(a + offset,b + offset,offsets[i]/DIM_SIZE);
        offset += offsets[i];
    }
}

void GetPivotIndexPair(const std::vector<float>& distances,PivotIndexPair& min_pair,PivotIndexPair& max_pair,int idx){
    float max_distance = -0x3f3f3f3f;
    float min_distance = 0x3f3f3f3f;
    for(int i = 0;i < distances.size();i++){
        if(max_distance < distances[i]){
            max_distance = distances[i];
        } 
        if(min_distance > distances[i]){
            min_distance = distances[i];
        } 
    }
    min_pair = {min_distance,idx};
    max_pair = {max_distance,idx};
}

float distanceRealVectorSumFuncWithWeights(VectorRecord* record1,VectorRecord* record2,const std::vector<uint32_t>& offsets,const std::vector<float> &weights){
	assert(record1!=nullptr);
	assert(record2!=nullptr);
	assert(record1->GetSize() == record2->GetSize());
	float* a = reinterpret_cast<float*>(record1->GetData());
	float* b = reinterpret_cast<float*>(record2->GetData());
    float res = 0;
    int offset = 0;
    for(int i = 0;i < offsets.size();i++){
        res += L2Distance(a + offset,b + offset,offsets[i]/DIM_SIZE) * weights[i];
    }
    return res;
}

float distanceRealVectorSumFunc(VectorRecord* record1,VectorRecord* record2,const std::vector<uint32_t>& offsets){
	assert(record1!=nullptr);
	assert(record2!=nullptr);
	assert(record1->GetSize() == record2->GetSize());
	float* a = reinterpret_cast<float*>(record1->GetData());
	float* b = reinterpret_cast<float*>(record2->GetData());
    float res = 0;
    int offset = 0;
    for(int i = 0;i < offsets.size();i++){
        res += L2Distance(a + offset,b + offset,offsets[i]/DIM_SIZE);
    }
    return res;
}

// template<const int N,const int M>
// void RecordPagePool<N,M>::ReserveVectorRecord(std::string &value,IndexPointer& index_pointer){
//     assert(index_pointer.buffer_id < NPages);
//     assert(value.size()==segment_size);
//     FixedSizeBuffer<N,M>* buffer = &buffers[index_pointer.buffer_id];
//     uint8_t* segment_pointer = &(buffer->Get()[bitmask_offset + index_pointer.offset * segment_size]);
//     std::memcpy(segment_pointer,value.data(),value.size());
// }

// template<const int N,const int M>
// VectorRecord RecordPagePool<N,M>::GetMemoryVectorRecord(IndexPointer& index_pointer){
//     assert(index_pointer.buffer_id < NPages);
//     FixedSizeBuffer<N,M>* buffer = &buffers[index_pointer.buffer_id];
//     // get a record buffer from fix buffer
//     return reinterpret_cast<VectorRecord>(&(buffer->Get()[bitmask_offset + index_pointer.offset * segment_size]));
// }

// template<const int N,const int M>
// std::string RecordPagePool<N,M>::DirectIoRead(const ItemPointer& item_pointer){
//     direct_io_times++;
//     std::string key = ItemPointerToString(item_pointer);
//     std::string value;
//     auto status = db->Get(rocksdb::ReadOptions(), key, &value);
//     assert(status.ok());
//     return value;
// }

// template<const int N,const int M>
// IndexPointer RecordPagePool<N,M>::New(){
//     // we can get a buffer from buffer_free_space, we will update
//     // LRU Cache outside. Because this method will be invoked only
//     // by LRU Cache.
//     if(!buffers_with_free_space.empty()){
//         auto buffer_id = uint32_t(*buffers_with_free_space.begin());
//         assert(buffers.find(buffer_id) != buffers.end());
//         auto &buffer = buffers[buffer_id];
//         // get the bitmask data
//         validity_t * bitmask_ptr = reinterpret_cast<validity_t *>(buffer);
//         ValidityMask mask(bitmask_ptr,bitmask_count);
//         idx_t offset = mask.GetOffset(allocated_record_counts[buffer_id]);
//         allocated_record_counts[buffer_id]++;
//         if(allocated_record_counts[buffer_id] == available_segments_per_buffer){
//             buffers_with_free_space.erase(buffer_id);
//         }
//         return IndexPointer(buffer_id,offset);
//     }
//     return InValidIndexPointer;
// }

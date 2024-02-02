#pragma once

#include<cstdint>
#include "helper.h"
#include "rocksdb/options.h"
#include "m3v.h"
#include "vector.h"
#include<unordered_map>
#include "rocksdb/db.h"

extern "C"{
    #include "postgres.h"
    #include "storage/itemptr.h"
}

// 256KB
constexpr u_int32_t BufferSize = 256 * 1024;
constexpr u_int32_t NPages = 200;
const int DIM_SIZE = sizeof(float);
const u_int32_t InValidBufferId = 0x3f3f3f3f;
const u_int32_t InValidOffset = 0x3f3f3f3f;

// ignore the Vector's 
//	int32 vl_len_; 
//	int16 dim;	  
//	int16 unused;
template<const int N,const int M>
class VectorRecord{
    // serialize vectors into a VectorRecord
    VectorRecord(const Vector* vecs,const ItemPointer &htid_){
        uint32_t offset = 0;
        for(int i = 0;i < M;i++){
            auto dims = vecs[i].dim;
            memccpy(data+offset,&vecs[i].x,DIM_SIZE * dims);
            offset += DIM_SIZE * dims;
        }
    }

	VectorRecord(const std::string &data_){
		assert(data_.size() == N);
		std::copy(data_.begin(), data_.begin() + data_.size(), data);
	}

    private:
        uint8_t data[N];
};

typedef uint64_t RecordPtr;

// every buffer size is 256KB, we use 256KB to get a good cache usage.
// M is the number of vector for a record, N is M vectors's size.
// |------------| 
// |   mask0    |
// |   mask1    |
// |  ........  |
// |  record0   |
// |  record1   |
// |  .......   |
// |------------|
template<const int N,const int M>
class FixedSizeBuffer{
	public:
		FixedSizeBuffer(){
			memset(buffer_data,0,sizeof(buffer_data));
		}

		uint8_t* Get();
    private:
        // std::unordered_map<ItemPointer,RecordPtr> record_table;
		uint8_t buffer_data[BufferSize];
};

class IndexPointer{
	public:
		constexpr IndexPointer(u_int32_t buffer_id_,u_int32_t offset_):buffer_id(buffer_id_),offset(offset_){
		}

    private:
        u_int32_t buffer_id;
        u_int32_t offset;
};

const IndexPointer InValidIndexPointer(InValidBufferId,InValidOffset);

template<const int N,const int M>
class RecordPagePool{
    public:
		RecordPagePool(uint32_t* offsets_){
			rocksdb::DB* db;
			rocksdb::Options options;
			// create a database
			options.create_if_missing = true;
			rocksdb::Status status = rocksdb::DB::Open(options, "./rocksdb_data/", &db);
			this->db = db;
			memccpy(offsets,offsets_,M*sizeof(uint32_t));
			segment_size = 0;
			for(int i = 0;i < M;i++) segment_size += offsets_[i];
			// calculate the mask count and record slots number
			assert(segment_size <= BufferSize - sizeof(validity_t));

			idx_t bits_per_value = sizeof(validity_t) * 8;
			idx_t byte_count = 0;

			bitmask_count = 0;
			available_segments_per_buffer = 0;

			while (byte_count < BufferSize) {
				if (!bitmask_count || (bitmask_count * bits_per_value) % available_segments_per_buffer == 0) {
					// we need to add another validity_t value to the bitmask, to allow storing another
					// bits_per_value segments on a buffer
					bitmask_count++;
					byte_count += sizeof(validity_t);
				}

				auto remaining_bytes = BufferSize - byte_count;
				auto remaining_segments = MinValue(remaining_bytes / segment_size, bits_per_value);

				if (remaining_segments == 0) {
					break;
				}

				available_segments_per_buffer += remaining_segments;
				byte_count += remaining_segments * segment_size;
			}

			bitmask_offset = bitmask_count * sizeof(validity_t);
			for(int buffer_id = 0;buffer_id < NPages;buffer_id++) buffers_with_free_space.insert(buffer_id);
			memset(allocated_record_counts,0,sizeof allocated_record_counts);
		}

		~RecordPagePool(){
			delete db;
		}

		// get an valid IndexPointer memory.
		IndexPointer New();

		std::string DirectIoRead(ItemPointer& item_pointer);

        VectorRecord<N,M>* GetMemoryVectorRecord(IndexPointer& index_pointer);

		void ReserveVectorRecord(std::string &value,IndexPointer& index_pointer);
    private:
        FixedSizeBuffer<N,M> buffers[NPages];
		uint32_t allocated_record_counts[NPages];
		// record every vector size
		uint32_t offsets[M];
		//! Allocation size of one segment in a buffer
		//! We only need this value to calculate bitmask_count, bitmask_offset, and
		//! available_segments_per_buffer
		idx_t segment_size;
		//! Number of validity_t values in the bitmask
		idx_t bitmask_count;
		//! First starting byte of the payload (segments)
		idx_t bitmask_offset;
		//! Number of possible segment allocations per buffer
		idx_t available_segments_per_buffer;
		//! Buffers with free space
		std::unordered_set<idx_t> buffers_with_free_space;
		rocksdb::DB* db;
};

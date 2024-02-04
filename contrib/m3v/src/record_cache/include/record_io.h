#pragma once
#include<iostream>
#include<cstdint>
#include "helper.h"
#include "rocksdb/options.h"
#include "m3v.h"
#include "vector.h"
#include<unordered_map>
#include "rocksdb/db.h"
#include "validity_mask.h"
extern "C" {
    #include "postgres.h"
    #include "storage/itemptr.h"
}

std::string ItemPointerToString(const ItemPointerData& key);

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
// template<const int N,const int M>
class VectorRecord{
	public:
		uint32_t GetSize() const{
			return size;
		}
		uint8_t* GetData() const{
			return data;
		}
		// serialize vectors into a VectorRecord
		// VectorRecord(const Vector* vecs,const ItemPointer &htid_){
		// 	uint32_t offset = 0;
		// 	for(int i = 0;i < M;i++){
		// 		auto dims = vecs[i].dim;
		// 		memccpy(data+offset,&vecs[i].x,DIM_SIZE * dims);
		// 		offset += DIM_SIZE * dims;
		// 	}
		// }

		VectorRecord(){
		}

		VectorRecord(const VectorRecord& record):data(record.GetData()),size(record.GetSize()){

		}

		VectorRecord(uint8_t* data_,int size_):data(data_),size(size_){
		}
		// VectorRecord(const std::string &data_){
		// 	assert(data_.size() == N);
		// 	std::copy(data_.begin(), data_.begin() + data_.size(), data);
		// }

		// void append_copy(float* data_,int size,int offset){
		// 	memccpy(data + offset,data_,size*DIM_SIZE);
		// }

		~VectorRecord(){
			// std::cout<<"free record"<<std::endl;
		}
    private:
        uint8_t* data;
		uint32_t size;
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
// template<const int N,const int M>
class FixedSizeBuffer{
	public:
		FixedSizeBuffer(){
			memset(buffer_data,0,sizeof(buffer_data));
		}

		uint8_t* Get(){
			return buffer_data;
		}
    private:
        // std::unordered_map<ItemPointer,RecordPtr> record_table;
		uint8_t buffer_data[BufferSize];
};

class IndexPointer{
	public:
		constexpr IndexPointer(u_int32_t buffer_id_,u_int32_t offset_):buffer_id(buffer_id_),offset(offset_){
		}
		bool operator !=(const IndexPointer &pointer){
			return this->buffer_id != pointer.buffer_id || this->offset != pointer.offset;
		}
		bool operator ==(const IndexPointer &pointer){
			return this->buffer_id == pointer.buffer_id && this->offset == pointer.offset;
		}
		uint64_t GetCombination(){
			return (static_cast<uint64_t>(buffer_id) << 32) | offset;
		}

		uint32_t GetOffset(){
			return offset;
		}

		void SetOffset(uint32_t offset_){
			offset = offset_;
		}

		uint32_t GetBufferId(){
			return buffer_id;
		}
    private:
        u_int32_t buffer_id;
        u_int32_t offset;
};

const IndexPointer InValidIndexPointer(InValidBufferId,InValidOffset);
class RecordPagePool{
    public:
		RecordPagePool(std::vector<uint32_t> &offsets_,std::string path,uint32_t number_vector_per_record_):direct_io_times(0),number_vector_per_record(number_vector_per_record_){ // "./rocksdb_data/"
			std::cout<<"init RecordPagePool"<<std::endl;
			buffers.resize(NPages);
			rocksdb::DB* db;
			rocksdb::Options options;
			// create a database
			options.create_if_missing = true;
			rocksdb::Status status = rocksdb::DB::Open(options, path, &db);
			this->db = db;
			offsets = offsets_;
			assert(offsets.size()==number_vector_per_record);
			segment_size = 0;
			for(int i = 0;i < number_vector_per_record;i++) segment_size += offsets_[i];
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
			// std::cout<<"free rocksdb"<<std::endl;
			delete db;
		}

		rocksdb::DB* get_rocks_db_instance(){
			assert(db!=nullptr);
			return db;
		}

		uint32_t GetAvailableSegmentsPerBuffer(){
			return available_segments_per_buffer;
		}

		// get an valid IndexPointer memory.
		IndexPointer New(){
		    // we can get a buffer from buffer_free_space, we will update
			// LRU Cache outside. Because this method will be invoked only
			// by LRU Cache.
			if(!buffers_with_free_space.empty()){
				auto buffer_id = uint32_t(*buffers_with_free_space.begin());
				assert(buffer_id < NPages);
				auto buffer = &buffers[buffer_id];
				// get the bitmask data
				validity_t * bitmask_ptr = reinterpret_cast<validity_t *>(buffer);
				ValidityMask mask(bitmask_ptr,bitmask_count);
				idx_t offset = mask.GetOffset(allocated_record_counts[buffer_id]);
				allocated_record_counts[buffer_id]++;
				if(allocated_record_counts[buffer_id] == available_segments_per_buffer){
					buffers_with_free_space.erase(buffer_id);
				}
				return IndexPointer(buffer_id,offset);
			}
			return InValidIndexPointer;
		}

		void ResetDirectIoTimes(){
			direct_io_times = 0;
		}

		uint32_t GetDirectIOTimes(){
			return direct_io_times;
		}

		std::string DirectIoRead(const ItemPointerData& item_pointer){
		    direct_io_times++;
			std::string key = ItemPointerToString(item_pointer);
			std::string value;
			auto status = db->Get(rocksdb::ReadOptions(), key, &value);
			// std::cout<<"Name: "<<db->GetName()<<" status: "<<status.ok()<<std::endl;
			// std::cout<<item_pointer->ip_blkid.bi_hi<<" "<<item_pointer->ip_blkid.bi_lo<<" "<<item_pointer->ip_posid<<std::endl;
			// std::cout<<"item_pointer: "<<ItemPointerToString(item_pointer)<<std::endl;
			assert(status.ok());
			// std::cout<<"status ok"<<std::endl;
			return value;
		}

        VectorRecord GetMemoryVectorRecord(IndexPointer& index_pointer){
			assert(index_pointer.GetBufferId() < NPages);
			FixedSizeBuffer* buffer = &buffers[index_pointer.GetBufferId()];
			// get a record buffer from fix buffer
			VectorRecord record(buffer->Get() + bitmask_offset + index_pointer.GetOffset() * segment_size,segment_size);
			record.GetSize();
			return record;
		}

		void ReserveVectorRecord(std::string &value,IndexPointer& index_pointer){
			if(value.size() != segment_size || index_pointer.GetBufferId() >= NPages){
				std::cout<<"value size: "<<value.size()<<" segment_size: "<<segment_size<<std::endl;
				std::cout<<"index pointer"<<index_pointer.GetBufferId()<<" "<<index_pointer.GetOffset()<<std::endl;
			}
			assert(index_pointer.GetBufferId() < NPages);
			assert(value.size()==segment_size);
			FixedSizeBuffer* buffer = &buffers[index_pointer.GetBufferId()];
			uint8_t* segment_pointer = &(buffer->Get()[bitmask_offset + index_pointer.GetOffset() * segment_size]);
			std::memcpy(segment_pointer,value.data(),value.size());
			// std::cout<<"finish ReserveVectorRecord"<<std::endl;
		}
    private:
        std::vector<FixedSizeBuffer> buffers;
		uint32_t allocated_record_counts[NPages];
		// record every vector size
		std::vector<uint32_t> offsets;
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
		uint32_t direct_io_times;
		uint32_t number_vector_per_record;
};

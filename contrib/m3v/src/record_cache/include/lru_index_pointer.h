#pragma once

#include<iostream>
#include "record_io.h"
#include <functional> // std::hash
extern "C"{
	#include "postgres.h"
}

// for test, we support 3 kinds of 
#define N1 128
#define M1 3
const int SHIFT1 = 48;
const int SHIFT2 = 32;

using HeapTid = uint64_t;

HeapTid GetHeapTid(const ItemPointer& item_pointer);

// IndexPointerLruCache is not concurrent safety.
typedef std::pair<HeapTid,IndexPointer> KeyValuePair;
typedef std::list<KeyValuePair> list_type;
typedef std::unordered_map<HeapTid, typename std::list<KeyValuePair>::iterator> Map;
// template<const int N,const int M>
class IndexPointerLruCache{
	public:
		void DebugTime(std::string message = "Time Cost"){
			double duration = 1000.0 * time / CLOCKS_PER_SEC;
    		std::cout <<message<<" "<<duration << " milliseconds" << std::endl;
		}

		void ResetTime(){
			time = 0;
		}

		void WriteRocksDB(ItemPointer item_pointer){
			
		}

		RecordPagePool* GetPool(){
			return &pool;
		}

		// pin feature needed:
		// when a page request multi records, we need to pin the records needed by
		// page. 
		// We use the high 16bytes as pin counts, the low 16bytes as offset
		// we can always get IndexPointer successfully.
		VectorRecord Get(const ItemPointer& k){
			// 1. try to get IndexPointer from cache and the get VectorRecord from RecordpagePool
			IndexPointer index_pointer = getCopy(k);
			if(index_pointer != InValidIndexPointer){
				// if we can get a valid index pointer, the record buffer must exists here.
				return pool.GetMemoryVectorRecord(index_pointer);
			}else{
				// 2. if we can't retrive from 1, we should do a DirectIORead from RocksDB, but we need
				// to evict a IndexPointer so we can get a free record memory.
				std::string record = pool.DirectIoRead(*k);
				// 2.1 if cache is full, we should evict a index pointer here
				// 2.2 if cache is not full, we should try to New a IndexPointer from RecordPagePool
				if(is_full()){
					index_pointer = remove_first_unpin();
					if(index_pointer == InValidIndexPointer){
						std::cout<<"pointer1: "<<k->ip_posid<<std::endl;
					}
				}else{
					index_pointer = pool.New();
				}

				assert(index_pointer!=InValidIndexPointer);
				insert(k,index_pointer);
				PinItemPointer(k);
				// we should preserve the value in RecordPagePool
				pool.ReserveVectorRecord(record,index_pointer);
				// std::cout<<"GetSize"<<pool.GetMemoryVectorRecord(index_pointer).GetSize()<<std::endl;
				return pool.GetMemoryVectorRecord(index_pointer);
			}
		}

		uint32_t GetIoTimes(){
			return pool.GetDirectIOTimes();
		}

		uint32_t GetPinCounts(){
			return pin_counts;
		}

		void ResetDirectIoTimes(){
			pool.ResetDirectIoTimes();
		}

		void UnPinItemPointer(const ItemPointer& k){
			if(contains(k)){
				assert(pin_counts>0);
				const auto iter = cache_.find(GetHeapTid(k));
				IndexPointer& index_pointer = iter->second->second;
				assert(((index_pointer.GetOffset() & HIGHMASK)>>16) >= 1);
				index_pointer.SetOffset(index_pointer.GetOffset()- (1<<16));
				pin_counts--;
			}
		}

		rocksdb::DB* GetDB(){
			return pool.get_rocks_db_instance();
		}

		IndexPointerLruCache(std::vector<uint32_t> &offsets_,uint32_t number_vector_per_record_,size_t maxSize = 40000, size_t elasticity = 20000):maxSize_(maxSize), elasticity_(elasticity),pool(offsets_,std::string(PROJECT_ROOT_PATH) + "/rocksdb_data",number_vector_per_record_) {
			maxSize_ =  pool.GetAvailableSegmentsPerBuffer() * NPages;
			elasticity_ = maxSize_;
			std::cout<<"init IndexPointerLruCache"<<std::endl;
		}
	
		~IndexPointerLruCache() {
		}
	private:
		const uint32_t HIGHMASK = 0xFFFF0000;

		bool is_full(){
			return cache_.size() == maxSize_;
  		}

		IndexPointer remove_first_unpin(){
			// remove the pin counts
			IndexPointer index_pointer = InValidIndexPointer;
			for (auto it = keys_.rbegin(); it != keys_.rend(); ++it) {
				if(((it->second.GetOffset() & HIGHMASK)) == 0){
					index_pointer = it->second;
					index_pointer.SetOffset((~HIGHMASK)&index_pointer.GetOffset());
					remove(it->first);break;
				}
			}
			return index_pointer;
		}

		void insert(const ItemPointer& k, IndexPointer v) {
			const auto iter = cache_.find(GetHeapTid(k));
			if (iter != cache_.end()) {
				iter->second->second = v;
				keys_.splice(keys_.begin(), keys_, iter->second);
				return;
			}
			auto key = GetHeapTid(k);
			keys_.emplace_front(key, std::move(v));
			cache_[key] = keys_.begin();
			prune();
		}

		bool remove(const HeapTid& k) {
			auto iter = cache_.find(k);
			if (iter == cache_.end()) {
				return false;
			}
			keys_.erase(iter->second);
			cache_.erase(iter);
			return true;
		}

		size_t size() const {
			return cache_.size();
		}
		bool empty() const {
			return cache_.empty();
		}
		void clear() {
			cache_.clear();
			keys_.clear();
		}
		
		size_t prune() {
			size_t maxAllowed = maxSize_ + elasticity_;
			if (maxSize_ == 0 || cache_.size() < maxAllowed) {
				return 0;
			}
			size_t count = 0;
			while (cache_.size() > maxSize_) {
				cache_.erase(keys_.back().first);
				keys_.pop_back();
				++count;
			}
			return count;
		}

		/**
		 * returns a copy of the stored object (if found)
		 * safe to use/recommended in multi-threaded apps
		 */
		IndexPointer getCopy(const ItemPointer& k){
			// remove pin counts
			const auto iter = cache_.find(GetHeapTid(k));
			if(iter != cache_.end()){
				PinItemPointer(k);
				keys_.splice(keys_.begin(), keys_, iter->second);
				IndexPointer pointer = iter->second->second;
				pointer.SetOffset(pointer.GetOffset()&(~HIGHMASK));
				return pointer;
			}else{
				return InValidIndexPointer;
			}
		}

		void PinItemPointer(const ItemPointer& k){
			if(contains(k)){
				const auto iter = cache_.find(GetHeapTid(k));
				IndexPointer& index_pointer = iter->second->second;
				index_pointer.SetOffset(index_pointer.GetOffset() + (1<<16));
				pin_counts++;
			}
		}

		bool contains(const ItemPointer& k) const {
			return cache_.find(GetHeapTid(k)) != cache_.end();
		}

		size_t getMaxSize() const { return maxSize_; }
		size_t getElasticity() const { return elasticity_; }
		size_t getMaxAllowedSize() const { return maxSize_ + elasticity_; }
	private:
		// Disallow copying.
		IndexPointerLruCache(const IndexPointerLruCache&) = delete;
		IndexPointerLruCache& operator=(const IndexPointerLruCache&) = delete;
    private:
		Map cache_;
		list_type keys_;
		size_t maxSize_;
		size_t elasticity_;
		RecordPagePool pool;
		uint32_t pin_counts = 0;
		std::clock_t time;
};

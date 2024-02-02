#pragma once

#include<iostream>
#include "itemptr.h"
#include "record_io.h"

typedef std::pair<ItemPointer,IndexPointer> KeyValuePair;
typedef std::list<KeyValuePair> list_type;
typedef std::unordered_map<ItemPointer, typename std::list<KeyValuePair>::iterator> Map;
template<const int N,const int M>
class IndexPointerLruCache{
	public:
		// we can always get IndexPointer successfully.
		VectorRecord<N,M>* Get(const ItemPointer& k);
	private:
		bool is_full(){
			return cache_.size() == maxSize_;
  		}

		IndexPointer remove_front(){
		 	ItemPointer item_pointer = keys_.end()->first;
			IndexPointer index_pointer = keys_.end()->second;
			remove(item_pointer);
			return IndexPointer;
		}

		void insert(const ItemPointer& k, IndexPointer v) {
			const auto iter = cache_.find(k);
			if (iter != cache_.end()) {
				iter->second->second = v;
				keys_.splice(keys_.begin(), keys_, iter->second);
				return;
			}
			keys_.emplace_front(k, std::move(v));
			cache_[k] = keys_.begin();
			prune();
		}

		bool remove(const ItemPointer& k) {
			auto iter = cache_.find(k);
			if (iter == cache_.end()) {
			return false;
			}
			keys_.erase(iter->second);
			cache_.erase(iter);
			return true;
		}
		explicit IndexPointerLruCache(size_t maxSize = 64, size_t elasticity = 10):maxSize_(maxSize), elasticity_(elasticity) {
		}
		virtual ~IndexPointerLruCache() = default;
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
			const auto iter = cache_.find(k);
			if(iter != cache_.end()){
				keys_.splice(keys_.begin(), keys_, iter->second);
				return iter->second->second;
			}else{
				return InValidIndexPointer;
			}
		}

		bool contains(const ItemPointer& k) const {
			return cache_.find(k) != cache_.end();
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
		RecordPagePool<N,M> pool;
};
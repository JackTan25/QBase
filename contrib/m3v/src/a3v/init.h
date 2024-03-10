#include "lru_index_pointer.h"

class GlobalInit{
    public:
        GlobalInit(){
            // init the rocksdb to store all the vector records and lrucache.
        }

        bool ContainsIndex(std::string index_name){
            return mp.count(index_name);
        }

        IndexPointerLruCache* GetIndexCacheByName(std::string index_name){
			// lock_.lock();
            return mp[index_name];
        }

        IndexPointerLruCache* GetIndexCache(std::vector<uint32_t> offsets,uint32_t number_vector_per_record_,std::string index_name){
			if(mp.count(index_name)) return mp[index_name];
            mp[index_name] = new IndexPointerLruCache(offsets,number_vector_per_record_,"/" + index_name);
            return mp[index_name];
        }

		~GlobalInit(){
			for(auto [k,v]: mp){
				delete v;
			}
		}
    private:
        // index_name => IndexPointerCache
        std::unordered_map<std::string,IndexPointerLruCache*> mp;
		// std::mutex lock_;
};

GlobalInit init;

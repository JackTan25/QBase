#include "lru_index_pointer.h"
#include "hnswlib.h"
#include "util.h"
#include <fstream>

#define INVALID_RADIUS __FLT_MIN__

std::string build_hnsw_index_file_path(Relation index);
// store query ids std::vector. The ids should be tids to specify the entry position.
std::string build_a3v_index_forest_query_ids_file_path(Relation index);
std::string build_memory_index_points_file_path(Relation index);
class GlobalInit{
    public:
        // GlobalInit(){
        //     // init the rocksdb to store all the vector records and lrucache.
        // }

		hnswlib::HierarchicalNSW<float>* LoadHnswIndex(Relation index,int dim);

		// we will insert alg_hnsw when we build a new hnsw ervery time.
		void InsertHnswIndex(std::string index_file_path,hnswlib::HierarchicalNSW<float>* alg_hnsw);

		// for every a3v index (in fact, it means this is a a3v index forest.), we will use a single record cache.
        bool ContainsIndexCache(std::string index_name);

        IndexPointerLruCache* GetIndexCacheByName(std::string index_name);

		// offsets store the bytes size of each vector,not dimension
        IndexPointerLruCache* GetIndexCache(std::vector<uint32_t>& offsets,uint32_t number_vector_per_record_,std::string index_name);
		void InsertNewTidForIndex(std::string index_name,ItemPointerData tid);

		ItemPointerData GetRootTidAtIndex(std::string index_name,int index);

		~GlobalInit();

    private:
        // index_name => IndexPointerCache
        std::unordered_map<std::string,IndexPointerLruCache*> mp;
		// index_file_name => HnswIndex
		std::unordered_map<std::string,hnswlib::HierarchicalNSW<float>*> alg_hnsws;
		// std::mutex lock_;
		// root link tids, we can get the simliarest point, that's the root point.
		// and then we can search root from the hsnw index, it will give us a index,
		// so we can get tids[index] to get the root.
		// index_name => root tids
		std::unordered_map<std::string,std::vector<ItemPointerData>> tids;
		std::unordered_map<std::string,bool> dirties;
};

extern GlobalInit init;
static ItemPointerData InvalidItemPointerData = {{0,0},InvalidOffsetNumber};
using PII = std::pair<std::vector<float>,ItemPointerData>;

// the default in-memory hsnw index's stored in index_name_memory_index.bin
// memory data store path: std::string path = std::string(PROJECT_ROOT_PATH) + "/memory_data_points.bin";
class InMemoryGlobal{
	 public:
        // InMemoryGlobal(){
        // }

		void appendDataPoints(const std::vector<PII>& data,Relation index);

		const std::vector<PII>& LoadDataPoints(Relation index);

		hnswlib::HierarchicalNSW<float>* LoadHnswIndex(Relation index,int dim);

		~InMemoryGlobal();

	private:
		// index_file_name => HnswIndex
		std::unordered_map<std::string,hnswlib::HierarchicalNSW<float>*> alg_hnsws;		
		// index_file_name => data points
		std::unordered_map<std::string,std::vector<PII>> points;
};

extern InMemoryGlobal memory_init;

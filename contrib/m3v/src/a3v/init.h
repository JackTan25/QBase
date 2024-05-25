#pragma once

#include "lru_index_pointer.h"
#include "hnswlib.h"
#include "memory_a3v.h"
#include <fstream>

#define INVALID_RADIUS __FLT_MIN__

std::string build_hnsw_index_file_path(Relation index);
std::string build_hnsw_index_file_hard_path(Relation index,int idx);
std::string build_hnsw_index_file_hard_path_prefix(Relation index);
// store query ids std::vector. The ids should be tids to specify the entry position.
std::string build_a3v_index_forest_query_ids_file_path(Relation index);
std::string build_memory_index_points_file_path(Relation index);
std::string build_memory_index_threshold_file_path(Relation index);
void readFloatFromFile(const std::string& filePath, float& value);
void writeFloatToFile(const std::string& filePath, float value);
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

		const std::vector<int>& GetDimensions(Relation index);

		~GlobalInit();

    private:
        // index_name => IndexPointerCache
        std::unordered_map<std::string,IndexPointerLruCache*> mp;
		// index_file_name => HnswIndex
		std::unordered_map<std::string,hnswlib::HierarchicalNSW<float>*> alg_hnsws;
		// index_file_name => (dimensions)
		std::unordered_map<std::string,std::vector<int>> dimensions; //todo: dimensions serialize and deserialize
		// std::mutex lock_;
		// root link tids, we can get the simliarest point, that's the root point.
		// and then we can search root from the hsnw index, it will give us a index,
		// so we can get tids[index] to get the root.
		// index_name => root tids
		std::unordered_map<std::string,std::vector<ItemPointerData>> tids;
		std::unordered_map<std::string,bool> dirties;
};

extern GlobalInit init;

// the default in-memory hsnw index's stored in index_name_memory_index.bin
// memory data store path: std::string path = std::string(PROJECT_ROOT_PATH) + "/memory_data_points.bin";
class InMemoryGlobal{
	 public:
        // InMemoryGlobal(){
		// 	elog(LOG,"init MemoryGlobal,Start Idle A3v Thread");
        // }
		float random_10(Relation index,std::vector<PII> &data_points);
		void restore_datapoints_from_hnsw(Relation index);
		void appendDataPoints(Relation index,m3vBuildState *buildstate);

		void SetDimensions(const std::vector<int> &dimensions_,Relation index);

		const std::vector<int>& GetDimensions(Relation index);

		const std::vector<PII>& LoadDataPoints(Relation index);

		std::vector<PII>* GetDataPointsPointer(Relation index);
		// for now, support single column firstly in 2024-4-17, 
		// support multi-vector in 4.18
		// support prefilter in 4.19.
		std::shared_ptr<MemoryA3v> GetMultiVectorMemoryIndex(Relation index,const std::vector<int>& dims,float* query,int &lable_);

		// build_memory_index_points_file_path
		std::shared_ptr<MemoryA3v> GetMultiVectorMemoryIndexById(std::string& path,int a3v_index_id);

		void appendHnswHardIndex(std::shared_ptr<hnswlib::HierarchicalNSW<float>> &hnsw_hard_index,Relation index);

		const float* appendHnswHardIndexData(int idx,Relation index,const float* data_point,uint64_t num,int vector_index);

		bool LoadHnswHardIndex(Relation index,const std::vector<int>& dims,int nums);

		~InMemoryGlobal();

	public:

		// for now, support single column firstly in 2024-4-17, 
		// support multi-vector in 4.18
		// support prefilter in 4.19.
		// void BuildMultiVectorMemoryIndex(Relation index,const std::vector<int>& dims);
		
		std::shared_ptr<hnswlib::HierarchicalNSW<float>>  LoadHnswIndex(Relation index,int dim,bool& init);
		std::unordered_map<std::string,float> thresholds;
		// index_file_name => HnswIndex (MetaHNSW)
		std::unordered_map<std::string,std::shared_ptr<hnswlib::HierarchicalNSW<float>>> alg_hnsws;		
		// index_file_name 
		// index_file_name => (data points)
		std::unordered_map<std::string,std::vector<PII>> points;
		// index_file_name => (dimensions)
		std::unordered_map<std::string,std::vector<int>> dimensions; //todo: dimensions serialize and deserialize
		// for one user a3v_index, it can reference to multi a3v_indexes(a forest, we need to retrive correlated one from hnsw index).
		// the rule is below:
		//	 we will find the top-1 point from hnsw index, and do search from here.
		// index_file_name => MemoryA3v
		std::unordered_map<std::string,std::vector<std::shared_ptr<MemoryA3v>>> memory_indexes;
		// dimension
		std::unordered_map<std::string,std::shared_ptr<hnswlib::SpaceInterface<float>>> index_space;
		// prefix_index_file_hard_path => HnswIndex
		std::unordered_map<std::string,std::vector<std::shared_ptr<hnswlib::HierarchicalNSW<float>>>> hard_hnsws;
		int query_times{0};
};

extern InMemoryGlobal memory_init;

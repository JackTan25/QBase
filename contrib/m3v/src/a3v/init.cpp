#include "init.h"
#include "hnswlib.h"

hnswlib::HierarchicalNSW<float>* GlobalInit::LoadHnswIndex(Relation index,int dim){
    std::string index_file_path = build_hnsw_index_file_path(index);
    if(alg_hnsws.count(index_file_path)){
        return alg_hnsws[index_file_path];
    }else{
        // try load hnsw index from index_file_path
        // Initing index
        hnswlib::L2Space space(dim);
        hnswlib::HierarchicalNSW<float>* alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, index_file_path);
        alg_hnsws[index_file_path] = alg_hnsw;
        return alg_hnsw;
    }
}

// we will insert alg_hnsw when we build a new hnsw ervery time.
void GlobalInit::InsertHnswIndex(std::string index_file_path,hnswlib::HierarchicalNSW<float>* alg_hnsw){
    alg_hnsws[index_file_path] = alg_hnsw;
}

// for every a3v index (in fact, it means this is a a3v index forest.), we will use a single record cache.
bool GlobalInit::ContainsIndexCache(std::string index_name){
    return mp.count(index_name);
}

IndexPointerLruCache* GlobalInit::GetIndexCacheByName(std::string index_name){
    // lock_.lock();
    return mp[index_name];
}

// offsets store the bytes size of each vector,not dimension
IndexPointerLruCache* GlobalInit::GetIndexCache(std::vector<uint32_t>& offsets,uint32_t number_vector_per_record_,std::string index_name){
    if(mp.count(index_name)) return mp[index_name];
    mp[index_name] = new IndexPointerLruCache(offsets,number_vector_per_record_,"/" + index_name);
    return mp[index_name];
}

void GlobalInit::InsertNewTidForIndex(std::string index_name,ItemPointerData tid){
    if(!tids.count(index_name)){
        std::string path = std::string(PROJECT_ROOT_PATH) + "/" + index_name + "_a3v_forest_root_ids.bin";
        if(file_exists(path)){
            tids[index_name] = DeserializeVector<ItemPointerData>(path);
            tids[index_name].push_back(tid);
        }else{
            // if none, we just hold it in memory and flush when we close system.
            tids[index_name].push_back(tid);
        }
    }else{
        // just push it 
        tids[index_name].push_back(tid);
    }
    dirties[index_name] = true;
}

ItemPointerData GlobalInit::GetRootTidAtIndex(std::string index_name,int index){
    if(!tids.count(index_name)){
        std::string path = std::string(PROJECT_ROOT_PATH) + "/" + index_name + "_a3v_forest_root_ids.bin";
        // the path must be existed
        tids[index_name] = DeserializeVector<ItemPointerData>(path);
    }
    return tids[index_name][index];
}

const std::vector<int>& GlobalInit::GetDimensions(Relation index){
    std::string index_file_path = build_memory_index_points_file_path(index);
    if(!dimensions.count(index_file_path)){
        for(int i = 0;i < index->rd_att->natts;++i){
            Form_pg_attribute attr = TupleDescAttr(index->rd_att, 0);
	        dimensions[index_file_path].push_back(attr->atttypmod);
        }
    }
    return dimensions[index_file_path];
}

GlobalInit::~GlobalInit(){
    // we need to release all new memory.
    for(auto [k,v]: mp){
        delete v;
    }
    for(auto [k,v]: alg_hnsws){
        delete v;
    }
    // serialize dirty root tids.
    for(auto [index_name,tid_array] : tids){
        if(dirties[index_name]){
            std::string path = std::string(PROJECT_ROOT_PATH) + "/" + index_name + "_a3v_forest_root_ids.bin";
            SerializeVector<ItemPointerData>(tid_array,path);
        }
    }
}

// MemoryGlobal
void InMemoryGlobal::appendDataPoints(const std::vector<PII>& data, Relation index){
    std::string index_file_path = build_memory_index_points_file_path(index);
    points[index_file_path] = data;
}

void InMemoryGlobal::SetDimensions(const std::vector<int> &dimensions_,Relation index){
    std::string index_file_path = build_memory_index_points_file_path(index);
    dimensions[index_file_path] = dimensions_;
}

const std::vector<int>& InMemoryGlobal::GetDimensions(Relation index){
    std::string index_file_path = build_memory_index_points_file_path(index);
    if(!dimensions.count(index_file_path)){
        for(int i = 0;i < index->rd_att->natts;++i){
            Form_pg_attribute attr = TupleDescAttr(index->rd_att, 0);
	        dimensions[index_file_path].push_back(attr->atttypmod);
        }
    }
    return dimensions[index_file_path];
}

const std::vector<PII>& InMemoryGlobal::LoadDataPoints(Relation index){
    std::string index_file_path = build_memory_index_points_file_path(index);
    if(points.count(index_file_path)){
        // elog(INFO,"LoadDataPoints: %s",index_file_path.c_str());
        return points[index_file_path];
    }else{
        points[index_file_path] = DeserializeVector<PII>(index_file_path);
        return points[index_file_path];
    }
}

std::vector<PII>* InMemoryGlobal::GetDataPointsPointer(Relation index){
    std::string index_file_path = build_memory_index_points_file_path(index);
    LoadDataPoints(index);
    return &points[index_file_path];
}

std::shared_ptr<hnswlib::HierarchicalNSW<float>> InMemoryGlobal::LoadHnswIndex(Relation index,int dim,bool &init){
    std::string index_file_path = build_hnsw_index_file_path(index);
    if(alg_hnsws.count(index_file_path)){
        init = false;
        return alg_hnsws[index_file_path];
    }else{
        // we can load from disk
        if(file_exists(index_file_path)){
            // try load hnsw index from index_file_path
            // Initing index
            index_space[index_file_path] = std::make_shared<hnswlib::L2Space>(dim);
            alg_hnsws[index_file_path] = std::make_shared<hnswlib::HierarchicalNSW<float>>(index_space[index_file_path].get(), index_file_path);
            init = false;
        }else{
            // give a new index
            int max_elements = 10000;   // Maximum number of elements, should be known beforehand
            int M = 16;                 // Tightly connected with internal dimensionality of the data
                                        // strongly affects the memory consumption
            int ef_construction = 200;  // Controls index search speed/build speed tradeoff
            // Initing index
            index_space[index_file_path] = std::make_shared<hnswlib::L2Space>(dim);
            alg_hnsws[index_file_path] = std::make_shared<hnswlib::HierarchicalNSW<float>>(index_space[index_file_path].get(), max_elements, M, ef_construction);
            init = true;
        }
        return alg_hnsws[index_file_path];
    }
}

// void InMemoryGlobal::BuildMultiVectorMemoryIndex(Relation index,const std::vector<int>& dims){
	
// }

std::shared_ptr<MemoryA3v> InMemoryGlobal::GetMultiVectorMemoryIndex(Relation index,const std::vector<int>& dims,float* query){
	std::string index_file_path = build_memory_index_points_file_path(index);
	bool init;
    int dim = 0;
    for(int i = 0;i < dims.size();i++) dim += dims[i];
	// support single index for now.
	std::shared_ptr<hnswlib::HierarchicalNSW<float>> hnsw_index = LoadHnswIndex(index,dim,init);
    // elog(INFO,"(int*)hnsw_index->dist_func_param_: %d",(*(size_t*)hnsw_index->dist_func_param_));
	if(init){
		int lable = -1;
		if(!memory_indexes.count(index_file_path)){
			lable = 0;
		}else{
			lable = memory_indexes[index_file_path].size();
		}
        std::shared_ptr<MemoryA3v> a3v_index = std::make_shared<MemoryA3v>(dims,memory_init.LoadDataPoints(index));
        // elog(INFO,"(int*)hnsw_index->dist_func_param_: %d",*(int*)hnsw_index->dist_func_param_);
		memory_indexes[index_file_path].push_back(a3v_index);
		hnsw_index->addPoint(query,lable);
		// we should add new memory index.
		
	}
	std::priority_queue<std::pair<float, hnswlib::labeltype>> result = hnsw_index->searchKnn(query,1);
    // CloseQueryThreshold
    // A3vMemoryIndexType(index)
	auto root_point = result.top();
	hnswlib::labeltype label = root_point.second;
    return memory_indexes[index_file_path][label];
}

InMemoryGlobal::~InMemoryGlobal(){
    // std::string path = std::string(PROJECT_ROOT_PATH) + "/memory_data_points.bin";
    for(auto item : points){
        std::string path = item.first;
        if(!file_exists(path) && points.size() > 0){
            SerializeVector<PII>(item.second,path);
        }
    }
}

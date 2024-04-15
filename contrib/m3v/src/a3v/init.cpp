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
            SerializeVector(tid_array,path);
        }
    }
}



// MemoryGlobal
void InMemoryGlobal::appendDataPoints(const std::vector<PII>& data,Relation index){
    std::string index_file_path = build_memory_index_points_file_path(index);
    points[index_file_path] = data;
}

const std::vector<PII>& InMemoryGlobal::LoadDataPoints(Relation index){
    std::string index_file_path = build_memory_index_points_file_path(index);
    if(points.count(index_file_path)){
        return points[index_file_path];
    }else{
        points[index_file_path] =  DeserializeVector<PII>(index_file_path);
        return points[index_file_path];
    }
}

hnswlib::HierarchicalNSW<float>* InMemoryGlobal::LoadHnswIndex(Relation index,int dim){
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

InMemoryGlobal::~InMemoryGlobal(){
    for(auto [k,v]: alg_hnsws){
        delete v;
    }
    // std::string path = std::string(PROJECT_ROOT_PATH) + "/memory_data_points.bin";
    for(auto item : points){
        std::string path = item.first;
        if(!file_exists(path) && points.size() > 0){
            SerializeVector(item.second,path);
        }
    }
}


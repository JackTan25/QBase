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

void writeFloatToFile(const std::string& filePath, float value) {
    std::ofstream outputFile(filePath);
    if (!outputFile.is_open()) {
        elog(ERROR, "Failed to open file for writing.");
    }
    outputFile << value << std::endl;
    outputFile.close();
    elog(INFO, "Float value has been written to file successfully.");
}

void readFloatFromFile(const std::string& filePath, float& value) {
    std::ifstream inputFile(filePath);
    if (!inputFile.is_open()) {
        elog(ERROR,"Failed to open file for reading.");
    }
    if (!(inputFile >> value)) {
        elog(ERROR,"Failed to read float value from file.");
    }
    inputFile.close();
    elog(INFO, "Float value has been read from file successfully: ");
}

float InMemoryGlobal::random_10(Relation index,std::vector<PII> &data_points){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, data_points.size() - 1);
    std::vector<int> indices;
    while (indices.size() < 10) {
        int index = dis(gen);
        if (std::find(indices.begin(), indices.end(), index) == indices.end()) {
            indices.push_back(index);
        }
    }
    std::string index_file_path = build_memory_index_points_file_path(index);
    std::vector<int> dimensions_ = dimensions[index_file_path];
    float dist = 0.0;
    for(int i = 0;i < data_points.size();i++){
        for(int j = i+1;j < data_points.size();j++){
            for(int k = 0;k <dimensions_.size();k++){
                dist += L2Distance(const_cast<float*>(data_points[i].first[k]),const_cast<float*>(data_points[j].first[k]),dimensions_[k]);
            }
        }
    }
    return dist/45.0;
}

// MemoryGlobal
void InMemoryGlobal::appendDataPoints(Relation index,m3vBuildState *buildstate){
    std::string index_file_path = build_memory_index_points_file_path(index);
    std::string index_file_threshold_path = build_memory_index_threshold_file_path(index);
    // add vector data pointer
    points[index_file_path] = buildstate->data_points;
    // calculate random distance here.
    float threshold = random_10(index,buildstate->data_points);
    thresholds[index_file_threshold_path] = threshold;
    writeFloatToFile(index_file_threshold_path,threshold);
    elog(INFO,"MetaHNSW threshold: %.6f",threshold);
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

bool InMemoryGlobal::LoadHnswHardIndex(Relation index,const std::vector<int>& dims,int nums){
    std::string index_file_path_prefix = build_hnsw_index_file_hard_path_prefix(index);
    if(hard_hnsws.count(index_file_path_prefix)){
        return false;
    }
    for(int i = 0;i < nums;i++){
        std::string index_file_path_pefix = build_hnsw_index_file_hard_path_prefix(index);
        std::string index_file_path = build_hnsw_index_file_hard_path(index,i);
        // we can load from disk
        if(file_exists(index_file_path)){
            // try load hnsw index from index_file_path
            // Initing index
            index_space[index_file_path] = std::make_shared<hnswlib::L2Space>(dims[i]);
            hard_hnsws[index_file_path_pefix].push_back(std::make_shared<hnswlib::HierarchicalNSW<float>>(index_space[index_file_path].get(), index_file_path));
        }else{
            ereport(FATAL, errcode(0), errmsg("can't find hnsw index hard file: %s",index_file_path.c_str()));
        }
    }
    restore_datapoints_from_hnsw(index);
    return true;
}

void InMemoryGlobal::appendHnswHardIndex(std::shared_ptr<hnswlib::HierarchicalNSW<float>> &hnsw_hard_index,Relation index){
    std::string hnsw_index_file_prefix = build_hnsw_index_file_hard_path_prefix(index);
    hard_hnsws[hnsw_index_file_prefix].push_back(hnsw_hard_index);
}

const float* InMemoryGlobal::appendHnswHardIndexData(int idx,Relation index,const float* data_point,int num,int vector_index){
    std::string hnsw_index_file_prefix = build_hnsw_index_file_hard_path_prefix(index);
    hard_hnsws[hnsw_index_file_prefix][idx]->addPoint(data_point,num);
    return (float*)hard_hnsws[hnsw_index_file_prefix][idx]->getDataByInternalId(vector_index);
}

void InMemoryGlobal::restore_datapoints_from_hnsw(Relation index){
    int column_nums = index->rd_att->natts;
    std::string memory_a3v_index_file_path = build_memory_index_points_file_path(index);
    std::string hnsw_index_file_prefix = build_hnsw_index_file_hard_path_prefix(index);
    int vector_nums = hard_hnsws[hnsw_index_file_prefix][0]->cur_element_count;
    for(int i = 0;i < vector_nums;i++){
        std::vector<const float*> a3v_data_points;
        hnswlib::labeltype label = 0;
        for(int j = 0;j < column_nums;j++){
            a3v_data_points.push_back((float*)hard_hnsws[hnsw_index_file_prefix][j]->getDataByInternalId(i));
            label = *hard_hnsws[hnsw_index_file_prefix][j]->getExternalLabeLp(i);
        }
        ItemPointerData tid = GetItemPointerDataByNumber(label);
        points[memory_a3v_index_file_path].push_back({a3v_data_points,tid});
    }
}

std::shared_ptr<MemoryA3v> InMemoryGlobal::GetMultiVectorMemoryIndexById(std::string& path,int a3v_index_id){
    if(!memory_indexes.count(path)||!int(memory_indexes[path].size())-1 < a3v_index_id){
        elog(ERROR,"GetMultiVectorMemoryIndexById Failed");
    }
    return memory_indexes[path][a3v_index_id];
}

std::shared_ptr<MemoryA3v> InMemoryGlobal::GetMultiVectorMemoryIndex(Relation index,const std::vector<int>& dims,float* query,int &label_){
	std::string index_file_path = build_memory_index_points_file_path(index);
	bool init;
    int dim = 0;
    for(int i = 0;i < dims.size();i++) dim += dims[i];
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
	}else{
        std::priority_queue<std::pair<float, hnswlib::labeltype>> result = hnsw_index->searchKnn(query,1);
        auto distance = result.top().first;
        // open a new a3v index
        if(distance > check_thresold){
            elog(INFO,"open new a3v index");
            std::shared_ptr<MemoryA3v> a3v_index = std::make_shared<MemoryA3v>(dims,memory_init.LoadDataPoints(index));
            // elog(INFO,"(int*)hnsw_index->dist_func_param_: %d",*(int*)hnsw_index->dist_func_param_);
            hnswlib::labeltype lable = memory_indexes[index_file_path].size();
            memory_indexes[index_file_path].push_back(a3v_index);
            hnsw_index->addPoint(query,lable);
        }
    }
	std::priority_queue<std::pair<float, hnswlib::labeltype>> result = hnsw_index->searchKnn(query,1);
    // CloseQueryThreshold
    // A3vMemoryIndexType(index)
	auto root_point = result.top();
	hnswlib::labeltype label = root_point.second;
    label_ = label;
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

#include <gtest/gtest.h>
#include "lru_index_pointer.h"
#include "elkan_kmeans_utils.h"
#include <sstream>
#include <iomanip>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>
#include <rocksdb/db.h>
#include<unordered_set>
#include <fstream>
#include <vector>
#include <string>
#include "page_sort_index.h"
extern "C"{
	#include "vector.h"
	// #include <itemptr.h>
}

TEST(Hack, AssertionT) {
    ASSERT_TRUE(true);
}

TEST(Hack, AssertionF) {
    ASSERT_FALSE(false);
}

TEST(M3V,RocksDB){
    // rocksdb::DB* db;
    // rocksdb::Options options;

    // // 打开数据库
    // options.create_if_missing = true;
    // rocksdb::Status status = rocksdb::DB::Open(options, std::string(PROJECT_ROOT_PATH) + "/db", &db);
    rocksdb::DB* db = nullptr;
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(50* 1024 * 1024);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // 打开数据库
	options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, std::string(PROJECT_ROOT_PATH) + "/db", &db);
    if (!status.ok()) {
        std::cerr << "Unable to open database: " << status.ToString() << std::endl;
    }

    std::string key = "hello";
    std::string value = "world";

    // 写入数据
    status = db->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        std::cerr << "写入失败: " << status.ToString() << std::endl;
    }

    // 读取数据
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (status.ok()) {
        std::cout << "读取: " << value << std::endl;
    } else {
        std::cerr << "读取失败: " << status.ToString() << std::endl;
    }

    // 关闭数据库
    delete db;
}

// not real float bytes data.
std::string build_data_string(int n,int num){
	std::string res = "";
	for(int i = 0;i < n;i++){
		float t = num;
		const unsigned char* pBytes = reinterpret_cast<const unsigned char*>(&t);
		for (size_t i = 0; i < DIM_SIZE; ++i) {
			res += pBytes[i];
		}
	}
	auto size_ = res.size();
	return res;
}

// 128 dimentions:
// one row multi vectors: [1,1,1,....],[2,2,2,....],[3,3,3.....]
void WriteIndexPointerKVs(int nums,rocksdb::DB *db){
    assert(db!=nullptr);
    for(int i = 0;i < nums;i++){
        // build a vector record
		ItemPointerData data;ItemPointer pointer = &data;pointer->ip_posid = i;data.ip_blkid.bi_hi = 0;data.ip_blkid.bi_lo = 0;
        // std::cout<<"WriteIndexPointerKVs: "<<ItemPointerToString(pointer)<<std::endl;
		db->Put(rocksdb::WriteOptions(),ItemPointerToString(*pointer),build_data_string(N1*3,i));
    }
}

TEST(M3V,BuildDataString){
	for(int i =1;i < 20;i++){
		std::string res = build_data_string(3,i);
		// std::cout<<res<<std::endl;
		const float* data = reinterpret_cast<const float*>(res.c_str());
		for(int j = 0;j < 3;j++){
			std::cout<<data[j]<<" ";
		}
		std::cout<<std::endl;
	}
}

// 256KB per buffer, segment_size is 384*4 = 1536 bytes, 
// then we can calcultae that:
// there are 170 segments + 3 masks, rest bytes size is 1000(about 1KB).
// 200 Pages, So we can add 200 * 170 = 34000 segments, if we give 40000 segments
// let get the first 34000 segments first, and then get 40000 segments again, there
// should be 6000 I/O.
TEST(M3V,RECORD_CACHE){
    std::cout << "Project root path is: " << PROJECT_ROOT_PATH << std::endl;
	VectorRecord record;
    // we can just do read operations from IndexPointerCache,
    // we will write kv directly into rocksdb.
	std::vector<uint32_t> offsets = {128*DIM_SIZE,128*DIM_SIZE,128*DIM_SIZE};
    IndexPointerLruCache cache_test(offsets,3);
	
    auto t0 = std::chrono::steady_clock::now();
	WriteIndexPointerKVs(40000,cache_test.GetDB());
    double duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    std::cout << "Time taken: " << duration << " milliseconds" << std::endl;
	ItemPointerData data;data.ip_blkid.bi_hi = 0;data.ip_blkid.bi_lo=0;data.ip_posid = 0;

	// ==============TEST1=======================
	double sum1 = 0;
	for(int i = 0;i < 34000;i++){
		data.ip_posid = i;
		auto t0 = std::chrono::steady_clock::now();
		VectorRecord res =  cache_test.Get(&data);
		sum1 += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
		std::string str(reinterpret_cast<const char*>(res.GetData()),res.GetSize());
		std::string expected = build_data_string(res.GetSize()/DIM_SIZE,i);
		if(str != expected){
			std::cout<<i<<" "<<str.size()<<" "<<expected.size()<<std::endl;
		}
		assert(str==expected);
		cache_test.UnPinItemPointer(&data);
	}
	assert(cache_test.GetIoTimes() == 34000);
	assert(cache_test.GetPinCounts() == 0);
	cache_test.ResetDirectIoTimes();
    std::cout << "TEST1 Time taken: " << sum1 << " nanoseconds" << std::endl;

	// ==============TEST2=======================
	double sum2 = 0;
	for(int i = 0;i < 30000;i++){
		data.ip_posid = i;
		auto t0 = std::chrono::steady_clock::now();
		VectorRecord res =  cache_test.Get(&data);
		sum2 += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
		std::string str(reinterpret_cast<const char*>(res.GetData()),res.GetSize());
		assert(str==build_data_string(res.GetSize()/DIM_SIZE,i));
		cache_test.UnPinItemPointer(&data);
	}
	assert(cache_test.GetIoTimes() == 0);
	assert(cache_test.GetPinCounts() == 0);
	cache_test.ResetDirectIoTimes();
    std::cout << "TEST2 Time taken: " << sum2 << " nanoseconds" << std::endl;

	// ==============TEST3=======================
	double sum3 = 0;
	for(int i = 0;i < 40000;i++){
		data.ip_posid = i;
		auto t0 = std::chrono::steady_clock::now();
		VectorRecord res =  cache_test.Get(&data);
		sum3 += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
		std::string str(reinterpret_cast<const char*>(res.GetData()),res.GetSize());
		assert(str==build_data_string(res.GetSize()/DIM_SIZE,i));
		cache_test.UnPinItemPointer(&data);
	}
	assert(cache_test.GetIoTimes() == 6000);
	assert(cache_test.GetPinCounts() == 0);
	cache_test.ResetDirectIoTimes();
    std::cout << "TEST3 Time taken: " << sum3 << " nanoseconds" << std::endl;

	// ==============TEST4=======================
	double sum4 = 0;
	for(int i = 0;i < 6000;i++){
		data.ip_posid = i;
		auto t0 = std::chrono::steady_clock::now();
		VectorRecord res =  cache_test.Get(&data);
		sum4 += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
		std::string str(reinterpret_cast<const char*>(res.GetData()),res.GetSize());
		assert(str==build_data_string(res.GetSize()/DIM_SIZE,i));
		cache_test.UnPinItemPointer(&data);
	}
	assert(cache_test.GetIoTimes() == 6000);
	assert(cache_test.GetPinCounts() == 0);
	cache_test.ResetDirectIoTimes();
    std::cout << "TEST4 Time taken: " << sum4 << " nanoseconds" << std::endl;
}

float L2Distance(float* vector_record1,float* vector_record2,int dims){
	float distance = 0.0;
	float diff;
	for(int i = 0; i < dims;i++){
		diff = vector_record1[i] - vector_record2[i];
		distance += diff * diff;
	}
	return sqrt((double)(distance));
}

float distanceVectorFunc(VectorRecord* record1,VectorRecord* record2){
	assert(record1!=nullptr);
	assert(record2!=nullptr);
	assert(record1->GetSize() == record2->GetSize());
	float* a = reinterpret_cast<float*>(record1->GetData());
	float* b = reinterpret_cast<float*>(record2->GetData()); 
	float dis = L2Distance(a,b,record1->GetSize()/DIM_SIZE);
	return dis;
}

std::string build_data_string2(const std::vector<int> &v){
	std::string res = "";
	for(int i = 0;i < v.size();i++){
		float t = v[i];
		const unsigned char* pBytes = reinterpret_cast<const unsigned char*>(&t);
		for (size_t i = 0; i < DIM_SIZE; ++i) {
			res += pBytes[i];
		}
	}
	auto size_ = res.size();
	return res;
}

void ReadUtil(std::string path,rocksdb::DB* db){
	ItemPointerData data_;ItemPointer pointer = &data_;pointer->ip_posid = 0;data_.ip_blkid.bi_hi = 0;data_.ip_blkid.bi_lo = 0;
	// 打开文件
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cerr << "Failed to open file\n";
        return;
    }

    std::vector<std::vector<int>> data; // 用来存储所有行的容器
    std::string line;

    // 逐行读取文件
    while (getline(file, line)) {
        std::vector<int> row; // 存储当前行的数据
        std::istringstream iss(line);
        int value;

        // 从行中读取每个整数
        while (iss >> value) {
            row.push_back(value);
        }

        // 将当前行添加到总数据中
        data.push_back(row);
    }

    // 关闭文件
    file.close();

    // 打印读取的数据
	int idx = 0;
    for (const auto& row : data) {
        auto res = build_data_string2(row);
		pointer->ip_posid = idx;
		db->Put(rocksdb::WriteOptions(),ItemPointerToString(*pointer),res);
		idx++;
    }
}

// Test Elkan_Kmeans, we use this to test performance
// and correctess. We use our partial expermental data
// to test.
// https://big-ann-benchmarks.com/neurips21.html
// 1. BIGANN 128 uint8
// 2. Facebook SimSearchNet++* 256 uint8
// 3. Yandex DEEP 96 float4
// for current implementation, we maybe waste memory,
// because we use float for any type even if the type 
// size is less than `size(float)`. But in fact, it shouldn't
// be the bottle-neck of our system.
// UnPin Unsafe, we don't test it here.
TEST(M3V,KMEANS_SPLIT){
    // 1. BIGANN
	// 2. Facebook SimSearchNet++*
	// 3. Yandex DEEP
	std::vector<VectorRecord> records;
	std::vector<uint32_t> offsets = {128*DIM_SIZE};
    IndexPointerLruCache cache_test(offsets,1);
	ItemPointerData data_;ItemPointer pointer = &data_;pointer->ip_posid = 0;data_.ip_blkid.bi_hi = 0;data_.ip_blkid.bi_lo = 0;
	ReadUtil("/home/jack/cpp_workspace/wrapdir/OneDb2/contrib/m3v/src/tests_data/bigann_vector_128_100M_1000.txt",cache_test.GetDB());
	// each time we prepare 1000 records and do split for it. we should make sure
	// we will always generate the same cluster (we use kmeans++)
	for(int i = 0;i < 1000;i++){
		pointer->ip_posid = i;
		auto record_pointer =  cache_test.Get(pointer);
		records.push_back(record_pointer);
	}

	ElkanKmeans elkan_kmeans(100,distanceVectorFunc,distanceRealVectorSumFunc,2,records,true,offsets);
	auto t0 = std::chrono::steady_clock::now();
	while(!elkan_kmeans.is_finished()){
		elkan_kmeans.iteration();
	}
    double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
    std::cout << "Elkan Kmeans++ Iterations Time cost: " << duration/1000000 << " milliseconds" << std::endl;
	auto records_ = elkan_kmeans.GetCenters();
	for(int i = 0;i < records_.size();i++){
		records_[i].DebugVectorRecord();
	}
	auto res1 = elkan_kmeans.GetAssigns();
	ElkanKmeans elkan_kmeans2(100,distanceVectorFunc,distanceRealVectorSumFunc,2,records,true,offsets);
	while(!elkan_kmeans2.is_finished()){
		elkan_kmeans2.iteration();
	}
	auto res2 = elkan_kmeans2.GetAssigns();
	assert(res1.size() == res2.size());
	std::unordered_set<bool> s;
	int cluster_1_size = 0;
	// we can pass, we will always get the same 
	for(int i = 0;i < res1.size();i++){
		s.insert(res1[i] == res2[i]);
		if(res1[i] == 0) cluster_1_size++;
	}
	std::cout<<"cluster 1 size: "<<cluster_1_size<<std::endl;
	assert(s.size() == 1);
	std::vector<VectorRecord> new_records;
	for(int i = 0;i < res1.size();i++){
		if(res1[i] == 0){
			new_records.push_back(records[i]);
		}
	}
	ElkanKmeans elkan_kmeans3(100,distanceVectorFunc,distanceRealVectorSumFunc,1,new_records,true,offsets);
	auto records3_ = elkan_kmeans3.GetCenters();
	records3_[0].DebugVectorRecord();
	// Test 1
	
	// Test 2

	// Test 3

	// Test 1,3

	// Test 2,3

	// Test 1,2
}

void ReadTwoVectorUtil(std::string path1,std::string path2,rocksdb::DB* db){
	ItemPointerData data_;ItemPointer pointer = &data_;pointer->ip_posid = 0;data_.ip_blkid.bi_hi = 0;data_.ip_blkid.bi_lo = 0;
	// open file1
	std::ifstream file1(path1);
    if (!file1.is_open()) {
        std::cerr << "Failed to open file\n";
        return;
    }
	// open file2
	std::ifstream file2(path2);
    if (!file2.is_open()) {
        std::cerr << "Failed to open file\n";
        return;
    }

	std::vector<std::vector<int>> data; // 用来存储所有行的容器
    std::string line1;
	std::string line2;
    // 逐行读取文件
    while (getline(file1, line1)&&getline(file2,line2)) {
		std::istringstream iss1(line1);
		std::istringstream iss2(line1);
        std::vector<int> row; // 存储当前行的数据
        int value;

        // 从行中读取每个整数
        while (iss1 >> value) {
            row.push_back(value);
        }
		while (iss2 >> value) {
            row.push_back(value);
        }
        // 将当前行添加到总数据中
        data.push_back(row);
    }
	file1.close();
	file2.close();
	int idx = 0;
    for (const auto& row : data) {
        auto res = build_data_string2(row);
		pointer->ip_posid = idx;
		db->Put(rocksdb::WriteOptions(),ItemPointerToString(*pointer),res);
		idx++;
    }
}

// Modify: We should choose pivot in every dimension standalone and then combine them.
// We should set a threshold to trigger the `AuxiliarySortPage` Index generation. The default could be
// 100 or less.
TEST(M3V,TestPageSortIndex){
	ItemPointerData data_;ItemPointer pointer = &data_;pointer->ip_posid = 0;data_.ip_blkid.bi_hi = 0;data_.ip_blkid.bi_lo = 0;
	std::vector<VectorRecord> records;
	// Test bigann_1000 and ssnpp_1000 composition
	std::vector<uint32_t> offsets = {128*DIM_SIZE,128*DIM_SIZE};
    IndexPointerLruCache cache_test(offsets,2);
	ReadTwoVectorUtil("/home/jack/cpp_workspace/wrapdir/OneDb2/contrib/m3v/src/tests_data/bigann_vector_128_100M_1000.txt",
	"/home/jack/cpp_workspace/wrapdir/OneDb2/contrib/m3v/src/tests_data/ssnpp_vector_256_100M_1000.txt",
	cache_test.GetDB());
	
	// up to now, we have added the two dimension vectors.
	// each time we prepare 1000 records and do split for it. we should make sure
	// we will always generate the same cluster (we use kmeans++)
	for(int i = 0;i < 1000;i++){
		pointer->ip_posid = i;
		auto record_pointer =  cache_test.Get(pointer);
		records.push_back(record_pointer);
	}
	ElkanKmeans elkan_kmeans(100,nullptr,distanceRealVectorSumFunc,2,records,true,offsets);
	// and then we should get each cluster center(we don't use the new generated two, we will try to find the one which is
	// closest to the elkan-kmeans++'s ones as new cluster centers)
	auto centers = elkan_kmeans.GetCenters();
	std::vector<VectorRecord> records1;std::vector<VectorRecord> records2;
	records1.reserve(1000);records2.reserve(1000);
	auto assigns = elkan_kmeans.GetAssigns();
	for(int i = 0;i < assigns.size();i++){
		if(assigns[i] == 0){
			records1.push_back(records[i]);
		}else{
			records2.push_back(records[i]);
		}
	}
	centers[0] = records2[0];
	// 1.build page sort index.
	AuxiliarySortPage pages;
	pages.SetPageSortIndexTidNums(0,1000); 
	// 1.1 Get Pivot Distance
	std::vector<PivotIndexPair> max_sorts1_;std::vector<PivotIndexPair> min_sorts1_;
	std::vector<PivotIndexPair> max_sorts2_;std::vector<PivotIndexPair> min_sorts2_;
	max_sorts1_.reserve(1000);min_sorts1_.reserve(1000);
	PivotIndexPair min_pair;PivotIndexPair max_pair;
	std::vector<uint32_t> offsets2;std::vector<float> distances2;
	distances2.resize(2);
	// std::cout<<"centers0 debug: ";centers[0].DebugVectorRecord();std::cout<<std::endl;
	// std::cout<<"records1_0 debug: ";records1[1].DebugVectorRecord();std::cout<<std::endl;
	// std::cout<<"records1_0 debug: ";records1[2].DebugVectorRecord();std::cout<<std::endl;
	VectorRecord query1 = records2[0]; // use query point which is not in the records1 
	VectorRecord query2 = records1[0]; // use query point which is not in the records2
	VectorRecord pivot1 = query1;
	VectorRecord pivot2 = query1;
	// cluster1 compute
	for(int idx1 = 0;idx1 < records1.size();idx1++){
		distanceRealVectorFunc(&pivot1,&records1[idx1],offsets,distances2);
		GetPivotIndexPair(distances2,min_pair,max_pair,idx1);
		max_sorts1_.push_back(max_pair);min_sorts1_.push_back(min_pair);
	}
	pages.SetPageSortIndexTidNums(1,max_sorts1_.size());
	pages.SetMaxSorts(1,max_sorts1_);pages.SetMinSorts(1,min_sorts1_);

	// cluster2 compute
	for(int idx2 = 0;idx2 < records2.size();idx2++){
		distanceRealVectorFunc(&pivot2,&records2[idx2],offsets,distances2);
		GetPivotIndexPair(distances2,min_pair,max_pair,idx2);
		max_sorts2_.push_back(max_pair);min_sorts2_.push_back(min_pair);
	}
	pages.SetPageSortIndexTidNums(2,max_sorts2_.size());
	pages.SetMaxSorts(2,max_sorts2_);pages.SetMinSorts(2,min_sorts2_);

	// now, let's monitor the sql, select * from t where 0.5 * d(v1,p) + 0.6 * (v2,p) < 3.3, we should promise the
	// ground-truth vector points should be in the result after min-max prune. also we can do a prune effect
	// check for new idea.
	std::unordered_set<int> ground_truth1;std::unordered_set<int> ground_truth2;
	std::vector<float> weights = {0.6,0.6};
	float radius = 700;

	std::ofstream file("/home/jack/cpp_workspace/wrapdir/OneDb2/contrib/m3v/src/tests/output.txt");
	if (!file.is_open()) {
		std::cerr << "can't open file" << std::endl;
	}
	file.precision(6);
	file << std::fixed;
	std::vector<float> values;
	// Test Spilt Page1
	for(int i = 0;i < records1.size();i++){
		float dist = distanceRealVectorSumFuncWithWeights(&query1,&records1[i],offsets,weights);
		distanceRealVectorSumFuncWithWeights(&query1,&records1[i],offsets,weights);
		values.push_back(dist);
		// std::cout<<dist<<std::endl;
		if(dist < radius){
			ground_truth1.insert(i);
		}
	}
	sort(values.begin(),values.end());
	for(auto value:values){
		file << value << std::endl;
	}
	file.close();

	double suma = 0;
	for(int i = 0; i < 100000;i++){
		auto ta = std::chrono::steady_clock::now();
		distanceRealVectorSumFuncWithWeights(&records[i%990],&query1,offsets,weights);
		suma += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - ta).count();
	}
	std::cout<<"suma: "<<suma<<" nanoseconds"<<std::endl;

	std::cout<<"ground_truth1 size: "<<ground_truth1.size()<<std::endl;
	auto t0 = std::chrono::steady_clock::now();
	float pivot_distance1 = distanceRealVectorSumFuncWithWeights(&centers[0],&query1,offsets,weights);
	auto valid_indexes1 =  pages.GetValidIndexes(1,radius,pivot_distance1,weights[0] + weights[1]);
	auto cost1 = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
	std::cout<<"cost1: "<<cost1<<" nanoseconds"<<std::endl;
	int res1 = 0;
	for(int i = 0;i < valid_indexes1.size();i++){
		if(ground_truth1.count(valid_indexes1[i])){
			res1++;
		}
	}
	std::cout<<"valid_indexes1 size: "<<valid_indexes1.size()<<std::endl;
	if(res1!=ground_truth1.size()){
		std::cout<<"res1: "<<res1<<" size1: "<<ground_truth1.size()<<std::endl;
	}
	assert(res1 == ground_truth1.size());
	// Test Split Page2
	for(int i = 0;i < records2.size();i++){
		float dist = distanceRealVectorSumFuncWithWeights(&query1,&records2[i],offsets,weights);
		// std::cout<<dist<<std::endl;
		if(dist < radius){
			ground_truth2.insert(i);
		} 
	}
	std::cout<<"ground_truth2 size: "<<ground_truth2.size()<<std::endl;
	auto t1 = std::chrono::steady_clock::now();
	float pivot_distance2 = distanceRealVectorSumFuncWithWeights(&pivot2,&query1,offsets,weights);
	auto valid_indexes2 = pages.GetValidIndexes(2,radius,pivot_distance2,weights[0] + weights[1]);
	auto cost2 = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t1).count();
	std::cout<<"cost2: "<<cost2<<" nanoseconds"<<std::endl;
	int res2 = 0;
	for(int i = 0;i < valid_indexes2.size();i++){
		if(ground_truth2.count(valid_indexes2[i])){
			res2++;
		}
	}
	std::cout<<"valid_indexes2 size: "<<valid_indexes2.size()<<std::endl;
	if(res2!=ground_truth2.size()){
		std::cout<<"res2: "<<res2<<" size2: "<<ground_truth2.size()<<std::endl;
	}
	assert(res2 == ground_truth2.size());
}

TEST(M3V,TestSerializeAndDeserialize){

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

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

extern "C"{
	#include "vector.h"
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
// size is lesson size(float). But in fact, it shouldn't
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

	ElkanKmeans elkan_kmeans(100,distanceVectorFunc,2,records,true);
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
	ElkanKmeans elkan_kmeans2(100,distanceVectorFunc,2,records,true);
	while(!elkan_kmeans2.is_finished()){
		elkan_kmeans2.iteration();
	}
	auto res2 = elkan_kmeans2.GetAssigns();
	assert(res1.size() == res2.size());
	std::unordered_set<bool> s;
	// we can pass, we will always get the same 
	for(int i = 0;i < res1.size();i++){
		s.insert(res1[i] == res2[i]);
	}
	assert(s.size() == 1);
	// Test 1
	
	// Test 2

	// Test 3

	// Test 1,3

	// Test 2,3

	// Test 1,2
}



int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
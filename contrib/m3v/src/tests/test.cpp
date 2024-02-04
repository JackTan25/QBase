#include <gtest/gtest.h>
#include "lru_index_pointer.h"
#include <sstream>
#include <iomanip>

TEST(Hack, AssertionT) {
    ASSERT_TRUE(true);
}

TEST(Hack, AssertionF) {
    ASSERT_FALSE(false);
}

TEST(M3V,RocksDB){
    rocksdb::DB* db;
    rocksdb::Options options;

    // 打开数据库
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, std::string(PROJECT_ROOT_PATH) + "/db", &db);
    
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
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

TEST(M3V,BuildDAtaString){

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
			std::cout<<str<<" "<<expected<<std::endl;
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

TEST(M3V,KMEANS_SPLIT){
    
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
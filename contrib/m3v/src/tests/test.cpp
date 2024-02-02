#include <gtest/gtest.h>
#include "rocksdb/db.h"

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
    rocksdb::Status status = rocksdb::DB::Open(options, "./db", &db);
    
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

TEST(M3V,RECORD_CACHE){

}

TEST(M3V,KMEANS_SPLIT){
    
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
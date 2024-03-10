#pragma once

#include <vector>
#include <fstream>
#include <iostream>
extern "C"
{
#include <postgres.h>
#include <catalog/pg_type_d.h>
#include <utils/array.h>

#include <commands/vacuum.h>
#include <miscadmin.h>
#include <utils/elog.h>
#include <utils/rel.h>
#include <utils/selfuncs.h>
// #include <tcop/utility.h>
#include <executor/spi.h>
#include <utils/builtins.h>
}

extern "C"
{
    PGDLLEXPORT PG_FUNCTION_INFO_V1(Print);
// PGDLLEXPORT PG_FUNCTION_INFO_V1(inference2);
#ifdef PG_MODULE_MAGIC
    // PG_MODULE_MAGIC;
#endif
}

Datum Print(PG_FUNCTION_ARGS)
{
    int sum_of_all = 0;
    std::vector<int> arr{1, 2, 3, 4, 5, 6, 7};
    for (auto &i : arr)
        sum_of_all += i;
    elog(INFO, "Please Help Me!!");
    PG_RETURN_FLOAT8(sum_of_all);
}

void SerializeVector(const std::vector<ItemPointerData>& data, const std::string& filename) {
    std::ofstream outFile(filename, std::ios::binary);
    if (!outFile) {
        std::cerr << "Cannot open the file for writing.\n";
        return;
    }

    for (const auto& item : data) {
        outFile.write((const char*)&item, sizeof(ItemPointerData));
    }

    outFile.close();
}

std::vector<ItemPointerData> DeserializeVector(const std::string& filename) {
    std::vector<ItemPointerData> data;
    std::ifstream inFile(filename, std::ios::binary);
    if (!inFile) {
        std::cerr << "Cannot open the file for reading.\n";
        return data; // 返回空向量
    }

    ItemPointerData item;
    while (inFile.read((char*)&item, sizeof(ItemPointerData))) {
        data.push_back(item);
    }

    inFile.close();
    return data;
}

// gcc -I$(pg_config --includedir-server) -shared -fPIC -o pg_exec.so pg_exec.c
// CREATE FUNCTION Print() RETURNS float8 AS 'util.so', 'Print' LANGUAGE C STRICT;

// 成功步骤:
// 1. g++ -I/data/include/postgresql/server -shared -fPIC -o util.so src/util.cpp
// 2. cp /home/jack/cpp_workspace/wrapdir/OneDb2/contrib/m3v/util.so /data/lib/postgresql/
// 3. CREATE FUNCTION Print() RETURNS float8 AS 'util.so', 'Print' LANGUAGE C STRICT;
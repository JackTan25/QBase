#pragma once

#include <vector>
#include <fstream>
#include <iostream>
#include "util.h"
#include "init.h"
#include "m3v.h"
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
    #include "storage/bufmgr.h"
}

extern "C"
{
    // PGDLLEXPORT PG_FUNCTION_INFO_V1(Print);
// PGDLLEXPORT PG_FUNCTION_INFO_V1(inference2);
#ifdef PG_MODULE_MAGIC
    // PG_MODULE_MAGIC;
#endif
}

std::string floatArrayToString(float* arr,int n) {
    std::ostringstream oss;
    oss << "[";

    for (size_t i = 0; i < n; ++i) {
        oss << arr[i];
        if (i != n - 1) {
            oss << ", ";
        }
    }

    oss << "]";
    return oss.str();
}

// Datum Print(PG_FUNCTION_ARGS)
// {
//     int sum_of_all = 0;
//     std::vector<int> arr{1, 2, 3, 4, 5, 6, 7};
//     for (auto &i : arr)
//         sum_of_all += i;
//     elog(INFO, "Please Help Me!!");
//     PG_RETURN_FLOAT8(sum_of_all);
// }

bool file_exists(const std::string& path) {
    std::ifstream file(path);
    return file.good();
}

OffsetNumber WriteA3vTupleToPage(Page page,Item item,Size size){
    Size free_size = PageGetFreeSpace(page);
    if(free_size < size){
        return InvalidOffsetNumber;
    }
    return PageAddItem(page,item, size, InvalidOffsetNumber, false, false);
}

PageId CombineGetPageId(uint16 bi_hi,uint16 bi_lo){
    PageId page_id = 0;
    page_id |= (bi_hi<<16);
    page_id |= (bi_lo);
    return page_id;
}

BlockIdData SplitPageId(uint32_t page_id){
    BlockIdData id;
    id.bi_hi = (0xFFFF<<16) & page_id;
    id.bi_lo = (0xFFFF) & page_id;
    return id;
}

std::string build_data_string(float* vec,int len){
	std::string res = "";
	for(int i = 0;i < len;i++){
		float t = vec[i];
		const unsigned char* pBytes = reinterpret_cast<const unsigned char*>(&t);
		for (size_t i = 0; i < FLOAT_SIZE; ++i) {
			res += pBytes[i];
		}
	}
	return res;
}

/*
 * New buffer
 */
Buffer m3vNewBuffer(Relation index, ForkNumber forkNum)
{
	Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}

void InsertNewQuery(IndexScanDesc scan,m3vScanOpaque so,int block_number){
    Buffer buf;
    Page page;
    int index_pages;
    // we need new a page
    if(block_number == INVALID_BLOCK_NUMBER){
        buf = m3vNewBuffer(scan->indexRelation, MAIN_FORKNUM);
        // because there is a meta page, we need to remove this one.
        page = BufferGetPage(buf);
        index_pages = RelationGetNumberOfBlocks(scan->indexRelation);
    }else{
        buf = ReadBuffer(scan->indexRelation,block_number);
        page = BufferGetPage(buf);
        index_pages = block_number;
        if(PageGetFreeSpace(page) < A3V_TUPLE_SZIE){
            ReleaseBuffer(buf);
            buf = m3vNewBuffer(scan->indexRelation, MAIN_FORKNUM);
            page = BufferGetPage(buf);
            index_pages++;
        }
    }
    std::vector<uint32_t> offsets;
    int size = 0;
    const std::vector<int> dimensions = init.GetDimensions(scan->indexRelation);
    for(int i = 0;i < so->columns;i++){
        size += dimensions[i] * sizeof(float);
        offsets.push_back(size);
    }
    so->cache = init.GetIndexCache(offsets, so->columns,std::string(RelationGetRelationName(scan->indexRelation)));
    rocksdb::DB* db = so->cache->GetDB();
    // add new root,and we need allocate it a new root query id
    auto a3vTuple = A3vTuple(0,so->tuple_nums-1,InvalidItemPointerData,InvalidItemPointerData,a3vAllocateQueryId(scan->indexRelation),INVALID_RADIUS);				
    OffsetNumber offset_number = WriteA3vTupleToPage(page,Item(&a3vTuple),A3V_TUPLE_SZIE);
    BlockIdData block_id = SplitPageId(index_pages);
    ItemPointerData new_item_pointer = {block_id,offset_number};
    so->root_tid = new_item_pointer;
    db->Put(rocksdb::WriteOptions(),ItemPointerToString(new_item_pointer),build_data_string(so->query_point,so->total_len));
    // insert into hnsw index.
    so->alg_hnsw->addPoint(so->query_point,a3vTuple.query_id);
    init.InsertNewTidForIndex(std::string(RelationGetRelationName(scan->indexRelation)),new_item_pointer);
    MarkBufferDirty(buf);
    ReleaseBuffer(buf);
}

bool A3vMemoryIndexType(Relation index){
    m3vOptions* opts =  (m3vOptions*)index->rd_options;
    if (!opts)
		return DEFAULT_INDEX_TYPE;
    return opts->memory_index;
}

float A3vCloseQueryThreshold(Relation index){
    m3vOptions* opts =  (m3vOptions*)index->rd_options;
    if (!opts)
		return CLOSE_QUERY_THRESHOLD;
    return opts->close_query_threshold;
}

std::string extract_btree_filter(const char* source_text){
    std::string sqlQuery = std::string(source_text);
    size_t wherePos = sqlQuery.find("where");
    size_t orderByPos = sqlQuery.find("order by");
    if (wherePos != std::string::npos && orderByPos != std::string::npos && orderByPos > wherePos) {
        size_t start = wherePos + 6;
        size_t length = orderByPos - start;
        std::string condition = sqlQuery.substr(start, length);
        elog(INFO,"Condition between WHERE and ORDER BY is: %s",condition.c_str());
        return condition;
    } else {
        elog(INFO,"Pure Vector Search");
        return "none filter";
    }
}

// gcc -I$(pg_config --includedir-server) -shared -fPIC -o pg_exec.so pg_exec.c
// CREATE FUNCTION Print() RETURNS float8 AS 'util.so', 'Print' LANGUAGE C STRICT;

// 成功步骤:
// 1. g++ -I/data/include/postgresql/server -shared -fPIC -o util.so src/util.cpp
// 2. cp /home/jack/cpp_workspace/wrapdir/OneDb2/contrib/m3v/util.so /data/lib/postgresql/
// 3. CREATE FUNCTION Print() RETURNS float8 AS 'util.so', 'Print' LANGUAGE C STRICT;

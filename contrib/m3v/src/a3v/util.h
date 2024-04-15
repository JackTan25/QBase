#pragma once

#ifdef __cplusplus
extern "C"
{
#endif
	// void Print();
#ifdef __cplusplus
}
#endif

#include <iostream>
#include <vector>
#include "m3v.h"
extern "C"
{
	#include <postgres.h>
	#include <utils/builtins.h>
	#include "storage/bufmgr.h"
}

static ItemPointerData InvalidItemPointerData = {{0,0},InvalidOffsetNumber};
using PII = std::pair<std::vector<float>,ItemPointerData>;
typedef uint32_t PageId;
#define FLOAT_SIZE sizeof(float)
template<class T>
std::vector<T> DeserializeVector(const std::string& filename);
template<class T>
void SerializeVector(const std::vector<T>& data, const std::string& filename);
bool file_exists(const std::string& path);
OffsetNumber WriteA3vTupleToPage(Page page,Item item,Size size);
PageId CombineGetPageId(uint16 bi_hi,uint16 bi_lo);
BlockIdData SplitPageId(uint32_t page_id);
int a3vAllocateQueryId(Relation index);
std::string build_data_string(float* vec,int len);
void InsertNewQuery(IndexScanDesc scan,m3vScanOpaque so,int block_number);
Buffer m3vNewBuffer(Relation index, ForkNumber forkNum);
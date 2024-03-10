#pragma once

#ifdef __cplusplus
extern "C"
{
#endif
	void Print();
#ifdef __cplusplus
}
#endif

#include <iostream>
#include <vector>
extern "C"
{
	#include <postgres.h>
	#include <utils/builtins.h>
	#include "storage/bufmgr.h"
}

std::vector<ItemPointerData> DeserializeVector(const std::string& filename);
void SerializeVector(const std::vector<ItemPointerData>& data, const std::string& filename);
#pragma once

#include<iostream>
#include "hnswlib.h"
#include<vector>
#include<queue>

extern "C"{
    #include "postgres.h"
    #include "storage/bufmgr.h"
}

const int terminate_multi_top_k = 50;
const int check_thresold = 1.27;
using ScorePair = std::pair<float,std::uint64_t>;
std::uint64_t GetNumberByItemPointerData(ItemPointer tid);
ItemPointerData GetItemPointerDataByNumber(hnswlib::labeltype label);
class MultiColumnHnsw{
	public:
		float l2_distance(std::vector<float> &data1,const float* query_point);
		float RankScore(hnswlib::labeltype label);
		MultiColumnHnsw(std::vector<std::shared_ptr<hnswlib::HierarchicalNSW<float>>> &hnsws_,std::vector<float*> &query_points_,int k_):hnsws(hnsws_),query_points(query_points_),k(k_){
			// get result_iterator for now.
			for(int i = 0;i < hnsws.size();i++){
				hnsws_iterators.push_back(std::make_shared<hnswlib::ResultIterator<float>>(hnsws[i].get(), (const void*)query_points[i]));
			}
		}
		bool GetNext();
		bool GetSingleNext();
		std::vector<std::shared_ptr<hnswlib::HierarchicalNSW<float>>> hnsws;
		// std::vector<std::shared_ptr<hnswlib::ResultIterator<float>>> hnsws_iterators;
		std::vector<std::shared_ptr<hnswlib::ResultIterator<float>>> hnsws_iterators;
		std::vector<float*> query_points;
		std::vector<float> weights;
		// <score,tid_number> s
		std::priority_queue<ScorePair> result_pq; // MaxHeap
		std::priority_queue<ScorePair,std::vector<ScorePair>,std::greater<ScorePair>> proc_pq; // MinHeap
		std::unordered_set<std::uint64_t> seen_tid;
		ItemPointerData result_tid;
		float distance;
		int k;
};

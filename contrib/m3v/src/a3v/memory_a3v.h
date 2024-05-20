#pragma once

#include<vector>
#include<unordered_map>
#include "simd_func.h"
#include "util.h"
// for guide_pq: <distance,id>, id stands for a node in `index`.
// for result_pq: <distance,id>, id stands for the index in `data_points`

const int CRACKTHRESHOLD = 128;
const int ReserveRange = 100;
const int A3V_HINT_QUERY_RECORDS = 7;
const float alpha_amplication = 1.13;
const float sigma = 0.8;
#define Min(x,y) ((x) < (y) ? (x) : (y))
#define Max(x,y) ((x) > (y) ? (x) : (y))
#define INITIAL_NODES 300 // hack value
class A3vNode{
	public:
		A3vNode();
		A3vNode(int left_node_,int right_node_,int start_,int end_,float radius_,std::vector<float>& query_,int id_);
		A3vNode(int left_node_,int right_node_,int start_,int end_,float radius_,int id_);
		void SetQuery(std::vector<float>& query_);
	public:
		int left_node; // left node
		int right_node;// right node
		int start,end; // the range of this node.
		float radius; // the range radius of this point
		std::vector<float> query;
		int id; // the id of this node in Global Nodes in MemoryA3v
};

class MemoryA3v{
	public:
		MemoryA3v(const std::vector<int>& dims,const std::vector<PII>& data_points_);

		// result_pq should be empty initially.
		void KnnCrackSearch(m3vScanOpaque so, float* query,int k, std::priority_queue<PQNode>& result_pq /**Max heap**/,const std::vector<int> &dimensions,float last_topk_mean);

		void RangeCrackSearch(m3vScanOpaque so, float* query,float radius,std::vector<int>& result_ids,const std::vector<int> &dimensions);

	public:
		void RangeCrackSearchAuxiliary(m3vScanOpaque so,int root_idx, float* query,float radius,std::vector<int>& result_ids,const std::vector<int> &dimensions,int dim);
				
		int CrackInTwo(int start_,int end_,float epsilon);

		int CrackInTwoMedicore(int start_,int end_,float radius,float newE,float* query,std::vector<int>& result_ids,float& maxDistance);

		std::vector<float> distances_caching; // error 2024.4.14
		std::vector<A3vNode> index;
		// every index will share this one.
		const std::vector<PII>& data_points;
		std::vector<int> swap_indexes;
		std::vector<int> dims_;
		std::atomic<size_t> query_records{0};
		float last_top_k_mean{0.0};
		std::mutex lock;
		std::vector<std::vector<float>> query_queue; 
};

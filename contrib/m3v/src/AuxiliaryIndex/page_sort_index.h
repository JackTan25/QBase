#pragma once
#include<iostream>
#include<vector>
#include<string.h>
#define PAGE_NUM 500
#define SORT_ENTRY_NUM 1024
#define INVALID_INDEX 0xFFFF
#define INVALID_BLOCK_NUMBER -1
using PivotIndexPair = std::pair<float,int>;
// Attentation:We Should Avoid SelfZero Trap.
class ValidBitmap{
	public:
		ValidBitmap(uint32_t tid_nums):tid_nums_(tid_nums){
			valid_indexes.reserve(tid_nums);
		}

		ValidBitmap(){
			valid_indexes.reserve(SORT_ENTRY_NUM);
		}

		void Reset();

		void SetIndexValid1(uint32_t idx);
		void SetIndexValid2(uint32_t idx);
		const std::vector<uint16_t>& GetValidIndexes(int tid_nums);
		// const std::vector<uint16_t>& MergeValidIndexes(const ValidBitmap& mp2,int tid_nums);

	private:
		uint8_t masks1[SORT_ENTRY_NUM/8];
		uint8_t masks2[SORT_ENTRY_NUM/8];
		uint32_t tid_nums_;
		std::vector<uint16_t> valid_indexes;
};

// Page Sort Index is used to store the m3v dimension distance sort.
// Uniformlly, the page zero is always a metapage. we think there should
// be at most 500 pages.
class PagePivotSortIndex{
	public:
		uint16_t GetMinValidIndexStart(const float& radius,const float& dist,const float& weight_sum);
		uint16_t GetMaxValidIndexStart(const float& radius,const float& dist,const float& weight_sum);
		void ResetBitmap();
		void SetTidNums(int tid_nums);
		void SetMinIndexValid(uint32_t idx);
		void SetMaxIndexValid(uint32_t idx);
		void SetMaxSorts(const std::vector<PivotIndexPair>& max_sorts);
		void SetMinSorts(const std::vector<PivotIndexPair>& min_sorts);
		int GetMinIndexAt(int idx){
			return min_sorts[idx].second;
		}
		int GetMaxIndexAt(int idx){
			return max_sorts[idx].second;
		}
		const std::vector<uint16_t>& GetValidIndexes(int tid_nums);
		uint16_t GetTidNums(){
			return tid_nums_;
		}
    private:
		// remember:
		// 	 we can use kmeans-1 after split use kmeans-2, so we can reuse real data point.
		// summary: query_point q,local_pivot l
		// x > d_1 - r  > d
		// 0. dist0_0 dist0_1 dist0_2 dist0_3 ...
		// 1. dist1_0 dist1_1 dist1_2 dist1_3 ...
		// 2. dist2_0 dist2_1 dist2_2 dist2_3 ...
		// ........
		// in the origin m3-tree paper, it uses the vertical max/min distance of each dimension with wj to get upper/lower bound.
		// in fact, we can do another way, that's flat way. we track min/max distance of every entry' all dimensions and then give
		// a sort. But we can't get the query node, so we use local pivot point(p) and then use `Triangle inequality` to avoit distance-
		// computations. The lemma is below:(whatever knn or range_query, we will maintain a radius r, for knn, it's k-th distance).
		// 	 d_1 = d(q,l), if d_1 - r > max_i * max_weight_sum (|| r - d_1 > max_i * max_weight_sum no,doubt), so we can prune all the entries 
		// before i-th entry(include i-th entry), we can choose max_weight_sum as dimension_num
		//				   if d_1 + r < min_i * min_weight_sum, so we can prune all the entries after i-th(include i-th) entry.(Attentation: but we need sum(w_j) >= 1.0)
		// for min prune, we can give the condition-limitation for the min sum(wj) in sql. we can choose min_weight_sum as 1.0
		// in fact, we can use max_weight_sum and min_weight_sum as accurate sum(weights).
		// we should combine the 1.0 and ru to use above lemmas.
		// The min distance and max distance are the min/max of the same dimension.
		// 
		// the usage principle:
		// 	  1. we can calculate the distance d1 of (query_point q,local_pivot l)
		// it means d1 = d(q,l)
		//	  2. we can perform prune by 
		// min_sort_index
		PivotIndexPair min_sorts[SORT_ENTRY_NUM];
		// max_sort_index
		PivotIndexPair max_sorts[SORT_ENTRY_NUM];
		// min sort array index
		// uint16_t min_indexes[SORT_ENTRY_NUM];
		// max sort array index
		// uint16_t max_indexes[SORT_ENTRY_NUM];
		// ValidBitmap max_valid_bitmap;
		int tid_nums_ = INVALID_INDEX;
		ValidBitmap valid_bitmap;
};

class AuxiliarySortPage{
	public:
		void SetDimensions(uint32_t dimension_nums){
			dimension_nums_ = dimension_nums;
		}
		void SetPageSortIndexTidNums(int page_idx,int tid_nums);
		const std::vector<uint16_t>& GetValidIndexes(int page_index,float radius,float dist,float weight_sum);
		void SetMaxSorts(int page_index,const std::vector<PivotIndexPair>& max_sorts);
		void SetMinSorts(int page_index,const std::vector<PivotIndexPair>& min_sorts);
	private:
		// do binary_search for min or max arraies
		uint16_t GetMinValidIndexStart(PagePivotSortIndex& page_pivot_sort,const float& radius,const float& dist,const float& weight_sum) const;

		uint16_t GetMaxValidIndexStart(PagePivotSortIndex& page_pivot_sort,const float& radius,const float& dist,const float& weight_sum) const;
	private:
		uint32_t dimension_nums_;
		PagePivotSortIndex sorts[PAGE_NUM];
};

#include<iostream>

#define PAGE_NUM 500
#define SORT_ENTRY_NUM 1024
// Page Sort Index is used to store the m3v dimension distance sort.
// Uniformlly, the page zero is always a metapage. we think there should
// be at most 500 pages.
class PagePivotSortIndex{
	public:

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
		// 	 d_1 = d(q,l), if d_1 - r > max_i * dim_nums (|| r - d_1 > max_i * dim_nums no,doubt), so we can prune all the entries 
		// before i-th entry(include i-th entry)
		//				   if d_1 + r < min_i (there is no dim_nums), so we can prune all the entries after i-th entry.(Attentation: but we need sum(w_j) >= 1.0)
		// for min prune, we can give the condition-limitation for the min sum(wi) in sql.
		// we should combine the 1.0 and ru to use above lemmas.
		// The min distance and max distance are the min/max of the same dimension.
		// 
		// the usage principle:
		// 	  1. we can calculate the distance d1 of (query_point q,local_pivot l)
		// it means d1 = d(q,l)
		//	  2. we can perform prune by 
		// min_sort_index
		float min_sorts[SORT_ENTRY_NUM];
		// max_sort_index
		float max_sorts[SORT_ENTRY_NUM];
		// min sort array index
		uint16_t min_indexes[SORT_ENTRY_NUM];
		// max sort array index
		uint16_t max_indexes[SORT_ENTRY_NUM];
};

class SortTable{
	public:
		SortTable(uint64_t tid_nums):tid_nums_(tid_nums){}
	private:
		uint64_t tid_nums_;
		PagePivotSortIndex sorts[PAGE_NUM];
};
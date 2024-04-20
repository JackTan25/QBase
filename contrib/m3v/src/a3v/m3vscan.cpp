#pragma once

#include "a3v/m3v.h"
#include "hnswlib.h"
#include "init.h"
#include <tuple>
#include "memory_a3v.h"
extern "C"{
	#include "postgres.h"
	#include "access/relscan.h"
	#include "pgstat.h"
	#include "storage/bufmgr.h"
	#include "storage/lmgr.h"
	#include "utils/memutils.h"
	#include "utils/float.h"
}

#define MAX_KNN_DISTANCE get_float8_infinity()
#define KNN_QUERY(scan) (scan->orderByData != NULL && ((scan->orderByData->sk_flags & SK_ISNULL) == false))
#define RANGE_QUERY(scan) (scan->keyData != NULL && ((scan->keyData->sk_flags & SK_ISNULL) == false))
#define CRACKTHRESHOLD 128

// https://citeseerx.ist.psu.edu/viewdoc/download;jsessionid=AB48C5606D99B76204CD6A51067CFB7F?doi=10.1.1.75.5014&rep=rep1&type=pdf
float8 getKDistance(pairingheap *heap, int k, int nn_size);
/**
 * postgres perf tutorial: https://www.youtube.com/watch?v=HghP4D72Noc
 */

/**
 * 1. page: current search page
 * 2. q: target vector
 * 3. k: order by a <=> q limit k;limit push down
 * 4. distance: distance of q and parent_page_entry
 *
 * For now, this is a bad implementation, we should add node when visiting inner node into nn, and then when we use this func,
 * remove the correlated node in nn. But this is a tricky implementation, so hold on for now.
 * q should be a vector**, it's a two dimension pointer array
 * 
 * we need two pq to do knn search.
 * 1. guide_pq. small heap
 * 2. result_pq. big heap
 */
void getKNNRecurse(Relation index, BlockNumber blkno, Datum q, int k, float8 distance, pairingheap *p, pairingheap *nn, FmgrInfo *procinfo, Oid collation, int *nn_size,int columns)
{
	// elog(INFO, "get knn blkno: %d", blkno);
	Buffer buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	Page page = BufferGetPage(buf);
	OffsetNumber offsets = PageGetMaxOffsetNumber(page);
	// InternalPage
	if (PageType(m3vPageGetOpaque(page)->type) == M3V_INNER_PAGE_TYPE)
	{
		bool root_flag = is_root(m3vPageGetOpaque(page)->type);
		for (OffsetNumber offset = FirstOffsetNumber; offset <= offsets; offset = OffsetNumberNext(offset))
		{
			m3vElementTuple etup = (m3vElementTuple)PageGetItem(page, PageGetItemId(page, offset));
			if (!root_flag && ((abs(etup->distance_to_parent - distance)) - etup->radius <= getKDistance(nn, k, *nn_size)) || root_flag)
			{
				float8 target_to_parent_dist = GetPointerDistances(etup->vecs, reinterpret_cast<Vector**>(DatumGetPointer(q)), procinfo, collation,columns);
				float8 estimated_dist = max((target_to_parent_dist - etup->radius), 0);
				if (estimated_dist <= getKDistance(nn, k, *nn_size))
				{
					m3vKNNCandidate *candidate = static_cast<m3vKNNCandidate*>(palloc0(sizeof(m3vKNNCandidate)));
					// tid is invalid now.
					candidate->son_blkno = etup->son_page;
					candidate->distance = estimated_dist;
					candidate->target_parent_distance = target_to_parent_dist;
					// min heap
					pairingheap_add(p,reinterpret_cast<pairingheap_node*>(Createm3vPairingKNNNode(candidate)));
				}

				if (target_to_parent_dist + etup->radius <= getKDistance(nn, k, *nn_size))
				{
					// NN_Update task
					m3vKNNCandidate *candidate_nn = static_cast<m3vKNNCandidate*>(palloc0(sizeof(m3vKNNCandidate)));
					// not leaf, don't update tid
					candidate_nn->distance = target_to_parent_dist + etup->radius;
					candidate_nn->tid = NULL;
					// elog(INFO, "KNN Internal Distance: %f, addr: %p", candidate_nn->distance, candidate_nn->tid);
					// PrintVector("and again knn vec: ", &etup->vec);
					// max heap
					// pairingheap_add(nn, Createm3vPairingKNNNode(candidate_nn));
					// m3vPairingKNNNode *n = (m3vPairingKNNNode *)pairingheap_first(nn);
					// *nn_size = *nn_size + 1;
					// while (*nn_size > k)
					// {
					// 	pairingheap_remove_first(nn);
					// 	*nn_size = *nn_size - 1;
					// }
				}
			}
		}
	}
	else
	{
		// LeafPage
		for (OffsetNumber offset = FirstOffsetNumber; offset <= offsets; offset = OffsetNumberNext(offset))
		{
			m3vElementLeafTuple etup = (m3vElementLeafTuple)PageGetItem(page, PageGetItemId(page, offset));
			if ((abs(etup->distance_to_parent - distance)) <= getKDistance(nn, k, *nn_size))
			{
				float8 dist = GetPointerDistances(etup->vecs,reinterpret_cast<Vector**>(q), procinfo, collation,columns);
				if (dist <= getKDistance(nn, k, *nn_size))
				{
					// NN_Update task
					m3vKNNCandidate *candidate_nn = static_cast<m3vKNNCandidate*>(palloc0(sizeof(m3vKNNCandidate)));
					// not leaf, don't update tid
					// this is accurate distance
					candidate_nn->distance = dist;
					candidate_nn->tid = &etup->data_tid;
					// elog(INFO, "KNN Leaf Distance: %f, addr: %p", candidate_nn->distance, candidate_nn->tid);
					// max heap
					pairingheap_add(nn, reinterpret_cast<pairingheap_node*>(Createm3vPairingKNNNode(candidate_nn)));
					*nn_size = *nn_size + 1;
					while (*nn_size > k)
					{
						pairingheap_remove_first(nn);
						*nn_size = *nn_size - 1;
					}
				}
			}
		}
	}
	UnlockReleaseBuffer(buf);
}

float8 getKDistance(pairingheap *nn, int k, int nn_size)
{
	if (pairingheap_is_empty(nn) || nn_size < k)
	{
		return MAX_KNN_DISTANCE;
	}
	else
	{
		m3vPairingKNNNode *n = (m3vPairingKNNNode *)pairingheap_first(nn);
		return ((m3vPairingKNNNode *)pairingheap_first(nn))->inner->distance;
	}
}

/**
 * KNN Query
 */
static List *
GetKNNScanItems(IndexScanDesc scan, Datum q)
{
	m3vScanOpaque so = (m3vScanOpaque)scan->opaque;
	Relation index = scan->indexRelation;
	FmgrInfo *procinfo = so->procinfo;
	Oid collation = so->collation;
	List *w = NIL;
	m3vMetaPage metap;
	m3vElementTuple etup;
	Buffer buf;
	OffsetNumber offsets;
	Page page;
	/* Get MetaPageData */
	m3vMetaPageData meta = m3vGetMetaPageInfo(index);
	metap = &meta;
	int columns = metap->columns;
	BlockNumber root_block = metap->root;
	// limit push down
	int limits = scan->orderByData->KNNValues;
	if (limits <= 0)
	{
		// DebugEntirem3vTree(m3vGetMetaPageInfo(index).root, index, 0);
		elog(ERROR, "knn query should specify a valid k value");
	}
	// /* Just SImple Test Read Tuple Data */
	// buf = ReadBuffer(index, root_block);
	// LockBuffer(buf, BUFFER_LOCK_SHARE);
	// page = BufferGetPage(buf);
	// etup = (m3vElementTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
	// PrintVector("knn scan ", &etup->vec);
	// w = lappend(w, etup);
	// UnlockReleaseBuffer(buf);
	// DebugEntirem3vTree(metap->root, index, 0);

	/**
	 *	Olc Algorithm: Read And Release
	 */
	// min heap
	pairingheap *p = pairingheap_allocate(CompareKNNCandidatesMinHeap, NULL);
	// max heap (use nn to return)
	pairingheap *nn = pairingheap_allocate(CompareKNNCandidates, NULL);
	m3vKNNCandidate *candidate = static_cast<m3vKNNCandidate*>(palloc0(sizeof(m3vKNNCandidate)));
	// if son_blkno is InvalidBlockNumber, so it's a leaf
	candidate->son_blkno = root_block;
	int nn_size = 0;
	// DebugEntirem3vTree(root_block, index, 0);
	pairingheap_add(p, reinterpret_cast<pairingheap_node*>(Createm3vPairingKNNNode(candidate)));
	while (!pairingheap_is_empty(p))
	{
		m3vPairingKNNNode *node = (m3vPairingKNNNode *)pairingheap_remove_first(p);
		// prune data
		if (node->inner->distance > getKDistance(nn, limits, nn_size))
		{
			continue;
		}
		getKNNRecurse(index, node->inner->son_blkno, q, limits, node->inner->target_parent_distance, p, nn, procinfo, collation, &nn_size,columns);
	}

	while (!pairingheap_is_empty(nn))
	{
		m3vPairingKNNNode *node = (m3vPairingKNNNode *)pairingheap_remove_first(nn);
		w = lappend(w, node->inner->tid);
	}
	return w;
}

void range_query(Relation index, Datum q, List **w, BlockNumber blkno, float8 radius, float8 distance, FmgrInfo *procinfo, Oid collation,int columns)
{
	Buffer buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	Page page = BufferGetPage(buf);
	m3vPageOpaque opaque = m3vPageGetOpaque(page);
	OffsetNumber offsets = PageGetMaxOffsetNumber(page);
	if (PageType(opaque->type) == M3V_LEAF_PAGE_TYPE)
	{
		for (OffsetNumber offset = FirstOffsetNumber; offset <= offsets; offset = OffsetNumberNext(offset))
		{
			m3vElementLeafTuple etup = (m3vElementLeafTuple)PageGetItem(page, PageGetItemId(page, offset));
			if (abs(etup->distance_to_parent - distance) <= radius)
			{
				float8 dist = GetPointerDistances(etup->vecs, reinterpret_cast<Vector**>(q), procinfo, collation,columns);
				if (dist <= radius)
				{
					*w = lappend(*w, &etup->data_tid);
				}
			}
		}
	}
	else
	{
		if (is_root(opaque->type))
		{
			for (OffsetNumber offset = FirstOffsetNumber; offset <= offsets; offset = OffsetNumberNext(offset))
			{
				m3vElementTuple etup = (m3vElementTuple)PageGetItem(page, PageGetItemId(page, offset));
				float8 dist = GetPointerDistances(etup->vecs, reinterpret_cast<Vector**>(q), procinfo, collation,columns);
				if (etup->radius + radius >= dist)
				{
					range_query(index, q, w, etup->son_page, radius, dist, procinfo, collation,columns);
				}
			}
		}
		else
		{
			for (OffsetNumber offset = FirstOffsetNumber; offset <= offsets; offset = OffsetNumberNext(offset))
			{
				m3vElementTuple etup = (m3vElementTuple)PageGetItem(page, PageGetItemId(page, offset));
				PrintVectors("range scan vector: ", etup->vecs,columns);
				if (abs(etup->distance_to_parent - distance) <= radius + etup->radius)
				{
					// bug !!!!
					float8 dist = GetDistance(reinterpret_cast<Datum>(etup->vecs), q, procinfo, collation);
					if (dist <= radius + etup->radius)
					{
						range_query(index, q, w, etup->son_page, radius, dist, procinfo, collation,columns);
					}
				}
			}
		}
	}
	UnlockReleaseBuffer(buf);
}

/**
 * Range Query
 */
static List *
GetRangeScanItems(IndexScanDesc scan, Datum q, float8 radius,int columns)
{
	/*
	 * range query 查询算法流程:
	 *	从n出发找到所有距离targe小于r的点
	 * 	RangeQuery(Object n,Object target,double r,double target_parent_distance)
	 *   1. n如果是叶子节点
	 *		遍历每一个entry
	 *		if |distance_p(entry) - target_parent_distance| <= r
	 *			if distance(entry,target) <= r
	 *				add entry to result.
	 *	2. n如果是内部节点
	 *		2.1 root节点
	 *			遍历每一个entry
	 *			if R(entry) + r >= dist(entry,targe)
	 *				RangeQuery(entry.son,target,r,distance(entry,target));
	 *		2.2 非root节点
	 *			遍历每一个entry
	 *			if |distance_p(entry) - target_parent_distance| <= r + R(entry)
	 *				if distance(entry,target) <= R(entry) + r
	 *					RangeQuery(entry.son,target,r,distance(entry,target));
	 */
	List *w = NIL;
	m3vScanOpaque so = (m3vScanOpaque)scan->opaque;
	/* Get MetaPageData */
	Relation index = scan->indexRelation;
	m3vMetaPageData meta = m3vGetMetaPageInfo(index);
	m3vMetaPage metap = &meta;
	BlockNumber root_block = metap->root;
	FmgrInfo *procinfo = so->procinfo;
	Oid collation = so->collation;
	range_query(index, q, &w, root_block, radius, 0, procinfo, collation,metap->columns);
	return w;
}

static Datum
GetScanValue2(IndexScanDesc scan,float8 * weights,bool is_knn,int nums){

}

/*
 * Get scan value, value should be a Vector** 
 */
static Datum
GetScanValue(IndexScanDesc scan,Vector** values,float8 * weights,bool is_knn,int nums)
{
	// every time we need to query the nearest query point root, so we can get a3v tree.
	m3vScanOpaque so = (m3vScanOpaque)scan->opaque;
	// get multi vector columns
	if(is_knn){
		// scan->numberOfOrderBys == 1, it means it's a single metric query.
		for(int i = 0;i < scan->numberOfOrderBys; i++){
			weights[i] = DatumGetFloat8(scan->orderByData[i].w);
			values[i] = DatumGetVector(scan->orderByData[i].sk_argument);
			elog(INFO,"w: %lf",weights[i]);
			PrintVector("vector knn: ",values[i]);
		}
	}else{
		for(int i = 0;i < scan->numberOfKeys; i++){
			weights[i] = DatumGetFloat8(scan->keyData[i].w);
			values[i] = DatumGetVector(scan->keyData[0].sk_argument);
			elog(INFO,"w: %lf",DatumGetFloat8(weights[i]));
			PrintVector("vector range query: ",values[i]);
		}
		elog(INFO,"radius: %lf",DatumGetFloat8(scan->keyData[0].query));
	}

	// if (is_knn&&scan->orderByData->sk_flags & SK_ISNULL)
	// 	value = PointerGetDatum(InitVector(GetDimensions(scan->indexRelation)));
	// else
	// {
	// 	no need to norm.
	// 	value = scan->orderByData->sk_argument;
	// 	// PrintVector("norm vector: ", DatumGetVector(value));
	// 	/* Value should not be compressed or toasted */
	// 	Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
	// 	Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

	// 	/* Fine if normalization fails */
	// 	if (so->normprocinfo != NULL)
	// 		m3vNormValue(so->normprocinfo, so->collation, &value, NULL);
	// }

	return PointerGetDatum(values);
}

/*
 * Get dimensions from metapage
 */
int GetDimensions(Relation index) {}

/*
 * Prepare for an index scan
 */
IndexScanDesc
m3vbeginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	m3vScanOpaque so;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	so = (m3vScanOpaque)palloc(sizeof(m3vScanOpaqueData));
	if(KNN_QUERY(scan)){
		so->result_ids = new std::vector<int>();
	}
	so->first = true;
	so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "M3v scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);
	m3vMetaPageData meta = m3vGetMetaPageInfo(index);
	m3vMetaPage metap = &meta;
	int columns = metap->columns;
	/* Set support functions */
	so->procinfo = index_getprocinfo(index, 1, M3V_DISTANCE_PROC);
	so->normprocinfo = m3vOptionalProcInfo(index, M3V_NORM_PROC);
	so->collation = index->rd_indcollation[0];
	so->columns = columns;
	scan->opaque = so;

	/*
	 * Get a shared lock. This allows vacuum to ensure no in-flight scans
	 * before marking tuples as deleted.
	 */
	LockPage(scan->indexRelation, M3V_SCAN_LOCK, ShareLock);

	return scan;
}

/*
 * End a scan and release resources
 */
void m3vendscan(IndexScanDesc scan)
{
	m3vScanOpaque so = (m3vScanOpaque)scan->opaque;

	/* Release shared lock */
	UnlockPage(scan->indexRelation, M3V_SCAN_LOCK, ShareLock);
	if(KNN_QUERY(scan)){
		delete so->result_ids;
	}
	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}

std::string build_hnsw_index_file_path(Relation index){
	return std::string(PROJECT_ROOT_PATH) + "/" + std::string(RelationGetRelationName(index)) + "_hnsw.bin";
}

std::string build_memory_index_points_file_path(Relation index){
	std::string path = std::string(PROJECT_ROOT_PATH) + "/" + std::string(RelationGetRelationName(index)) + "_memory_points.bin";
	return path;
}

// store query ids std::vector. The ids should be tids to specify the entry position.
std::string build_a3v_index_forest_query_ids_file_path(Relation index){
	return std::string(PROJECT_ROOT_PATH) + "/" + std::string(RelationGetRelationName(index)) + "_a3v_forest_root_ids.bin";
}

/*
 * Fetch the next tuple in the given scan
 * https://www.postgresql.org/docs/current/parallel-plans.html#PARALLEL-SCANS
 */
/**
 * for knn, we need two priority_queue.
 * guide_pq: small heap
 * result: big heap
*/
void do_knn_search_a3v(m3vScanOpaque scan, Relation index,m3vScanOpaque result,ItemPointerData root_tid,std::vector<float> weights,float* vectors,std::vector<int> offsets,int k,std::vector<ItemPointerData> &p){
	
}

void do_range_search_a3v(m3vScanOpaque so,Relation index,m3vScanOpaque result,ItemPointerData root_tid,std::vector<float> weights,float* vectors,std::vector<int> offsets,float radius,std::vector<ItemPointerData> &p){
	
}

// multi-vector search:
// 1. single vector search, we should drop 
// 2. multi vector search, we should do similar search (do w1.0 search with crack and then do search without crack)
// todo!: multi-bacth similar query requests.
// todo!: planner prefilter and range filter.
bool MemoryA3vIndexGetTuple(IndexScanDesc scan,ItemPointerData& result_tid){
	m3vScanOpaque so = (m3vScanOpaque)(scan->opaque);
	Relation index = scan->indexRelation;
	Vector* query_point;
	if(so->first){
		so->result_idx = 0;
		so->result_ids->clear();
		so->data_points = memory_init.GetDataPointsPointer(index);
		// 1. get query point
		const std::vector<int> dimensions =  memory_init.GetDimensions(scan->indexRelation);
		int sums = 0,offset = 0;
		for(int i = 0;i < dimensions.size();i++){
			sums += dimensions[i];
		}
		float query[sums];
		for(int i = 0;i < dimensions.size();i++){
			if(KNN_QUERY(scan)){
				so->weights[i] = DatumGetFloat8(scan->orderByData[i].w);
				query_point = DatumGetVector(scan->orderByData[i].sk_argument);
			}else{
				so->weights[i] = DatumGetFloat8(scan->keyData[i].w);
				query_point = DatumGetVector(scan->keyData[i].sk_argument);
			}
			memcpy(query + offset,query_point->x,sizeof(float) * dimensions[i]);
			offset += dimensions[i];
		}
		// get memory_index from memory init.
		std::shared_ptr<MemoryA3v> a3v_index = memory_init.GetMultiVectorMemoryIndex(index,dimensions,query);
		// knn search
		if(KNN_QUERY(scan)){
			std::priority_queue<PQNode> result_pq;
			a3v_index->KnnCrackSearch(so,query,scan->orderByData->KNNValues,result_pq,dimensions);
			while(!result_pq.empty()){
				so->result_ids->push_back(result_pq.top().second);result_pq.pop();
			}
		}else{
			float8 radius = DatumGetFloat8(scan->keyData->sk_argument);
			a3v_index->RangeCrackSearch(so,query,radius,*so->result_ids,dimensions);
			// after search, we need to sort for tids, this is used to improve cache hits.
			sort(so->result_ids->begin(),so->result_ids->end());
		}
		so->first = false;
	}
	if(so->result_idx < so->result_ids->size()){
		int idx = (*so->result_ids)[so->result_ids->size() - 1 - so->result_idx++];
		result_tid = (*so->data_points)[idx].second;
		return true;
	}
	return false;
}

/**
 * IndexPages Outline like below:
 * |--------------------------|
 * |		MetaPage		  |
 * |--------------------------|
 * |	   IndexPage0		  |
 * |--------------------------|
 * |	   IndexPage1		  |
 * |--------------------------|
 * |       ..........		  |
 * |--------------------------|
 * There is a problem, should we hold all entries in one and the same IndexPage as possible as we can? 
 * It'a trade-off, we think the similar queries are successive, so the most entries from the same a3v index
 * can be in the same index tree, and that's Okay.
 * About the index entry structure is like below:
 * |--------------------------------------------------------------------------|
 * | low | high | page_id0 | page_id1 | radius | query_id | offset0 | offset1 |
 * |  4B   4B       4B         4B        4B        4B         2B        2B	  |
 * |--------------------------------------------------------------------------|
 * low: this internal entry's left index in tids array
 * high: this internal entry's right index in tids array
 * page_id0: this internal entry's left entry's page id which tells us where it's.
 * page_id1: this internal entry's left entry's page id which tells us where it's.
 * radius: this internal entry's radius
 * // I think we must need this one, because we will do page cluster, so the ItemPointer of this internal query entry will change
 * // we use query id to
 * query id: this internal entry's query id key, we use this to get the true record from record cache. 
 * offset0: this internal entry's left entry's offset in its page.
 * offset1: this internal entry's right entry's offset in its page.
 * The whole size of one entry is 28 bytes. For leaf entry, it only needs `low` and `high`. But we don't
 * distinct them separately, we keep the same the format instead.
*/

bool m3vgettuple(IndexScanDesc scan, ScanDirection dir)
{
	/*
	 * We just support KnnQuery And RangeQuery
	 */
	// elog(INFO, "RANGE_QUERY: %d", RANGE_QUERY(scan));
	// elog(INFO, "KNN_QUERY: %d", KNN_QUERY(scan));
	if (!RANGE_QUERY(scan) && !KNN_QUERY(scan) || RANGE_QUERY(scan) && KNN_QUERY(scan))
	{
		elog(ERROR, "just support Knn Query and Range Query");
	}

	if(A3vMemoryIndexType(scan->indexRelation)){
		ItemPointerData result_tid;
		// auto begin_query = std::chrono::steady_clock::now();
		bool has_next = MemoryA3vIndexGetTuple(scan,result_tid);
		// auto end_query = std::chrono::steady_clock::now();
		// elog(INFO,"time cost %d millseconds",std::chrono::duration_cast<std::chrono::milliseconds>(end_query - begin_query).count());
		#if PG_VERSION_NUM >= 120000
		scan->xs_heaptid = result_tid;
		#else
				scan->xs_ctup.t_self = hc->data_tid;
		#endif
		return has_next;
	}

	m3vScanOpaque so = (m3vScanOpaque)scan->opaque;
	MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);
	// if it's the first time to get a3v root.
	if(so->first){
		m3vMetaPageData metaPage =  m3vGetMetaPageInfo(scan->indexRelation);
		int dim =0;
		for(int i = 0;i < metaPage.columns;i++){
			dim += metaPage.dimentions[i]; // Dimension of the elements
		}
		so->total_len  =dim;
		so->columns = metaPage.columns;
		hnswlib::HierarchicalNSW<float>* alg_hnsw;
		if(metaPage.simliar_query_root_nums == 0){
			// it's the first time to do query operation, we should insert this one to hnsw index.
			int max_elements = 10000;   // Maximum number of elements, should be known beforehand
			int M = 16;                 // Tightly connected with internal dimensionality of the data
										// strongly affects the memory consumption
			int ef_construction = 200;  // Controls index search speed/build speed tradeoff

			// Initing index
			hnswlib::L2Space space(dim);
			alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, max_elements, M, ef_construction);
			std::string path = build_hnsw_index_file_path(scan->indexRelation);
			init.InsertHnswIndex(path,alg_hnsw);
			float vector[dim];
			int offset = 0;
			// we don't distinct knn query and range query, so we can use one and the same a3v tree index
			// for the similar query point whatever knn query or range query.
			for(int i = 0;i < metaPage.columns;i++){
				if(RANGE_QUERY(scan)){
					// Range Query
					Vector* range_scan_point = DatumGetVector(scan->keyData[i].sk_argument);
					memcpy(vector + offset,range_scan_point->x,sizeof(float) * metaPage.dimentions[i]);
				}else{
					// KNN Query
					Vector* range_scan_point = DatumGetVector(scan->orderByData[i].sk_argument);
					memcpy(vector + offset,range_scan_point->x,sizeof(float) * metaPage.dimentions[i]);
				}
				offset += metaPage.dimentions[i];
			}
			// add the first vector query point into hnsw;
			alg_hnsw->addPoint(vector,++metaPage.simliar_query_root_nums);
			// commit the meta page info modity.
			a3vUpdateMetaPage(scan->indexRelation,metaPage.simliar_query_root_nums,metaPage.tuple_nums,MAIN_FORKNUM);
			// trigger a3v tree build algorithm, and then we insert the root entry postion in buffer page
			// into init's tids, we will 

			// attentation: every query point data in the query a3v tree will be saved in rocksdb, and we will retrive it by record cache.
		}else{
			// so we should get hnsw index from `init`, if we can't find it out, we should load it from disk.
			// try get root a3v index from hnsw index, and after find out the root, we should give the root entry postion in buffer page
			// into m3vScanOpaque
			alg_hnsw = init.LoadHnswIndex(scan->indexRelation,dim);
		}
		so->alg_hnsw = alg_hnsw;
		so->tuple_nums = metaPage.tuple_nums;
	}
	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	// first we should get query point from the hnsw index to find the closest a3v index tree.
	if(so->first){
		int offset = 0;
		float* data = so->query_point;
		const std::vector<int>& dimensions = init.GetDimensions(scan->indexRelation);
		for(int i = 0;i < so->columns;i++){
			if(RANGE_QUERY(scan)){
				// Range Query
				Vector* range_scan_point = DatumGetVector(scan->keyData[i].sk_argument);
				so->weights[i] = DatumGetFloat8(scan->keyData[i].w);
				memcpy(data + offset,range_scan_point->x,sizeof(float) * dimensions[i]);
			}else{
				// KNN Query
				Vector* knn_scan_point = DatumGetVector(scan->orderByData[i].sk_argument);
				so->weights[i] = DatumGetFloat8(scan->orderByData[i].w);
				memcpy(data + offset,knn_scan_point->x,sizeof(float) * dimensions[i]);
			}
			offset += dimensions[i];
		}
		std::priority_queue<std::pair<float, hnswlib::labeltype>> result = so->alg_hnsw->searchKnn(so->query_point,1);
		auto root_point = result.top();
		hnswlib::labeltype label = root_point.second;
		// open a new root a3v index.
		if(root_point.first > A3vCloseQueryThreshold(scan->indexRelation)){
			int index_pages = RelationGetNumberOfBlocks(scan->indexRelation);
			// we need to new the first page now,and insert it into hnsw index.
			// there is only one meta page.
			if(index_pages == 1){
				InsertNewQuery(scan,so,INVALID_BLOCK_NUMBER);
			}else{
				// Get Last Page and try to insert tuple.If it's fill, we need to new a page.
				InsertNewQuery(scan,so,index_pages);
			}
		}else{
			so->root_tid = init.GetRootTidAtIndex(std::string(RelationGetRelationName(scan->indexRelation)),label);
			
		}
	}

	if (KNN_QUERY(scan))
	{
		if (so->first)
		{
			Datum value;

			/* Count index scan for stats */
			pgstat_count_index_scan(scan->indexRelation);

			/* Safety check */
			if (scan->orderByData == NULL)
				elog(ERROR, "cannot scan m3v index without order");

			/* Get scan value */
			float8* weights = static_cast<float8*>(palloc0(sizeof(float8) * scan->numberOfOrderBys));
			Vector** values = static_cast<Vector**>(palloc0(sizeof(Vector* )* scan->numberOfOrderBys));
			// value = GetScanValue(scan,values,weights,true,scan->numberOfOrderBys);
			// value = GetScanValue2(scan,values,weights,true,scan->numberOfOrderBys);
			// so->w = GetKNNScanItems(scan, value);
			int k = scan->orderByData->KNNValues;
			// /* Release shared lock */
			// UnlockPage(scan->indexRelation, M3V_SCAN_LOCK, ShareLock);

			so->first = false;
		}
	}
	else
	{
		if (so->first)
		{
			float8 radius = DatumGetFloat8(scan->keyData->sk_argument);
			float8* weights = static_cast<float8*>(palloc0(sizeof(float8) * scan->numberOfKeys));
			// maybe bug!!!!
			Vector** values = static_cast<Vector**>(palloc0(sizeof(Vector* )* scan->numberOfKeys));
			Datum q = GetScanValue(scan,values,weights,false,scan->numberOfKeys);
			// let's optmize this after meta index and vector separate idea.
			// so->w = GetRangeScanItems(scan, q, radius,so->columns);
			so->w = NULL;
			so->first = false;
		}
		// elog(ERROR, "just support KNN Query for Now!");
	}

	while (list_length(so->w) > 0)
	{
		ItemPointer tid = static_cast<ItemPointer>(llast(so->w));
		list_delete_last(so->w);
#if PG_VERSION_NUM >= 120000
		scan->xs_heaptid = *tid;
#else
		scan->xs_ctup.t_self = hc->data_tid;
#endif
		/*
		 * Typically, an index scan must maintain a pin on the index page
		 * holding the item last returned by amgettuple. However, this is not
		 * needed with the current vacuum strategy, which ensures scans do not
		 * visit tuples in danger of being marked as deleted.because here will mark
		 * deleted by index selft.
		 * https://www.postgresql.org/docs/current/index-locking.html
		 */

		scan->xs_recheckorderby = false;
		return true;
	}

	MemoryContextSwitchTo(oldCtx);
	return false;
}

/*
 * Start or restart an index scan. Init OrderBy Keys And Condition Keys
 */
void m3vrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	m3vScanOpaque so = (m3vScanOpaque)scan->opaque;

	so->first = true;
	MemoryContextReset(so->tmpCtx);

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
}
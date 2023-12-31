#include "postgres.h"

#include "access/relscan.h"
#include "m3v.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/float.h"

#define MAX_KNN_DISTANCE get_float8_infinity()
#define KNN_QUERY(scan) (scan->orderByData != NULL && ((scan->orderByData->sk_flags & SK_ISNULL) == false))
#define RANGE_QUERY(scan) (scan->keyData != NULL && ((scan->keyData->sk_flags & SK_ISNULL) == false))

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
				float8 target_to_parent_dist = GetPointerDistances(etup->vecs, DatumGetPointer(q), procinfo, collation,columns);
				float8 estimated_dist = max((target_to_parent_dist - etup->radius), 0);
				if (estimated_dist <= getKDistance(nn, k, *nn_size))
				{
					m3vKNNCandidate *candidate = palloc0(sizeof(m3vKNNCandidate));
					// tid is invalid now.
					candidate->son_blkno = etup->son_page;
					candidate->distance = estimated_dist;
					candidate->target_parent_distance = target_to_parent_dist;
					// min heap
					pairingheap_add(p, Createm3vPairingKNNNode(candidate));
				}

				if (target_to_parent_dist + etup->radius <= getKDistance(nn, k, *nn_size))
				{
					// NN_Update task
					m3vKNNCandidate *candidate_nn = palloc0(sizeof(m3vKNNCandidate));
					// not leaf, don't update tid
					candidate_nn->distance = target_to_parent_dist + etup->radius;
					candidate_nn->tid = NIL;
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
				float8 dist = GetDistance(etup->vecs, q, procinfo, collation);
				if (dist <= getKDistance(nn, k, *nn_size))
				{
					// NN_Update task
					m3vKNNCandidate *candidate_nn = palloc0(sizeof(m3vKNNCandidate));
					// not leaf, don't update tid
					// this is accurate distance
					candidate_nn->distance = dist;
					candidate_nn->tid = &etup->data_tid;
					// elog(INFO, "KNN Leaf Distance: %f, addr: %p", candidate_nn->distance, candidate_nn->tid);
					// max heap
					pairingheap_add(nn, Createm3vPairingKNNNode(candidate_nn));
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
	m3vKNNCandidate *candidate = palloc0(sizeof(m3vKNNCandidate));
	// if son_blkno is InvalidBlockNumber, so it's a leaf
	candidate->son_blkno = root_block;
	int nn_size = 0;
	// DebugEntirem3vTree(root_block, index, 0);
	pairingheap_add(p, Createm3vPairingKNNNode(candidate));
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
				float8 dist = GetPointerDistances(etup->vecs, q, procinfo, collation,columns);
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
				float8 dist = GetPointerDistances(etup->vecs, q, procinfo, collation,columns);
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
					float8 dist = GetDistance(etup->vecs, q, procinfo, collation);
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

/*
 * Get scan value, value should be a Vector** 
 */
static Datum
GetScanValue(IndexScanDesc scan,float8 ** weights)
{
	m3vScanOpaque so = (m3vScanOpaque)scan->opaque;
	Datum value;
	*weights = palloc0(sizeof(float8) * scan->numberOfOrderBys);
	Vector** values = palloc0(sizeof(Vector*) * scan->numberOfOrderBys);
	// get multi vector columns
	for(int i = 0;i < scan->numberOfOrderBys; i++){
		*weights[i] = DatumGetFloat8(scan->orderByData[i].w);
		values[i] = DatumGetVector(scan->orderByData[i].sk_argument);
		elog(INFO,"w: %lf",weights[i]);
		PrintVector("vector knn: ",values[i]);
	}
	if (scan->orderByData->sk_flags & SK_ISNULL)
		value = PointerGetDatum(InitVector(GetDimensions(scan->indexRelation)));
	else
	{
		value = scan->orderByData->sk_argument;
		// PrintVector("norm vector: ", DatumGetVector(value));
		/* Value should not be compressed or toasted */
		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/* Fine if normalization fails */
		if (so->normprocinfo != NULL)
			m3vNormValue(so->normprocinfo, so->collation, &value, NULL);
	}

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

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}

/*
 * Fetch the next tuple in the given scan
 * https://www.postgresql.org/docs/current/parallel-plans.html#PARALLEL-SCANS
 */
bool m3vgettuple(IndexScanDesc scan, ScanDirection dir)
{
	m3vScanOpaque so = (m3vScanOpaque)scan->opaque;
	MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	/*
	 * We just support KnnQuery And RangeQuery
	 */
	// elog(INFO, "RANGE_QUERY: %d", RANGE_QUERY(scan));
	// elog(INFO, "KNN_QUERY: %d", KNN_QUERY(scan));
	if (!RANGE_QUERY(scan) && !KNN_QUERY(scan) || RANGE_QUERY(scan) && KNN_QUERY(scan))
	{
		elog(ERROR, "just support Knn Query and Range Query");
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
			float8* weights;
			value = GetScanValue(scan,&weights);

			so->w = GetKNNScanItems(scan, value);

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
			float8* weights;
			Datum q = GetScanValue(scan,&weights);
			so->w = GetRangeScanItems(scan, q, radius,so->columns);
			so->first = false;
		}
		// elog(ERROR, "just support KNN Query for Now!");
	}

	while (list_length(so->w) > 0)
	{
		ItemPointer tid = llast(so->w);
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
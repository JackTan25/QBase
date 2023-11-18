#include "postgres.h"

#include <math.h>

#include "mtree.h"
#include "storage/bufmgr.h"
#include "vector.h"

/**
 * Hacking !!!!!
 * This is a hack func, we will Replace the specified item in offset, never append data
 * It's only used in mtree, because it's unsafe.
 **/
void PageReplaceItem(Page page, OffsetNumber offset, Item item, Size size)
{
	Size alignedSize = MAXALIGN(size);
	ItemId item_id = PageGetItemId(page, offset);
	int lp_off = item_id->lp_off;
	int lp_len = item_id->lp_len;
	Assert(alignedSize == lp_len);
	memcpy((char *)page + lp_off, item, lp_len);
}

/*
 * Get the max number of connections in an upper layer for each element in the index
 */
int MtreeGetM(Relation index)
{
	MtreeOptions *opts = (MtreeOptions *)index->rd_options;

	if (opts)
		return opts->m;

	return MTREE_DEFAULT_M;
}

/*
 * Get the size of the dynamic candidate list in the index
 */
int MtreeGetEfConstruction(Relation index)
{
	MtreeOptions *opts = (MtreeOptions *)index->rd_options;

	if (opts)
		return opts->efConstruction;

	return MTREE_DEFAULT_EF_CONSTRUCTION;
}

/*
 * Get proc
 */
FmgrInfo *
MtreeOptionalProcInfo(Relation index, uint16 procnum)
{
	if (!OidIsValid(index_getprocid(index, 1, procnum)))
		return NULL;

	return index_getprocinfo(index, 1, procnum);
}

/*
 * We recommend cosine similarity. The choice of distance function typically doesn’t matter much.
 * embeddings are normalized to length 1, which means that:
 * 	  1. Cosine similarity can be computed slightly faster using just a dot product
 *    2. Cosine similarity and Euclidean distance will result in the identical rankings
 *
 * Divide by the norm
 *
 * Returns false if value should not be indexed
 *
 * The caller needs to free the pointer stored in value
 * if it's different than the original value
 */
bool MtreeNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector *result)
{
	// double norm = DatumGetFloat8(FunctionCall1Coll(procinfo, collation, *value));

	// if (norm > 0)
	// {
	// 	Vector *v = DatumGetVector(*value);

	// 	if (result == NULL)
	// 		result = InitVector(v->dim);

	// 	for (int i = 0; i < v->dim; i++)
	// 		result->x[i] = v->x[i] / norm;

	// 	*value = PointerGetDatum(result);
	// 	// PrintVector("Norm Vector: ", DatumGetVector(*value));
	// 	return true;
	// }

	// return false;
	return true;
}

void DebugEntireMtreeTree(BlockNumber root, Relation index, int level)
{
	Buffer buffer = ReadBuffer(index, root);
	Page page = BufferGetPage(buffer);
	ReleaseBuffer(buffer);
	elog(INFO, "level %d blkno %d", level, root);
	if (PageType(MtreePageGetOpaque(page)->type) == MTREE_INNER_PAGE_TYPE)
	{
		PrintInternalPageVectors("internal page", page);
		for (OffsetNumber offset = FirstOffsetNumber; offset <= PageGetMaxOffsetNumber(page); offset = OffsetNumberNext(offset))
		{
			DebugEntireMtreeTree(((MtreeElementTuple)PageGetItem(page, PageGetItemId(page, offset)))->son_page, index, level + 1);
		}
	}
	else
	{
		PrintLeafPageVectors("leaf page", page);
	}
}

/*
 * New buffer
 */
Buffer
MtreeNewBuffer(Relation index, ForkNumber forkNum)
{
	Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}

/*
 * page_type and is_root
 */
uint8 union_type(uint8 type, uint8 is_root)
{
	// remember to return
	return (uint8)((type << 4) | is_root);
}

bool is_root(uint8 union_data)
{
	return (union_data & 0x01) == 1;
}

uint8 PageType(uint8 union_data)
{
	return (union_data >> 4) & 0x03;
}

/**
 * Debug Page Opaque Data Info
 */
void DebugPageOpaque(char *msg, Page page, BlockNumber blkno)
{
	MtreePageOpaque qpaque = MtreePageGetOpaque(page);
	// if blkno is InvalidBlockNumber, it'll be -1 here, same with parent_block
	elog(INFO, "%s ---> this blkno: %d, parent_block: %d, is_root: %d, page_type: %d,offset: %d", msg, blkno, qpaque->parent_blkno, is_root(qpaque->type), PageType(qpaque->type), qpaque->offset);
}

/**
 * Debug MetaPage Info
 */
void DebugMetaPage(MtreeMetaPageData page)
{
	elog(INFO, "root block: %d, columns: %d", page.root, page.columns);
}

/*
 * Init page
 */
void MtreeInitPage(Buffer buf, Page page, BlockNumber blkno, uint8 type, uint8 is_root, OffsetNumber offset)
{
	PageInit(page, BufferGetPageSize(buf), sizeof(MtreePageOpaqueData));
	MtreePageGetOpaque(page)->parent_blkno = blkno;
	// elog(INFO, "union type: %d , %d, %d , %d ", union_type(type, is_root), type << 4, is_root, (uint8)((type << 4) | is_root));
	MtreePageGetOpaque(page)->type = union_type(type, is_root);
	MtreePageGetOpaque(page)->offset = offset;
}

/**
 * Update Page Opaque
 */
void MtreeUpdatePageOpaque(OffsetNumber offset, uint8 is_root, BlockNumber blkno, Page page, uint8 type)
{
	MtreePageGetOpaque(page)->parent_blkno = blkno;
	MtreePageGetOpaque(page)->type = union_type(type, is_root);
	MtreePageGetOpaque(page)->offset = offset;
}

/*
 * Set element tuple
 */
void MtreeSetElementTuple(MtreeElementTuple etup, MtreeElement element)
{
	etup->distance_to_parent = element->distance_to_parent;
	// etup->parent_page = element->parent_page;
	etup->radius = element->radius;
	etup->son_page = element->son_page;
	Assert(element->vec != NULL);
	memcpy(&etup->vec, element->vec, VECTOR_SIZE(element->vec->dim));
}

/*
 * Set element leaf tuple
 */
void MtreeSetLeafElementTuple(MtreeElementLeafTuple etup, MtreeElement element)
{
	etup->distance_to_parent = element->distance_to_parent;
	// etup->parent_page = element->parent_page;
	etup->data_tid = *(element->item_pointer);
	memcpy(&etup->vec, element->vec, VECTOR_SIZE(element->vec->dim));
	/**
	 *	Don't use "etup->vec = *(element->vec);" directly, beacuse the FLEXIBLE_MEMEER_ARRAY
	 *  can't copy by this way.
	 */

	// elog(INFO, "DataId: %d %d", etup->data_tid.ip_blkid, etup->data_tid.ip_posid);
}

/*
 * Allocate an element
 */
MtreeElement
MtreeInitElement(ItemPointer tid, float8 radius, float8 distance_to_parent, BlockNumber son_page, Vector *vector)
{
	MtreeElement element = palloc(sizeof(MtreeElementData));
	element->distance_to_parent = distance_to_parent;
	element->radius = radius;
	element->son_page = son_page;
	// element->parent_page = parent_page;
	element->item_pointer = tid;
	element->vec = vector;
	return element;
}

/*
 * Add a heap TID to an element
 */
void MtreeAddHeapTid(MtreeElement element, ItemPointer heaptid) {}

/*
 * Init and register page
 */
void MtreeInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, uint8 type, uint8 is_root)
{
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	MtreeInitPage(*buf, *page, InvalidBlockNumber, type, is_root, InvalidOffsetNumber);
}

/*
 * Commit buffer
 */
void MtreeCommitBuffer(Buffer buf, GenericXLogState *state)
{
	MarkBufferDirty(buf);
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Get the metapage info
 */
MtreeMetaPageData MtreeGetMetaPageInfo(Relation index)
{
	Buffer buf;
	Page page;
	MtreeMetaPageData metap;

	buf = ReadBuffer(index, MTREE_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = *MtreePageGetMeta(page);

	UnlockReleaseBuffer(buf);
	return metap;
}

void MtreeFreeElement(MtreeElement element)
{
	// nothing to free, because we never palloc pointer vars for element
	pfree(element);
}

/*
 * Get the entry point
 */
// MtreeElement
// MtreeGetEntryPoint(Relation index)
// {
// 	MtreeElement entryPoint;

// 	MtreeGetMetaPageInfo(index, NULL, &entryPoint);

// 	return entryPoint;
// }

/*
 * Update the metapage info
 */
static void
MtreeUpdateMetaPageInfo(Page page, BlockNumber root)
{
	MtreeMetaPage metap = MtreePageGetMeta(page);
	metap->root = root;
}

/*
 * Update the metapage
 */
void MtreeUpdateMetaPage(Relation index, BlockNumber root, ForkNumber forkNum)
{
	Buffer buf;
	Page page;
	GenericXLogState *state;

	buf = ReadBufferExtended(index, forkNum, MTREE_METAPAGE_BLKNO, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	MtreeUpdateMetaPageInfo(page, root);

	MtreeCommitBuffer(buf, state);
}

/*
 * Get the distance for a candidate
 */
// static float
// GetCandidateDistance(MtreeCandidate *hc, Datum q, FmgrInfo *procinfo, Oid collation)
// {
// 	return DatumGetFloat8(FunctionCall2Coll(procinfo, collation, q, PointerGetDatum(hc->element->vec)));
// }

/**
 * Distance Computation for two vector
 */
float GetDistance(Datum q1, Datum q2, FmgrInfo *procinfo, Oid collation)
{
	return DatumGetFloat8(FunctionCall2Coll(procinfo, collation, q1, q2));
}
/*
 * Create a candidate for the entry point
 */
// MtreeCandidate *
// MtreeEntryCandidate(MtreeElement entryPoint, Datum q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec)
// {
// 	MtreeCandidate *hc = palloc(sizeof(MtreeCandidate));

// 	hc->element = entryPoint;
// 	if (index == NULL)
// 		hc->distance = GetCandidateDistance(hc, q, procinfo, collation);
// 	else
// 		MtreeLoadElement(hc->element, &hc->distance, &q, index, procinfo, collation, loadVec);
// 	return hc;
// }
float8 max(float8 a, float8 b)
{
	if (a > b)
		return a;
	else
		return b;
}

/**
 *	SplitInternalPage
 *  Remember to do copy up, not pull up. this is different with Bplus Tree,
 *  because we need to maintain the child ball partition
 */
Page SplitInternalPage(Page internal_page, Page new_page, FmgrInfo *procinfo, Oid collation, MtreeElementTuple left_centor, MtreeElementTuple insert_data, OffsetNumber left_off, Datum *left_copy_up, Datum *right_copy_up, float8 *left_radius, float8 *right_radius)
{
	PrintInternalPageVectors("Old Page Before Split Page Vector: ", internal_page);
	PrintInternalPageVectors("New Page Before Spilt Page Vector: ", new_page);

	Page temp_page;
	MtreePairingHeapUtils heap;
	OffsetNumber offsets;
	MtreeElementTuple elem;
	Size etupSize;
	bool in_left = false;
	etupSize = MTREE_ELEMENT_TUPLE_SIZE(left_centor->vec.dim);

	offsets = PageGetMaxOffsetNumber(internal_page);
	// replace left_offset,right centor as a insert_data
	Assert(left_off >= FirstOffsetNumber && left_off <= offsets);
	// Hacking
	PageReplaceItem(internal_page, left_off, (Item)left_centor, etupSize);
	// PageAddItem(internal_page, (Item)left_centor, etupSize, left_off, true, false);

	// Make Sure that the left_off's entry should be still in left page
	Assert(offsets > FirstOffsetNumber);
	OffsetNumber leftRand = rand() % (offsets / 2) + 1;
	OffsetNumber rightRand = rand() % (offsets - offsets / 2) + offsets / 2 + 1;

	*left_copy_up = PointerGetDatum(&((MtreeElementTuple)PageGetItem(internal_page, PageGetItemId(internal_page, leftRand)))->vec);
	*right_copy_up = PointerGetDatum(&((MtreeElementTuple)PageGetItem(internal_page, PageGetItemId(internal_page, rightRand)))->vec);

	// update son block for left_copy_up and right_copy_up

	// init left heap and right heap
	heap.left = pairingheap_allocate(CompareNearestCandidates, NULL);
	heap.right = pairingheap_allocate(CompareNearestCandidates, NULL);
	/**
	 *	special part: MtreePageOpaqueData
	 */
	temp_page = PageGetTempPageCopySpecial(internal_page);

	for (int offset = FirstOffsetNumber; offset <= offsets; offset = OffsetNumberNext(offset))
	{
		MtreeElementTuple etup = (MtreeElementTuple)PageGetItem(internal_page, PageGetItemId(internal_page, offset));
		heap.visited[offset - 1] = false;
		// left candidate
		MtreeCandidate *left_candidate = palloc0(sizeof(MtreeCandidate));
		left_candidate->distance = GetDistance(PointerGetDatum(&etup->vec), *left_copy_up, procinfo, collation);
		left_candidate->element = PointerGetDatum(etup);
		left_candidate->id = offset;
		// elog(INFO, "left id: %d,distance: %f ", left_candidate->id, left_candidate->distance);
		// right candidate
		MtreeCandidate *right_candidate = palloc0(sizeof(MtreeCandidate));
		right_candidate->distance = GetDistance(PointerGetDatum(&etup->vec), *right_copy_up, procinfo, collation);
		right_candidate->element = PointerGetDatum(etup);
		right_candidate->id = offset;
		// elog(INFO, "right id: %d,distance: %f ", right_candidate->id, right_candidate->distance);
		pairingheap_add(heap.left, &(CreatePairingHeapNode(left_candidate)->ph_node));
		// elog(INFO, "test: %d", ((MtreePairingHeapNode *)pairingheap_first(heap.left))->inner->id);
		pairingheap_add(heap.right, &(CreatePairingHeapNode(right_candidate)->ph_node));
		// elog(INFO, "leaf blkno: %d, offset: %d", etup->data_tid.ip_blkid, etup->data_tid.ip_posid);
	}
	// elog(INFO, "test: %d", ((MtreePairingHeapNode *)pairingheap_first(heap.left))->inner->id);
	for (OffsetNumber idx = 0; idx < offsets; idx = OffsetNumberNext(idx))
	{
		bool flag = false;
		// left iteration
		if (idx % 2 == 0)
		{
			while (!flag)
			{
				MtreePairingHeapNode *c = ((MtreePairingHeapNode *)pairingheap_remove_first(heap.left));
				if (!heap.visited[c->inner->id - 1])
				{
					if (c->inner->id == left_off)
					{
						in_left = true;
					}
					heap.visited[c->inner->id - 1] = true;
					MtreeElementTuple elem = DatumGetMtreeElementTuple(c->inner->element);
					*left_radius = max(*left_radius, c->inner->distance + elem->radius);
					// update distance to parent
					elem->distance_to_parent = c->inner->distance;
					PageAddItem(temp_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
					flag = true;
				}
			}
		}
		else
		{
			while (!flag)
			{
				// right iteration
				MtreePairingHeapNode *c = ((MtreePairingHeapNode *)pairingheap_remove_first(heap.right));
				if (!heap.visited[c->inner->id - 1])
				{
					heap.visited[c->inner->id - 1] = true;
					elem = DatumGetMtreeElementTuple(c->inner->element);
					*right_radius = max(*right_radius, c->inner->distance + elem->radius);
					// update distance to parent
					elem->distance_to_parent = c->inner->distance;
					PageAddItem(new_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
					flag = true;
				}
			}
		}
	}

	PrintInternalPageVectors("Old Page After Split Page Vector: ", temp_page);
	PrintInternalPageVectors("New Page After Spilt Page Vector: ", new_page);
	// Insert insert_data
	PrintVector("left_copy_up vector", DatumGetVector(*left_copy_up));
	PrintVector("right_copy_up vector", DatumGetVector(*right_copy_up));
	PrintVector("insert_data vector", &insert_data->vec);
	float8 left_distance = GetDistance(*left_copy_up, PointerGetDatum(&insert_data->vec), procinfo, collation);
	float8 right_distance = GetDistance(*right_copy_up, PointerGetDatum(&insert_data->vec), procinfo, collation);
	elem = insert_data;
	if (left_distance < right_distance)
	{
		elem->distance_to_parent = left_distance;
		*left_radius = max(*left_radius, left_distance + insert_data->radius);
		PageAddItem(temp_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
	}
	else if (left_distance > right_distance)
	{
		elem->distance_to_parent = right_distance;
		*right_radius = max(*right_radius, right_distance + insert_data->radius);
		PageAddItem(new_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
	}
	else
	{
		int left_offsets = PageGetMaxOffsetNumber(temp_page);
		int right_offsets = PageGetMaxOffsetNumber(new_page);
		if (left_offsets > right_offsets)
		{
			elem->distance_to_parent = right_distance;
			*right_radius = max(*right_radius, right_distance);
			PageAddItem(new_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
		}
		else
		{
			elem->distance_to_parent = left_distance;
			*left_radius = max(*left_radius, left_distance);
			PageAddItem(temp_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
		}
	}
	*left_copy_up = PointerGetDatum(((MtreeElementTuple)PageGetItem(internal_page, PageGetItemId(internal_page, leftRand))));
	*right_copy_up = PointerGetDatum(((MtreeElementTuple)PageGetItem(internal_page, PageGetItemId(internal_page, rightRand))));
	DatumGetMtreeElementTuple(*left_copy_up)->radius = *left_radius;
	DatumGetMtreeElementTuple(*right_copy_up)->radius = *right_radius;
	if (!in_left)
	{
		Page temp_page2 = PageGetTempPage(new_page);
		memcpy((char *)temp_page2, (char *)new_page, PageGetPageSize(new_page));
		memcpy((char *)new_page, (char *)temp_page, PageGetPageSize(temp_page));
		memcpy((char *)temp_page, (char *)temp_page2, PageGetPageSize(temp_page2));
		pfree(temp_page2);
		// swap left_copy_up and right_copy_up
		Datum temp = *left_copy_up;
		*left_copy_up = *right_copy_up;
		*right_copy_up = temp;
		PrintInternalPageVectors("Old Page After Split in left Page Vector: ", temp_page);
		PrintInternalPageVectors("New Page After Spilt in left Page Vector: ", new_page);
	}
	PrintInternalPageVectors("Old Page After Split Page2 Vector: ", temp_page);
	PrintInternalPageVectors("New Page After Spilt Page2 Vector: ", new_page);
	return temp_page;
}

/**
 *	SplitLeafPage
 */
Page SplitLeafPage(Page page, Page new_page, FmgrInfo *procinfo, Oid collation, MtreeElementLeafTuple insert_data, Datum *left_centor, Datum *right_centor, float8 *left_radius, float8 *right_radius)
{
	// PrintLeafPageVectors("Old Page Before Split Page Vector: ", page);
	// PrintLeafPageVectors("New Page Before Spilt Page Vector: ", new_page);
	Page temp_page;
	MtreePairingHeapUtils heap;
	OffsetNumber offsets;
	MtreeElementLeafTuple elem;

	offsets = PageGetMaxOffsetNumber(page);
	Assert(offsets > FirstOffsetNumber);
	OffsetNumber leftRand = rand() % (offsets / 2) + 1;
	OffsetNumber rightRand = rand() % (offsets - offsets / 2) + offsets / 2 + 1;
	// PrintLeafPageVectors("left page old: ", page);
	*left_centor = PointerGetDatum(&((MtreeElementLeafTuple)PageGetItem(page, PageGetItemId(page, leftRand)))->vec);
	*right_centor = PointerGetDatum(&((MtreeElementLeafTuple)PageGetItem(page, PageGetItemId(page, rightRand)))->vec);

	PrintVector("Split left centor: ", DatumGetVector(*left_centor));
	PrintVector("Split right centor: ", DatumGetVector(*right_centor));
	// init left_radius and right_radius
	*left_radius = 0;
	*right_radius = 0;
	// init left heap and right heap
	heap.left = pairingheap_allocate(CompareNearestCandidates, NULL);
	heap.right = pairingheap_allocate(CompareNearestCandidates, NULL);
	/**
	 *	special part: MtreePageOpaqueData
	 */
	temp_page = PageGetTempPageCopySpecial(page);

	Size etupSize = MTREE_ELEMENT_LEAF_TUPLE_SIZE(insert_data->vec.dim);

	for (int offset = FirstOffsetNumber; offset <= offsets; offset = OffsetNumberNext(offset))
	{
		MtreeElementLeafTuple etup = (MtreeElementLeafTuple)PageGetItem(page, PageGetItemId(page, offset));
		heap.visited[offset - 1] = false;
		// left candidate
		MtreeCandidate *left_candidate = palloc0(sizeof(MtreeCandidate));
		left_candidate->distance = GetDistance(PointerGetDatum(&etup->vec), *left_centor, procinfo, collation);
		left_candidate->element = PointerGetDatum(etup);
		left_candidate->id = offset;
		// elog(INFO, "left id: %d,distance: %f ", left_candidate->id, left_candidate->distance);
		// right candidate
		MtreeCandidate *right_candidate = palloc0(sizeof(MtreeCandidate));
		right_candidate->distance = GetDistance(PointerGetDatum(&etup->vec), *right_centor, procinfo, collation);
		right_candidate->element = PointerGetDatum(etup);
		right_candidate->id = offset;
		// elog(INFO, "right id: %d,distance: %f ", right_candidate->id, right_candidate->distance);
		pairingheap_add(heap.left, &(CreatePairingHeapNode(left_candidate)->ph_node));
		// elog(INFO, "test: %d", ((MtreePairingHeapNode *)pairingheap_first(heap.left))->inner->id);
		pairingheap_add(heap.right, &(CreatePairingHeapNode(right_candidate)->ph_node));
		// elog(INFO, "leaf blkno: %d, offset: %d", etup->data_tid.ip_blkid, etup->data_tid.ip_posid);
	}
	// elog(INFO, "test: %d", ((MtreePairingHeapNode *)pairingheap_first(heap.left))->inner->id);
	for (OffsetNumber idx = 0; idx < offsets; idx = OffsetNumberNext(idx))
	{
		bool flag = false;
		// left iteration
		if (idx % 2 == 0)
		{
			while (!flag)
			{
				MtreePairingHeapNode *c = ((MtreePairingHeapNode *)pairingheap_remove_first(heap.left));
				if (!heap.visited[c->inner->id - 1])
				{
					heap.visited[c->inner->id - 1] = true;
					*left_radius = max(*left_radius, c->inner->distance);
					// update distance to parent
					MtreeElementLeafTuple elem = DatumGetMtreeElementLeafTuple(c->inner->element);
					elem->distance_to_parent = c->inner->distance;
					PageAddItem(temp_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
					flag = true;
				}
			}
		}
		else
		{
			while (!flag)
			{
				// right iteration
				MtreePairingHeapNode *c = ((MtreePairingHeapNode *)pairingheap_remove_first(heap.right));
				if (!heap.visited[c->inner->id - 1])
				{
					heap.visited[c->inner->id - 1] = true;
					*right_radius = max(*right_radius, c->inner->distance);
					// update distance to parent
					elem = DatumGetMtreeElementLeafTuple(c->inner->element);
					elem->distance_to_parent = c->inner->distance;
					PageAddItem(new_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
					flag = true;
				}
			}
		}
	}

	// PrintLeafPageVectors("test new leaf page before ", new_page);
	// PrintVector("insert data", &insert_data->vec);
	// Insert insert_data
	float8 left_distance = GetDistance(*left_centor, PointerGetDatum(&insert_data->vec), procinfo, collation);
	float8 right_distance = GetDistance(*right_centor, PointerGetDatum(&insert_data->vec), procinfo, collation);
	elem = insert_data;
	if (left_distance < right_distance)
	{
		elem->distance_to_parent = left_distance;
		*left_radius = max(*left_radius, left_distance);
		PageAddItem(temp_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
	}
	else if (left_distance > right_distance)
	{
		elem->distance_to_parent = right_distance;
		*right_radius = max(*right_radius, right_distance);
		PageAddItem(new_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
	}
	else
	{
		int left_offsets = PageGetMaxOffsetNumber(temp_page);
		int right_offsets = PageGetMaxOffsetNumber(new_page);
		if (left_offsets > right_offsets)
		{
			elem->distance_to_parent = right_distance;
			*right_radius = max(*right_radius, right_distance);
			PageAddItem(new_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
		}
		else
		{
			elem->distance_to_parent = left_distance;
			*left_radius = max(*left_radius, left_distance);
			PageAddItem(temp_page, (Item)(elem), etupSize, InvalidOffsetNumber, false, false);
		}
	}
	PrintLeafPageVectors("temp page vectors:\n ", temp_page);
	PrintLeafPageVectors("new page:\n ", new_page);
	return temp_page;
	// PageRestoreTempPage(temp_page, page);
	// Debug split page info
	// PrintLeafPageVectors("Old Page Split Page Vector: ", page);
	// PrintLeafPageVectors("New Page Spilt Page Vector: ", new_page);
}

/*
 * Compare knn candidate distances
 * max heap
 */
int CompareKNNCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	// order by desc
	if (((const MtreePairingKNNNode *)a)->inner->distance < ((const MtreePairingKNNNode *)b)->inner->distance)
		return -1;

	if (((const MtreePairingKNNNode *)a)->inner->distance > ((const MtreePairingKNNNode *)b)->inner->distance)
		return 1;

	bool a_null = (((const MtreePairingKNNNode *)a)->inner->tid == NIL);
	bool b_null = (((const MtreePairingKNNNode *)b)->inner->tid == NIL);
	if (a_null && !b_null)
	{
		return 1;
	}
	else if (!a_null && b_null)
	{
		return -1;
	}
	return 0;
}

/*
 * Compare knn candidate distances
 * min heap
 */
int CompareKNNCandidatesMinHeap(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	// order by desc
	if (((const MtreePairingKNNNode *)a)->inner->distance > ((const MtreePairingKNNNode *)b)->inner->distance)
		return -1;

	if (((const MtreePairingKNNNode *)a)->inner->distance < ((const MtreePairingKNNNode *)b)->inner->distance)
		return 1;

	return 0;
}

/*
 * Compare candidate distances
 * min heap
 */
int CompareNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const MtreePairingHeapNode *)a)->inner->distance < ((const MtreePairingHeapNode *)b)->inner->distance)
		return 1;

	if (((const MtreePairingHeapNode *)a)->inner->distance > ((const MtreePairingHeapNode *)b)->inner->distance)
		return -1;

	return 0;
}

/*
 * Compare candidate distances
 * min heap
 */
int CompareDistanceOnlyNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const MtreePairingDistanceOnlyHeapNode *)a)->inner->distance < ((const MtreePairingDistanceOnlyHeapNode *)b)->inner->distance)
		return 1;

	if (((const MtreePairingDistanceOnlyHeapNode *)a)->inner->distance > ((const MtreePairingDistanceOnlyHeapNode *)b)->inner->distance)
		return -1;

	return 0;
}

/*
 * Compare candidate distances
 */
static int
CompareFurthestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg)
{
	if (((const MtreePairingHeapNode *)a)->inner->distance < ((const MtreePairingHeapNode *)b)->inner->distance)
		return -1;

	if (((const MtreePairingHeapNode *)a)->inner->distance > ((const MtreePairingHeapNode *)b)->inner->distance)
		return 1;

	return 0;
}

/*
 * Create a pairing heap node for a candidate
 */
MtreePairingKNNNode *CreateMtreePairingKNNNode(MtreeKNNCandidate *c)
{
	MtreePairingKNNNode *node = palloc(sizeof(MtreePairingKNNNode));

	node->inner = c;
	return node;
}

/*
 * Create a pairing heap node for a candidate
 */
static MtreePairingHeapNode *
CreatePairingHeapNode(MtreeCandidate *c)
{
	MtreePairingHeapNode *node = palloc(sizeof(MtreePairingHeapNode));

	node->inner = c;
	return node;
}

/*
 * Create a pairing heap node for a candidate
 */
MtreePairingDistanceOnlyHeapNode *
CreatePairingDistanceOnlyHeapNode(MtreeDistanceOnlyCandidate *c)
{
	MtreePairingDistanceOnlyHeapNode *node = palloc(sizeof(MtreePairingDistanceOnlyHeapNode));

	node->inner = c;
	return node;
}

/*
 * Calculate the distance between elements
 */
static float
MtreeGetDistance(MtreeElement a, MtreeElement b, int lc, FmgrInfo *procinfo, Oid collation) {}

/*
 * Check if an element is closer to q than any element from R
 */
static bool CheckElementCloser(MtreeCandidate *e, List *r, int lc, FmgrInfo *procinfo, Oid collation)
{
	ListCell *lc2;

	foreach (lc2, r)
	{
		MtreeCandidate *ri = lfirst(lc2);
		float distance = MtreeGetDistance(e->element, ri->element, lc, procinfo, collation);

		if (distance <= e->distance)
			return false;
	}

	return true;
}

/*
 * Algorithm 4 from paper
 */
static List *
SelectNeighbors(List *c, int m, int lc, FmgrInfo *procinfo, Oid collation, MtreeCandidate **pruned)
{
	List *r = NIL;
	List *w = list_copy(c);
	pairingheap *wd;

	if (list_length(w) <= m)
		return w;

	wd = pairingheap_allocate(CompareNearestCandidates, NULL);

	while (list_length(w) > 0 && list_length(r) < m)
	{
		/* Assumes w is already ordered desc */
		MtreeCandidate *e = llast(w);
		bool closer;

		w = list_delete_last(w);

		closer = CheckElementCloser(e, r, lc, procinfo, collation);

		if (closer)
			r = lappend(r, e);
		else
			pairingheap_add(wd, &(CreatePairingHeapNode(e)->ph_node));
	}

	/* Keep pruned connections */
	while (!pairingheap_is_empty(wd) && list_length(r) < m)
		r = lappend(r, ((MtreePairingHeapNode *)pairingheap_remove_first(wd))->inner);

	/* Return pruned for update connections */
	if (pruned != NULL)
	{
		if (!pairingheap_is_empty(wd))
			*pruned = ((MtreePairingHeapNode *)pairingheap_first(wd))->inner;
		else
			*pruned = linitial(w);
	}

	return r;
}

/*
 * Compare candidate distances
 */
static int
#if PG_VERSION_NUM >= 130000
CompareCandidateDistances(const ListCell *a, const ListCell *b)
#else
CompareCandidateDistances(const void *a, const void *b)
#endif
{
	MtreeCandidate *hca = lfirst((ListCell *)a);
	MtreeCandidate *hcb = lfirst((ListCell *)b);

	if (hca->distance < hcb->distance)
		return 1;

	if (hca->distance > hcb->distance)
		return -1;

	return 0;
}

/*
 * Load an element and optionally get its distance from q
 */
void MtreeLoadElement(MtreeElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec) {}
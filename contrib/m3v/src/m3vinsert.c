#include "postgres.h"

#include <math.h>
#include "util.h"
#include "m3v.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "vector.h"
#include "storage/bufmgr.h"

bool Insertm3v(BlockNumber root_block, Relation index, m3vElement element, bool *isnull, Relation heapRel, FmgrInfo *procinfo, Oid collation, GenericXLogState *state);
/*
 * Get NodePage
 */

/*
 * Get the insert page
 */
static BlockNumber
GetInsertPage(Relation index)
{
	Buffer buf;
	Page page;
	m3vMetaPage metap;
	BlockNumber insertPage;

	buf = ReadBuffer(index, M3V_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = m3vPageGetMeta(page);

	UnlockReleaseBuffer(buf);

	return insertPage;
}

void WriteElementToPage(m3vElement e, Page page)
{
	/* Calculate sizes */
	Size etupSize = M3V_ELEMENT_TUPLE_SIZE(e->vec->dim);

	/* Prepare element tuple */
	m3vElementTuple etup = palloc0(etupSize);
	m3vSetElementTuple(etup, e);
	PageAddItem(page, (Item)etup, etupSize, InvalidOffsetNumber, false, false);
	// PrintVector("write vector: ", &(((m3vElementLeafTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber)))->vec));
	// don't pfree it, because we will free it in m3vinsert() func when MemoryContextDelete(insertCtx);
	// more info about memort context for pg https://www.cybertec-postgresql.com/en/memory-context-for-postgresql-memory-management/
	// pfree(etup);
}

void WriteLeafElementToPage(m3vElement e, Page page)
{
	/* Calculate sizes */
	Size etupLeafSize = M3V_ELEMENT_LEAF_TUPLE_SIZE(e->vec->dim);

	/* Prepare element tuple */
	m3vElementLeafTuple etup = palloc0(etupLeafSize);
	m3vSetLeafElementTuple(etup, e);
	PageAddItem(page, (Item)etup, etupLeafSize, InvalidOffsetNumber, false, false);
	// PrintVector("write vector: ", &(((m3vElementLeafTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber)))->vec));
	// don't pfree it, because we will free it in m3vinsert() func when MemoryContextDelete(insertCtx);
	// more info about memort context for pg https://www.cybertec-postgresql.com/en/memory-context-for-postgresql-memory-management/
	// pfree(etup);
}

/*
 * Insert a tuple into the index
 */
bool m3vInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel, m3vBuildState *buildstate, GenericXLogState *state)
{
	elog(INFO, "numbers %d", MAX_GENERIC_XLOG_PAGES);
	Datum value;
	Page new_page;
	FmgrInfo *normprocinfo;
	m3vElement entryPoint;
	m3vElement element;
	int m;
	int efConstruction = m3vGetEfConstruction(index);
	FmgrInfo *procinfo = index_getprocinfo(index, 1, M3V_DISTANCE_PROC);
	Oid collation = index->rd_indcollation[0];
	m3vElement dup;
	Vector *normvec;
	LOCKMODE lockmode = ShareLock;
	/* Detoast once for all calls */
	value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
	normprocinfo = m3vOptionalProcInfo(index, M3V_NORM_PROC);
	/* Normalize if needed */
	if (normprocinfo != NULL)
	{
		if (buildstate == NULL)
		{
			normvec = NULL;
		}
		else
		{
			normvec = buildstate->normvec;
		}
		if (!m3vNormValue(normprocinfo, collation, &value, normvec))
			return false;
	}

	// PrintVector("Insert Vector: ", DatumGetVector(value));
	m3vMetaPageData meta = m3vGetMetaPageInfo(index);
	m3vMetaPage metap = &meta;
	// if there is no root page, we need to
	// create a new root page
	if (metap->root == InvalidBlockNumber)
	{
		// P_NEW is not race-condition-proof, so we need to add a lock
		// when we try to New a Page for index.
		LockRelationForExtension(index, ExclusiveLock);
		Buffer new_buffer = m3vNewBuffer(index, MAIN_FORKNUM);
		elog(INFO, "New Buffer And Lock %d 115", new_buffer);
		// set new root
		metap->root = BufferGetBlockNumber(new_buffer);
		UnlockRelationForExtension(index, ExclusiveLock);

		/* Init new page */
		new_page = GenericXLogRegisterBuffer(state, new_buffer, GENERIC_XLOG_FULL_IMAGE);

		// elog(INFO, "resgister %d 122", new_buffer);
		// set parent page as InvalidBlockNumber
		m3vInitPage(new_buffer, new_page, InvalidBlockNumber, M3V_LEAF_PAGE_TYPE, 1, InvalidOffsetNumber);
		m3vElement element = m3vInitElement(heap_tid, 0, 0, InvalidBlockNumber, NULL);
		element->vec = DatumGetVector(value);
		// PrintVector("insert vector ", element->vec);
		WriteLeafElementToPage(element, new_page);
		// elog(INFO, "is root: %d", is_root(m3vPageGetOpaque(new_page)->type));
		/* Commit */
		MarkBufferDirty(new_buffer);

		/* Unlock previous buffer */
		UnlockReleaseBuffer(new_buffer);
		m3vUpdateMetaPage(index, metap->root, MAIN_FORKNUM);
		// DebugEntirem3vTree(m3vGetMetaPageInfo(index).root, index, 0);
		return true;
	}

	// get root page by using meta_page
	Assert(metap->root != InvalidBlockNumber);
	// wrap value as a m3vElement
	element = m3vInitElement(heap_tid, 0, 0, InvalidBlockNumber, NULL);
	element->vec = DatumGetVector(value);

	// PrintVector("insert vector ", element->vec);
	// strat record log.
	Insertm3v(metap->root, index, element, isnull, heapRel, procinfo, collation, state);
	// DebugEntirem3vTree(5, index, 0);
	// DebugEntirem3vTree(3, index, 0);
	elog(INFO, "new root a: %d", m3vGetMetaPageInfo(index).root);
	DebugEntirem3vTree(m3vGetMetaPageInfo(index).root, index, 0);
	return true;
}

// m3vElement:
/* data */
// 1. the radius of this entry
// float8 radius;
// 2. distance to parent
// float8 distance_to_parent;
// 3. is leaf_node
// bool is_leaf;
// 4. the pointer to the leaf page
// BlockNumber son_page;
// 5. parent_buffer
// BlockNumber parent_page;
// 6. center data
// Insertm3v is recursive insert.
bool Insertm3v(BlockNumber root_block, Relation index, m3vElement element, bool *isnull, Relation heapRel, FmgrInfo *procinfo, Oid collation, GenericXLogState *state)
{
	Print();
	PrintVector("insert vector: ", element->vec);
	// elog(INFO, "item pointer2: %d %d", element->item_pointer->ip_blkid, element->item_pointer->ip_posid);
	Buffer buf;
	// old page
	Page page;
	// right page
	Page new_page;
	bool append_only = false;

	Size etupSize;
	Size etupLeafSize;
	Size etupCombineSize;
	Size etupLeafCombineSize;
	float8 left_radius;
	float8 right_radius;
	Datum left_centor;
	Datum right_centor;
	etupSize = M3V_ELEMENT_TUPLE_SIZE(element->vec->dim);
	etupCombineSize = etupSize + sizeof(ItemIdData);

	etupLeafSize = M3V_ELEMENT_LEAF_TUPLE_SIZE(element->vec->dim);
	etupLeafCombineSize = etupLeafSize + sizeof(ItemIdData);

	/**
	 * be careful of buffer count leak:
	 * 		https://postgrespro.com/list/thread-id/1580034
	 */
	buf = ReadBuffer(index, root_block);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	elog(INFO, "Lock Buffer: %d", buf);
	page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
	// elog(INFO, "resgister %d 201", buf);
	// PrintLeafPageVectors("try insert page: ", page);
	// leaf page
	if (PageType(m3vPageGetOpaque(page)->type) == M3V_LEAF_PAGE_TYPE)
	{
		// if (PageGetFreeSpace(page) >= etupLeafCombineSize)
		if (PageGetMaxOffsetNumber(page) < 2)
		{
			// push directly
			WriteLeafElementToPage(element, page);

			/* Commit */
			MarkBufferDirty(buf);

			// /* Unlock previous buffer */
			UnlockReleaseBuffer(buf);
			elog(INFO, "UnLock Buffer 215 %d", buf);
			return true;
		}
		else
		{
			// split,
			// future: we use kmeans++ method to do cluster and speed up
			// search, for now just use normal
			// now: random choose, OffsetNumber starts from 1.

			// P_NEW is not race-condition-proof, so we need to add a lock
			// when we try to New a Page for index.
			LockRelationForExtension(index, ExclusiveLock);
			Buffer right_buf = m3vNewBuffer(index, MAIN_FORKNUM);
			elog(INFO, "New Buffer And Lock %d 233", right_buf);
			BlockNumber right_page_block_number = BufferGetBlockNumber(right_buf);
			UnlockRelationForExtension(index, ExclusiveLock);
			// record wal log
			new_page = GenericXLogRegisterBuffer(state, right_buf, GENERIC_XLOG_FULL_IMAGE);
			// elog(INFO, "resgister %d 235", right_buf);
			// set parent page
			m3vInitPage(right_buf, new_page, m3vPageGetOpaque(page)->parent_blkno, M3V_LEAF_PAGE_TYPE, 0, InvalidOffsetNumber);
			m3vElementLeafTuple etup_leaf = palloc0(etupLeafSize);
			m3vSetLeafElementTuple(etup_leaf, element);

			/**
			 * populate a new root page here
			 */
			if (is_root(m3vPageGetOpaque(page)->type))
			{
				// do split
				Page temp_page = SplitLeafPage(page, new_page, procinfo, collation, etup_leaf, &left_centor, &right_centor, &left_radius, &right_radius);
				// PrintVector("Split left centor: ", DatumGetVector(left_centor));
				// PrintVector("Split right centor: ", DatumGetVector(right_centor));
				/**
				 * 1. new root page
				 * 2. update page and new_page's Opaque data
				 * 3. add left_centor and right_centor to root page
				 */
				// P_NEW is not race-condition-proof, so we need to add a lock
				// when we try to New a Page for index.
				// 1. new root page and update meta
				LockRelationForExtension(index, ExclusiveLock);
				Buffer new_root_buffer = m3vNewBuffer(index, MAIN_FORKNUM);
				elog(INFO, "New Buffer And Lock %d 261", new_root_buffer);
				// record wal log
				Page new_root_page = GenericXLogRegisterBuffer(state, new_root_buffer, GENERIC_XLOG_FULL_IMAGE);
				// elog(INFO, "resgister %d 261", new_root_buffer);
				BlockNumber new_root_page_block_number = BufferGetBlockNumber(new_root_buffer);
				m3vInitPage(new_root_buffer, new_root_page, InvalidBlockNumber, M3V_INNER_PAGE_TYPE, 1, InvalidOffsetNumber);
				// set new root
				m3vUpdateMetaPage(index, new_root_page_block_number, MAIN_FORKNUM);
				UnlockRelationForExtension(index, ExclusiveLock);

				// 2. update page and new_page's Opaque data
				m3vUpdatePageOpaque(FirstOffsetNumber, 0, new_root_page_block_number, temp_page, M3V_LEAF_PAGE_TYPE);
				m3vUpdatePageOpaque(OffsetNumberNext(FirstOffsetNumber), 0, new_root_page_block_number, new_page, M3V_LEAF_PAGE_TYPE);

				// 3.add left_centor and right_centor to root page
				m3vElement left_element = m3vInitElement(NULL, left_radius, 0, root_block, DatumGetVector(left_centor));
				m3vElement right_element = m3vInitElement(NULL, right_radius, 0, right_page_block_number, DatumGetVector(right_centor));
				Assert(PageGetFreeSpace(new_root_page) > 2 * etupCombineSize);
				WriteElementToPage(left_element, new_root_page);
				WriteElementToPage(right_element, new_root_page);
				// restore
				PageRestoreTempPage(temp_page, page);
				/* Commit */
				MarkBufferDirty(buf);
				MarkBufferDirty(new_root_buffer);
				MarkBufferDirty(right_buf);

				/* Unlock previous buffer */
				UnlockReleaseBuffer(right_buf);
				elog(INFO, "UnLock Buffer 284 %d", right_buf);
				UnlockReleaseBuffer(new_root_buffer);
				elog(INFO, "UnLock Buffer 286 %d", new_root_buffer);
				UnlockReleaseBuffer(buf);
				elog(INFO, "UnLock Buffer 288 %d", buf);
				/**
				 * Debug Left Page, Right Page, Root Page
				 */
				PrintLeafPageVectors("left page: ", page);
				PrintLeafPageVectors("right page: ", new_page);
				PrintInternalPageVectors("root page: ", new_root_page);

				DebugPageOpaque("left page opaque", page, root_block);
				DebugPageOpaque("right page opaque", new_page, right_page_block_number);
				DebugPageOpaque("root page opaque", new_root_page, new_root_page_block_number);
				DebugMetaPage(m3vGetMetaPageInfo(index));
				DebugEntirem3vTree(m3vGetMetaPageInfo(index).root, index, 0);
				return false;
			}
			else
			{

				// Insert Parent
				/**
				 * 1. Split Page and give left centor,right centor,
				 * 2. update new_page's Opaque data
				 * 3. Do UpdateParentRecurse
				 */
				// 1. do split
				Page temp_page = SplitLeafPage(page, new_page, procinfo, collation, etup_leaf, &left_centor, &right_centor, &left_radius, &right_radius);

				BlockNumber parent_blkno = m3vPageGetOpaque(page)->parent_blkno;
				Buffer parent_buf = ReadBuffer(index, parent_blkno);
				// No need Lock, we have lock in find path
				// LockBuffer(parent_buf, BUFFER_LOCK_EXCLUSIVE);
				Page parent_page = GenericXLogRegisterBuffer(state, parent_buf, GENERIC_XLOG_FULL_IMAGE);
				// 2. update new_page's Opaque data
				m3vUpdatePageOpaque(OffsetNumberNext(PageGetMaxOffsetNumber(parent_page)), 0, parent_blkno, new_page, M3V_LEAF_PAGE_TYPE);

				// 3.add left_centor and right_centor to root page
				m3vElement left_element = m3vInitElement(NULL, left_radius, 0, root_block, DatumGetVector(left_centor));
				m3vElement right_element = m3vInitElement(NULL, right_radius, 0, right_page_block_number, DatumGetVector(right_centor));

				m3vElementTuple left_etup = palloc0(etupSize);
				m3vElementTuple right_etup = palloc0(etupSize);
				m3vSetElementTuple(left_etup, left_element);
				m3vSetElementTuple(right_etup, right_element);

				// update opaque (just new_page for now, temp_page can hold on, if there is something changed, it will update in UpdateParentRecurse() )
				m3vUpdatePageOpaque(OffsetNumberNext(PageGetMaxOffsetNumber(parent_page)), 0, parent_blkno, new_page, M3V_LEAF_PAGE_TYPE);

				UpdateParentRecurse(parent_page, parent_blkno, index, procinfo, collation, temp_page, new_page, left_etup, right_etup, m3vPageGetOpaque(temp_page)->offset, state);
				// restore
				PageRestoreTempPage(temp_page, page);
				PrintLeafPageVectors("test leaf page", temp_page);
				PrintLeafPageVectors("test new leaf page", new_page);
				PrintInternalPageVectors("after UpdateParentRecurse ", parent_page);
				elog(INFO, "parent page blkno %d", parent_blkno);
				/* remove theses, just for debug,please rewrite Commit */
				MarkBufferDirty(buf);
				MarkBufferDirty(right_buf);
				MarkBufferDirty(parent_buf);
				// UnlockReleaseBuffer(buf);
				UnlockReleaseBuffer(right_buf);
				elog(INFO, "UnLock Buffer 342 %d", right_buf);
				// no need to unlock, unlock will be done in UpdateParentRecurse.
				ReleaseBuffer(parent_buf);
				elog(INFO, "Just Release Buffer 345 %d", parent_buf);
				// need to unlock, unlock will not be done in UpdateParentRecurse.
				// it says the leaf page is buf, it will split into right_buf and buf, so unlock buf,
				// also unlock right_buf above. But we don't need to release buf, beacuse it will
				// be released in this Insertm3v func tail.
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				elog(INFO, "UnLock Buffer 351 %d", buf);
				DebugEntirem3vTree(3, index, 0);
			}
		}
	}
	else
	{
		// Internal Page, Update Parent recursively
		// 1. find sutiable entry and insert data
		// algorithm: choose the entry which make distance(entry,insert_data) <= radius(entry)
		// if there are multi statisfied ones, choose the minium distance(entry,insert_data)
		// if none, choose minium distance(entry,insert_data), where distance(entry,insert_data) > radius(entry)

		// init left heap and right heap
		m3vPairingHeapUtils heap;
		OffsetNumber offsets;
		BlockNumber son_page;
		// Attention optimization: reuse heap
		offsets = PageGetMaxOffsetNumber(page);
		heap.left = pairingheap_allocate(CompareDistanceOnlyNearestCandidates, NULL);
		heap.right = pairingheap_allocate(CompareDistanceOnlyNearestCandidates, NULL);
		for (int offset = FirstOffsetNumber; offset <= offsets; offset = OffsetNumberNext(offset))
		{
			m3vElementTuple etup = (m3vElementTuple)PageGetItem(page, PageGetItemId(page, offset));
			m3vDistanceOnlyCandidate *candidate = palloc0(sizeof(m3vDistanceOnlyCandidate));
			candidate->distance = GetDistance(PointerGetDatum(&etup->vec), PointerGetDatum(element->vec), procinfo, collation);
			candidate->son_page = etup->son_page;

			elog(INFO, "distance: %f, son_page: %d, radius: %f", candidate->distance, candidate->son_page, etup->radius);
			if (candidate->distance <= etup->radius)
			{
				pairingheap_add(heap.left, &(CreatePairingDistanceOnlyHeapNode(candidate)->ph_node));
			}
			else
			{
				pairingheap_add(heap.right, &(CreatePairingDistanceOnlyHeapNode(candidate)->ph_node));
			}
		}
		if (!pairingheap_is_empty(heap.left))
		{
			son_page = ((m3vPairingDistanceOnlyHeapNode *)pairingheap_remove_first(heap.left))->inner->son_page;
		}
		else
		{
			son_page = ((m3vPairingDistanceOnlyHeapNode *)pairingheap_remove_first(heap.right))->inner->son_page;
		}
		append_only = Insertm3v(son_page, index, element, isnull, heapRel, procinfo, collation, state);
	}
	if (append_only)
	{
		UnlockReleaseBuffer(buf);
		elog(INFO, "UnLock Buffer 395 %d", buf);
	}
	else
	{
		ReleaseBuffer(buf);
		elog(INFO, "Just Release Buffer %d", buf);
	}
	elog(INFO, "new root b: %d", m3vGetMetaPageInfo(index).root);
	// DebugEntirem3vTree(m3vGetMetaPageInfo(index).root, index, 0);
	// if (m3vPageGetOpaque(page)->parent_blkno == 3)
	// {
	// 	DebugEntirem3vTree(3, index, 0);
	// }
	DebugEntirem3vTree(3, index, 0);
	return append_only;
}

/**
 * Split Update up, remember to update the child when the internal
 * page splits.
 * 1. parent_page is the one who needs to insert left_centor and right_centor
 * 2. left centor and right centor is from parent_page's child.
 * 3. remember update distance_to_parent for left_centor and right_centor, offset
 *
 * Attention: We have locked parent page outside, but we will unlock all buffer here.
 */

/**
 *	We should maintain two maps in MetaPage: (Optimizations)
 *	1. blockno -> parent_page
 *	2. blockno -> offset (in parent page)
 *	Why do we need this? because the data in m3v page is not sorted. It's not like the traditonal Bplus tree.
 */
void UpdateParentRecurse(Page parent_page, BlockNumber parent_block_num, Relation index, FmgrInfo *procinfo, Oid collation, Page left_son_page, Page right_son_page, m3vElementTuple left_centor, m3vElementTuple right_centor, OffsetNumber left_offset, GenericXLogState *state)
{
	PrintVector("UpdateParentRecurse left centor: ", &left_centor->vec);
	PrintVector("UpdateParentRecurse right centor: ", &right_centor->vec);
	Size etupSize;
	Size etupCombineSize;
	etupSize = M3V_ELEMENT_TUPLE_SIZE(left_centor->vec.dim);
	etupCombineSize = etupSize + sizeof(ItemIdData);
	// need to split?
	// Attention: We just to compare one size, beacuse left_centor will be replaced, not append
	// maybe we can maintain a page mapping
	// if (PageGetFreeSpace(parent_page) < etupCombineSize)
	if (PageGetMaxOffsetNumber(parent_page) >= 2)
	{
		// left_copy_up and right_copy_up are all m3vElementTuple
		Datum left_copy_up;
		Datum right_copy_up;
		float8 left_radius;
		float8 right_radius;
		Page new_page;
		// P_NEW is not race-condition-proof, so we need to add a lock
		// when we try to New a Page for index.
		LockRelationForExtension(index, ExclusiveLock);
		Buffer right_buf = m3vNewBuffer(index, MAIN_FORKNUM);
		elog(INFO, "New Buffer And Lock %d 453", right_buf);
		BlockNumber right_page_block_number = BufferGetBlockNumber(right_buf);
		UnlockRelationForExtension(index, ExclusiveLock);
		// record wal log
		new_page = GenericXLogRegisterBuffer(state, right_buf, GENERIC_XLOG_FULL_IMAGE);
		// elog(INFO, "resgister %d 453", right_buf);
		m3vInitPage(right_buf, new_page, InvalidBlockNumber, M3V_INNER_PAGE_TYPE, 0, InvalidOffsetNumber);
		Page temp_page = SplitInternalPage(parent_page, new_page, procinfo, collation, left_centor, right_centor, left_offset, &left_copy_up, &right_copy_up, &left_radius, &right_radius);
		// After Split, we should update the left_son_page and right_son_page
		// Attentation: just read buffer;modify;mark dirty;commit;release buffer. (it's safe, we have a exclusive lock above)
		// in the future, we will maintain a page mapping in meta_page for parent blkno and offset. (optimization)
		// 1. for left page, just update offset
		for (OffsetNumber offset = FirstOffsetNumber; offset <= PageGetMaxOffsetNumber(temp_page); offset = OffsetNumberNext(offset))
		{
			// it must be m3vElementTuple, because temp page and new page are all internal page
			BlockNumber son_page = ((m3vElementTuple)PageGetItem(temp_page, PageGetItemId(temp_page, offset)))->son_page;
			Buffer buffer = ReadBuffer(index, son_page);
			Page page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
			m3vUpdatePageOpaque(offset, 0, parent_block_num, page, PageType(m3vPageGetOpaque(page)->type));
			// commit
			MarkBufferDirty(buffer);
			ReleaseBuffer(buffer);
		}
		// 2. for right page, update parent_block and offset.(parent block can be infered before split, can we do some optimization here?)
		for (OffsetNumber offset = FirstOffsetNumber; offset <= PageGetMaxOffsetNumber(new_page); offset = OffsetNumberNext(offset))
		{
			BlockNumber son_page = ((m3vElementTuple)PageGetItem(new_page, PageGetItemId(new_page, offset)))->son_page;
			Buffer buffer = ReadBuffer(index, son_page);
			Page page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
			m3vUpdatePageOpaque(offset, 0, right_page_block_number, page, PageType(m3vPageGetOpaque(page)->type));
			// commit
			MarkBufferDirty(buffer);
			ReleaseBuffer(buffer);
		}

		if (is_root(m3vPageGetOpaque(parent_page)->type))
		{
			// populate new root page, update meta
			/**
			 * 1. new root page
			 * 2. update page and new_page's Opaque data
			 * 3. add left_centor and right_centor to root page
			 */
			// P_NEW is not race-condition-proof, so we need to add a lock
			// when we try to New a Page for index.
			// 1. new root page and update meta
			LockRelationForExtension(index, ExclusiveLock);
			Buffer new_root_buffer = m3vNewBuffer(index, MAIN_FORKNUM);
			elog(INFO, "New Buffer And Lock %d 474", new_root_buffer);
			// record wal log
			Page new_root_page = GenericXLogRegisterBuffer(state, new_root_buffer, GENERIC_XLOG_FULL_IMAGE);

			// elog(INFO, "resgister %d 471", new_root_buffer);
			BlockNumber new_root_page_block_number = BufferGetBlockNumber(new_root_buffer);
			m3vInitPage(new_root_buffer, new_root_page, InvalidBlockNumber, M3V_INNER_PAGE_TYPE, 1, InvalidOffsetNumber);
			// set new root
			m3vUpdateMetaPage(index, new_root_page_block_number, MAIN_FORKNUM);
			UnlockRelationForExtension(index, ExclusiveLock);

			// 2. update page and new_page's Opaque data
			m3vUpdatePageOpaque(FirstOffsetNumber, 0, new_root_page_block_number, temp_page, M3V_INNER_PAGE_TYPE);
			m3vUpdatePageOpaque(OffsetNumberNext(FirstOffsetNumber), 0, new_root_page_block_number, new_page, M3V_INNER_PAGE_TYPE);

			// 3.add left_centor and right_centor to root page
			m3vElement left_element = DatumGetm3vElementTuple(left_copy_up);
			m3vElement right_element = DatumGetm3vElementTuple(right_copy_up);
			left_element->son_page = parent_block_num;
			right_element->son_page = right_page_block_number;
			Assert(PageGetFreeSpace(new_root_page) > 2 * etupCombineSize);
			PageAddItem(new_root_page, (Item)left_element, etupSize, InvalidOffsetNumber, false, false);
			PageAddItem(new_root_page, (Item)right_element, etupSize, InvalidOffsetNumber, false, false);
			MarkBufferDirty(new_root_buffer);
			UnlockReleaseBuffer(new_root_buffer);
			elog(INFO, "UnLock Buffer 497 %d", new_root_buffer);
			UnlockReleaseBuffer(right_buf);
			elog(INFO, "UnLock Buffer 499 %d", right_buf);
			// restore temp page
			PageRestoreTempPage(temp_page, parent_page);
			PrintInternalPageVectors("test parent_page", parent_page);
			elog(INFO, "test parent_page %d", parent_block_num);
			DebugEntirem3vTree(3, index, 0);
			// unlock current page buffer.
			Buffer parent_buffer = ReadBuffer(index, parent_block_num);
			UnlockReleaseBuffer(parent_buffer);
			elog(INFO, "UnLock Buffer 503 %d", parent_buffer);
		}
		else
		{
			// update son page
			m3vElement left_element = DatumGetm3vElementTuple(left_copy_up);
			m3vElement right_element = DatumGetm3vElementTuple(right_copy_up);
			left_element->son_page = parent_block_num;
			right_element->son_page = right_page_block_number;

			Buffer new_parent_buf = ReadBuffer(index, m3vPageGetOpaque(parent_page)->parent_blkno);
			// we have locked outside.
			// LockBuffer(new_parent_buf, BUFFER_LOCK_EXCLUSIVE);
			// wal log record
			Page new_parent_page = GenericXLogRegisterBuffer(state, new_parent_buf, GENERIC_XLOG_FULL_IMAGE);
			// elog(INFO, "resgister %d 500", new_parent_buf);
			// unlock current page buffer.
			Buffer parent_buffer = ReadBuffer(index, parent_block_num);
			UnlockReleaseBuffer(parent_buffer);
			elog(INFO, "UnLock Buffer 516 %d", parent_buffer);
			UpdateParentRecurse(new_parent_page, m3vPageGetOpaque(parent_page)->parent_blkno, index, procinfo, collation, temp_page, new_page, DatumGetm3vElementTuple(left_copy_up), DatumGetm3vElementTuple(right_copy_up), m3vPageGetOpaque(parent_page)->offset, state);
			// restore temp page
			elog(INFO, "prent blkno: %d", m3vPageGetOpaque(parent_page)->parent_blkno);
			elog(INFO, "prent blkno: %d", m3vPageGetOpaque(temp_page)->parent_blkno);
			// in uppper UpdateParentRecurse, we will update the son_page's parent_block,
			// so the newest parent_blkno is in parent_page.
			BlockNumber newest_blkno = m3vPageGetOpaque(parent_page)->parent_blkno;
			PageRestoreTempPage(temp_page, parent_page);
			m3vUpdatePageOpaqueParentBlockNumber(newest_blkno, parent_page);
			PrintInternalPageVectors("test parent_page", parent_page);
			MarkBufferDirty(new_parent_buf);
			ReleaseBuffer(new_parent_buf);
			UnlockReleaseBuffer(right_buf);
			DebugEntirem3vTree(parent_block_num, index, 0);
			elog(INFO, "UnLock Buffer 522 %d", right_buf);
		}
	}
	else
	{
		// replace and append directly
		// overwirte takes effect??
		PageReplaceItem(parent_page, left_offset, (Item)left_centor, etupSize);
		PrintInternalPageVectors("after replace item", parent_page);
		// PageAddItem(parent_page, (Item)left_centor, etupSize, left_offset, true, false);
		PageAddItem(parent_page, (Item)right_centor, etupSize, InvalidOffsetNumber, false, false);
		BlockNumber blkno = m3vPageGetOpaque(parent_page)->parent_blkno;
		// unlock current page buffer.
		Buffer parent_buffer = ReadBuffer(index, parent_block_num);
		UnlockReleaseBuffer(parent_buffer);
		elog(INFO, "UnLock Buffer 536 %d", parent_buffer);
		// we need to unlock until root. todo!
		while (blkno != InvalidBlockNumber)
		{
			Buffer buffer = ReadBuffer(index, blkno);
			blkno = (m3vPageGetOpaque(GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE))->parent_blkno);
			UnlockReleaseBuffer(buffer);
			elog(INFO, "UnLock Buffer 543 %d", buffer);
		}
		DebugEntirem3vTree(3, index, 0);
	}
}

/*
 * Insert a tuple into the index
 */
bool m3vinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
				 Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
				 ,
				 bool indexUnchanged
#endif
				 ,
				 IndexInfo *indexInfo)
{
	MemoryContext oldCtx;
	MemoryContext insertCtx;

	/* Skip nulls */
	if (isnull[0])
		return false;

	/* Create memory context */
	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "M3v insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);
	oldCtx = MemoryContextSwitchTo(insertCtx);
	XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 150);
	GenericXLogState *state = GenericXLogStart(index);
	/* Insert tuple */
	m3vInsertTuple(index, values, isnull, heap_tid, heap, NULL, state);
	GenericXLogFinish(state);
	/* Delete memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);

	return false;
}

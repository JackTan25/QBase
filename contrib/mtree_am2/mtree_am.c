/*-------------------------------------------------------------------------
 *
 * mtree_am.c
 *	  mtree table access method code
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  mtree_am/mtree_am.c
 *
 *
 * NOTES
 *	  This file introduces the table access method mtree, which can
 *	  be used as a template for other table access methods, and guarantees
 *	  that any data inserted into it gets sent to the void.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"
#include "utils/rel.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/amapi.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "utils_mtree/utils_tup.h"
#include "access/hio.h"
#include "utils/snapmgr.h"
#include "storage/bufmgr.h"
#include "mtree/mtree.h"
#include "storage/bufpage.h"
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(mtree_am_handler);

Oid *getOids(Relation relation);

typedef struct RangeQueryNode
{
	TupleTableSlot slot;
	struct RangeQueryNode *next;
} RangeQueryNode;

/* Base structures for scans */
typedef struct MtreeScanDescData
{
	TableScanDescData rs_base; /* AM independent part of the descriptor */

	/* Add more fields here as needed by the AM. */
	// rs_ctup are used to get the tableTupleSlot
	HeapTupleData rs_ctup;
	// current_buffer is the page buffer in buffer pool
	Buffer current_buffer;
	// BlockNumber root_number;
	int blockNums;
	int current_number;
	int offsetNums;
	int LastMaxOffsetNums;
	OffsetNumber current_offsetNum;
	// mtree_scan_range_head
	RangeQueryNode *header;
} MtreeScanDescData;
typedef struct MtreeScanDescData *MtreeScanDesc;

typedef struct Wrappe
{
	Page page_new;
	HeapTuple op1;
	HeapTuple op2;
} Wrapper;

Wrapper GetWrapper(Page page, HeapTuple tup1, HeapTuple tup2)
{
	Wrapper wrap;
	wrap.op1 = tup1;
	wrap.op2 = tup2;
	wrap.page_new = page;
	return wrap;
}

static const TableAmRoutine mtree_methods;

HeapTuple FetchHeapTuple(Relation rel, Page page, OffsetNumber offset);
static float8 Distance(Relation rel, HeapTuple tuple1, HeapTuple tuple2);
static bool IsLeafPage(Relation rel, Page page);
/*
 * Read in a buffer in mode, using bulk-insert strategy if bistate isn't NULL.
 */
static Buffer
ReadBufferBI(Relation relation, BlockNumber targetBlock,
			 ReadBufferMode mode, BulkInsertState bistate)
{
	Buffer buffer;

	/* If not bulk-insert, exactly like ReadBuffer */
	if (!bistate)
		return ReadBufferExtended(relation, MAIN_FORKNUM, targetBlock,
								  mode, NULL);

	/* If we have the desired block already pinned, re-pin and return it */
	if (bistate->current_buf != InvalidBuffer)
	{
		if (BufferGetBlockNumber(bistate->current_buf) == targetBlock)
		{
			/*
			 * Currently the LOCK variants are only used for extending
			 * relation, which should never reach this branch.
			 */
			Assert(mode != RBM_ZERO_AND_LOCK &&
				   mode != RBM_ZERO_AND_CLEANUP_LOCK);

			IncrBufferRefCount(bistate->current_buf);
			return bistate->current_buf;
		}
		/* ... else drop the old buffer */
		ReleaseBuffer(bistate->current_buf);
		bistate->current_buf = InvalidBuffer;
	}

	/* Perform a read using the buffer strategy */
	buffer = ReadBufferExtended(relation, MAIN_FORKNUM, targetBlock,
								mode, bistate->strategy);

	/* Save the selected block as target for future inserts */
	IncrBufferRefCount(buffer);
	bistate->current_buf = buffer;

	return buffer;
}

/* ------------------------------------------------------------------------
 * Slot related callbacks for mtree AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
mtree_slot_callbacks(Relation relation)
{
	/*
	 * Here you would most likely want to invent your own set of
	 * slot callbacks for your AM.
	 */
	// return &TTSOpsMinimalTuple;
	// src/backend/executor/nodeSeqscan.c: ExecInitScanTupleSlot
	// table_slot_callbacks use these to get the slot
	return &TTSOpsBufferHeapTuple;
}

/* ------------------------------------------------------------------------
 * Table Scan Callbacks for mtree AM
 * ------------------------------------------------------------------------
 */

static TableScanDesc
mtree_scan_begin(Relation relation, Snapshot snapshot,
				 int nkeys, ScanKey key,
				 ParallelTableScanDesc parallel_scan,
				 uint32 flags)
{
	MtreeScanDesc scan;
	elog(INFO, "mtree_scan_begin");
	scan = (MtreeScanDesc)palloc(sizeof(MtreeScanDescData));
	scan->header = palloc(sizeof(RangeQueryNode));
	scan->header->next = NULL;
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;
	// scan->rs_ctup.t_tableOid = RelationGetRelid(relation);
	// return (TableScanDesc) scan;
	// Buffer buf = ReadBuffer(relation,0);
	// scan->current_buffer = buf;
	// scan->root_number = 0;
	scan->rs_ctup.t_tableOid = RelationGetRelid(relation);
	scan->blockNums = RelationGetNumberOfBlocks(scan->rs_base.rs_rd);
	scan->current_number = 0;
	if (scan->blockNums > 0)
	{
		Buffer buf = ReadBuffer(scan->rs_base.rs_rd, scan->current_number);
		Page page = BufferGetPage(buf);
		scan->offsetNums = PageGetMaxOffsetNumber(page);
		scan->current_buffer = buf;
	}
	else
	{
		// created compaction delete
		scan->current_buffer = -1;
		scan->offsetNums = 0;
	}

	scan->current_offsetNum = FirstOffsetNumber;

	// get last block's maxOffsetNum
	if (scan->blockNums > 0)
	{
		Buffer buf = ReadBuffer(scan->rs_base.rs_rd, scan->blockNums - 1);
		Page page = BufferGetPage(buf);
		scan->LastMaxOffsetNums = PageGetMaxOffsetNumber(page);
		ReleaseBuffer(buf);
	}
	return (TableScanDesc)scan;
}

static void
mtree_scan_end(TableScanDesc sscan)
{
	// heap_endscan(sscan);
	MtreeScanDesc scan = (MtreeScanDesc)sscan;
	if (scan->current_buffer != -1)
	{
		ReleaseBuffer(scan->current_buffer);
	}
	UnlockBuffers();
	LWLockReleaseAll();
	elog(INFO, "mtree_scan_end");
	pfree(scan);
}

static void
mtree_scan_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
				  bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	/* nothing to do */
	elog(INFO, "mtree_scan_rescan");
}

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

void add_range_query_result(MtreeScanDesc sscan, HeapTuple tuple, Page page, Buffer buffer, OffsetNumber offset)
{
	RangeQueryNode *node = palloc(sizeof(RangeQueryNode));
	ItemId lpp = PageGetItemId(page, offset);
	sscan->rs_ctup.t_data = (HeapTupleHeader)PageGetItem(page, lpp);
	sscan->rs_ctup.t_len = ItemIdGetLength(lpp);
	ItemPointerSet(&(sscan->rs_ctup.t_self), page, offset);
	ExecStoreBufferHeapTuple(&sscan->rs_ctup, &node->slot, buffer);
	node->next = sscan->header->next;
	sscan->header->next = node;
}

// 对于单模态的而言: select * from t where a <-> const < r;
// 对于多模态而言: select * from t where (a,b,c) <-> const < r;
// 表达式下推: 我们需要知道where 后面的这一部分怎么下推到最下层这里来,这一点应该是没有太多的难度的,
// 目前打通就是直接使用即可,后续下推开箱即用
void RangeQuery(Relation rel, MtreeScanDesc sscan, HeapTuple target, Page n, Buffer buffer, double r, double target_parent_distance, bool is_root)
{
	if (IsLeafPage(rel, n))
	{
		for (int i = FirstOffsetNumber; i <= PageGetMaxOffsetNumber(n); i++)
		{
			// 获取offsetnum处的Tuple
			HeapTuple data = FetchHeapTuple(rel, n, i);
			MtreeEntry *entry = DecodeTupleToEntry(data, getOids(rel));
			// float8 distance = Distance(rel,data,target);
			if (abs(entry->distance_to_parent - target_parent_distance) <= r)
			{
				if (Distance(rel, data, target) <= r)
				{
					add_range_query_result(sscan, data, n, buffer, i);
				}
			}
		}
	}
	else
	{
		for (int i = FirstOffsetNumber; i <= PageGetMaxOffsetNumber(n); i++)
		{
			// 获取offsetnum处的Tuple
			HeapTuple data = FetchHeapTuple(rel, n, i);
			MtreeEntry *entry = DecodeTupleToEntry(data, getOids(rel));
			float8 distance = Distance(rel, data, target);
			Buffer buffer = ReadBuffer(rel, entry->son_page);
			Page page = BufferGetPage(buffer);
			if (is_root)
			{
				if (entry->radius + r >= distance)
				{
					// RangeQuery(entry.son,target,r,distance(entry,target));
					RangeQuery(rel, sscan, target, page, buffer, r, distance, false);
				}
			}
			else
			{
				if (abs(entry->distance_to_parent - target_parent_distance) <= r + entry->radius)
				{
					if (Distance(rel, data, target) <= r)
					{
						RangeQuery(rel, sscan, target, page, buffer, r, distance, false);
					}
				}
			}
		}
	}
}

/*
 * KNN 查询算法流程:
 *	找到n位根的树中距离target最近的k个点
 *	D_k = +无穷
 *   func_dmin_node(Object node,Object target,double r){
 *		max(distance(O(node),target)-r,0);
 *	}
 *
 *	func_dmax_node(Object node,Object target,double r){
 *		distance(O(node),target)+r
 *	}
 *
 *	1. 初始化优先队列pr{root,D_k,-1};
 *	2. 初始化结果优先队列NN{};
 * 	KnnQuery(Object n,Object target,int k):
 *		3. chooseNode: 从Pr当中拿到最适合的node
 *		while pr.size() > 0{
 *			p =  pr.pop();
 *			4. KnnNodeQuery(p.node,target,k,p.target_parent_distance);
 *		}
 *		return NN.onjects
 *	KnnNodeQuery(Object node,Object target,int k,double target_parent_distance):
 *		5.1 不是叶子节点
 *			遍历每一个entry
 *			5.1.1 根节点
 *				dist = func_dmin_node(entry,target,R(entry))
 *				if dist <= D_k
 *					pr.push({entry.son,dist,distance(enrty,target)})
 *				// 其实最多达到k
 *				NN.push({NULL,func_dmax_node(entry,target,R(entry))})
 *				while(NN.size() > k)
 *					NN.pop();
 *				d_k = top(NN).second
 *			5.1.2 非根节点
 *				// 基本原则: 如果区域最边缘的点到Q的距离(也就是区域距离Q最近的点)小于dk，那么纳入考虑
 *				if |distance_p(entry) - target_parent_distance| <= D_K + R(entry)
 *					do 5.1.1
 *		5.2 是叶子节点
 *			遍历每一个entry
 *			if |distance_p(entry) - target_parent_distance| <= D_K
 *					dist = func_dmin_node(entry,target,R(entry))
 *					if dist <= D_k
 *						NN.push({NULL,distance(entry,target)})
 *						while(NN.size() > k)
 *							NN.pop();
 *						d_k = top(NN).second
 * 注意我们每次查询到一个结果都是给到slot来做.
 */
// 优先队列选择使用 src/include/lib/pairingheap.h
void KnnNodeQuery(Page n, HeapTuple target, int k, double target_parent_distance)
{
}

void KnnQuery(Page n, HeapTuple target, int k)
{
}

static bool
mtree_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
					   TupleTableSlot *slot)
{
	MtreeScanDesc scan = (MtreeScanDesc)sscan;
	// heapgettup_pagemode(),heapgettup();参考这些方法来给rs_ctup赋值
	// ExecStoreBufferHeapTuple(&scan->rs_ctup, slot,scan->rs_cbuf);
	/* nothing to do */
	if (scan->current_buffer == -1)
	{
		return false;
	}
	Page page = BufferGetPage(scan->current_buffer);
	// 1. Is there rested items in current_page?
	if (scan->current_offsetNum <= scan->offsetNums)
	{
		// try to read next item
		ItemId lpp = PageGetItemId(page, scan->current_offsetNum);
		scan->rs_ctup.t_data = (HeapTupleHeader)PageGetItem(page, lpp);
		scan->rs_ctup.t_len = ItemIdGetLength(lpp);
		ItemPointerSet(&(scan->rs_ctup.t_self), page, scan->current_offsetNum);
		ExecStoreBufferHeapTuple(&scan->rs_ctup, slot, scan->current_buffer);
		scan->current_offsetNum++;
		// elog(INFO,"mtree_scan_getnextslot");
		UnlockBuffers();
		LWLockReleaseAll();
		return true;
	}
	else if (scan->current_number < scan->blockNums - 1)
	{
		// 2.Is there one more block?
		ReleaseBuffer(scan->current_buffer);
		// elog(INFO,"another block: %d ",scan->current_offsetNum-1);
		scan->current_number++;
		scan->current_offsetNum = FirstOffsetNumber;
		Buffer buf = ReadBuffer(scan->rs_base.rs_rd, scan->current_number);
		scan->current_buffer = buf;
		Page page = BufferGetPage(buf);
		if (scan->current_number == scan->blockNums - 1)
		{
			scan->offsetNums = scan->LastMaxOffsetNums;
		}
		else
		{
			scan->offsetNums = PageGetMaxOffsetNumber(page);
		}
		// elog(INFO,"maxoffsets: %d, blocknum: %d, blocknums: %d ",scan->offsetNums,scan->current_number,scan->blockNums);
		if (scan->current_offsetNum > scan->offsetNums)
		{
			UnlockBuffers();
			LWLockReleaseAll();
			return false;
		}
		ItemId lpp = PageGetItemId(page, scan->current_offsetNum);
		scan->rs_ctup.t_data = (HeapTupleHeader)PageGetItem(page, lpp);
		scan->rs_ctup.t_len = ItemIdGetLength(lpp);
		ItemPointerSet(&(scan->rs_ctup.t_self), page, scan->current_offsetNum);
		ExecStoreBufferHeapTuple(&scan->rs_ctup, slot, scan->current_buffer);
		scan->current_offsetNum++;
		// elog(INFO,"mtree_scan_getnextslot");
		UnlockBuffers();
		LWLockReleaseAll();
		return true;
	}
	else
	{
		ReleaseBuffer(scan->current_buffer);
		scan->current_buffer = -1;
		UnlockBuffers();
		LWLockReleaseAll();
		return false;
	}
	// if(scan->current_number == 0){
	// 	scan->current_number++;
	// 	return true;
	// }else{
	// 	return false;
	// }
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for mtree AM
 * ------------------------------------------------------------------------
 */

static IndexFetchTableData *
mtree_index_fetch_begin(Relation rel)
{
	elog(INFO, "mtree_index_fetch_begin");
	return NULL;
}

static void
mtree_index_fetch_reset(IndexFetchTableData *scan)
{
	/* nothing to do here */
	elog(INFO, "mtree_index_fetch_reset");
}

static void
mtree_index_fetch_end(IndexFetchTableData *scan)
{
	/* nothing to do here */
	elog(INFO, "mtree_index_fetch_end");
}

static bool
mtree_index_fetch_tuple(struct IndexFetchTableData *scan,
						ItemPointer tid,
						Snapshot snapshot,
						TupleTableSlot *slot,
						bool *call_again, bool *all_dead)
{
	/* there is no data */
	elog(INFO, "mtree_index_fetch_tuple");
	return 0;
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for
 * mtree AM.
 * ------------------------------------------------------------------------
 */

static bool
mtree_fetch_row_version(Relation relation,
						ItemPointer tid,
						Snapshot snapshot,
						TupleTableSlot *slot)
{
	/* nothing to do */
	return false;
}

static void
mtree_get_latest_tid(TableScanDesc sscan,
					 ItemPointer tid)
{
	/* nothing to do */
}

static bool
mtree_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return false;
}

static bool
mtree_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
							   Snapshot snapshot)
{
	return false;
}

static TransactionId
mtree_index_delete_tuples(Relation rel,
						  TM_IndexDeleteOp *delstate)
{
	return InvalidTransactionId;
}

HeapTuple FetchHeapTuple(Relation rel, Page page, OffsetNumber offset)
{
	HeapTuple rs_ctup = (HeapTuple)palloc0(HEAPTUPLESIZE);
	Assert(offset >= FirstOffsetNumber && offset <= PageGetMaxOffsetNumber(page));
	ItemId lpp = PageGetItemId(page, offset);
	rs_ctup->t_data = (HeapTupleHeader)PageGetItem(page, lpp);
	rs_ctup->t_len = ItemIdGetLength(lpp);
	rs_ctup->t_tableOid = RelationGetRelid(rel);
	ItemPointerSet(&(rs_ctup->t_self), page, offset);
	return rs_ctup;
}

// 用于判断一个节点是不是叶子节点
// 取出一个tuple判断是不是叶子节点即可
static bool IsLeafPage(Relation rel, Page page)
{
	// 不能是空Page
	Assert(PageGetMaxOffsetNumber(page) > 0);
	HeapTuple rs_ctup = FetchHeapTuple(rel, page, FirstOffsetNumber);
	MtreeEntry *entry = DecodeTupleToEntry(&rs_ctup, getOids(rel));
	pfree(rs_ctup);
	return entry->is_leaf;
}

// 现在为了测试,我们模拟一个distance(后面将会从sql传入)
static float8 Distance(Relation rel, HeapTuple tuple1, HeapTuple tuple2)
{
	Datum data1 = GetDatumFromTuple(rel, tuple1);
	// Datum data2 =  GetDatumFromTuple(rel,tuple2);
	// Oid oid = rel->rd_att->attrs[0].atttypid;
	// Form_pg_type type = GetType(oid);
	// if(strcmp(type->typname.data,"int4") == 0){
	// 	int32 d1 = DatumGetInt32(data1);
	// 	int32 d2 = DatumGetInt32(data2);
	// 	return (float8)(abs(d1-d2));
	// }else{
	// 	// we just support int4 for now.
	// 	exit(-1);
	// }

	// all this heaptuple is MtreeEntry
	Oid *oids = getOids(rel);
	Form_pg_type type = GetType(oids[0]);
	if (strcmp(type->typname.data, "int4") == 0)
	{
		MtreeEntry *entry1 = DecodeTupleToEntry(tuple1, oids);
		MtreeEntry *entry2 = DecodeTupleToEntry(tuple2, oids);
		int32 d1 = DatumGetInt32(entry1->entry_data);
		int32 d2 = DatumGetInt32(entry2->entry_data);
		return (float8)(abs(d1 - d2));
	}
	else
	{
		exit(-1);
	}
	return 0;
}

HeapTuple GetMinTuple(Relation rel, Page page, HeapTuple *tuples, HeapTuple tuple, bool *isCleared, OffsetNumber n, OffsetNumber *offset)
{
	float8 distance = 0x3f3f3f3f;
	OffsetNumber id = 0;
	for (int i = 1; i <= n; i++)
	{
		if (isCleared[i])
		{
			continue;
		}
		float8 dist = Distance(rel, tuple, tuples[i]);
		if (dist < distance)
		{
			distance = dist;
			id = i;
		}
	}
	if (id = 0)
	{
		return NULL;
	}
	else
	{
		isCleared[id] = true;
		*offset = id;
		return FetchHeapTuple(rel, page, id);
	}
}

void PageAddFromRawtuple(Page page, HeapTuple tuple)
{
	OffsetNumber offnum = PageAddItem(page, (Item)tuple->t_data,
									  tuple->t_len, InvalidOffsetNumber, false, true);
	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "failed to add tuple to page");
}

// reference to src/backend/access/nbtree/nbtinsert.c [_bt_split()]
// 现在要分裂page0,我们会new一个新page1,和一个临时page_tmp
// 右半部分放到page1，而左半部分放到page_tmp,然后将page_tmp全量copy到
// page0即可
// 返回一个新的page
// 分裂我们需要注意的是,对于叶子节点的分裂,我们是copy up
// 对于内部节点的分裂我们是
static Wrapper Split(Relation relation, Page page, HeapTuple insertTuple, bool isLeaf)
{
	// ToDo:
	// 1.拿到新的page
	HeapTuple tuple = NULL;
	Buffer buffer1 = ReadBufferBI(relation, P_NEW, RBM_ZERO_AND_LOCK, NULL);
	Page page1 = BufferGetPage(buffer1);
	Page page_tmp = PageGetTempPage(page);
	Oid *oids = getOids(relation);
	// init two pages
	PageInit(page1, BufferGetPageSize(buffer1), 0);
	PageInit(page_tmp, BufferGetPageSize(buffer1), 0);
	// 实现Split算法
	// 2.1 使用promote算法，随机选择
	OffsetNumber offsetNums = PageGetMaxOffsetNumber(page);
	OffsetNumber leftRand = rand() % (offsetNums / 2) + 1;
	OffsetNumber rightRand = rand() % (offsetNums - offsetNums / 2) + offsetNums / 2 + 1;
	Assert(leftRand != rightRand);
	HeapTuple tuple1 = FetchHeapTuple(relation, page, leftRand);
	HeapTuple tuple2 = FetchHeapTuple(relation, page, rightRand);
	bool *isCleared = (bool *)malloc((offsetNums + 1) * sizeof(bool));
	HeapTuple *tuples = (HeapTuple *)malloc((offsetNums + 1) * sizeof(HeapTuple));
	memset(isCleared, false, sizeof(bool) * (offsetNums + 1));
	memset(isCleared, NULL, sizeof(HeapTuple) * (offsetNums + 1));
	for (int i = FirstOffsetNumber; i <= offsetNums; i++)
	{
		tuples[i] = FetchHeapTuple(relation, page, i);
	}

	float8 distance1 = 0.0, distance2 = 0.0;
	OffsetNumber offset;
	// 2.2 Partition
	// remember to update the tuple1 and tuple2's distance
	// 对于leafPage的分裂的话我们需要采取的是的copy up
	// 对于internalPage我们需要采取的是push up
	for (int i = FirstOffsetNumber; i <= offsetNums; i++)
	{
		if (i % 2 == 0)
		{
			tuple = GetMinTuple(relation, page, tuples, tuple1, isCleared, offsetNums, &offset);
			if (tuple == NULL)
			{
				// Copy tmp -> page
				PageRestoreTempPage(page_tmp, page);
				break;
				// return GetWrapper(page1,tuple1,tuple2);
			}
			else
			{
				// 否则就走push up
				if (!isLeaf && offset == leftRand)
				{
					continue;
				}
				// 如果是LeafPage就走copy up
				distance1 = Distance(relation, tuple, tuple1);
				MtreeEntry *entry = DecodeTupleToEntry(tuple, oids);
				entry->distance_to_parent = distance1;
				PageAddFromRawtuple(page_tmp, tuple);
			}
		}
		else
		{
			tuple = GetMinTuple(relation, page, tuples, tuple2, isCleared, offsetNums, &offset);
			if (tuple == NULL)
			{
				// Copy tmp -> page
				PageRestoreTempPage(page_tmp, page);
				break;
				// return GetWrapper(page1,tuple1,tuple2);
			}
			else
			{
				distance2 = Distance(relation, tuple, tuple2);
				MtreeEntry *entry = DecodeTupleToEntry(tuple, oids);
				entry->distance_to_parent = distance2;
				PageAddFromRawtuple(page1, tuple);
			}
		}
		pfree(tuples);
		pfree(isCleared);
	}
	// update radius
	MtreeEntry *entry1 = DecodeTupleToEntry(tuple1, oids);
	MtreeEntry *entry2 = DecodeTupleToEntry(tuple2, oids);
	entry1->radius = distance1;
	entry2->radius = distance2;
	entry1->is_leaf = false;
	entry2->is_leaf = false;
	if (insertTuple != NULL)
	{
		PageAddFromRawtuple(page1, insertTuple);
	}
	return GetWrapper(page1, tuple1, tuple2); // won't get here
}

// insert算法的实现:
// 插入tuple,插入的page里面有很多tuples
// 1.如果currentId指定的Page(称之为currentNode)不是叶子节点
// 		1.1 找到所有使得distance(tuples[i],tuple) < r(tuple)
// 			如果得到的结果集不为空,选择distance(tuples[i],tuple)最小的
// 			那个插入即可
//		1.2 如果1.1得到结果集为空,那就从所有的distance(tuples[i],tuple)
//			当中找最小距离的插入
// 2.如果currentId指定的Page是叶子节点
//      2.1 如果没有满，直接放进去即可
// 		2.2 如果满了使用Split()分裂即可

// Split的算法实现:
// 	需要讲currentNode分裂为两份,这里要做两件事情
//	a. Promote() b.Partition()
//  对于a采取随机分别从左右两半选两个,对于b则是采用平衡算法
//  比如我们根据a选出了o1和o2,那么接下来我们就开始平衡算法:
//		1.使用op1去从currentNode选出距离最近的点作为分裂出来的左半节点的一部分
//			同时将这个点从currentNode当中删除
//      2.使用op2去从currentNode选出距离最近的点作为分裂出来的右半节点的一部分
//			同时将这个点从currentNode当中删除
// 循环往复直到currentNode的点被分完，我们称左半节点为N1,右半节点为N2
// 1.如果currentNode为root
//    分配新root,将o1与o2存入即可
// 2.如果currentNode不为root
//   将父亲节点的那个entry更改为op1,同时存入op2
//	 如果导致满了继续split即可（一个node最少可以容纳两个entry,对于类型长度大的需要使用外部存储的技术）
// hack:注意分裂要更新对应entry与parent相关的信息，比如distance,parentId等
// 对于叶子节点,其radius和distance_to_parent是没有意义的,我们不需要专门更新其内容
// 对于内部节点,一定要记住严格更新radius和distance_to_parent,这对于查询优化尤其重要
// ToDo: KNN search and RQ search
void insert(Relation rel, int currentId, HeapTuple tuple, int options)
{
	// 预留每一个page至少1000的space,超过就直接split
	Buffer buffer = ReadBuffer(rel, currentId);
	Page page = BufferGetPage(buffer);
	if (!IsLeafPage(rel, page))
	{
		float8 min_distance = -0x3f3f3f3f;
		OffsetNumber offset = -1;
		MtreeEntry *entry_ = NULL;
		// 寻找距离最近的那个entry
		for (int i = FirstOffsetNumber; i <= PageGetMaxOffsetNumber(page); i++)
		{
			// 获取offsetnum处的Tuple
			HeapTuple data = FetchHeapTuple(rel, page, i);
			MtreeEntry *entry = DecodeTupleToEntry(data, getOids(rel));
			float8 distance = Distance(rel, data, tuple);
			if (distance < min_distance)
			{
				min_distance = distance;
				offset = i;
				entry_ = entry;
			}
		}
		Assert(entry_ != NULL);
		Assert(offset != -1);
		insert(rel, entry_->son_page, tuple, options);
	}
	else
	{
		// 预留1000的空间，如果小于认为是满了,需要进行split操作
		if (PageGetFreeSpace(page) < 1000)
		{
			// ToDo: Split
			ChaninSplitBack(rel, page, tuple, currentId);
		}
		else
		{
			RelationPutHeapTuple(rel, buffer, tuple,
								 (options & HEAP_INSERT_SPECULATIVE) != 0);
		}
	}
	LWLockReleaseAll();
	UnlockBuffers();
}

Page GetSpeciPage(Relation rel, int currentId)
{
	Buffer buffer = ReadBuffer(rel, currentId);
	Page page = BufferGetPage(buffer);
	return page;
}

OffsetNumber findIndex(Relation relation, Page page, int page_id)
{
	OffsetNumber numbers = PageGetMaxOffsetNumber(page);
	for (OffsetNumber offset = FirstOffsetNumber; offset <= numbers; offset++)
	{
		HeapTuple data = FetchHeapTuple(relation, page, offset);
		MtreeEntry *entry = DecodeTupleToEntry(data, getOids(relation));
		if (entry->son_page == page_id)
		{
			return offset;
		}
	}
	elog(PANIC, "we can't find the entry in parent page");
}

// update parent pageId
void updateParentPageId(Relation relation, Page page, int parent_pageId)
{
	OffsetNumber numbers = PageGetMaxOffsetNumber(page);
	for (OffsetNumber offset = FirstOffsetNumber; offset <= numbers; offset++)
	{
		HeapTuple data = FetchHeapTuple(relation, page, offset);
		MtreeEntry *entry = DecodeTupleToEntry(data, getOids(relation));
		entry->parent_page = parent_pageId;
	}
}

// updatePage will replace old tuple in offset will new tuple
// for mtree, we don't need to think about the order
void updatePage(Relation relation, Page page, OffsetNumber offset, HeapTuple new_tuple)
{
	Page page_tmp = PageGetTempPage(page);
	for (OffsetNumber new_offset = FirstOffsetNumber; new_offset <= PageGetMaxOffsetNumber(page); new_offset++)
	{
		if (offset == new_offset)
		{
			HeapTuple tuple = FetchHeapTuple(relation, page, new_offset);
			PageAddFromRawtuple(page_tmp, tuple);
		}
	}
	PageAddFromRawtuple(page_tmp, new_tuple);
	PageRestoreTempPage(page_tmp, page);
}

// if we trigger the split, we will split and insert result to
// parent recursively, until finish all work.
// this `page` is a leaf page.

// ToDo: Update DistanceToParent
// ToDo: Update ParentId
// D
void ChaninSplitBack(Relation rel, Page page, HeapTuple tuple, int current_id)
{
	// split from leafPage
	// page -> page, page_new, tuple1, tuple2
	// replace old_tuple in page with tuple1
	// insert tuple2 into page
	Wrapper wrap = Split(rel, page, tuple, false);
	int rootId = GetRootPageId(rel);
	// RelationPutHeapTuple()
	Oid *oids = getOids(rel);
	MtreeEntry *entry1 = DecodeTupleToEntry(wrap.op1, oids);
	MtreeEntry *entry2 = DecodeTupleToEntry(wrap.op2, oids);
	int parentId = entry1->parent_page;
	// getParentPage
	Page parentPage = GetSpeciPage(rel, parentId);
	OffsetNumber parentOffset = findIndex(rel, parentPage, current_id);
	updatePage(rel, parentPage, parentOffset, wrap.op1);
	// ToDo: Update DistanceToParent
	// ToDo: Update ParentId
	if (PageGetFreeSpace(parentPage) < 1000)
	{
		// recursively insert and Split
		if (parentId == rootId)
		{
			// 1. is root page?
			Buffer buffer = ReadBufferBI(rel, P_NEW, RBM_ZERO_AND_LOCK, NULL);
			SetRootPageId(rel, buffer);
			Page newRootPage = BufferGetPage(buffer);
			Wrapper wrap2 = Split(rel, parentPage, wrap.op2, false);
			PageAddFromRawtuple(newRootPage, wrap2.op1);
			PageAddFromRawtuple(newRootPage, wrap2.op2);
		}
		else
		{
			// 2. not root page
			ChaninSplitBack(rel, parentPage, wrap.op2, parentId);
		}
	}
	else
	{
		// 添加新的tuple
		PageAddFromRawtuple(parentPage, wrap.op2);
	}
}

// 递归分裂,给定参数Page,PageId,InsertTuple完成递归分裂
// 这里不需要继续往下查找,而是直接向上递归的分裂即可
// rel: 要向哪一张表插入数据
// targetPage: 需要插入insertTuple 到哪一个Page
// page_id: targetPage对应的pageId
// isLeaf: targetPage是叶子page吗
// 小于1000就需要进行分裂，1000这个阈值可以保证至少还可以放两个进去
void recurseSplit(Relation rel, Page targePage, HeapTuple insertTuple, int page_id, bool isLeaf)
{
	// 每次分裂必做的事情
	// 分裂完成后
	// 1.对于上推的tuple,更新下面信息
	/*    // 1. the radius of this entry
		float8 radius; 不用管，在Split的时候已经做好了
		// 2. distance to parent
		float8 distance_to_parent;
		// 3. center data 不用管
		Datum entry_data;
		// 4. is leaf_node
		bool is_leaf; 必然是false，已经在Split做好了
		// 5. the pointer to the leaf page
		int32 son_page;
		// 6. parent_buffer
		int32 parent_page;
	*/

	// 2.对于留在老page的什么也不用动
	// 3.对于放在新page的
	/*    // 1. the radius of this entry
		float8 radius; 不需要动
		// 2. distance to parent
		float8 distance_to_parent; Split的时候就已经做好了
		// 3. center data
		Datum entry_data; 不需要动
		// 4. is leaf_node
		bool is_leaf;  不需要动
		// 5. the pointer to the leaf page
		int32 son_page; 不需要动
		// 6. parent_buffer
		int32 parent_page;
	*/
	int rootId = GetRootPageId(rel);
	if (isLeaf)
	{
		// copy up
		Wrapper wrap = Split(rel, targePage, insertTuple, true);
		Oid *oids = getOids(rel);
		MtreeEntry *entry1 = DecodeTupleToEntry(wrap.op1, oids);
		MtreeEntry *entry2 = DecodeTupleToEntry(wrap.op2, oids);
		// 1.如果本身就是rootPage
		if (page_id == rootId)
		{
			// 获取新的rootPage,
			Buffer buffer = ReadBufferBI(rel, P_NEW, RBM_ZERO_AND_LOCK, NULL);
			int nBlocks = RelationGetNumberOfBlocks(rel);
			SetRootPageId(rel, nBlocks - 1);
			Page newRootPage = BufferGetPage(buffer);
			PageInit(newRootPage, BufferGetPageSize(buffer), 0);
			// 需要放入两个新的已经完成了radius计算的tuple,这里应该是不会出现分裂的
			// 一个page最少也可以容纳2个tuples
			PageAddFromRawtuple(newRootPage, wrap.op1);
			PageAddFromRawtuple(newRootPage, wrap.op2);
			// 必做事情,对于上推的tuple
			entry1->son_page = page_id;
			entry2->son_page = page_id;
			entry1->parent_page = -1;
			entry2->parent_page = -1;
			entry1->is_leaf = false;
			entry2->is_leaf = false;
			// 必做事情,对于放在新page的
			updateParentPageId(rel, wrap.page_new, nBlocks - 1);
		}
		else
		{
			// 2.本身不是rootPage
			// 首先拿到parentPage
			int parent_pageId = entry1->parent_page;
			// 将tuple放入到新的parent_page当中当中
			Page parent_page = GetSpeciPage(rel, parent_pageId);
			PageAddFromRawtuple(parent_page, wrap.op1);
			PageAddFromRawtuple(parent_page, wrap.op2);
			// 拿一下parent_page的parentId
			MtreeEntry *entry = DecodeTupleToEntry(FetchHeapTuple(rel, parent_page, FirstOffsetNumber), oids);
			int now_parent_page_id = entry->parent_page;
			Page now_parent_page = GetSpeciPage(rel, now_parent_page_id);
			// 必做事情,对于上推的tuple
			entry1->son_page = page_id;
			entry2->son_page = page_id;
			entry1->is_leaf = false;
			entry2->is_leaf = false;
			entry1->parent_page = now_parent_page_id;
			entry2->parent_page = now_parent_page_id;
			// 关键的distance_to_parent
			OffsetNumber offset = findIndex(rel, now_parent_page, parent_pageId);
			HeapTuple tuple = FetchHeapTuple(rel, now_parent_page, offset);
			entry = DecodeTupleToEntry(tuple, oids);
			entry1->distance_to_parent = Distance(rel, tuple, wrap.op1);
			entry2->distance_to_parent = Distance(rel, tuple, wrap.op2);
			// 必做事情,对于放在新page的
			updateParentPageId(rel, wrap.page_new, parent_pageId);
			if (PageGetFreeSpace(parent_page) < 1000)
			{
				recurseSplit(rel, parent_page, NULL, parent_pageId, false);
			}
		}
	}
	else
	{
		// copy up
		Wrapper wrap = Split(rel, targePage, insertTuple, true);
		Oid *oids = getOids(rel);
		MtreeEntry *entry1 = DecodeTupleToEntry(wrap.op1, oids);
		MtreeEntry *entry2 = DecodeTupleToEntry(wrap.op2, oids);
		// 1.如果本身就是rootPage
		if (page_id == rootId)
		{
			// 获取新的rootPage,
			Buffer buffer = ReadBufferBI(rel, P_NEW, RBM_ZERO_AND_LOCK, NULL);
			int nBlocks = RelationGetNumberOfBlocks(rel);
			SetRootPageId(rel, nBlocks - 1);
			Page newRootPage = BufferGetPage(buffer);
			PageInit(newRootPage, BufferGetPageSize(buffer), 0);
			// 需要放入两个新的已经完成了radius计算的tuple,这里应该是不会出现分裂的
			// 一个page最少也可以容纳2个tuples
			PageAddFromRawtuple(newRootPage, wrap.op1);
			PageAddFromRawtuple(newRootPage, wrap.op2);
			// 必做事情,对于上推的tuple
			entry1->son_page = page_id;
			entry2->son_page = page_id;
			entry1->parent_page = -1;
			entry2->parent_page = -1;
			entry1->is_leaf = false;
			entry2->is_leaf = false;
			// 必做事情,对于放在新page的
			updateParentPageId(rel, wrap.page_new, nBlocks - 1);
		}
		else
		{
			// 2.本身不是rootPage
			// 首先拿到parentPage
			int parent_pageId = entry1->parent_page;
			// 将tuple放入到新的parent_page当中当中
			Page parent_page = GetSpeciPage(rel, parent_pageId);
			PageAddFromRawtuple(parent_page, wrap.op1);
			PageAddFromRawtuple(parent_page, wrap.op2);
			// 拿一下parent_page的parentId
			MtreeEntry *entry = DecodeTupleToEntry(FetchHeapTuple(rel, parent_page, FirstOffsetNumber), oids);
			int now_parent_page_id = entry->parent_page;
			Page now_parent_page = GetSpeciPage(rel, now_parent_page_id);
			// 必做事情,对于上推的tuple
			entry1->son_page = page_id;
			entry2->son_page = page_id;
			entry1->is_leaf = false;
			entry2->is_leaf = false;
			entry1->parent_page = now_parent_page_id;
			entry2->parent_page = now_parent_page_id;
			// 关键的distance_to_parent
			OffsetNumber offset = findIndex(rel, now_parent_page, parent_pageId);
			HeapTuple tuple = FetchHeapTuple(rel, now_parent_page, offset);
			entry = DecodeTupleToEntry(tuple, oids);
			entry1->distance_to_parent = Distance(rel, tuple, wrap.op1);
			entry2->distance_to_parent = Distance(rel, tuple, wrap.op2);
			// 必做事情,对于放在新page的
			updateParentPageId(rel, wrap.page_new, parent_pageId);
			if (PageGetFreeSpace(parent_page) < 1000)
			{
				recurseSplit(rel, parent_page, NULL, parent_pageId, false);
			}
		}
	}
}

// mtree Insert: we need to do split.
static void
mtree_tuple_insert(Relation relation, TupleTableSlot *slot,
				   CommandId cid, int options, BulkInsertState bistate)
{
	// 预分配MetaPage
	preMetaPage(relation);
	bool shouldFree;
	int32 rootId = GetRootPageId(relation);
	// 构造叶子节点的数据
	MtreeEntry entry;
	entry.distance_to_parent = -1;
	HeapTuple tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
	entry.entry_data = GetDatumFromTuple(relation, tuple);
	entry.is_leaf = true;
	tuple = EncodeEntryIntoTuple(&entry, getOids(relation));
	// 没有数据
	if (rootId == -1)
	{
		Buffer buffer = ReadBufferBI(relation, P_NEW, RBM_ZERO_AND_LOCK, NULL);
		Page p = BufferGetPage(buffer);
		PageInit(p, BufferGetPageSize(buffer), 0);
		// 一个tuple应该不会过载
		RelationPutHeapTuple(relation, buffer, tuple,
							 (options & HEAP_INSERT_SPECULATIVE) != 0);
		MarkBufferDirty(buffer);
	}
	insert(relation, rootId, tuple, options);
}

/**
 *  |   RootPage     |
 *  |  rootId int32  |
 *  |  metric data   |
 *
 */
void SetRootPageId(Relation rel, int32 id)
{
	// 获取MetaPage
	Page page = BufferGetPage(0);
	char *data = (char *)(page);
	memcpy(data, &id, sizeof(int32));
}

int32 GetRootPageId(Relation rel)
{
	// 获取MetaPage
	Page page = BufferGetPage(0);
	int rootId = -1;
	char *data = (char *)(page);
	memcpy(&rootId, data, sizeof(int32));
	return rootId;
}

void preMetaPage(Relation rel)
{
	int blockNums = RelationGetNumberOfBlocks(rel);
	// 初始化我们的MetaBlock
	// 目前是用于存储root节点，后面有其他需要再添加会更改
	if (blockNums == 0)
	{
		Buffer buffer = ReadBufferBI(rel, P_NEW, RBM_ZERO_AND_LOCK, NULL);
		Page page = BufferGetPage(buffer);
		PageInit(page, BufferGetPageSize(buffer), 0);
		SetRootPageId(rel, -1);
	}
}

Oid *getOids(Relation relation)
{
	int NumOfAttrs = relation->rd_att->natts;
	Oid *oids = (Oid *)palloc0_array(Oid, NumOfAttrs);
	for (int i = 0; i < NumOfAttrs; i++)
	{
		oids[i] = relation->rd_att->attrs[i].atttypid;
	}
	return oids;
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for mtree AM.
 * ----------------------------------------------------------------------------
 */
// we don't care about transaction for now
// but we will improve this in the future
static void
mtree_tuple_insert2(Relation relation, TupleTableSlot *slot,
					CommandId cid, int options, BulkInsertState bistate)
{
	Buffer buffer;
	bool shouldFree;
	Buffer vmbuffer = InvalidBuffer;
	// Print_slot(slot);

	START_CRIT_SECTION();
	/*
	 * Find buffer to insert this tuple into.  If the page is all visible,
	 * this will also pin the requisite visibility map page.
	 */
	// Debug Info
	TupleDesc typeinfo = slot->tts_tupleDescriptor;
	Form_pg_attribute attributeP = TupleDescAttr(typeinfo, 0);
	// we don't need vm_page to see the visibility (for now, mtree_am does't
	// think about the mvcc)
	// ======================================================================
	// 测试MtreeEntry的存储与读
	MtreeEntry entry;
	entry.distance_to_parent = 100.0;
	entry.entry_data = Int32GetDatum(127);
	entry.is_leaf = false;
	entry.radius = 200;
	Oid *oids = getOids(relation);
	HeapTuple tup = EncodeEntryIntoTuple(&entry, oids);
	MtreeEntry *e = DecodeTupleToEntry(tup, oids);
	elog(INFO, "%lf", e->distance_to_parent);
	pfree(oids);
	// ======================================================================

	// first, try to put tuple in the last block
	BlockNumber nblocks = RelationGetNumberOfBlocks(relation);
	if (nblocks == 0)
	{
		buffer = ReadBufferBI(relation, P_NEW, RBM_ZERO_AND_LOCK, bistate);
		Page page = BufferGetPage(buffer);
		if (!PageIsNew(page))
			elog(ERROR, "page %u of relation \"%s\" should be empty but is not",
				 BufferGetBlockNumber(buffer),
				 RelationGetRelationName(relation));
		PageInit(page, BufferGetPageSize(buffer), 0);
	}
	else
	{
		buffer = ReadBuffer(relation, nblocks - 1);
	}
	Page page = BufferGetPage(buffer);
	Size pageFreeSpace = PageGetHeapFreeSpace(page);
	// elog(INFO,"pageFreeSpace: %d",pageFreeSpace);
	// note: tuple->data is the real data, and we will
	// give the blockNum with the data, and then we will
	// use this mechenism to build our mtree_index
	HeapTuple tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
	if (pageFreeSpace > 1000)
	{
		// Print_slot(tuple);
		RelationPutHeapTuple(relation, buffer, tuple,
							 (options & HEAP_INSERT_SPECULATIVE) != 0);
		MarkBufferDirty(buffer);

		ReleaseBuffer(buffer);
		/* nothing to do */
		// elog(INFO,"Insert a tuple into mtree_tab");
	}
	else
	{
		ReleaseBuffer(buffer);
		// Get a New Buffer
		buffer = ReadBufferBI(relation, P_NEW, RBM_ZERO_AND_LOCK, bistate);
		page = BufferGetPage(buffer);
		if (!PageIsNew(page))
			elog(ERROR, "page %u of relation \"%s\" should be empty but is not",
				 BufferGetBlockNumber(buffer),
				 RelationGetRelationName(relation));
		// Init Page
		PageInit(page, BufferGetPageSize(buffer), 0);
		MarkBufferDirty(buffer);
		RelationPutHeapTuple(relation, buffer, tuple,
							 (options & HEAP_INSERT_SPECULATIVE) != 0);
		// elog(INFO,"Insert a tuple into mtree_tab");
		ReleaseBuffer(buffer);
	}
	// elog(INFO,"Insert a tuple into mtree_tab");
	END_CRIT_SECTION();
	LWLockReleaseAll();
	UnlockBuffers();
}

static void
mtree_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
							   CommandId cid, int options,
							   BulkInsertState bistate,
							   uint32 specToken)
{
	/* nothing to do */
}

static void
mtree_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
								 uint32 spekToken, bool succeeded)
{
	/* nothing to do */
}

static void
mtree_multi_insert(Relation relation, TupleTableSlot **slots,
				   int ntuples, CommandId cid, int options,
				   BulkInsertState bistate)
{
	elog(INFO, "Insert multi tuple into mtree_tab");
	/* nothing to do */
}

static TM_Result
mtree_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
				   Snapshot snapshot, Snapshot crosscheck, bool wait,
				   TM_FailureData *tmfd, bool changingPart)
{
	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static TM_Result
mtree_tuple_update(Relation relation, ItemPointer otid,
				   TupleTableSlot *slot, CommandId cid,
				   Snapshot snapshot, Snapshot crosscheck,
				   bool wait, TM_FailureData *tmfd,
				   LockTupleMode *lockmode, bool *update_indexes)
{
	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static TM_Result
mtree_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
				 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				 LockWaitPolicy wait_policy, uint8 flags,
				 TM_FailureData *tmfd)
{
	/* nothing to do, so it is always OK */
	return TM_Ok;
}

static void
mtree_finish_bulk_insert(Relation relation, int options)
{
	/* nothing to do */
	elog(INFO, "Insert bulk tuple into mtree_tab");
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for mtree AM.
 * ------------------------------------------------------------------------
 */

static void
mtree_relation_set_new_filelocator(Relation rel,
								   const RelFileLocator *newrnode,
								   char persistence,
								   TransactionId *freezeXid,
								   MultiXactId *minmulti)
{
	/* nothing to do */
	SMgrRelation srel;

	/*
	 * Initialize to the minimum XID that could put tuples in the table. We
	 * know that no xacts older than RecentXmin are still running, so that
	 * will do.
	 */
	*freezeXid = RecentXmin;

	/*
	 * Similarly, initialize the minimum Multixact to the first value that
	 * could possibly be stored in tuples in the table.  Running transactions
	 * could reuse values from their local cache, so we are careful to
	 * consider all currently running multis.
	 *
	 * XXX this could be refined further, but is it worth the hassle?
	 */
	*minmulti = GetOldestMultiXactId();

	srel = RelationCreateStorage(*newrnode, persistence, true);

	/*
	 * If required, set up an init fork for an unlogged table so that it can
	 * be correctly reinitialized on restart.  An immediate sync is required
	 * even if the page has been logged, because the write did not go through
	 * shared_buffers and therefore a concurrent checkpoint may have moved the
	 * redo pointer past our xlog record.  Recovery may as well remove it
	 * while replaying, for example, XLOG_DBASE_CREATE* or XLOG_TBLSPC_CREATE
	 * record. Therefore, logging is necessary even if wal_level=minimal.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_MATVIEW ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}

	smgrclose(srel);
}

static void
mtree_relation_nontransactional_truncate(Relation rel)
{
	/* nothing to do */
}

static void
mtree_copy_data(Relation rel, const RelFileLocator *newrnode)
{
	/* there is no data */
}

static void
mtree_copy_for_cluster(Relation OldTable, Relation NewTable,
					   Relation OldIndex, bool use_sort,
					   TransactionId OldestXmin,
					   TransactionId *xid_cutoff,
					   MultiXactId *multi_cutoff,
					   double *num_tuples,
					   double *tups_vacuumed,
					   double *tups_recently_dead)
{
	/* no data, so nothing to do */
}

static void
mtree_vacuum(Relation onerel, VacuumParams *params,
			 BufferAccessStrategy bstrategy)
{
	/* no data, so nothing to do */
}

static bool
mtree_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
							  BufferAccessStrategy bstrategy)
{
	/* no data, so no point to analyze next block */
	return false;
}

static bool
mtree_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
							  double *liverows, double *deadrows,
							  TupleTableSlot *slot)
{
	/* no data, so no point to analyze next tuple */
	return false;
}

static double
mtree_index_build_range_scan(Relation tableRelation,
							 Relation indexRelation,
							 IndexInfo *indexInfo,
							 bool allow_sync,
							 bool anyvisible,
							 bool progress,
							 BlockNumber start_blockno,
							 BlockNumber numblocks,
							 IndexBuildCallback callback,
							 void *callback_state,
							 TableScanDesc scan)
{
	/* no data, so no tuples */
	return 0;
}

static void
mtree_index_validate_scan(Relation tableRelation,
						  Relation indexRelation,
						  IndexInfo *indexInfo,
						  Snapshot snapshot,
						  ValidateIndexState *state)
{
	/* nothing to do */
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the mtree AM
 * ------------------------------------------------------------------------
 */

static uint64
mtree_relation_size(Relation rel, ForkNumber forkNumber)
{
	/* there is nothing */
	uint64 nblocks = 0;

	/* InvalidForkNumber indicates returning the size for all forks */
	if (forkNumber == InvalidForkNumber)
	{
		for (int i = 0; i < MAX_FORKNUM; i++)
			nblocks += smgrnblocks(RelationGetSmgr(rel), i);
	}
	else
		nblocks = smgrnblocks(RelationGetSmgr(rel), forkNumber);

	return nblocks * BLCKSZ;
}

/*
 * Check to see whether the table needs a TOAST table.
 */
static bool
mtree_relation_needs_toast_table(Relation rel)
{
	/* no data, so no toast table needed */
	return false;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the mtree AM
 * ------------------------------------------------------------------------
 */

static void
mtree_estimate_rel_size(Relation rel, int32 *attr_widths,
						BlockNumber *pages, double *tuples,
						double *allvisfrac)
{
	/* no data available */
	if (attr_widths)
		*attr_widths = 0;
	if (pages)
		*pages = 0;
	if (tuples)
		*tuples = 0;
	if (allvisfrac)
		*allvisfrac = 0;
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the mtree AM
 * ------------------------------------------------------------------------
 */

static bool
mtree_scan_bitmap_next_block(TableScanDesc scan,
							 TBMIterateResult *tbmres)
{
	/* no data, so no point to scan next block */
	return false;
}

static bool
mtree_scan_bitmap_next_tuple(TableScanDesc scan,
							 TBMIterateResult *tbmres,
							 TupleTableSlot *slot)
{
	/* no data, so no point to scan next tuple */
	return false;
}

static bool
mtree_scan_sample_next_block(TableScanDesc scan,
							 SampleScanState *scanstate)
{
	/* no data, so no point to scan next block for sampling */
	return false;
}

static bool
mtree_scan_sample_next_tuple(TableScanDesc scan,
							 SampleScanState *scanstate,
							 TupleTableSlot *slot)
{
	/* no data, so no point to scan next tuple for sampling */
	return false;
}

/* ------------------------------------------------------------------------
 * Definition of the mtree table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine mtree_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = mtree_slot_callbacks,

	.scan_begin = mtree_scan_begin,
	.scan_end = mtree_scan_end,
	.scan_rescan = mtree_scan_rescan,
	.scan_getnextslot = mtree_scan_getnextslot,

	/* these are common helper functions */
	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	.index_fetch_begin = mtree_index_fetch_begin,
	.index_fetch_reset = mtree_index_fetch_reset,
	.index_fetch_end = mtree_index_fetch_end,
	.index_fetch_tuple = mtree_index_fetch_tuple,

	.tuple_insert = mtree_tuple_insert,
	.tuple_insert_speculative = mtree_tuple_insert_speculative,
	.tuple_complete_speculative = mtree_tuple_complete_speculative,
	.multi_insert = mtree_multi_insert,
	.tuple_delete = mtree_tuple_delete,
	.tuple_update = mtree_tuple_update,
	.tuple_lock = mtree_tuple_lock,
	.finish_bulk_insert = mtree_finish_bulk_insert,

	.tuple_fetch_row_version = mtree_fetch_row_version,
	.tuple_get_latest_tid = mtree_get_latest_tid,
	.tuple_tid_valid = mtree_tuple_tid_valid,
	.tuple_satisfies_snapshot = mtree_tuple_satisfies_snapshot,
	.index_delete_tuples = mtree_index_delete_tuples,

	.relation_set_new_filelocator = mtree_relation_set_new_filelocator,
	.relation_nontransactional_truncate = mtree_relation_nontransactional_truncate,
	.relation_copy_data = mtree_copy_data,
	.relation_copy_for_cluster = mtree_copy_for_cluster,
	.relation_vacuum = mtree_vacuum,
	.scan_analyze_next_block = mtree_scan_analyze_next_block,
	.scan_analyze_next_tuple = mtree_scan_analyze_next_tuple,
	.index_build_range_scan = mtree_index_build_range_scan,
	.index_validate_scan = mtree_index_validate_scan,

	.relation_size = mtree_relation_size,
	.relation_needs_toast_table = mtree_relation_needs_toast_table,

	.relation_estimate_size = mtree_estimate_rel_size,

	.scan_bitmap_next_block = mtree_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = mtree_scan_bitmap_next_tuple,
	.scan_sample_next_block = mtree_scan_sample_next_block,
	.scan_sample_next_tuple = mtree_scan_sample_next_tuple};

Datum mtree_am_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&mtree_methods);
}

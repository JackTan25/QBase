#pragma once

#include "a3v/init.h"
#include "a3v/util.h"
#include "a3v/m3v.h"
// #include "lru_index_pointer.h"
extern "C"{
	#include "postgres.h"
	#include <math.h>

	#include "catalog/index.h"
	#include "miscadmin.h"
	#include "lib/pairingheap.h"
	#include "nodes/pg_list.h"
	#include "storage/bufmgr.h"
	#include "utils/memutils.h"

	#if PG_VERSION_NUM >= 140000
	#include "utils/backend_progress.h"
	#elif PG_VERSION_NUM >= 120000
	#include "pgstat.h"
	#endif

	#if PG_VERSION_NUM >= 120000
	#include "access/tableam.h"
	#include "commands/progress.h"
	#else
	#define PROGRESS_CREATEIDX_TUPLES_DONE 0
	#endif

	#if PG_VERSION_NUM >= 130000
	#define CALLBACK_ITEM_POINTER ItemPointer tid
	#else
	#define CALLBACK_ITEM_POINTER HeapTuple hup
	#endif

	#if PG_VERSION_NUM >= 120000
	#define UpdateProgress(index, val) pgstat_progress_update_param(index, val)
	#else
	#define UpdateProgress(index, val) ((void)val)
	#endif
}

GlobalInit init;
InMemoryGlobal in_memory_init;


std::vector<uint32_t> GetOffsets(Relation index){
	// get index vector columns
	int columns = index->rd_att->natts;
	std::vector<uint32_t> offsets;
	// DIM_SIZE
	for (int i = 0; i < columns; i++)
	{
		int dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
		offsets.push_back(dim * DIM_SIZE);
	}
	return offsets;
}

/*
 * Create the metapage
 */
static void
CreateMetaPage(m3vBuildState *buildstate)
{
	Relation index = buildstate->index;
	ForkNumber forkNum = buildstate->forkNum;
	Buffer buf;
	Page page;
	GenericXLogState *state;
	m3vMetaPage metap;

	buf = m3vNewBuffer(index, forkNum);
	m3vInitRegisterPage(index, &buf, &page, &state, M3V_META_PAGE_TYPE, 0);

	/* Set metapage data */
	metap = m3vPageGetMeta(page);
	((PageHeader)page)->pd_lower =
		((char *)metap + sizeof(m3vMetaPageData)) - (char *)page;
		// Init state, we can't get any query roots.
	metap->simliar_query_root_nums = 0;
	metap->columns = buildstate->index->rd_att->natts;
	for (int i = 0; i < metap->columns; i++)
	{
		// set dimentions
		metap->dimentions[i] = TupleDescAttr(index->rd_att, 0)->atttypmod;
	}
	metap->root = InvalidBlockNumber;
	metap->columns = RelationGetNumberOfAttributes(index);
	m3vCommitBuffer(buf, state);
}

/*
 * Free elements
 */
static void
FreeElements(m3vBuildState *buildstate)
{
	ListCell *lc;

	foreach (lc, buildstate->elements)
		m3vFreeElement(static_cast<m3vElement>(lfirst(lc)));

	list_free(buildstate->elements);
}

/*
 * Flush pages
 */
static void
FlushPages(m3vBuildState *buildstate)
{
	buildstate->flushed = true;
	FreeElements(buildstate);
}

/*
 * Insert tuple
 */
static bool
InsertTuple(Relation index, Datum *values, m3vElement element, m3vBuildState *buildstate, m3vElement *dup)
{
	// support m3v_index insert func
}

/*
 * Callback for table_index_build_scan
 * insert all VectorRecord into RocksDB
 */
static void
BuildA3vCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			  bool *isnull, bool tupleIsAlive, void *state)
{
	elog(INFO,"memory_index: %d",A3vGetIndexType(index));
	elog(INFO,"BuildA3vCallback");
	m3vBuildState *buildstate = (m3vBuildState *)state;
	buildstate->tids.push_back(*tid);
	// we will insert data into rocksdb
	IndexPointerLruCache* cache = NULL;
	if(!init.ContainsIndexCache(RelationGetRelationName(index))){
		auto offsets = GetOffsets(index);
		cache = init.GetIndexCache(offsets,offsets.size(),RelationGetRelationName(index));
	}else{
		cache = init.GetIndexCacheByName(RelationGetRelationName(index));
	}
	rocksdb::DB* db =  cache->GetDB();
	// we need to serialize tids into disk, so we can restore it when start postgres again.
	// The tids array is the core of a3v index. we need to support multi vector build and search.
	// And we will store a hsnw index to find the closest root of a3v indexes.
	// PROJECT_ROOT_PATH
	std::string index_name = std::string(RelationGetRelationName(index));
	std::string file_name = std::string(PROJECT_ROOT_PATH) + "/" + index_name + ".bin";
	elog(INFO,"FILENAME: %s",file_name.c_str());
	SerializeVector<ItemPointerData>(buildstate->tids,file_name);
	// std::vector<ItemPointerData> tids = DeserializeVector(file_name);
	// for(int i = 0;i < tids.size();i++){
	// 	elog(INFO,"Block:(%d,%d) Offset:%d",tids[i].ip_blkid.bi_hi,tids[i].ip_blkid.bi_lo,tids[i].ip_posid);
	// }
	db->Put(rocksdb::WriteOptions(), ItemPointerToString(*tid), build_data_string_datum(values,index->rd_att->natts));
	buildstate->tuples_num++;
}

/*
 * Callback for table_index_build_scan
 * insert all VectorRecord into RocksDB
 */
static void
BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			  bool *isnull, bool tupleIsAlive, void *state)
{
	m3vBuildState *buildstate = (m3vBuildState *)state;
	MemoryContext oldCtx;
	m3vElement element;
	m3vElement dup = NULL;
	bool inserted;

#if PG_VERSION_NUM < 130000
	ItemPointer tid = &hup->t_self;
#endif

	/* Skip nulls */
	if (isnull[0])
		return;

	if (buildstate->indtuples >= buildstate->maxInMemoryElements)
	{
		if (!buildstate->flushed)
		{
			ereport(NOTICE,
					(errmsg("m3v graph no longer fits into maintenance_work_mem after " INT64_FORMAT " tuples", (int64)buildstate->indtuples),
					 errdetail("Building will take significantly more time."),
					 errhint("Increase maintenance_work_mem to speed up builds.")));

			FlushPages(buildstate);
		}
		
		oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);
		element = m3vInitElement(tid, 0, 0, InvalidBlockNumber,values,RelationGetNumberOfAttributes(index));
		if (m3vInsertTuple(buildstate->index, element, isnull, tid, buildstate->heap, buildstate,static_cast<GenericXLogState*>(state),RelationGetNumberOfAttributes(index)))
			UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++buildstate->indtuples);

		/* Reset memory context */
		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(buildstate->tmpCtx);

		return;
	}

	/* Allocate necessary memory outside of memory context */
	// element = m3vInitElement(tid, 0, 0, InvalidBlockNumber,values,RelationGetNumberOfAttributes(index));
	// element->vec = palloc(VECTOR_SIZE(buildstate->dimensions));

	/* Use memory context since detoast can allocate */
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);
	// PrintVector("vecs[0]: ",DatumGetVector(PointerGetDatum(PG_DETOAST_DATUM(values[0]))));
	// PrintVector("vecs[1]: ",DatumGetVector(PointerGetDatum(PG_DETOAST_DATUM(values[1]))));
	element = m3vInitElement(tid, 0, 0, InvalidBlockNumber,values,RelationGetNumberOfAttributes(index));
	// element = m3vInitElement(tid, 0, 0, InvalidBlockNumber,values,RelationGetNumberOfAttributes(index));
	XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 150);
	GenericXLogState *xlg_state = GenericXLogStart(index);
	/* Insert tuple */
	for(int i = 0;i < RelationGetNumberOfAttributes(index);i++){
		PrintVector("vec: ",element->vecs[i]);
	}
	inserted = m3vInsertTuple(buildstate->index, element, isnull, tid, buildstate->heap, buildstate, xlg_state,RelationGetNumberOfAttributes(index));
	GenericXLogFinish(xlg_state);
	/* Reset memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);

	/* Add outside memory context */
	if (dup != NULL)
		m3vAddHeapTid(dup, tid);

	/* Add to buildstate or free */
	if (inserted)
		buildstate->elements = lappend(buildstate->elements, element);
	else
	if(inserted)
		m3vFreeElement(element);
}

/*
 * Get the max number of elements that fit into maintenance_work_mem
 */
static double
m3vGetMaxInMemoryElements(int m, double ml, int dimensions)
{
	Size elementSize = sizeof(m3vElementData);
	double avgLevel = -log(0.5) * ml;

	elementSize += sizeof(m3vNeighborArray) * (avgLevel + 1);
	elementSize += sizeof(m3vCandidate) * (m * (avgLevel + 2));
	elementSize += sizeof(ItemPointerData);
	elementSize += VECTOR_SIZE(dimensions);
	return (maintenance_work_mem * 1024L) / elementSize;
}

/*
 * Initialize the build state
 */
static void
InitBuildState(m3vBuildState *buildstate, Relation heap, Relation index, IndexInfo *indexInfo, ForkNumber forkNum)
{
	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;
	buildstate->forkNum = forkNum;

	buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

	// buildstate->each_dimentions[0] = TupleDescAttr(index->rd_att, 0)->atttypmod;

	/* Require column to have dimensions to be indexed */
	if (buildstate->dimensions < 0)
		elog(ERROR, "column does not have dimensions");

	if (buildstate->dimensions > M3V_MAX_DIM)
		elog(ERROR, "column cannot have more than %d dimensions for m3v index", M3V_MAX_DIM);

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;
	buildstate->tuples_num = 0;
	/* Get support functions */
	buildstate->procinfo = index_getprocinfo(index, 1, M3V_DISTANCE_PROC);
	buildstate->normprocinfo = m3vOptionalProcInfo(index, M3V_NORM_PROC);
	buildstate->collation = index->rd_indcollation[0];

	buildstate->elements = NIL;
	buildstate->entryPoint = NULL;
	buildstate->flushed = false;

	/* Reuse for each tuple */
	buildstate->normvec = InitVector(buildstate->dimensions);

	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "M3v build temporary context",
											   ALLOCSET_DEFAULT_SIZES);
	// build meta page
	CreateMetaPage(buildstate);
}

/*
 * Free resources
 */
static void
FreeBuildState(m3vBuildState *buildstate)
{
	// UpdateMetaPage's tuples_num
	a3vUpdateMetaPage(buildstate->index,0,buildstate->tuples_num,MAIN_FORKNUM);
	pfree(buildstate->normvec);
	MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Build graph
 */
static void
BuildGraph(m3vBuildState *buildstate, ForkNumber forkNum)
{
	UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_M3V_PHASE_LOAD);

#if PG_VERSION_NUM >= 120000
	buildstate->reltuples = table_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
												   true, true, BuildA3vCallback, (void *)buildstate, NULL);
#else
	buildstate->reltuples = IndexBuildHeapScan(buildstate->heap, buildstate->index, buildstate->indexInfo,
											   true, BuildCallback, (void *)buildstate, NULL);
#endif
}

/*
 * Build the index
 */
static void
BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
		   m3vBuildState *buildstate, ForkNumber forkNum)
{
	InitBuildState(buildstate, heap, index, indexInfo, forkNum);

	if (buildstate->heap != NULL)
		BuildGraph(buildstate, forkNum);

	if (!buildstate->flushed)
		FlushPages(buildstate);

	FreeBuildState(buildstate);
}

/*
 * Build the index for a logged table
 */
IndexBuildResult *
m3vbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	m3vBuildState buildstate;
	// elog(INFO,"vector numbers: %d",	index->rd_att->natts);
	
	// we should init the 
	BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;
	// when build finished, we should start to generate AuxiliarySortPage Index.
	// 
	return result;
}

/*
 * Build the index for an unlogged table
 */
void m3vbuildempty(Relation index)
{
	IndexInfo *indexInfo = BuildIndexInfo(index);
	m3vBuildState buildstate;

	BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}

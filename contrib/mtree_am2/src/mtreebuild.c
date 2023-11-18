#include "postgres.h"

#include <math.h>

#include "catalog/index.h"
#include "mtree.h"
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

/*
 * Create the metapage
 */
static void
CreateMetaPage(MtreeBuildState *buildstate)
{
	Relation index = buildstate->index;
	ForkNumber forkNum = buildstate->forkNum;
	Buffer buf;
	Page page;
	GenericXLogState *state;
	MtreeMetaPage metap;

	buf = MtreeNewBuffer(index, forkNum);
	MtreeInitRegisterPage(index, &buf, &page, &state, MTREE_META_PAGE_TYPE, 0);

	/* Set metapage data */
	metap = MtreePageGetMeta(page);
	((PageHeader)page)->pd_lower =
		((char *)metap + sizeof(MtreeMetaPageData)) - (char *)page;

	metap->columns = buildstate->index->rd_att->natts;
	for (int i = 0; i < metap->columns; i++)
	{
		// set dimentions
		metap->dimentions[i] = TupleDescAttr(index->rd_att, 0)->atttypmod;
	}
	metap->root = InvalidBlockNumber;
	MtreeCommitBuffer(buf, state);
}

/*
 * Free elements
 */
static void
FreeElements(MtreeBuildState *buildstate)
{
	ListCell *lc;

	foreach (lc, buildstate->elements)
		MtreeFreeElement(lfirst(lc));

	list_free(buildstate->elements);
}

/*
 * Flush pages
 */
static void
FlushPages(MtreeBuildState *buildstate)
{
	buildstate->flushed = true;
	FreeElements(buildstate);
}

/*
 * Insert tuple
 */
static bool
InsertTuple(Relation index, Datum *values, MtreeElement element, MtreeBuildState *buildstate, MtreeElement *dup)
{
	// support mtree_index insert func
}

/*
 * Callback for table_index_build_scan
 */
static void
BuildCallback(Relation index, CALLBACK_ITEM_POINTER, Datum *values,
			  bool *isnull, bool tupleIsAlive, void *state)
{
	MtreeBuildState *buildstate = (MtreeBuildState *)state;
	MemoryContext oldCtx;
	MtreeElement element;
	MtreeElement dup = NULL;
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
					(errmsg("mtree graph no longer fits into maintenance_work_mem after " INT64_FORMAT " tuples", (int64)buildstate->indtuples),
					 errdetail("Building will take significantly more time."),
					 errhint("Increase maintenance_work_mem to speed up builds.")));

			FlushPages(buildstate);
		}

		oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

		if (MtreeInsertTuple(buildstate->index, values, isnull, tid, buildstate->heap, buildstate, state))
			UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++buildstate->indtuples);

		/* Reset memory context */
		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(buildstate->tmpCtx);

		return;
	}

	/* Allocate necessary memory outside of memory context */
	element = MtreeInitElement(tid, 0, 0, InvalidBlockNumber, InvalidBlockNumber);
	element->vec = palloc(VECTOR_SIZE(buildstate->dimensions));

	/* Use memory context since detoast can allocate */
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);
	XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 150);
	GenericXLogState *xlg_state = GenericXLogStart(index);
	/* Insert tuple */
	inserted = MtreeInsertTuple(buildstate->index, values, isnull, tid, buildstate->heap, buildstate, xlg_state);
	GenericXLogFinish(xlg_state);
	/* Reset memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);

	/* Add outside memory context */
	if (dup != NULL)
		MtreeAddHeapTid(dup, tid);

	/* Add to buildstate or free */
	if (inserted)
		buildstate->elements = lappend(buildstate->elements, element);
	else
		MtreeFreeElement(element);
}

/*
 * Get the max number of elements that fit into maintenance_work_mem
 */
static double
MtreeGetMaxInMemoryElements(int m, double ml, int dimensions)
{
	Size elementSize = sizeof(MtreeElementData);
	double avgLevel = -log(0.5) * ml;

	elementSize += sizeof(MtreeNeighborArray) * (avgLevel + 1);
	elementSize += sizeof(MtreeCandidate) * (m * (avgLevel + 2));
	elementSize += sizeof(ItemPointerData);
	elementSize += VECTOR_SIZE(dimensions);
	return (maintenance_work_mem * 1024L) / elementSize;
}

/*
 * Initialize the build state
 */
static void
InitBuildState(MtreeBuildState *buildstate, Relation heap, Relation index, IndexInfo *indexInfo, ForkNumber forkNum)
{
	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;
	buildstate->forkNum = forkNum;

	buildstate->m = MtreeGetM(index);
	buildstate->efConstruction = MtreeGetEfConstruction(index);

	buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

	buildstate->each_dimentions[0] = TupleDescAttr(index->rd_att, 0)->atttypmod;

	/* Require column to have dimensions to be indexed */
	if (buildstate->dimensions < 0)
		elog(ERROR, "column does not have dimensions");

	if (buildstate->dimensions > MTREE_MAX_DIM)
		elog(ERROR, "column cannot have more than %d dimensions for mtree index", MTREE_MAX_DIM);

	if (buildstate->efConstruction < 2 * buildstate->m)
		elog(ERROR, "ef_construction must be greater than or equal to 2 * m");

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;

	/* Get support functions */
	buildstate->procinfo = index_getprocinfo(index, 1, MTREE_DISTANCE_PROC);
	buildstate->normprocinfo = MtreeOptionalProcInfo(index, MTREE_NORM_PROC);
	buildstate->collation = index->rd_indcollation[0];

	buildstate->elements = NIL;
	buildstate->entryPoint = NULL;
	buildstate->ml = MtreeGetMl(buildstate->m);
	buildstate->maxLevel = MtreeGetMaxLevel(buildstate->m);
	buildstate->maxInMemoryElements = MtreeGetMaxInMemoryElements(buildstate->m, buildstate->ml, buildstate->dimensions);
	buildstate->flushed = false;

	/* Reuse for each tuple */
	buildstate->normvec = InitVector(buildstate->dimensions);

	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "Mtree build temporary context",
											   ALLOCSET_DEFAULT_SIZES);
	// build meta page
	CreateMetaPage(buildstate);
}

/*
 * Free resources
 */
static void
FreeBuildState(MtreeBuildState *buildstate)
{
	pfree(buildstate->normvec);
	MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Build graph
 */
static void
BuildGraph(MtreeBuildState *buildstate, ForkNumber forkNum)
{
	UpdateProgress(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_MTREE_PHASE_LOAD);

#if PG_VERSION_NUM >= 120000
	buildstate->reltuples = table_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
												   true, true, BuildCallback, (void *)buildstate, NULL);
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
		   MtreeBuildState *buildstate, ForkNumber forkNum)
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
mtreebuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	MtreeBuildState buildstate;

	BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

/*
 * Build the index for an unlogged table
 */
void mtreebuildempty(Relation index)
{
	IndexInfo *indexInfo = BuildIndexInfo(index);
	MtreeBuildState buildstate;

	BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}

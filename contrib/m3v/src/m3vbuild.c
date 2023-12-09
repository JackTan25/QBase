
#include "postgres.h"

#include <math.h>

#include "catalog/index.h"
#include "m3v.h"
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

	metap->columns = buildstate->index->rd_att->natts;
	for (int i = 0; i < metap->columns; i++)
	{
		// set dimentions
		metap->dimentions[i] = TupleDescAttr(index->rd_att, 0)->atttypmod;
	}
	metap->root = InvalidBlockNumber;
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
		m3vFreeElement(lfirst(lc));

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

		if (m3vInsertTuple(buildstate->index, values, isnull, tid, buildstate->heap, buildstate, state))
			UpdateProgress(PROGRESS_CREATEIDX_TUPLES_DONE, ++buildstate->indtuples);

		/* Reset memory context */
		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(buildstate->tmpCtx);

		return;
	}

	/* Allocate necessary memory outside of memory context */
	element = m3vInitElement(tid, 0, 0, InvalidBlockNumber, InvalidBlockNumber);
	element->vec = palloc(VECTOR_SIZE(buildstate->dimensions));

	/* Use memory context since detoast can allocate */
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);
	XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 150);
	GenericXLogState *xlg_state = GenericXLogStart(index);
	/* Insert tuple */
	inserted = m3vInsertTuple(buildstate->index, values, isnull, tid, buildstate->heap, buildstate, xlg_state);
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

	buildstate->m = m3vGetM(index);
	buildstate->efConstruction = m3vGetEfConstruction(index);

	buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

	buildstate->each_dimentions[0] = TupleDescAttr(index->rd_att, 0)->atttypmod;

	/* Require column to have dimensions to be indexed */
	if (buildstate->dimensions < 0)
		elog(ERROR, "column does not have dimensions");

	if (buildstate->dimensions > M3V_MAX_DIM)
		elog(ERROR, "column cannot have more than %d dimensions for m3v index", M3V_MAX_DIM);

	if (buildstate->efConstruction < 2 * buildstate->m)
		elog(ERROR, "ef_construction must be greater than or equal to 2 * m");

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;

	/* Get support functions */
	buildstate->procinfo = index_getprocinfo(index, 1, M3V_DISTANCE_PROC);
	buildstate->normprocinfo = m3vOptionalProcInfo(index, M3V_NORM_PROC);
	buildstate->collation = index->rd_indcollation[0];

	buildstate->elements = NIL;
	buildstate->entryPoint = NULL;
	buildstate->ml = m3vGetMl(buildstate->m);
	buildstate->maxLevel = m3vGetMaxLevel(buildstate->m);
	buildstate->maxInMemoryElements = m3vGetMaxInMemoryElements(buildstate->m, buildstate->ml, buildstate->dimensions);
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

	BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;

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

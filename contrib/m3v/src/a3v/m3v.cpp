#pragma once

#include "a3v/m3v.h"
#include "a3v_async_server.h"
#include "lru_index_pointer.h"
#include "util.h"
#include "simd_func.h"
#include<thread>
extern "C"
{	
	#include "postgres.h"
	#include <float.h>
	#include <math.h>

	#include "access/amapi.h"
	#include "commands/vacuum.h"
	#include "utils/guc.h"
	#include "utils/selfuncs.h"

	#if PG_VERSION_NUM >= 120000
	#include "commands/progress.h"
	#endif
	PGDLLEXPORT PG_FUNCTION_INFO_V1(a3vhandler);
};

int m3v_ef_search;
static relopt_kind m3v_relopt_kind;
int			a3v_lock_tranche_id;

void
A3vInitLockTranche(void)
{
	int		   *tranche_ids;
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	tranche_ids = (int*)ShmemInitStruct("a3v LWLock ids",
								  sizeof(int) * 1,
								  &found);
	if (!found)
		tranche_ids[0] = LWLockNewTrancheId();
	a3v_lock_tranche_id = tranche_ids[0];
	LWLockRelease(AddinShmemInitLock);

	/* Per-backend registration of the tranche ID */
	LWLockRegisterTranche(a3v_lock_tranche_id, "A3vBuild");
}

bool a3v_memory_index;

/*
 * Initialize index options and variables
 */
void m3vInit(void)
{
	// migrate m3v.
	elog(LOG,"init M3V,Start A3vAsyncRecieve Server");
	std::thread A3VServer(A3vAsyncRecieveServer);
    A3VServer.detach();
	elog(LOG,"init M3V,Start A3vAsyncRecieve Server Successfully");
	SetSIMDFunc();
	m3v_relopt_kind = add_reloption_kind();
	add_bool_reloption(m3v_relopt_kind, "memory_index", "the index type is memory index or disk index",
					  DEFAULT_INDEX_TYPE
	#if PG_VERSION_NUM >= 130000
						,AccessExclusiveLock
	#endif
			);
	
	add_real_reloption(m3v_relopt_kind, "close_query_threshold", "the close query threshold to build new index",
					  CLOSE_QUERY_THRESHOLD,CLOSE_MIN_QUERY_THRESHOLD,CLOSE_MAX_QUERY_THRESHOLD
	#if PG_VERSION_NUM >= 130000
						,AccessExclusiveLock
	#endif
			);

	DefineCustomBoolVariable("a3v.memory_index", "Sets the Index type",
							"Valid value is true or false", &a3v_memory_index,
							DEFAULT_INDEX_TYPE, PGC_USERSET, 0, NULL, NULL, NULL);

	MarkGUCPrefixReserved("m3v");
}

std::string build_data_string_datum(Datum* values,int columns){
	std::string res = "";
	for(int i = 0;i < columns;i++){
		Vector* vector = DatumGetVector(values[i]);
		for(int i = 0;i < vector->dim;i++){
			float t = vector->x[i];
			const unsigned char* pBytes = reinterpret_cast<const unsigned char*>(&t);
			for (size_t i = 0; i < DIM_SIZE; ++i) {
				res += pBytes[i];
			}
		}
	}
	return res;
}

/*
 * Get the name of index build phase
 */
#if PG_VERSION_NUM >= 120000
static char *
m3vbuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
	case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
		return "initializing";
	case PROGRESS_M3V_PHASE_LOAD:
		return "loading tuples";
	default:
		return NULL;
	}
}
#endif

/*
 * Estimate the cost of an index scan
 */
static void
m3vcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				Cost *indexStartupCost, Cost *indexTotalCost,
				Selectivity *indexSelectivity, double *indexCorrelation,
				double *indexPages)
{
	// // for now, make sure it goes m3v index
	// *indexStartupCost = 0;
	// *indexTotalCost = 0;
	// *indexSelectivity = 0;
	// *indexCorrelation = 0;
	// *indexPages = 0;
	IndexOptInfo *index = path->indexinfo;
	List *qinfos;
	GenericCosts costs;

    MemSet(&costs, 0, sizeof(costs));
    // We have to visit all index tuples anyway
    costs.numIndexTuples = 1000000;

    // Use generic estimate
    genericcostestimate(root, path, loop_count, &costs);

    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
	// if(costs.indexStartupCost == 0.0){
	// 	elog(INFO,"try correct total cost here");
	// 	*indexTotalCost = *indexTotalCost + 0.375;
	// }
    *indexSelectivity = 0.01;
    *indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

/*
 * Parse and validate the reloptions
 */
static bytea *
m3voptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"memory_index",RELOPT_TYPE_BOOL,offsetof(m3vOptions,memory_index)},
		{"close_query_threshold",RELOPT_TYPE_REAL,offsetof(m3vOptions,close_query_threshold)},
	};

#if PG_VERSION_NUM >= 130000
	return (bytea *)build_reloptions(reloptions, validate,
									 m3v_relopt_kind,
									 sizeof(m3vOptions),
									 tab, lengthof(tab));
#else
	relopt_value *options;
	int numoptions;
	m3vOptions *rdopts;

	options = parseRelOptions(reloptions, validate, m3v_relopt_kind, &numoptions);
	rdopts = allocateReloptStruct(sizeof(m3vOptions), options, numoptions);
	fillRelOptions((void *)rdopts, sizeof(m3vOptions), options, numoptions,
				   validate, tab, lengthof(tab));

	return (bytea *)rdopts;
#endif
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
m3vvalidate(Oid opclassoid)
{
	return true;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */

Datum a3vhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	// 构建索引操作符类支持的函数的数量,从1开始编号,一直到amsupport
	amroutine->amsupport = 100;
#if PG_VERSION_NUM >= 130000
	amroutine->amoptsprocnum = 0;
#endif
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = false; /* can change direction mid-scan */
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
#if PG_VERSION_NUM >= 130000
	amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
#endif
	amroutine->amkeytype = InvalidOid;

	/* Interface functions */
	amroutine->ambuild = m3vbuild;
	amroutine->ambuildempty = m3vbuildempty;
	amroutine->aminsert = m3vinsert;
	amroutine->ambulkdelete = m3vbulkdelete;
	amroutine->amvacuumcleanup = m3vvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = m3vcostestimate;
	amroutine->amoptions = m3voptions;
	amroutine->amproperty = NULL; /* TODO AMPROP_DISTANCE_ORDERABLE */
#if PG_VERSION_NUM >= 120000
	amroutine->ambuildphasename = m3vbuildphasename;
#endif
	amroutine->amvalidate = m3vvalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->amcanrelaxedorderbyop = true;
	amroutine->ambeginscan = m3vbeginscan;
	amroutine->amrescan = m3vrescan;
	amroutine->amgettuple = m3vgettuple;
	// amroutine->amgetbitmap = m3vgetbitmap;
	amroutine->amgetbitmap = NULL; // todo(Support BitmapScan);
	amroutine->amendscan = m3vendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	/* Interface functions to support parallel index scans */
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

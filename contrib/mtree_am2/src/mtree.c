#include "postgres.h"

#include <float.h>
#include <math.h>

#include "access/amapi.h"
#include "commands/vacuum.h"
#include "mtree.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"

#if PG_VERSION_NUM >= 120000
#include "commands/progress.h"
#endif

int mtree_ef_search;
static relopt_kind mtree_relopt_kind;

/*
 * Initialize index options and variables
 */
void MtreeInit(void)
{
	// migrate mtree.
}

/*
 * Get the name of index build phase
 */
#if PG_VERSION_NUM >= 120000
static char *
mtreebuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
	case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
		return "initializing";
	case PROGRESS_MTREE_PHASE_LOAD:
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
mtreecostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				  Cost *indexStartupCost, Cost *indexTotalCost,
				  Selectivity *indexSelectivity, double *indexCorrelation,
				  double *indexPages)
{
	// for now, make sure it goes mtree index
	*indexStartupCost = 0;
	*indexTotalCost = 0;
	*indexSelectivity = 0;
	*indexCorrelation = 0;
	*indexPages = 0;
}

/*
 * Parse and validate the reloptions
 */
static bytea *
mtreeoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"m", RELOPT_TYPE_INT, offsetof(MtreeOptions, m)},
		{"ef_construction", RELOPT_TYPE_INT, offsetof(MtreeOptions, efConstruction)},
	};

#if PG_VERSION_NUM >= 130000
	return (bytea *)build_reloptions(reloptions, validate,
									 mtree_relopt_kind,
									 sizeof(MtreeOptions),
									 tab, lengthof(tab));
#else
	relopt_value *options;
	int numoptions;
	MtreeOptions *rdopts;

	options = parseRelOptions(reloptions, validate, mtree_relopt_kind, &numoptions);
	rdopts = allocateReloptStruct(sizeof(MtreeOptions), options, numoptions);
	fillRelOptions((void *)rdopts, sizeof(MtreeOptions), options, numoptions,
				   validate, tab, lengthof(tab));

	return (bytea *)rdopts;
#endif
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
mtreevalidate(Oid opclassoid)
{
	return true;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(mtreehandler);
Datum mtreehandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	// 构建索引操作符类支持的函数的数量,从1开始编号,一直到amsupport
	amroutine->amsupport = 4;
#if PG_VERSION_NUM >= 130000
	amroutine->amoptsprocnum = 0;
#endif
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = false; /* can change direction mid-scan */
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
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
	amroutine->ambuild = mtreebuild;
	amroutine->ambuildempty = mtreebuildempty;
	amroutine->aminsert = mtreeinsert;
	amroutine->ambulkdelete = mtreebulkdelete;
	amroutine->amvacuumcleanup = mtreevacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = mtreecostestimate;
	amroutine->amoptions = mtreeoptions;
	amroutine->amproperty = NULL; /* TODO AMPROP_DISTANCE_ORDERABLE */
#if PG_VERSION_NUM >= 120000
	amroutine->ambuildphasename = mtreebuildphasename;
#endif
	amroutine->amvalidate = mtreevalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->ambeginscan = mtreebeginscan;
	amroutine->amrescan = mtreerescan;
	amroutine->amgettuple = mtreegettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = mtreeendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	/* Interface functions to support parallel index scans */
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

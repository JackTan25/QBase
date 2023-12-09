#include "postgres.h"

#include <float.h>
#include <math.h>

#include "access/amapi.h"
#include "commands/vacuum.h"
#include "m3v.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"

#if PG_VERSION_NUM >= 120000
#include "commands/progress.h"
#endif

int m3v_ef_search;
static relopt_kind m3v_relopt_kind;

/*
 * Initialize index options and variables
 */
void m3vInit(void)
{
	// migrate m3v.
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
	// for now, make sure it goes m3v index
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
m3voptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"m", RELOPT_TYPE_INT, offsetof(m3vOptions, m)},
		{"ef_construction", RELOPT_TYPE_INT, offsetof(m3vOptions, efConstruction)},
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
PGDLLEXPORT PG_FUNCTION_INFO_V1(m3vhandler);
Datum m3vhandler(PG_FUNCTION_ARGS)
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
	amroutine->ambeginscan = m3vbeginscan;
	amroutine->amrescan = m3vrescan;
	amroutine->amgettuple = m3vgettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = m3vendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	/* Interface functions to support parallel index scans */
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

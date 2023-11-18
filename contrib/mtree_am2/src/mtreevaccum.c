#include "postgres.h"

#include <math.h>

#include "commands/vacuum.h"
#include "mtree.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"

/*
 * Check if deleted list contains an index TID
 */
static bool
DeletedContains(HTAB *deleted, ItemPointer indextid)
{
	bool found;

	hash_search(deleted, indextid, HASH_FIND, &found);
	return found;
}

/*
 * Mark items as deleted
 */
static void
MarkDeleted(MtreeVacuumState *vacuumstate)
{
}

/*
 * Free resources
 */
static void
FreeVacuumState(MtreeVacuumState *vacuumstate)
{
	hash_destroy(vacuumstate->deleted);
	FreeAccessStrategy(vacuumstate->bas);
	pfree(vacuumstate->ntup);
	MemoryContextDelete(vacuumstate->tmpCtx);
}

/*
 * Bulk delete tuples from the index
 */
IndexBulkDeleteResult *
mtreebulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				IndexBulkDeleteCallback callback, void *callback_state)
{
	MtreeVacuumState vacuumstate;

	InitVacuumState(&vacuumstate, info, stats, callback, callback_state);

	/* Pass 1: Remove heap TIDs */
	RemoveHeapTids(&vacuumstate);

	/* Pass 3: Mark as deleted */
	MarkDeleted(&vacuumstate);

	FreeVacuumState(&vacuumstate);

	return vacuumstate.stats;
}

/*
 * Clean up after a VACUUM operation
 */
IndexBulkDeleteResult *
mtreevacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation rel = info->index;

	if (info->analyze_only)
		return stats;

	/* stats is NULL if ambulkdelete not called */
	/* OK to return NULL if index not changed */
	if (stats == NULL)
		return NULL;

	stats->num_pages = RelationGetNumberOfBlocks(rel);

	return stats;
}

void RemoveHeapTids(MtreeVacuumState *vacuumstate) {}

/*
 * Initialize the vacuum state
 */
void InitVacuumState(MtreeVacuumState *vacuumstate, IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state)
{
}
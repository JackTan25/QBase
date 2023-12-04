#ifndef MTREE_H
#define MTREE_H

#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/reloptions.h"
#include "nodes/execnodes.h"
#include "port.h" /* for random() */
#include "utils/sampling.h"
#include "vector.h"

#if PG_VERSION_NUM < 110000
#error "Requires PostgreSQL 11+"
#endif

#define MTREE_MAX_DIM 2000

#define MTREE_INVALID_DISTANCE -1

/* Support functions */
#define MTREE_DISTANCE_PROC 1
#define MTREE_NORM_PROC 2
#define MTREE_KMEANS_DISTANCE_PROC 3
#define MTREE_KMEANS_NORM_PROC 4

#define MTREE_VERSION 1
#define MTREE_MAGIC_NUMBER 0xA953A953
#define MTREE_PAGE_ID 0xFF90

/* Preserved page numbers */
#define MTREE_METAPAGE_BLKNO 0
#define MTREE_HEAD_BLKNO 1 /* first element page */

/* Must correspond to page numbers since page lock is used */
#define MTREE_UPDATE_LOCK 0
#define MTREE_SCAN_LOCK 1

/* MTREE parameters */
#define MTREE_DEFAULT_M 16
#define MTREE_MIN_M 2
#define MTREE_MAX_M 100
#define MTREE_DEFAULT_EF_CONSTRUCTION 64
#define MTREE_MIN_EF_CONSTRUCTION 4
#define MTREE_MAX_EF_CONSTRUCTION 1000
#define MTREE_DEFAULT_EF_SEARCH 40
#define MTREE_MIN_EF_SEARCH 1
#define MTREE_MAX_EF_SEARCH 1000

/* Page types */
#define MTREE_LEAF_PAGE_TYPE 0
#define MTREE_INNER_PAGE_TYPE 1
#define MTREE_META_PAGE_TYPE 2

/* Make graph robust against non-HOT updates */
#define MTREE_HEAPTIDS 10

#define MTREE_UPDATE_ENTRY_GREATER 1
#define MTREE_UPDATE_ENTRY_ALWAYS 2

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_MTREE_PHASE_LOAD 2

#define MTREE_ELEMENT_TUPLE_SIZE(_dim) MAXALIGN(offsetof(MtreeElementTupleData, vec) + VECTOR_SIZE(_dim))
#define MTREE_ELEMENT_LEAF_TUPLE_SIZE(_dim) MAXALIGN(offsetof(MtreeElementLeafTupleData, vec) + VECTOR_SIZE(_dim))
#define MTREE_NEIGHBOR_TUPLE_SIZE(level, m) MAXALIGN(offsetof(MtreeNeighborTupleData, indextids) + ((level) + 2) * (m) * sizeof(ItemPointerData))

#define MtreePageGetOpaque(page) ((MtreePageOpaque)PageGetSpecialPointer(page))
#define MtreePageGetMeta(page) ((MtreeMetaPage)PageGetContents(page))

#if PG_VERSION_NUM >= 150000
#define RandomDouble() pg_prng_double(&pg_global_prng_state)
#else
#define RandomDouble() (((double)random()) / MAX_RANDOM_VALUE)
#endif

#if PG_VERSION_NUM < 130000
#define list_delete_last(list) list_truncate(list, list_length(list) - 1)
#define list_sort(list, cmp) list_qsort(list, cmp)
#endif

#define MtreeIsElementTuple(tup) ((tup)->type == MTREE_ELEMENT_TUPLE_TYPE)
#define MtreeIsNeighborTuple(tup) ((tup)->type == MTREE_NEIGHBOR_TUPLE_TYPE)

/* 2 * M connections for ground layer */
#define MtreeGetLayerM(m, layer) (layer == 0 ? (m) * 2 : (m))

/* Optimal ML from paper */
#define MtreeGetMl(m) (1 / log(m))

/* Ensure fits on page and in uint8 */
#define MtreeGetMaxLevel(m) Min(((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(MtreePageOpaqueData)) - offsetof(MtreeNeighborTupleData, indextids) - sizeof(ItemIdData)) / (sizeof(ItemPointerData)) / m) - 2, 255)

/* Variables */
extern int mtree_ef_search;

typedef struct MtreeNeighborArray MtreeNeighborArray;

// ==========================
// tanboyu
// ==========================
// this is not a strict m3-tree implemnetation,
// I will use the combination way to implement it.
// Reference to paper: 'Dynamic Similarity Search in Multi-Metric Spaces'
// and  'M-tree: An efficient access method for similarity search in metric spaces'
// Don't use the 3.1 to implement the Range Query And KNN Query
typedef struct MtreeElementData
{
	/* data */
	// 1. the radius of this entry
	float8 radius;
	// 2. distance to parent
	float8 distance_to_parent;
	// 3. the pointer to the leaf page
	BlockNumber son_page;
	// parent_buffer,we can get from Opaque
	// BlockNumber parent_page;
	// 4. center data
	Vector *vec;
	// 5. item_poniter
	ItemPointer item_pointer;
} MtreeElementData;

// One block can only hold 200 entrys at most
#define MaxElementNumsPerBlock 200

typedef MtreeElementData *MtreeElement;

typedef struct MtreeCandidate
{
	int id;
	float8 distance;
	Datum element;
} MtreeCandidate;

typedef struct MtreeKNNCandidate
{
	ItemPointer tid;
	ItemPointerData data;
	BlockNumber son_blkno;
	// func_dmin_node(entry,target,R(entry)),
	// estimated distance
	float8 distance;
	// the parent entry's distance from target
	float8 target_parent_distance;
} MtreeKNNCandidate;

typedef struct MtreePairingKNNNode
{
	pairingheap_node ph_node;
	MtreeKNNCandidate *inner;
} MtreePairingKNNNode;

typedef struct MtreeDistanceOnlyCandidate
{
	float8 distance;
	BlockNumber son_page;
} MtreeDistanceOnlyCandidate;

typedef struct MtreeNeighborArray
{
	int length;
	MtreeCandidate *items;
} MtreeNeighborArray;

typedef struct MtreePairingHeapNode
{
	pairingheap_node ph_node;
	MtreeCandidate *inner;
} MtreePairingHeapNode;

typedef struct MtreePairingDistanceOnlyHeapNode
{
	pairingheap_node ph_node;
	MtreeDistanceOnlyCandidate *inner;
} MtreePairingDistanceOnlyHeapNode;

/* MTREE index options */
typedef struct MtreeOptions
{
	int32 vl_len_;		/* varlena header (do not touch directly!) */
	int m;				/* number of connections */
	int efConstruction; /* size of dynamic candidate list */
} MtreeOptions;

typedef struct MtreeBuildState
{
	/* Info */
	Relation heap;
	Relation index;
	IndexInfo *indexInfo;
	ForkNumber forkNum;

	/* Settings */
	int dimensions;
	int m;
	int efConstruction;

	/* Statistics */
	double indtuples;
	double reltuples;

	/* Support functions */
	FmgrInfo *procinfo;
	FmgrInfo *normprocinfo;
	Oid collation;

	/* Variables */
	List *elements;
	MtreeElement entryPoint;
	double ml;
	int maxLevel;
	double maxInMemoryElements;
	bool flushed;
	Vector *normvec;

	/* Memory */
	MemoryContext tmpCtx;

	/* mtree state */
	uint16 each_dimentions[FLEXIBLE_ARRAY_MEMBER];
} MtreeBuildState;

typedef struct MtreeMetaPageData
{
	// the root page of current mtree
	BlockNumber root;
	// cols number of index
	uint16 columns;
	// dimentions of every col
	uint16 dimentions[FLEXIBLE_ARRAY_MEMBER];
} MtreeMetaPageData;

typedef MtreeMetaPageData *MtreeMetaPage;

typedef struct MtreePairngHeapUtils
{
	pairingheap *left;
	pairingheap *right;
	bool visited[FLEXIBLE_ARRAY_MEMBER];
} MtreePairingHeapUtils;

typedef MtreePairingHeapUtils *MtreePairingHeapP;

typedef struct MtreePageOpaqueData
{
	// BlockNumber nextblkno;
	// uint16 unused;
	// uint16 page_id; /* for identification of MTREE indexes */
	BlockNumber parent_blkno;
	uint8 type;
	// offset in parent page
	OffsetNumber offset;
} MtreePageOpaqueData;

typedef MtreePageOpaqueData *MtreePageOpaque;

typedef struct MtreeElementTupleData
{
	/* data */
	// 1. the radius of this entry
	float8 radius;
	// 2. distance to parent
	float8 distance_to_parent;
	// 3. the pointer to the leaf page
	BlockNumber son_page;
	// parent_blkno, we can get from Opaque
	// BlockNumber parent_page;
	// 4. center data
	Vector vec;
} MtreeElementTupleData;

typedef MtreeElementTupleData *MtreeElementTuple;

typedef struct MtreeElementLeafTupleData
{
	/* data */
	// 1. distance to parent
	float8 distance_to_parent;
	// parent_blokno, we can get from Opaque
	// BlockNumber parent_page;
	// 2. heap tid
	ItemPointerData data_tid;
	// 3. real value data
	Vector vec;
} MtreeElementLeafTupleData;

typedef MtreeElementLeafTupleData *MtreeElementLeafTuple;

typedef struct MtreeNeighborTupleData
{
	uint8 type;
	uint8 unused;
	uint16 count;
	ItemPointerData indextids[FLEXIBLE_ARRAY_MEMBER];
} MtreeNeighborTupleData;

typedef MtreeNeighborTupleData *MtreeNeighborTuple;

typedef struct MtreeScanOpaqueData
{
	bool first;
	List *w;
	MemoryContext tmpCtx;

	/* Support functions */
	FmgrInfo *procinfo;
	FmgrInfo *normprocinfo;
	Oid collation;
} MtreeScanOpaqueData;

typedef MtreeScanOpaqueData *MtreeScanOpaque;

typedef struct MtreeVacuumState
{
	/* Info */
	Relation index;
	IndexBulkDeleteResult *stats;
	IndexBulkDeleteCallback callback;
	void *callback_state;

	/* Settings */
	int m;
	int efConstruction;

	/* Support functions */
	FmgrInfo *procinfo;
	Oid collation;

	/* Variables */
	HTAB *deleted;
	BufferAccessStrategy bas;
	MtreeNeighborTuple ntup;
	MtreeElementData highestPoint;

	/* Memory */
	MemoryContext tmpCtx;
} MtreeVacuumState;

/* Methods */
int MtreeGetM(Relation index);
int MtreeGetEfConstruction(Relation index);
FmgrInfo *MtreeOptionalProcInfo(Relation index, uint16 procnum);
bool MtreeNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector *result);
void MtreeCommitBuffer(Buffer buf, GenericXLogState *state);
Buffer MtreeNewBuffer(Relation index, ForkNumber forkNum);
void MtreeInitPage(Buffer buf, Page page, BlockNumber blkno, uint8 type, uint8 is_root, OffsetNumber offset);
void MtreeInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, uint8 type, uint8 is_root);
void MtreeInit(void);
uint8 union_type(uint8 type, uint8 is_root);
bool is_root(uint8 union_data);
uint8 PageType(uint8 union_data);
List *MtreeSearchLayer(Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo, Oid collation, int m, bool inserting, MtreeElement skipElement);
MtreeElement MtreeGetEntryPoint(Relation index);
MtreeMetaPageData MtreeGetMetaPageInfo(Relation index);
MtreeElement MtreeInitElement(ItemPointer tid, float8 radius, float8 distance_to_parent, BlockNumber son_page, Vector *vector);
float GetDistance(Datum q1, Datum q2, FmgrInfo *procinfo, Oid collation);
void MtreeFreeElement(MtreeElement element);
MtreeElement MtreeInitElementFromBlock(BlockNumber blkno, OffsetNumber offno);
void MtreeInsertElement(MtreeElement element, MtreeElement entryPoint, Relation index, FmgrInfo *procinfo, Oid collation, int m, int efConstruction, bool existing);
MtreeElement MtreeFindDuplicate(MtreeElement e);
MtreeCandidate *MtreeEntryCandidate(MtreeElement em, Datum q, Relation rel, FmgrInfo *procinfo, Oid collation, bool loadVec);
void MtreeUpdateMetaPage(Relation index, BlockNumber root, ForkNumber forkNum);
void MtreeSetNeighborTuple(MtreeNeighborTuple ntup, MtreeElement e, int m);
void MtreeAddHeapTid(MtreeElement element, ItemPointer heaptid);
void MtreeInitNeighbors(MtreeElement element, int m);
bool MtreeInsertTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel, MtreeBuildState *buildstate, GenericXLogState *state);
void MtreeUpdateNeighborPages(Relation index, FmgrInfo *procinfo, Oid collation, MtreeElement e, int m, bool checkExisting);
void MtreeLoadElementFromTuple(MtreeElement element, MtreeElementTuple etup, bool loadHeaptids, bool loadVec);
void MtreeLoadElement(MtreeElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec);
void MtreeSetElementTuple(MtreeElementTuple etup, MtreeElement element);
void MtreeSetLeafElementTuple(MtreeElementLeafTuple etup, MtreeElement element);
void MtreeUpdateConnection(MtreeElement element, MtreeCandidate *hc, int m, int lc, int *updateIdx, Relation index, FmgrInfo *procinfo, Oid collation);
void MtreeLoadNeighbors(MtreeElement element, Relation index, int m);
Page SplitLeafPage(Page page, Page new_page, FmgrInfo *procinfo, Oid collation, MtreeElementLeafTuple insert_data, Datum *left_centor, Datum *right_centor, float8 *left_radius, float8 *right_radius);
void MtreeUpdatePageOpaque(OffsetNumber offset, uint8 is_root, BlockNumber blkno, Page page, uint8 type);
void MtreeUpdatePageOpaqueParentBlockNumber(BlockNumber blkno, Page page);
void DebugPageOpaque(char *msg, Page page, BlockNumber blkno);
float8 max(float8 a, float8 b);
void DebugMetaPage(MtreeMetaPageData page);
void UpdateParentRecurse(Page parent_page, BlockNumber parent_block_num, Relation index, FmgrInfo *procinfo, Oid collation, Page left_son_page, Page right_son_page, MtreeElementTuple left_centor, MtreeElementTuple right_centor, OffsetNumber left_offset, GenericXLogState *state);
static int CompareNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg);
MtreePairingKNNNode *CreateMtreePairingKNNNode(MtreeKNNCandidate *c);
static MtreePairingHeapNode *CreatePairingHeapNode(MtreeCandidate *c);
MtreePairingDistanceOnlyHeapNode *CreatePairingDistanceOnlyHeapNode(MtreeDistanceOnlyCandidate *c);
int CompareDistanceOnlyNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg);
int CompareKNNCandidatesMinHeap(const pairingheap_node *a, const pairingheap_node *b, void *arg);
int CompareKNNCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg);
Page SplitInternalPage(Page internal_page, Page new_page, FmgrInfo *procinfo, Oid collation, MtreeElementTuple left_centor, MtreeElementTuple right_centor, OffsetNumber left_off, Datum *left_copy_up, Datum *right_copy_up, float8 *left_radius, float8 *right_radius);
void PageReplaceItem(Page page, OffsetNumber offset, Item item, Size size);
void DebugEntireMtreeTree(BlockNumber root, Relation index, int level);
#define DatumGetMtreeElementLeafTuple(x) ((MtreeElementLeafTuple)PG_DETOAST_DATUM(x))
#define DatumGetMtreeElementTuple(x) ((MtreeElementTuple)PG_DETOAST_DATUM(x))
/* Index access methods */
IndexBuildResult *mtreebuild(Relation heap, Relation index, IndexInfo *indexInfo);
void mtreebuildempty(Relation index);
bool mtreeinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
				 ,
				 bool indexUnchanged
#endif
				 ,
				 IndexInfo *indexInfo);
IndexBulkDeleteResult *mtreebulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *mtreevacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
IndexScanDesc mtreebeginscan(Relation index, int nkeys, int norderbys);
void mtreerescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool mtreegettuple(IndexScanDesc scan, ScanDirection dir);
void mtreeendscan(IndexScanDesc scan);

#endif

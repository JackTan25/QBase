#pragma once
#include<vector>
#include<string>
#include "lru_index_pointer.h"
#include "hnsw.h"
extern "C"{
	#ifndef M3V_H
	#define M3V_H

	#include "postgres.h"
	#include "vector.h"
	#include "access/generic_xlog.h"
	#include "access/reloptions.h"
	#include "nodes/execnodes.h"
	#include "port.h" /* for random() */
	#include "utils/sampling.h"
	#include "storage/itemptr.h"
}

// we store the vector data in 
using PII = std::pair<std::vector<const float*>,ItemPointerData>;
using PQNode = std::pair<float,int>;
#if PG_VERSION_NUM < 110000
#error "Requires PostgreSQL 11+"
#endif

#define M3V_MAX_DIM 2000

#define M3V_INVALID_DISTANCE -1

/* Support functions */
#define M3V_DISTANCE_PROC 1
#define M3V_NORM_PROC 2
#define M3V_KMEANS_DISTANCE_PROC 3
#define M3V_KMEANS_NORM_PROC 4

#define M3V_VERSION 1
#define M3V_MAGIC_NUMBER 0xA953A953
#define M3V_PAGE_ID 0xFF90

/* Preserved page numbers */
#define M3V_METAPAGE_BLKNO 0
#define M3V_HEAD_BLKNO 1 /* first element page */

/* Must correspond to page numbers since page lock is used */
#define M3V_UPDATE_LOCK 0
#define M3V_SCAN_LOCK 1

/* M3V parameters */
#define M3V_DEFAULT_M 16
#define M3V_MIN_M 2
#define M3V_MAX_M 100
#define M3V_DEFAULT_EF_CONSTRUCTION 64
#define M3V_MIN_EF_CONSTRUCTION 4
#define M3V_MAX_EF_CONSTRUCTION 1000
#define M3V_DEFAULT_EF_SEARCH 40
#define M3V_MIN_EF_SEARCH 1
#define M3V_MAX_EF_SEARCH 1000

/* Page types */
#define M3V_LEAF_PAGE_TYPE 0
#define M3V_INNER_PAGE_TYPE 1
#define M3V_META_PAGE_TYPE 2

/* Make graph robust against non-HOT updates */
#define M3V_HEAPTIDS 10

#define M3V_UPDATE_ENTRY_GREATER 1
#define M3V_UPDATE_ENTRY_ALWAYS 2

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 */
#define PROGRESS_M3V_PHASE_LOAD 2

#define A3V_TUPLE_SZIE sizeof(A3vTuple)

#define M3V_ELEMENT_TUPLE_SIZE(_dim) MAXALIGN(offsetof(m3vElementTupleData, vecs) + VECTOR_SIZE(_dim))
#define M3V_ELEMENT_TUPLE_SIZES(_vecs,_columns,result) \
	do { \
        (result) += (MAXALIGN(offsetof(m3vElementTupleData, vecs))); \
        for (int i = (0); i < (_columns); ++i) { \
            (result) += VECTOR_SIZE(_vecs[i]->dim); \
        } \
    } while (0)

#define M3V_ELEMENT_POINTER_TUPLE_SIZES(_vecs,_columns,result) \
	do { \
		(result) += (MAXALIGN(offsetof(m3vElementTupleData, vecs))); \
		Vector* vec = &_vecs[0]; \
		int offset = 0;	\
        for (int i = (0); i < (_columns); ++i) { \
            (result) += VECTOR_SIZE(vec->dim); \
			offset = VECTOR_SIZE(vec->dim); \
			vec = reinterpret_cast<Vector*>(PointerGetDatum(vec) + offset); \
        } \
    } while (0)

#define M3V_ELEMENT_LEAF_TUPLE_SIZE(_dim) MAXALIGN(offsetof(m3vElementLeafTupleData, vecs) + VECTOR_SIZE(_dim))
#define M3V_ELEMENT_LEAF_TUPLE_SIZES(_vecs,_columns,result) \
	do { \
		(result) += (MAXALIGN(offsetof(m3vElementLeafTupleData, vecs))); \
        for (int i = (0); i < (_columns); ++i) { \
            (result) += VECTOR_SIZE(_vecs[i]->dim); \
        } \
    } while (0)

#define M3V_ELEMENT_POINTER_LEAF_TUPLE_SIZES(_vecs,_columns,result) \
	do { \
		(result) += (MAXALIGN(offsetof(m3vElementLeafTupleData, vecs))); \
		Vector* vec = &_vecs[0]; \
		int offset = 0;	\
        for (int i = (0); i < (_columns); ++i) { \
            (result) += VECTOR_SIZE(vec->dim); \
			offset = VECTOR_SIZE(vec->dim); \
			vec = reinterpret_cast<Vector*>(PointerGetDatum(vec) + offset); \
        } \
    } while (0)

#define M3V_NEIGHBOR_TUPLE_SIZE(level, m) MAXALIGN(offsetof(m3vNeighborTupleData, indextids) + ((level) + 2) * (m) * sizeof(ItemPointerData))

#define m3vPageGetOpaque(page) ((m3vPageOpaque)PageGetSpecialPointer(page))
#define m3vPageGetMeta(page) ((m3vMetaPage)PageGetContents(page))

#if PG_VERSION_NUM >= 150000
#define RandomDouble() pg_prng_double(&pg_global_prng_state)
#else
#define RandomDouble() (((double)random()) / MAX_RANDOM_VALUE)
#endif

#if PG_VERSION_NUM < 130000
#define list_delete_last(list) list_truncate(list, list_length(list) - 1)
#define list_sort(list, cmp) list_qsort(list, cmp)
#endif

#define m3vIsElementTuple(tup) ((tup)->type == M3V_ELEMENT_TUPLE_TYPE)
#define m3vIsNeighborTuple(tup) ((tup)->type == M3V_NEIGHBOR_TUPLE_TYPE)

/* 2 * M connections for ground layer */
#define m3vGetLayerM(m, layer) (layer == 0 ? (m) * 2 : (m))

/* Optimal ML from paper */
#define m3vGetMl(m) (1 / log(m))

/* Ensure fits on page and in uint8 */
#define m3vGetMaxLevel(m) Min(((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(m3vPageOpaqueData)) - offsetof(m3vNeighborTupleData, indextids) - sizeof(ItemIdData)) / (sizeof(ItemPointerData)) / m) - 2, 255)

/* Variables */
extern int m3v_ef_search;

typedef struct m3vNeighborArray m3vNeighborArray;

// ==========================
// tanboyu
// ==========================
// this is not a strict m3-tree implemnetation,
// I will use the combination way to implement it.
// Reference to paper: 'Dynamic Similarity Search in Multi-Metric Spaces'
// and  'M-tree: An efficient access method for similarity search in metric spaces'
// Don't use the 3.1 to implement the Range Query And KNN Query
typedef struct m3vElementData
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
	// 5. item_poniter
	ItemPointer item_pointer;
	// 4. center data
	Vector *vecs[FLEXIBLE_ARRAY_MEMBER];
} m3vElementData;

// One block can only hold 200 entrys at most
#define MaxElementNumsPerBlock 200

typedef m3vElementData *m3vElement;

typedef struct m3vCandidate
{
	int id;
	float8 distance;
	Datum element;
} m3vCandidate;

typedef struct m3vKNNCandidate
{
	ItemPointer tid;
	ItemPointerData data;
	BlockNumber son_blkno;
	// func_dmin_node(entry,target,R(entry)),
	// estimated distance
	float8 distance;
	// the parent entry's distance from target
	float8 target_parent_distance;
} m3vKNNCandidate;

typedef struct m3vPairingKNNNode
{
	pairingheap_node ph_node;
	m3vKNNCandidate *inner;
} m3vPairingKNNNode;

typedef struct m3vDistanceOnlyCandidate
{
	float8 distance;
	BlockNumber son_page;
} m3vDistanceOnlyCandidate;

typedef struct m3vNeighborArray
{
	int length;
	m3vCandidate *items;
} m3vNeighborArray;

typedef struct m3vPairingHeapNode
{
	pairingheap_node ph_node;
	m3vCandidate *inner;
} m3vPairingHeapNode;

typedef struct m3vPairingDistanceOnlyHeapNode
{
	pairingheap_node ph_node;
	m3vDistanceOnlyCandidate *inner;
} m3vPairingDistanceOnlyHeapNode;

/* M3V index options */
typedef struct m3vOptions
{
	int32 vl_len_;		/* varlena header (do not touch directly!) */
	bool memory_index;
	float close_query_threshold;
} m3vOptions;

typedef struct m3vBuildState
{
	/* Info */
	Relation heap;
	Relation index;
	IndexInfo *indexInfo;
	ForkNumber forkNum;

	/* Settings */
	int dimensions;

	/* Statistics */
	double indtuples;
	double reltuples;

	/* Support functions */
	FmgrInfo *procinfo;
	FmgrInfo *normprocinfo;
	Oid collation;

	/* Variables */
	List *elements;
	m3vElement entryPoint;
	double ml;
	int maxLevel;
	double maxInMemoryElements;
	bool flushed;
	Vector *normvec;

	/* Memory */
	MemoryContext tmpCtx;

	// for disk_a3v, we store the tids and data (in rocksdb) seperately.
	// we need to store all heap tids, for crack and query.
	std::vector<ItemPointerData> tids;
	// for data_points, we should store tids and data together.
	std::vector<PII> data_points;
	int tuples_num;
	std::vector<int> dims;
	bool is_first;
	int cur_c;
} m3vBuildState;

typedef struct m3vMetaPageData
{
	// the root page of current m3v
	BlockNumber root;
	// cols number of index
	uint16 columns;
	uint16 simliar_query_root_nums;
	uint32_t tuple_nums;
	// dimentions of every col
	uint16 dimentions[FLEXIBLE_ARRAY_MEMBER];
} m3vMetaPageData;

typedef m3vMetaPageData *m3vMetaPage;

typedef struct m3vPairngHeapUtils
{
	pairingheap *left;
	pairingheap *right;
	bool visited[FLEXIBLE_ARRAY_MEMBER];
} m3vPairingHeapUtils;

typedef m3vPairingHeapUtils *m3vPairingHeapP;

typedef struct m3vPageOpaqueData
{
	// BlockNumber nextblkno;
	// uint16 unused;
	// uint16 page_id; /* for identification of M3V indexes */
	BlockNumber parent_blkno;
	uint8 type;
	// offset in parent page
	OffsetNumber offset;
} m3vPageOpaqueData;

typedef m3vPageOpaqueData *m3vPageOpaque;

typedef struct m3vElementTupleData
{
	/* data */
	// 1. the radius of this entry
	float8 radius;
	// 2. distance to parent(1.0)
	float8 distance_to_parent;
	// 3. the pointer to the leaf page
	BlockNumber son_page;
	// parent_blkno, we can get from Opaque
	// BlockNumber parent_page;
	// 4. center data
	Vector vecs[FLEXIBLE_ARRAY_MEMBER];
} m3vElementTupleData;

typedef m3vElementTupleData *m3vElementTuple;

typedef struct m3vElementLeafTupleData
{
	/* data */
	// 1. distance to parent
	float8 distance_to_parent;
	// parent_blokno, we can get from Opaque
	// BlockNumber parent_page;
	// 2. heap tid
	ItemPointerData data_tid;
	// 3. real value data,in fact this struct doesn't cost m3vElementLeafTupleData's real size.
	// but it pointer to the first Vector(if there is over one Vector).
	Vector vecs[FLEXIBLE_ARRAY_MEMBER];
} m3vElementLeafTupleData;

typedef m3vElementLeafTupleData *m3vElementLeafTuple;

typedef struct m3vNeighborTupleData
{
	uint8 type;
	uint8 unused;
	uint16 count;
	ItemPointerData indextids[FLEXIBLE_ARRAY_MEMBER];
} m3vNeighborTupleData;

typedef m3vNeighborTupleData *m3vNeighborTuple;
// ItemPointerData is used to specify the tuple position of 
typedef std::tuple<float, ItemPointerData> knn_guide;
// for q's index to get heap table's heaptid.
typedef std::tuple<float, int> knn_result;

struct MinHeapComp {
    bool operator()(const knn_guide& lhs, const knn_guide& rhs) const {
        return std::get<0>(lhs) > std::get<0>(rhs);
    }
};

struct MaxHeapComp {
    bool operator()(const knn_result& lhs, const knn_result& rhs) const {
        return std::get<0>(lhs) < std::get<0>(rhs);
    }
};

typedef struct A3vTuple{
	uint32_t low;
	uint32_t high;
	ItemPointerData left;
	ItemPointerData right;
	uint32_t query_id;
	float radius;
	A3vTuple(uint32_t low_,uint32_t high_,ItemPointerData& left_,ItemPointerData& right_,uint32_t query_id_,float radius_):
	low(low_),high(high_),left(left_),right(right_),query_id(query_id_),radius(radius_){
	}
}A3vTuple;

typedef struct m3vScanOpaqueData
{
	bool first;
	List *w;
	MemoryContext tmpCtx;

	/* Support functions */
	FmgrInfo *procinfo;
	FmgrInfo *normprocinfo;
	Oid collation;
	int columns;
	std::vector<ItemPointerData> tids;
	std::vector<PII>* data_points;
	std::vector<int>* result_ids;
	int result_idx;
	// 1. we support 3 vector search at most.
	std::vector<float>* weights;
	// 2. the largest dimension is 300, in fact we should make it configureable in CMakeLists.
	// for now, we use this for experiment.
	float query_point[300];
	// 3. dimentions
	// uint16 dimentions[3];
	// 4. float lens,if vector(3),vector(4),vector(5),then it's 12
	uint16 total_len;
	// 4. use hnsw to search the close root index.
	hnswlib::HierarchicalNSW<float>* alg_hnsw;
	ItemPointerData root_tid;
	IndexPointerLruCache* cache;
	int index_pages;
	int tuple_nums;
	int search_type;
	MultiColumnHnsw* hard_hnsws;
	bool use_hard_hnsw;
	bool load_hnsw_from_disk;
	int range_next_times{0};
} m3vScanOpaqueData;

typedef m3vScanOpaqueData *m3vScanOpaque;

typedef struct m3vVacuumState
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
	m3vNeighborTuple ntup;
	// we don't care about this for now.
	// m3vElementData highestPoint;
	/* Memory */
	MemoryContext tmpCtx;
} M3vVacuumState;

/* Methods */
FmgrInfo *m3vOptionalProcInfo(Relation index, uint16 procnum);
bool m3vNormValue(FmgrInfo *procinfo, Oid collation, Datum *value, Vector *result);
void m3vCommitBuffer(Buffer buf, GenericXLogState *state);
Buffer m3vNewBuffer(Relation index, ForkNumber forkNum);
void m3vInitPage(Buffer buf, Page page, BlockNumber blkno, uint8 type, uint8 is_root, OffsetNumber offset);
void m3vInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, uint8 type, uint8 is_root);
void m3vInit(void);
uint8 union_type(uint8 type, uint8 is_root);
bool is_root(uint8 union_data);
uint8 PageType(uint8 union_data);
List *m3vSearchLayer(Datum q, List *ep, int ef, int lc, Relation index, FmgrInfo *procinfo, Oid collation, int m, bool inserting, m3vElement skipElement);
m3vElement m3vGetEntryPoint(Relation index);
m3vMetaPageData m3vGetMetaPageInfo(Relation index);
m3vElement m3vInitElement(ItemPointer tid, float8 radius, float8 distance_to_parent, BlockNumber son_page, Datum *values,int columns);
m3vElement m3vInitVectorElement(ItemPointer tid, float8 radius, float8 distance_to_parent, BlockNumber son_page, Vector* vec,int columns);
float GetDistance(Datum q1, Datum q2, FmgrInfo *procinfo, Oid collation);
float GetDistances(Vector* vecs1,Vector* vecs2, FmgrInfo *procinfo, Oid collation,int columns);
float GetPointerDistances(Vector* vecs1,Vector** vecs2, FmgrInfo *procinfo, Oid collation,int columns);
void m3vFreeElement(m3vElement element);
m3vElement m3vInitElementFromBlock(BlockNumber blkno, OffsetNumber offno);
void m3vInsertElement(m3vElement element, m3vElement entryPoint, Relation index, FmgrInfo *procinfo, Oid collation, int m, int efConstruction, bool existing);
m3vElement m3vFindDuplicate(m3vElement e);
m3vCandidate *m3vEntryCandidate(m3vElement em, Datum q, Relation rel, FmgrInfo *procinfo, Oid collation, bool loadVec);
void m3vUpdateMetaPage(Relation index, BlockNumber root, ForkNumber forkNum);
void m3vSetNeighborTuple(m3vNeighborTuple ntup, m3vElement e, int m);
void m3vAddHeapTid(m3vElement element, ItemPointer heaptid);
void m3vInitNeighbors(m3vElement element, int m);
bool m3vInsertTuple(Relation index, m3vElement element, bool *isnull, ItemPointer heap_tid, Relation heapRel, m3vBuildState *buildstate, GenericXLogState *state,int columns);
void m3vUpdateNeighborPages(Relation index, FmgrInfo *procinfo, Oid collation, m3vElement e, int m, bool checkExisting);
void m3vLoadElementFromTuple(m3vElement element, m3vElementTuple etup, bool loadHeaptids, bool loadVec);
void m3vLoadElement(m3vElement element, float *distance, Datum *q, Relation index, FmgrInfo *procinfo, Oid collation, bool loadVec);
void m3vSetElementTuple(m3vElementTuple etup, m3vElement element,int columns);
void m3vSetLeafElementTuple(m3vElementLeafTuple etup, m3vElement element,int columns);
void m3vUpdateConnection(m3vElement element, m3vCandidate *hc, int m, int lc, int *updateIdx, Relation index, FmgrInfo *procinfo, Oid collation);
void m3vLoadNeighbors(m3vElement element, Relation index, int m);
Page SplitLeafPage(Page page, Page new_page, FmgrInfo *procinfo, Oid collation, m3vElementLeafTuple insert_data, Datum *left_centor, Datum *right_centor, float8 *left_radius, float8 *right_radius,int columns);
void m3vUpdatePageOpaque(OffsetNumber offset, uint8 is_root, BlockNumber blkno, Page page, uint8 type);
void m3vUpdatePageOpaqueParentBlockNumber(BlockNumber blkno, Page page);
void DebugPageOpaque(char *msg, Page page, BlockNumber blkno);
float8 max(float8 a, float8 b);
void DebugMetaPage(m3vMetaPageData page);
void UpdateParentRecurse(Page parent_page, BlockNumber parent_block_num, Relation index, FmgrInfo *procinfo, Oid collation, Page left_son_page, Page right_son_page, m3vElementTuple left_centor, m3vElementTuple right_centor, OffsetNumber left_offset, GenericXLogState *state,int columns);
static int CompareNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg);
m3vPairingKNNNode *Createm3vPairingKNNNode(m3vKNNCandidate *c);
static m3vPairingHeapNode *CreatePairingHeapNode(m3vCandidate *c);
m3vPairingDistanceOnlyHeapNode *CreatePairingDistanceOnlyHeapNode(m3vDistanceOnlyCandidate *c);
int CompareDistanceOnlyNearestCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg);
int CompareKNNCandidatesMinHeap(const pairingheap_node *a, const pairingheap_node *b, void *arg);
int CompareKNNCandidates(const pairingheap_node *a, const pairingheap_node *b, void *arg);
Page SplitInternalPage(Page internal_page, Page new_page, FmgrInfo *procinfo, Oid collation, m3vElementTuple left_centor, m3vElementTuple right_centor, OffsetNumber left_off, Datum *left_copy_up, Datum *right_copy_up, float8 *left_radius, float8 *right_radius,int columns);
void PageReplaceItem(Page page, OffsetNumber offset, Item item, Size size);
void DebugEntirem3vTree(BlockNumber root, Relation index, int level,int columns);
#define DatumGetm3vElementLeafTuple(x) ((m3vElementLeafTuple)PG_DETOAST_DATUM(x))
#define DatumGetm3vElementTuple(x) ((m3vElementTuple)PG_DETOAST_DATUM(x))
/* Index access methods */
IndexBuildResult *m3vbuild(Relation heap, Relation index, IndexInfo *indexInfo);
void m3vbuildempty(Relation index);
bool m3vinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
			   ,
			   bool indexUnchanged
#endif
			   ,
			   IndexInfo *indexInfo);
IndexBulkDeleteResult *m3vbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *m3vvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
IndexScanDesc m3vbeginscan(Relation index, int nkeys, int norderbys);
void m3vrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool m3vgettuple(IndexScanDesc scan, ScanDirection dir);
int64 m3vgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
void m3vendscan(IndexScanDesc scan);
void a3vUpdateMetaPage(Relation index,uint16 simliar_query_root_nums,uint32_t tuple_nums, ForkNumber forkNum);
std::string build_data_string_datum(Datum* values,int columns);

#endif

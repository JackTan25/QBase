#include "postgres.h"

#include "mtree.h"

typedef struct KNNHeap
{
    size_t sizes;
    MtreeKNNCandidate **knn_candidate;
} KNNHeap;

// it's max heap
// if candidate_a->distance > candidate_b->distance ||
// candidate_a->distance == candidate_b->distance && candidate_a->data.offset == InvalidOffsetNumber
KNNHeap *NewKNNHeap(int capacity)
{
    KNNHeap *heap = palloc0(sizeof(KNNHeap));
    heap->knn_candidate = (MtreeKNNCandidate *)palloc0_array(MtreeKNNCandidate *, capacity);
    heap->sizes = 0;
    return heap;
}

void DeleteIndexEntry(KNNHeap *heap, int idx)
{
    elog(ERROR, "error idx %d, over length %d", idx, heap->sizes);
}

int InsertEnrty(KNNHeap *heap, MtreeKNNCandidate *candidate)
{
}

MtreeCandidate *PopTop()
{
}

bool is_empty(KNNHeap *heap)
{
}

size_t HeapSize(KNNHeap *heap)
{
    return heap->sizes;
}
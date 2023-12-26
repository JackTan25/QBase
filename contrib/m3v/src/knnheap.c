#include "postgres.h"

#include "m3v.h"

typedef struct KNNHeap
{
	size_t sizes;
	m3vKNNCandidate **knn_candidate;
} KNNHeap;

// it's max heap
// if candidate_a->distance > candidate_b->distance ||
// candidate_a->distance == candidate_b->distance && candidate_a->data.offset == InvalidOffsetNumber
KNNHeap *NewKNNHeap(int capacity)
{
	KNNHeap *heap = palloc0(sizeof(KNNHeap));
	heap->knn_candidate = (m3vKNNCandidate *)palloc0_array(m3vKNNCandidate *, capacity);
	heap->sizes = 0;
	return heap;
}

void DeleteIndexEntry(KNNHeap *heap, int idx)
{
	elog(ERROR, "error idx %d, over length %d", idx, heap->sizes);
}

int InsertEnrty(KNNHeap *heap, m3vKNNCandidate *candidate)
{
}

m3vCandidate *PopTop()
{
}

bool is_empty(KNNHeap *heap)
{
}

size_t HeapSize(KNNHeap *heap)
{
	return heap->sizes;
}
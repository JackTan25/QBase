#ifndef VECTOR_H
#define VECTOR_H

#define VECTOR_MAX_DIM 16000

#define VECTOR_SIZE(_dim) (offsetof(Vector, x) + sizeof(float) * (_dim))
#define DatumGetVector(x) ((Vector *)PG_DETOAST_DATUM(x))
#define PG_GETARG_VECTOR_P(x) DatumGetVector(PG_GETARG_DATUM(x))
#define PG_RETURN_VECTOR_P(x) PG_RETURN_POINTER(x)

extern "C"{
	#include "storage/bufmgr.h"
}

typedef struct Vector
{
	int32 vl_len_; /* varlena header (do not touch directly!) */
	int16 dim;	   /* number of dimensions */
	int16 unused;
	// float distance_to_parent_in_dim;
	// float radius_in_dim;
	float x[FLEXIBLE_ARRAY_MEMBER];
} Vector;

Vector *InitVector(int dim);
void PrintLeafPageVectors(char *msg, Page page,int columns);
void PrintInternalPageVectors(char *msg, Page page,int columns);
void PrintVector(char *msg, Vector *vector);
void PrintVectors(char *msg, Vector *vector,int columns);
void PrintPointerVectors(char *msg, Vector **vector,int columns);
int vector_cmp_internal(Vector *a, Vector *b);

#endif

/*
 * contrib/mtree_am/mtree_numeric.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/
#include "postgres.h"
#include "fmgr.h"
#include "utils/numeric.h"

/*****************************************************************************
 * New Operators
 *
 * A practical numeric datatype would provide much more than this, of course.
 *****************************************************************************/
PG_FUNCTION_INFO_V1(numeric_dist);
Datum
numeric_dist(PG_FUNCTION_ARGS)
{
	Numeric* a = (Numeric*) PG_GETARG_NUMERIC(0);
    Numeric* b = (Numeric*) PG_GETARG_NUMERIC(1);
    Numeric* result;
    if(a>b){
        result = a - b;
    }else{
        result = b - a;
    }
	PG_RETURN_POINTER(result);
}
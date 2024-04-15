#include <stddef.h>
typedef float (*SIMDFuncType)(const void *pv1, const void *pv2, const void *dim);
extern SIMDFuncType SIMDFunc;
void SetSIMDFunc();

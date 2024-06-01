#include <stddef.h>
typedef float (*SIMDFuncType)(const void *pv1, const void *pv2, const void *dim);
float hyper_distance_func_with_weights(const float *query,const std::vector<const float*>& data_point, const std::vector<int> &dimensions,std::vector<float>& weights,float* distance_1);
float hyper_distance_func_with_weights_internal_query(const float *query,const float* data_point, const std::vector<int> &dimensions,std::vector<float>& weights,float* distance_1);
float optimized_simd_distance_func(float *query,float* data_point,int dim);
extern SIMDFuncType SIMDFunc;
void SetSIMDFunc();

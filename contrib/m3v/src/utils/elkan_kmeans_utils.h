// elkan_kmeans is a better way to speed up the partition vectors into two pattitions.
// reference to https://github.com/JackTan25/kmeans_util_based_on_eigen.git, we use eigen libarary
// to speed up the spilt processure.
// reference paper: 《Using the Triangle Inequality to Accelerate  K-Means》
// we use elkan-kmeans++ to acclerate kmeans++ speed and use it for m3v tree split.
// thanks to pgvecto.rs's implementation in rust.
#pragma once

#include<iostream>
#include<random>
#include "record_io.h"

// ItemPointerData centers_pointer_initial = {{0xFFFF,0xFF00},0};

// ItemPointer GetNextItemPointer(){
// 	if(centers_pointer_initial.ip_posid < 0xFF){
// 		centers_pointer_initial.ip_posid++;
// 	}else{
// 		centers_pointer_initial.ip_blkid.bi_lo++;
// 		centers_pointer_initial.ip_posid = 0;
// 	}
// 	return &centers_pointer_initial;
// }
#define CENTER_MEMORY_SIZE 3 * 1024
uint8_t centers_memory1[CENTER_MEMORY_SIZE];
uint8_t centers_memory2[CENTER_MEMORY_SIZE];

// square[j,i] means the distance's square of centers[i] and records[j]
class Square{
    public:
        Square(int center_size,int record_size):x(record_size),y(center_size){
            distances.resize(x*y);
        }

        float& get_index_value_ref(int j,int i){
            return distances[i*x+j];
        }
    private:
        // x is the point size in records
        int x;
        // y is the point size in centers
        int y;
        std::vector<float> distances;
};

const float DELTA = 1.0 / 1024.0;

// define VectorRecord Compute Distance Function Pointer Type
using RecordDistanceFunc  = float(*)(VectorRecord* v1,VectorRecord* v2);
using RecordRealSumVectorFunc = float(*)(VectorRecord* record1,VectorRecord* record2,const std::vector<uint32_t>& offsets);
class ElkanKmeans{
    public:
		void InitCenters(int vector_size){
			int off = 0;
			for(int i = 0;i < centers_size_;i++){
				if(use_center_memory1){
					memset(centers_memory1,0,sizeof(centers_memory1));
					centers[i] = VectorRecord(centers_memory1 + off,vector_size);
				}else{
					memset(centers_memory2,0,sizeof(centers_memory2));
					centers[i] = VectorRecord(centers_memory2 + off,vector_size);
				}
				off += vector_size;
			}
			use_center_memory1 = !use_center_memory1;
		}

        // initial the ElkanKmeans:
        // 1. pick the initial k centers
        // 2. update lower_bounds and upper_bounds
        ElkanKmeans(uint16_t iterations_times,RecordDistanceFunc distanceFunc,RecordRealSumVectorFunc  distanceSumFunc,int centers_size,std::vector<VectorRecord>& records,bool stable,const std::vector<uint32_t> &offsets):
        centers_size_(centers_size),distanceFunc_(distanceFunc),distanceSumFunc_(distanceSumFunc),iterations_times_(iterations_times),records_(records),lower_bounds(centers_size,records.size()),finished(false),
		current_iterate_times(0),use_center_memory1(false),stable_(stable),offsets_(offsets){
            // initial centers using kmeans++ algorithm
            int n = records.size();
            centers.resize(centers_size_);
            upper_bounds.resize(n);
            float float_infinity = std::numeric_limits<float>::max();
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> dist_real(0.0, 1.0);
            std::uniform_int_distribution<> dist_int(0, n);
            std::vector<float4> weights(n,float_infinity);
            assigns.resize(n);
			if(stable_){
				centers[0] = records_[0];
			}else{
				centers[0] = records_[dist_int(gen)];
			}
			
            // try initialize center0, and then use kmeans++ alogrithm to 
            // initial the rest centers.
            for(int i = 0;i < centers_size;i++){
                float sum = 0;
                for(int j = 0;j < n;j++){
                    float dis = GetVectorDistance(&records[j],&centers[i]);
                    float& low_bound = lower_bounds.get_index_value_ref(j,i);
                    low_bound = dis;
                    if(dis * dis < weights[j]){
                        weights[j] = dis * dis;
                    }
                    sum += weights[j];
                }
                if(i+1 == centers_size){
                    break;
                }
				float choice = sum * dist_real(gen);
				if(stable_){
					choice = sum * 0.5;
				}
                
                int index = n-1;
                for(int j = 0;j < n;j++){
                    choice -= weights[j];
                    if (choice <= 0.0){
                        index = j;
                        break;
                    }
                }
                centers[i+1] = records[index];
            }

            // update upperbound
            for(int i = 0;i < n;i++){
                float minimal = float_infinity;
                int target = 0;
                for(int j = 0;j < centers_size;j++){
                    float dis = lower_bounds.get_index_value_ref(i,j);
                    if(dis < minimal){
                        minimal = dis;
                        target = j;
                    }
                }
                assigns[i] = target;
                upper_bounds[i] = minimal;
            }
        }

        // apply paper's 7 steps, we will repeat this iterate until convergence.
        bool iteration(){
            if(finished) return true;
            float float_infinity = std::numeric_limits<float>::max();
            Square cc(centers_size_,centers_size_);
            int n = records_.size();
            std::vector<float> sp(centers_size_,0);
                       std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> dist_real(0.0, 1.0);
            std::uniform_int_distribution<> dist_int(0, n);
            int change = 0;
            // step 1
            for(int i = 0;i < centers_size_;i++){
                for(int j =i+1;j<centers_size_;j++){
                    float dis = distanceFunc_(&centers[i],&centers[j]);
                    cc.get_index_value_ref(i,j) = dis;
                    cc.get_index_value_ref(j,i) = dis;
                }
            }

            for(int i = 0;i < centers_size_;i++){
                float minimal = float_infinity;
                for(int j = 0;j < centers_size_;j++){
                    if(i == j){
                        continue;
                    }
                    float dis = cc.get_index_value_ref(i,j);
                    if(dis < minimal){
                        minimal = dis;
                    }
                }
                sp[i] = minimal;
            }

            // step2
            for(int i = 0;i  < n;i++){
                if (upper_bounds[i] <= sp[assigns[i]]){
                    continue;
                }
                float minimal = distanceFunc_(&records_[i],&centers[assigns[i]]);
                lower_bounds.get_index_value_ref(i,assigns[i]) = minimal;
                upper_bounds[i] = minimal;

                // Step 3
                for(int j = 0;j < centers_size_;j++){
                    if(j == assigns[i]){
                        continue;
                    }
                    if(upper_bounds[i] <= lower_bounds.get_index_value_ref(i,j)){
                        continue;
                    }
                    if(upper_bounds[i] <= cc.get_index_value_ref(assigns[i],j)){
                        continue;
                    }
                    if(minimal > lower_bounds.get_index_value_ref(i,j) || minimal > cc.get_index_value_ref(assigns[i],j)){
                        float dis = distanceFunc_(&records_[i],&centers[j]);
                        lower_bounds.get_index_value_ref(i,j) = dis;
                        if(dis < minimal){
                            minimal = dis;
                            assigns[i] = j;
                            upper_bounds[i] = dis;
                            change += 1;
                        }
                    }
                }
            }

            // Step 4,7
            std::vector<float> count(centers_size_,0);
            // preserve the previous c'
            std::vector<VectorRecord> olds = centers;
            InitCenters(records_[0].GetSize());
            int dims = centers[0].GetSize()/DIM_SIZE;
            for(int i = 0;i < n;i++){
				float* float1 = reinterpret_cast<float*>(centers[assigns[i]].GetData());
				float* float2 = reinterpret_cast<float*>(records_[i].GetData());
				for(int dim = 0;dim < dims;dim++){
					float1[dim] += float2[dim];
				}
                count[assigns[i]] += 1.0;
            }

            for(int i = 0;i < centers_size_;i++){
                if(count[i] == 0.0){
                    continue;
                }
                float* record = reinterpret_cast<float*>(centers[i].GetData());
               
                for(int dim = 0; dim < dims;dim++){
                    record[dim] /= count[i];
                }
            }

            for(int i = 0;i < centers_size_;i++){
                if(count[i] != 0){
                    continue;
                }
                int o = 0;
                for(;;){
                    float alpha = dist_real(gen);
					if(stable_){
						alpha = 0.5;
					}
                    float beta = (count[o] - 1.0) / (n - centers_size_);
                    if(alpha < beta){
                        break;
                    }
                    o = (o + 1) % centers_size_;
                }
                centers[i] = centers[o];
                float* record_i = reinterpret_cast<float*>(centers[i].GetData());
                float* record_o = reinterpret_cast<float*>(centers[o].GetData());
                for(int dim = 0; dim < dims;dim++){
                    if(dim % 2 == 0){
                        record_i[dim] *= 1.0 + DELTA;
                        record_o[dim] *= 1.0 - DELTA;
                    }else{
                        record_i[dim] *= 1.0 - DELTA;
                        record_o[dim] *= 1.0 + DELTA;
                    }
                }
                count[i] = count[o]/2.0;
                count[o] = count[o] - count[i];
            }
            // should we do normalize?
            // for current implementation, we use l2 distance,
            // so we don't need to do this.
            /* for(int i = 0; i < centers_size_;i++){
            *   normalize(centers);
            *  }
            */

           // Step 5,6
           std::vector<float> dist1(centers_size_,0);
           for(int i = 0;i < centers_size_;i++){
              dist1[i] = distanceFunc_(&olds[i],&centers[i]);
           }

           for(int i = 0;i < n;i++){
                for(int j = 0;j < centers_size_;j++){
                    lower_bounds.get_index_value_ref(i,j) = std::max(lower_bounds.get_index_value_ref(i,j)-dist1[j],float(0.0));
                }
           }
           for(int i = 0;i < n;i++){
                upper_bounds[i] += dist1[assigns[i]];
           }
		   current_iterate_times++;
		   if(current_iterate_times >= iterations_times_){
				finished = true;
				return true;
		   }
           // should we end iteration
           return change == 0;
        }
        
        void set_finished(){
            finished = true;
        }

		bool is_finished(){
			return finished;
		}

        const std::vector<int>& GetAssigns(){
            return assigns;
        }

        const std::vector<VectorRecord>& GetCenters(){
			std::vector<float> dists(centers_size_,0x3f3f3f3f);
			for(int i = 0;i < assigns.size();i++){
				// find the closest point for every center
				float dist = GetVectorDistance(&centers[assigns[i]],&records_[i]);
				if(dist < dists[assigns[i]]){
					centers[assigns[i]] = records_[i];
					dists[assigns[i]] = dist;
				}
			}
            return centers;
        }

		float GetVectorDistance(VectorRecord* v1,VectorRecord* v2){
			if(distanceFunc_){
				return distanceFunc_(v1,v2);
			}else{
				assert(distanceRealVectorSumFunc!=nullptr);
				return distanceSumFunc_(v1,v2,offsets_);
			}
		}
    private:
        uint16_t centers_size_;
        RecordDistanceFunc distanceFunc_;
		RecordRealSumVectorFunc distanceSumFunc_;
        uint16_t iterations_times_;
		std::vector<uint32_t> offsets_;
        std::vector<VectorRecord>& records_;
        std::vector<VectorRecord> centers;
		std::vector<std::pair<IndexPointer,VectorRecord>> centers_temp;
        // records[i] belongs to centers[assigns[i]]
        std::vector<int> assigns;
        Square lower_bounds;
        std::vector<float> upper_bounds;
        // iteratoration finished
        bool finished;
		uint16_t current_iterate_times;
		bool use_center_memory1;
		bool stable_;
};

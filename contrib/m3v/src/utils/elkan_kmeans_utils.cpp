// elkan_kmeans is a better way to speed up the partition vectors into two pattitions.
// reference to https://github.com/JackTan25/kmeans_util_based_on_eigen.git, we use eigen libarary
// to speed up the spilt processure.
// reference paper: 《Using the Triangle Inequality to Accelerate  K-Means》
// we use elkan-kmeans++ to acclerate kmeans++ speed and use it for m3v tree split.
// thanks to pgvecto.rs's implementation in rust.
#include<iostream>
#include<random>
#include "record_io.h"

// square[j,i] means the distance's square of centors[i] and records[j]
class Square{
    public:
        Square(int centor_size,int record_size):x(record_size),y(centor_size){
            distances.resize(x*y);
        }

        float& get_index_value_ref(int j,int i){
            return distances[i*x+j];
        }
    private:
        // x is the point size in records
        int x;
        // y is the point size in centors
        int y;
        std::vector<float> distances;
};

const float DELTA = 1.0 / 1024.0;

// define VectorRecord Compute Distance Function Pointer Type
using RecordDistanceFunc  = float4(*)(VectorRecord* v1,VectorRecord* v2);
// The new algorithm chapter: 7 steps
class ElkanKmeans{
    public:
        // initial the ElkanKmeans:
        // 1. pick the initial k centors
        // 2. update lower_bounds and upper_bounds
        ElkanKmeans(uint16_t iterations_times,RecordDistanceFunc distanceFunc,int centors_size,std::vector<VectorRecord>& records):
        centors_size_(centors_size),distanceFunc_(distanceFunc),iterations_times_(iterations_times_),records_(records),lower_bounds(centors_size,records.size()),finished(false){
            // initial centors using kmeans++ algorithm
            int n = records.size();
            centors.resize(centors_size);
            upper_bounds.resize(n);
            float float_infinity = std::numeric_limits<float>::max();
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> dist_real(0.0, 1.0);
            std::uniform_int_distribution<> dist_int(0, n);
            std::vector<float4> weights(n,float_infinity);
            assigns.reserve(n);
            // try initialize centor0, and then use kmeans++ alogrithm to 
            // initial the rest centors.
            for(int i = 0;i < centors_size;i++){
                float sum = 0;
                for(int j = 0;j < n;j++){
                    float dis = distanceFunc(&records[j],&centors[i]);
                    float& low_bound = lower_bounds.get_index_value_ref(j,i);
                    low_bound = dis;
                    if(dis * dis < weights[j]){
                        weights[j] = dis * dis;
                    }
                    sum += weights[j];
                }
                if(i+1 == centors_size){
                    break;
                }
                float choice = sum * dist_real(gen);
                int index = n-1;
                for(int j = 0;j < n;j++){
                    choice -= weights[j];
                    if (choice <= 0.0){
                        index = j;
                        break;
                    }
                }
                centors[i+1] = records[index];
            }

            // update upperbound
            for(int i = 0;i < n;i++){
                float minimal = float_infinity;
                int target = 0;
                for(int j = 0;j < centors_size;j++){
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
            Square cc(centors_size_,centors_size_);
            int n = records_.size();
            std::vector<float> sp(centors_size_,0);
                       std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> dist_real(0.0, 1.0);
            std::uniform_int_distribution<> dist_int(0, n);
            int change = 0;
            // step 1
            for(int i = 0;i < centors_size_;i++){
                for(int j =i+1;j<centors_size_;j++){
                    float dis = distanceFunc_(&centors[i],&centors[j]);
                    cc.get_index_value_ref(i,j) = dis;
                    cc.get_index_value_ref(j,i) = dis;
                }
            }

            for(int i = 0;i < centors_size_;i++){
                float minimal = float_infinity;
                for(int j = 0;j < centors_size_;j++){
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
                float minimal = distanceFunc_(&records_[i],&centors[assigns[i]]);
                lower_bounds.get_index_value_ref(i,assigns[i]) = minimal;
                upper_bounds[i] = minimal;

                // Step 3
                for(int j = 0;j < centors_size_;j++){
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
                        float dis = distanceFunc_(&records_[i],&centors[j]);
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
            std::vector<float> count(centors_size_,0);
            // preserve the previous c'
            std::vector<VectorRecord> olds = centors;
            for(int i = 0;i < centors_size_;i++){
                memset(centors[i].GetData(),0,centors[i].GetSize());
            }
            int dims = centors[0].GetSize()/DIM_SIZE;
            for(int i = 0;i < n;i++){
                centors[assigns[i]] = records_[i];
                count[assigns[i]] += 1.0;
            }
            for(int i = 0;i < centors_size_;i++){
                if(count[i] == 0.0){
                    continue;
                }
                float* record = reinterpret_cast<float*>(centors[i].GetData());
               
                for(int dim = 0; dim < dims;dim++){
                    record[dim] /= count[i];
                }
            }

            for(int i = 0;i < centors_size_;i++){
                if(count[i] != 0){
                    continue;
                }
                int o = 0;
                for(;;){
                    float alpha = dist_real(gen);
                    float beta = (count[o] - 1.0) / (n - centors_size_);
                    if(alpha < beta){
                        break;
                    }
                    o = (o + 1) % centors_size_;
                }
                centors[i] = centors[o];
                float* record_i = reinterpret_cast<float*>(centors[i].GetData());
                float* record_o = reinterpret_cast<float*>(centors[o].GetData());
                for(int dim = 0; dim < dims;dim++){
                    if(dim % 2 == 0){
                        record_i[dim] = 1.0 + DELTA;
                        record_o[dim] = 1.0 - DELTA;
                    }else{
                        record_i[dim] = 1.0 - DELTA;
                        record_o[dim] = 1.0 + DELTA;
                    }
                }
                count[i] = count[o]/2.0;
                count[o] = count[o] - count[i];
            }
            // should we do normalize?
            // for current implementation, we use l2 distance,
            // so we don't need to do this.
            /* for(int i = 0; i < centors_size_;i++){
            *   normalize(centors);
            *  }
            */

           // Step 5,6
           std::vector<float> dist1(centors_size_,0);
           for(int i = 0;i < centors_size_;i++){
              dist1[i] = distanceFunc_(&olds[i],&centors[i]);
           }

           for(int i = 0;i < n;i++){
                for(int j = 0;j < centors_size_;j++){
                    lower_bounds.get_index_value_ref(i,j) = std::max(lower_bounds.get_index_value_ref(i,j)-dist1[j],float(0.0));
                }
           }
           for(int i = 0;i < n;i++){
                upper_bounds[i] += dist1[assigns[i]];
           }
           // should we end iteration
           return change == 0;
        }
        
        void set_finished(){
            finished = true;
        }

        const std::vector<int>& GetAssigns(){
            return assigns;
        }

        const std::vector<VectorRecord>& GetCentors(){
            return centors;
        }
    private:
        uint16_t centors_size_;
        RecordDistanceFunc distanceFunc_;
        uint16_t iterations_times_;
        std::vector<VectorRecord>& records_;
        std::vector<VectorRecord> centors;
        // records[i] belongs to centors[assigns[i]]
        std::vector<int> assigns;
        Square lower_bounds;
        std::vector<float> upper_bounds;
        // iteratoration finished
        bool finished;
};
#pragma once

#include "memory_a3v.h"
#include "util.h"
const int TopKOptimizationThreshold = 1024;
const int amplication_param = 1.15;
A3vNode::A3vNode():left_node(-1),right_node(-1),id(0),radius(0),start(-1),end(-1){}

A3vNode::A3vNode(int left_node_,int right_node_,int start_,int end_,float radius_,std::vector<float>& query_,int id_){
    left_node = left_node_;right_node = right_node_;
    start = start_;end = end_;radius = radius_;
    query = query_;id = id_;
}

A3vNode::A3vNode(int left_node_,int right_node_,int start_,int end_,float radius_,int id_){
    left_node = left_node_;right_node = right_node_;
    start = start_;end = end_;radius = radius_;
    id = id_;
}

void A3vNode::SetQuery(std::vector<float>& query_){
    query = query_;
}

MemoryA3v::MemoryA3v(const std::vector<int> &dims,const std::vector<PII>& data_points_):dims_(dims),data_points(data_points_),swap_indexes(data_points.size()){
    distances_caching.resize(data_points.size());
    for(int i = 0;i < data_points.size();i++) swap_indexes[i] = i,distances_caching[swap_indexes[i]] = -1; // the initial value should be -1??
    // init root
    index.push_back(A3vNode(-1,-1,0,data_points.size()-1,-1.0,0));
    index.reserve(ReserveA3VNodes);
}

int MemoryA3v::CrackInTwo(int start_,int end_,float epsilon){
    int i = start_,j = end_;
    while(true){
        while(distances_caching[swap_indexes[i]] <= epsilon && i < end_+1) i++;
        while(distances_caching[swap_indexes[j]] > epsilon && j > start_-1) j--;
        if(i >= j) break;
        std::swap(swap_indexes[i],swap_indexes[j]);
        // std::swap(distances_caching[swap_indexes[i]],distances_caching[swap_indexes[j]]);
    }
    return j;
}

// result_pq should be empty initially.
void MemoryA3v::KnnCrackSearch(std::vector<float> &weights, float* query,int k,std::priority_queue<PQNode>& result_pq /**Max heap**/ ,const std::vector<int> &dimensions,float last_top_k_mean){
    // Min heap
    std::priority_queue<PQNode,std::vector<PQNode>,std::greater<PQNode>> guide_pq;
    std::vector<float> rnd_dists;
    // elog(INFO,"k of knn crack is %d",k);
    float median = 0.0,leftMinDist = 0.0,rightMinDist = 0.0,temp_distance_1 = 0.0;int crack = 0;
    float min_w = 1.0;
    int sum = 0;
    for(int i = 0;i < dimensions.size();i++){
        min_w = std::min(min_w,weights[i]);sum += dimensions[i];
    }
    if(dimensions.size() == 1){
        min_w = 1.0;
    }
    bool enable_top_k_optimization = (sum >= TopKOptimizationThreshold);
    // init, we should give the root node as a guide way for guide_pq.
    guide_pq.push({0,0});
    while(!guide_pq.empty() && (result_pq.size() < k || guide_pq.top() < result_pq.top())){
        elog(LOG,"test");
        auto& t = index[guide_pq.top().second];
        int root_idx = guide_pq.top().second;
        // leaf node process
        if(t.left_node == -1 && t.right_node == -1){
            for(int i = t.start;i <= t.end;i++){
                float d = hyper_distance_func_with_weights(query,data_points[swap_indexes[i]].first,dimensions,weights,&temp_distance_1);
                distances_caching[swap_indexes[i]] = temp_distance_1;
                if(result_pq.size() < k){
                    result_pq.push({d,swap_indexes[i]});
                }else if(d < result_pq.top().first){
                    result_pq.pop();
                    result_pq.push({d,swap_indexes[i]});
                }
            }
            guide_pq.pop();
            if(t.end - t.start + 1 > CRACKTHRESHOLD){
                rnd_dists.clear();
                rnd_dists.push_back(distances_caching[swap_indexes[t.start + (std::rand() % (t.end - t.start + 1))]]);
                rnd_dists.push_back(distances_caching[swap_indexes[t.start + (std::rand() % (t.end - t.start + 1))]]);
                rnd_dists.push_back(distances_caching[swap_indexes[t.start + (std::rand() % (t.end - t.start + 1))]]);
                sort(rnd_dists.begin(),rnd_dists.end());
                float median = rnd_dists[1];
                crack = CrackInTwo(t.start,t.end,median);
                t.radius = median;
                std::vector<float> vec(query, query + sum);
                t.SetQuery(vec);

                if(crack >= t.start && t.end >= crack + 1){
                    // if extend the capcity of index, the reference t won't reference to the correct position.
                    index.push_back(A3vNode(-1,-1,t.start,crack,-1,index.size()));
                    index[root_idx].left_node = int(index.size())-1;
                    // t.left_node = index.size()-1;
                    index.push_back(A3vNode(-1,-1,crack+1,t.end,-1,index.size()));
                    index[root_idx].right_node = int(index.size())-1;
                    // t.right_node = index.size()-1;
                }
            }
        }else{
            float dist = hyper_distance_func_with_weights_internal_query(query,index[root_idx].query.data(),dimensions,weights,&temp_distance_1);
            guide_pq.pop();
            leftMinDist = Max(0.0f,dist - t.radius);
            if(enable_top_k_optimization){
                rightMinDist = Max(last_top_k_mean,min_w * t.radius - dist);
            }else{
                rightMinDist = Max(0.0f,min_w * t.radius - dist);
            }
            
            if(result_pq.size() < k){
                guide_pq.push({leftMinDist,t.left_node});
                guide_pq.push({rightMinDist,t.right_node});
            }else{
                if(leftMinDist < result_pq.top().first){
                    guide_pq.push({leftMinDist,t.left_node});
                }
                if(rightMinDist < result_pq.top().first){
                    guide_pq.push({rightMinDist,t.right_node});
                }
            }
        }
    }
}

void MemoryA3v::RangeCrackSearch(std::vector<float> &weights,float* query,float radius,std::vector<PQNode>& result_ids,const std::vector<int> &dimensions){
    result_ids.clear();result_ids.reserve(ReserveRange);
    int dim = 0;
    for(int i = 0;i < dimensions.size();i++) dim += dimensions[i];
    RangeCrackSearchAuxiliary(weights,0,query,radius,result_ids,dimensions,dim);
    // elog(INFO,"range crack end");
}

// for range query.
int MemoryA3v::CrackInTwoMedicore(int start_,int end_,float radius,float median,float* query,std::vector<PQNode>& result_ids,float& maxDistance){
    int i = start_, j = end_;
    // elog(INFO,"start: %d, end: %d",start_,end_);
    while(true){
        float distance_i = distances_caching[swap_indexes[i]];
        while(distance_i <= median && i < end_ + 1){
            // elog(INFO,"i in: %d ",i);
            if(distance_i <= radius){
                if(distance_i > maxDistance){
                    maxDistance = distance_i;
                }
                // we use dist1.0 to crack, so we use distance_caching_1.0 to add result, we will add
                // outside.
                // result_ids.push_back(swap_indexes[i]);
            }
            i++;
            if(i < end_+1) distance_i = distances_caching[swap_indexes[i]];
        }

        float distance_j = distances_caching[swap_indexes[j]];
        while(distance_j > median && j > start_ - 1){
            // elog(INFO,"j in: %d ",j);
            if(distance_j <= radius){
                if(distance_j > maxDistance){
                    maxDistance = distance_j;
                }
                // we use dist1.0 to crack, so we use distance_caching_1.0 to add result, we will add
                // outside.
                // result_ids.push_back(swap_indexes[j]);
            }
            j--;
            if(j > start_ - 1) distance_j = distances_caching[swap_indexes[j]];
        }
        if(i >= j) break;
        std::swap(swap_indexes[i],swap_indexes[j]);
        // std::swap(distances_caching[swap_indexes[i]],distances_caching[swap_indexes[j]]);
    }
    // elog(INFO,"has break now");
    return j;
}

void MemoryA3v::RangeCrackSearchAuxiliary(std::vector<float> &weights,int root_idx, float* query,float radius,std::vector<PQNode>& result_ids,const std::vector<int> &dimensions,int dim){
    std::vector<float> rnd_dists;
    float maxDistance, dist, median,temp_distance_1;
    int crack;
    // leaf node
    A3vNode& root = index[root_idx];
    if(root.left_node == -1 && root.right_node == -1){
        for(int i = root.start;i <= root.end;i++){
            dist = hyper_distance_func_with_weights(query,data_points[swap_indexes[i]].first,dimensions,weights,&temp_distance_1);
            distances_caching[swap_indexes[i]] = temp_distance_1;
            if(dist <= radius){
                result_ids.push_back({0.0,swap_indexes[i]});
            }
        }

        if(root.end - root.start + 1 > CRACKTHRESHOLD)
        {
            rnd_dists.clear();
            rnd_dists.push_back(distances_caching[swap_indexes[root.start + (std::rand() % (root.end - root.start + 1))]]);
            rnd_dists.push_back(distances_caching[swap_indexes[root.start + (std::rand() % (root.end - root.start + 1))]]);
            rnd_dists.push_back(distances_caching[swap_indexes[root.start + (std::rand() % (root.end - root.start + 1))]]);
            sort(rnd_dists.begin(),rnd_dists.end());
            median = rnd_dists[1];
            maxDistance = -1.0;
            // elog(INFO,"CrackInTwoMedicore Start");
            crack = CrackInTwoMedicore(root.start,root.end,radius,median,query,result_ids,maxDistance);
            // elog(INFO,"CrackInTwoMedicore End");
            std::vector<float> vec(query, query + dim);
            index[root_idx].SetQuery(vec);
            index[root_idx].radius = median;
            if(crack >= root.start && root.end >= crack + 1){
                index.push_back(A3vNode(-1,-1,root.start,crack,-1,index.size()));
                index[root_idx].left_node = int(index.size())-1;
                index.push_back(A3vNode(-1,-1,crack+1,root.end,-1,index.size()));
                index[root_idx].right_node = int(index.size())-1;
            }
        }
    }else{
        dist = hyper_distance_func_with_weights_internal_query(query,index[root_idx].query.data(),dimensions,weights,&temp_distance_1);
        if(dist < radius + root.radius){
            if(dist < radius - root.radius){
                auto& left_node = index[root.left_node];
                for(int j = left_node.start;j <= left_node.end;j++){
                    result_ids.push_back({0.0,swap_indexes[j]});
                }
                RangeCrackSearchAuxiliary(weights,root.right_node,query,radius,result_ids,dimensions,dim);
            }else{
                RangeCrackSearchAuxiliary(weights,root.left_node,query,radius,result_ids,dimensions,dim);
                if(dist >= root.radius - radius){
                    root = index[root_idx];
                    RangeCrackSearchAuxiliary(weights,root.right_node,query,radius,result_ids,dimensions,dim);
                }
            }
        }else{
            RangeCrackSearchAuxiliary(weights,root.right_node,query,radius,result_ids,dimensions,dim);
        }
    }
}
// build call back

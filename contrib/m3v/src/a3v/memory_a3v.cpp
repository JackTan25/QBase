#pragma once

#include "memory_a3v.h"
#include "util.h"

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

MemoryA3v::MemoryA3v(int dim,std::vector<PII>& data_points_):dim_(dim),data_points(data_points_),swap_indexes(data_points.size()){
    distances_caching.reserve(data_points.size());
    for(int i = 0;i < data_points.size();i++) swap_indexes[i] = i,distances_caching[swap_indexes[i]] = -1; // the initial value should be -1??
    // init root
    index.push_back(A3vNode(-1,-1,0,data_points.size()-1,-1.0,0));
}

int MemoryA3v::CrackInTwo(int start_,int end_,float epsilon){
    int i = start_,j = end_;
    while(true){
        while(distances_caching[swap_indexes[i]] <= epsilon && i < end_+1) i++;
        while(distances_caching[j] > epsilon && j > start_-1) j--;
        if(i >= j) break;
        std::swap(swap_indexes[i],swap_indexes[j]);
        std::swap(distances_caching[swap_indexes[i]],distances_caching[swap_indexes[j]]);
    }
    return j;
}

// result_pq should be empty initially.
void MemoryA3v::KnnCrackSearch(float* query,int k,std::priority_queue<PQNode>& result_pq /**Max heap**/ ){
    // Min heap
    std::priority_queue<PQNode,std::vector<PQNode>,std::greater<PQNode>> guide_pq;
    std::vector<float> rnd_dists;
    float median,leftMinDist,rightMinDist;int crack;
    // init, we should give the root node as a guide way for guide_pq.
    guide_pq.push({0,0});
    while(!guide_pq.empty() && (result_pq.size() < k || guide_pq.top() < result_pq.top())){
        auto& t = index[guide_pq.top().second];
        // leaf node process
        if(t.left_node == -1 && t.right_node == -1){
            for(int i = t.start;i <= t.end;i++){
                distances_caching[swap_indexes[i]] = SIMDFunc(data_points[swap_indexes[i]].first.data(),query,&dim_);
                if(result_pq.size() < k){
                    result_pq.push({distances_caching[swap_indexes[i]],i});
                }else if(distances_caching[swap_indexes[i]] < result_pq.top().first){
                    result_pq.pop();
                    result_pq.push({distances_caching[swap_indexes[i]],i});
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
                std::vector<float> vec(query, query + dim_);
                t.SetQuery(vec);

                if(crack >= t.start && t.end >= crack + 1){
                    index.push_back(A3vNode(-1,-1,t.start,crack,-1,index.size()));
                    t.left_node = index.size()-1;
                    index.push_back(A3vNode(-1,-1,crack+1,t.end,-1,index.size()));
                    t.right_node = index.size()-1;
                }
            }
        }else{
            float dist = SIMDFunc(t.query.data(),query,&dim_);
            guide_pq.pop();
            leftMinDist = Max(0.0f,dist - t.radius);
            rightMinDist = Max(0.0f,t.radius - dist);
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

// for range query.
int MemoryA3v::CrackInTwoMedicore(int start_,int end_,float radius,float median,float* query,std::vector<int>& result_ids,float& maxDistance){
    int i = start_, j = end_;
    while(true){
        float distance_i = distances_caching[swap_indexes[i]];
        while(distance_i <= median && i < end_ + 1){
            if(distance_i <= radius){
                if(distance_i > maxDistance){
                    maxDistance = distance_i;
                }
                result_ids.push_back(swap_indexes[i]);
            }
            i++;
            if(i < end_+1) distance_i = distances_caching[swap_indexes[i]];
        }

        float distance_j = distances_caching[swap_indexes[j]];
        while(distance_j > median && j > start_ - 1){
            if(distance_j <= radius){
                if(distance_j <= radius){
                    if(distance_j > maxDistance){
                        maxDistance = distance_j;
                    }
                    result_ids.push_back(swap_indexes[j]);
                }
                j--;
                if(j > start_ - 1) distance_j = distances_caching[swap_indexes[j]];
            }
        }

        if(i >= j) break;
        std::swap(swap_indexes[i],swap_indexes[j]);
        std::swap(distances_caching[swap_indexes[i]],distances_caching[swap_indexes[j]]);
    }
}

void MemoryA3v::RangeCrackSearch(float* query,float radius,std::vector<int>& result_ids){
    result_ids.clear();result_ids.reserve(ReserveRange);
    RangeCrackSearchAuxiliary(index[0],query,radius,result_ids);
}

void MemoryA3v::RangeCrackSearchAuxiliary(A3vNode &root, float* query,float radius,std::vector<int>& result_ids){
    std::vector<float> rnd_dists;
    float maxDistance, dist, median;
    int crack;
    // leaf node
    if(root.left_node == -1 && root.right_node == -1){
        for(int i = root.start;i <= root.end;i++){
            distances_caching[swap_indexes[i]] = SIMDFunc(data_points[swap_indexes[i]].first.data(),query,&dim_);
        }

        if(root.end - root.start + 1 <= CRACKTHRESHOLD){
            for(int i = root.start;i <= root.end;i++){
                if(distances_caching[swap_indexes[i]] <= radius){
                    result_ids.push_back(swap_indexes[i]);
                }
            }
        }else{
            rnd_dists.clear();
            rnd_dists.push_back(distances_caching[swap_indexes[root.start + (std::rand() % (root.end - root.start + 1))]]);
            rnd_dists.push_back(distances_caching[swap_indexes[root.start + (std::rand() % (root.end - root.start + 1))]]);
            rnd_dists.push_back(distances_caching[swap_indexes[root.start + (std::rand() % (root.end - root.start + 1))]]);
            sort(rnd_dists.begin(),rnd_dists.end());
            median = rnd_dists[1];
            maxDistance = -1.0;
            crack = CrackInTwoMedicore(root.start,root.end,radius,median,query,result_ids,maxDistance);
            std::vector<float> vec(query, query + dim_);
            root.SetQuery(vec);
            root.radius = median;
            if(crack >= root.start && root.end >= crack + 1){
                index.push_back(A3vNode(-1,-1,root.start,crack,-1,index.size()));
                root.left_node = index.size()-1;
                index.push_back(A3vNode(-1,-1,crack+1,root.end,-1,index.size()));
                root.right_node = index.size()-1;
            }
        }
    }else{
        dist = SIMDFunc(root.query.data(),query,&dim_);
        if(dist < radius + root.radius){
            if(dist < radius - root.radius){
                auto& left_node = index[root.left_node];
                for(int j = left_node.start;j <= left_node.end;j++){
                    result_ids.push_back(swap_indexes[j]);
                }
                RangeCrackSearchAuxiliary(index[root.right_node],query,radius,result_ids);
            }else{
                RangeCrackSearchAuxiliary(index[root.left_node],query,radius,result_ids);
                if(dist >= root.radius - radius){
                    RangeCrackSearchAuxiliary(index[root.right_node],query,radius,result_ids);
                }
            }
        }else{
            RangeCrackSearchAuxiliary(index[root.right_node],query,radius,result_ids);
        }
    }
}

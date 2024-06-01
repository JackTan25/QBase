#include "hnsw.h"
#include "simd_func.h"

std::uint64_t GetNumberByItemPointerData(ItemPointer tid){
	std::int32_t blockId = ItemPointerGetBlockNumberNoCheck(tid);
    std::int32_t offset = ItemPointerGetOffsetNumberNoCheck(tid);
    std::uint64_t number = blockId;
    number = (number << 32) + offset;
	return number;
}

ItemPointerData GetItemPointerDataByNumber(hnswlib::labeltype label){
    ItemPointerData tid;
    BlockNumber blkno = (std::uint32_t) (label >> 32);
    OffsetNumber offset = (std::uint32_t) label;
    ItemPointerSet(&tid, blkno, offset);
    return tid;
}

float MultiColumnHnsw::l2_distance(std::vector<float> &data1,const float* query_point){
    // float res = 0.0;
    // for(int i = 0;i <data1.size();i++){
    //     float temp = (data1[i] -query_point[i]); temp = temp * temp;
    //     res += temp;
    // }
    // return res;
    int dim = (int)data1.size();
    return optimized_simd_distance_func(data1.data(),const_cast<float*>(query_point),dim);
}

float MultiColumnHnsw::RankScore(hnswlib::labeltype label){
    float score = 0.0;
    for(int i = 0;i < query_points.size();i++){
        std::vector<float> hit_data = hnsws[i]->getDataByLabel<float>(label);
        score += l2_distance(hit_data,query_points[i]) * weights[i];
    }
    return score;
}


bool MultiColumnHnsw::RangeNext(){
    while(true){
        auto result = hnsws_iterators[0]->Next();
        result_tid = GetItemPointerDataByNumber(result->GetLabel());
        distance = result->GetDistance();
        if(!result->HasResult()) return false;
        if(!inRange){
            if(distanceQueue.size() < distanceQueueThreshold ||
            distanceQueue.top() > result->GetDistance()){
                if(distanceQueue.size() >= distanceQueueThreshold) distanceQueue.pop();
                distanceQueue.push(result->GetDistance());
            }else{
                return false;
            }
        }
        if(result->GetDistance() > range){
            RangeTimes++;
            if(inRange && RangeTimes >= distanceThreshold){
                return false;
            }else{
                continue;
            }
        }else{
            inRange = true;
        }
        return true;
    }
}

bool MultiColumnHnsw::GetSingleNext(){
    auto result = hnsws_iterators[0]->Next();
    if(result->HasResult()){
        result_tid = GetItemPointerDataByNumber(result->GetLabel());
        distance = result->GetDistance();
        if(!xs_inorder_scan){
            if (distanceQueue.size() == range &&
                distanceQueue.top() < result->GetDistance())
            {
                // elog(INFO,"xs_inorder_scan is true now.");
                xs_inorder_scan=true;
            }
            else
            {
                if (distanceQueue.size() >= range)
                {
                    distanceQueue.pop();
                }
                distanceQueue.push(result->GetDistance());
            }
        }
        return true;
    }
    // elog(LOG,"hnsw has no more.");
    return false;
}

bool MultiColumnHnsw::GetNext(){
    float score = 0.0;
    bool finished = false;
    int  consecutive_drops = 0;
    while(!finished){
        for(int col = 0; col < query_points.size();col++){
            const void* query_point = query_points[col];
            auto result = hnsws_iterators[col]->Next();
            if(!result->HasResult()){
                finished = true;break;
            }
            hnswlib::labeltype label = result->GetLabel();
            distance = result->GetDistance();
            if(seen_tid.find(label) != seen_tid.end()){
                continue;
            }
            seen_tid.insert(label);
            consecutive_drops++;
            float score = RankScore(label);
            if(proc_pq.size() < k || proc_pq.top().first > score){
				if(proc_pq.size() == k)
					proc_pq.pop();
				proc_pq.push(std::make_pair(score, label));
				consecutive_drops = 0;
			}
            if(consecutive_drops >= terminate_multi_top_k){
                finished = true;break;
            }
        }
    }
    while(!proc_pq.empty()){
        result_pq.push(proc_pq.top());proc_pq.pop();
    }
    if(result_pq.empty()){
        return false;
    }else{
        result_tid = GetItemPointerDataByNumber(result_pq.top().second);  
        result_pq.pop();  
        return true;
    }
}

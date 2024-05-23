#include "hnsw.h"

std::uint64_t GetNumberByItemPointerData(ItemPointer tid){
	std::int32_t blockId = ItemPointerGetBlockNumberNoCheck(tid);
    std::int32_t offset = ItemPointerGetOffsetNumberNoCheck(tid);
    std::uint64_t number = blockId;
    number = (number << 32) + offset;
	return number;
}

ItemPointerData GetItemPointerDataByNumber(hnswlib::labeltype label){
    ItemPointerData tid;memset(&tid,0,sizeof(ItemPointerData));
    tid.ip_blkid.bi_hi = (label>>32)&(0xff00);tid.ip_blkid.bi_hi = (label>>32)&(0x00ff);
    tid.ip_posid = label & (0xffff);
    return tid;
}

float MultiColumnHnsw::l2_distance(std::vector<float> &data1,const float* query_point){
    float res = 0.0;
    for(int i = 0;i <data1.size();i++){
        float temp = (data1[i] -query_point[i]); temp = temp * temp;
        res += temp;
    }
    return sqrt(res);
}

float MultiColumnHnsw::RankScore(hnswlib::labeltype label){
    float score = 0.0;
    for(int i = 0;i < query_points.size();i++){
        std::vector<float> hit_data = hnsws[i]->getDataByLabel<float>(label);
        score += l2_distance(hit_data,query_points[i]) * weights[i];
    }
    return score;
}

bool MultiColumnHnsw::GetSingleNext(){
    auto result = hnsws_iterators[0]->Next();
    if(result->HasResult()){
        result_tid = GetItemPointerDataByNumber(result->GetLabel());
        distance = result->GetDistance();
        return true;
    }
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
    }
}

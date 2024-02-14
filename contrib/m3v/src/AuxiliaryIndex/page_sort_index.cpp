#include "page_sort_index.h"

std::vector<uint16_t> empty;

void ValidBitmap::Reset(){
    valid_indexes.clear();
    memset(masks,0,sizeof masks);
}

void ValidBitmap::SetIndexValid(uint32_t idx){
    int entry_offset = idx/8;
    int bit_offset = idx%8;
    masks[entry_offset] = masks[entry_offset]|(1<<bit_offset);
}

const std::vector<uint16_t>& ValidBitmap::GetValidIndexes(int tid_nums){
    int start = 0;
    bool finished = false;
    for(int i = 0;i < SORT_ENTRY_NUM/8;i++){
        if(masks[i] == 0){
                continue;
        }
        for(int j = 0;j < 8;j++){
            if(i*8 + j > tid_nums){
                finished = true;break;
            }
            if(masks[i]&(1<<j)){
                valid_indexes.push_back(i*8 + j);
            }
        }
        if(finished) break;
    }
    return valid_indexes;
}

const std::vector<uint16_t>& AuxiliarySortPage::GetValidIndexes(int page_index,float radius,float dist,float weight_sum){
    PagePivotSortIndex& page_pivot_sort = sorts[page_index];
    page_pivot_sort.ResetBitmap();
    // 1. do min prune
    uint16_t min_idx = page_pivot_sort.GetMinValidIndexStart(radius,dist,weight_sum);
    if(min_idx == INVALID_INDEX) return empty;
    for(int i = 0;i <= min_idx;i++){
        page_pivot_sort.SetIndexValid(i);
    }
    // 2. do max prune
    uint16_t max_idx = page_pivot_sort.GetMaxValidIndexStart(radius,dist,weight_sum);
    if(min_idx == INVALID_INDEX) return empty;
    uint16_t tid_nums = page_pivot_sort.GetTidNums();
    for(int i = max_idx;i < tid_nums;i++){
        page_pivot_sort.SetIndexValid(i);
    }
    return page_pivot_sort.GetValidIndexes(tid_nums);
}   

void PagePivotSortIndex::ResetBitmap(){
    valid_bitmap.Reset();
}

void PagePivotSortIndex::SetIndexValid(uint32_t idx){
    valid_bitmap.SetIndexValid(idx);
}

const std::vector<uint16_t>& PagePivotSortIndex::GetValidIndexes(int tid_nums){
    valid_bitmap.GetValidIndexes(tid_nums);
}

uint16_t PagePivotSortIndex::GetMinValidIndexStart(const float& radius,const float& dist,const float& weight_sum){
    // find the first one which is less or equal to dist + r
    int l = 0,r = tid_nums_-1;
    float dest = dist + r;
    while(l < r){
        int mid = (l+r+1)/2;
        if(min_sorts[mid] * weight_sum <= dest){
            l = mid;
        }else{
            r = mid - 1;
        }
    }
    return min_sorts[l]<= dest?l:INVALID_INDEX;
}

void PagePivotSortIndex::SetTidNums(int tid_nums){
    tid_nums_ = tid_nums;
}

uint16_t PagePivotSortIndex::GetMaxValidIndexStart(const float& radius,const float& dist,const float& weight_sum){
    // find the first one which is greater or equal to dist - r
    int l = 0,r = tid_nums_-1;
    float dest = dist - r;
    while(l < r){
        int mid = (l+r)/2;
        if(min_sorts[mid] * weight_sum >= dest){
            r = mid;
        }else{
            l = mid + 1;
        }
    }
    return min_sorts[l]>= dest?l:INVALID_INDEX;
}

uint16_t AuxiliarySortPage::GetMinValidIndexStart(PagePivotSortIndex& page_pivot_sort,const float& radius,const float& dist,const float& weight_sum) const{
    return page_pivot_sort.GetMinValidIndexStart(radius,dist,weight_sum);
}

uint16_t AuxiliarySortPage::GetMaxValidIndexStart(PagePivotSortIndex& page_pivot_sort,const float& radius,const float& dist,const float& weight_sum) const{
    return page_pivot_sort.GetMaxValidIndexStart(radius,dist,weight_sum);
}

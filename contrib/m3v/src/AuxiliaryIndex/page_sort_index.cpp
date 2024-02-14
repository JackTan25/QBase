#include "page_sort_index.h"
#include<algorithm>
#include<cassert>

std::vector<uint16_t> empty;

void ValidBitmap::Reset(){
    valid_indexes.clear();
    memset(masks1,0,sizeof masks1);memset(masks2,0,sizeof masks2);
}

void ValidBitmap::SetIndexValid1(uint32_t idx){
    int entry_offset = idx/8;
    int bit_offset = idx%8;
    masks1[entry_offset] = masks1[entry_offset]|(1<<bit_offset);
}

void ValidBitmap::SetIndexValid2(uint32_t idx){
    int entry_offset = idx/8;
    int bit_offset = idx%8;
    masks2[entry_offset] = masks2[entry_offset]|(1<<bit_offset);
}

const std::vector<uint16_t>& ValidBitmap::GetValidIndexes(int tid_nums){
    int start = 0;
    bool finished = false;
    for(int i = 0;i < SORT_ENTRY_NUM/8;i++){
        if(masks1[i] == 0){
                continue;
        }
        if(masks2[i] == 0){
                continue;
        }
        for(int j = 0;j < 8;j++){
            if(i*8 + j > tid_nums){
                finished = true;break;
            }
            if((masks1[i]&(1<<j))&&(masks2[i]&(1<<j))){
                valid_indexes.push_back(i*8 + j);
            }
        }
        if(finished) break;
    }
    return valid_indexes;
}

// const std::vector<uint16_t>& ValidBitmap::MergeValidIndexes(const ValidBitmap& mp2,int tid_nums){
//     int start = 0;
//     bool finished = false;
//     for(int i = 0;i < SORT_ENTRY_NUM/8;i++){
//         if(masks[i] == 0){
//                 continue;
//         }
//         if(mp2.masks[i] == 0)
//         for(int j = 0;j < 8;j++){
//             if(i*8 + j > tid_nums){
//                 finished = true;break;
//             }
//             if(masks[i]&(1<<j)&&mp2.masks[i]&(1<<j)){
//                 valid_indexes.push_back(i*8 + j);
//             }
//         }
//         if(finished) break;
//     }
//     return valid_indexes;
// }

const std::vector<uint16_t>& AuxiliarySortPage::GetValidIndexes(int page_index,float radius,float dist,float weight_sum){
    PagePivotSortIndex& page_pivot_sort = sorts[page_index];
    page_pivot_sort.ResetBitmap();
    // 1. do min prune
    uint16_t min_idx = page_pivot_sort.GetMinValidIndexStart(radius,dist,weight_sum);
    if(min_idx == INVALID_INDEX) return empty;
    for(int i = 0;i <= min_idx;i++){
        page_pivot_sort.SetMinIndexValid(page_pivot_sort.GetMinIndexAt(i));
    }
    // 2. do max prune
    uint16_t max_idx = page_pivot_sort.GetMaxValidIndexStart(radius,dist,weight_sum);
    if(max_idx == INVALID_INDEX) return empty;
    uint16_t tid_nums = page_pivot_sort.GetTidNums();
    for(int i = max_idx;i < tid_nums;i++){
        page_pivot_sort.SetMaxIndexValid(page_pivot_sort.GetMaxIndexAt(i));
    }
    return page_pivot_sort.GetValidIndexes(tid_nums);
}   

void PagePivotSortIndex::ResetBitmap(){
    valid_bitmap.Reset();
}

void PagePivotSortIndex::SetMinIndexValid(uint32_t idx){
    valid_bitmap.SetIndexValid1(idx);
}

void PagePivotSortIndex::SetMaxIndexValid(uint32_t idx){
    valid_bitmap.SetIndexValid2(idx);
}

const std::vector<uint16_t>& PagePivotSortIndex::GetValidIndexes(int tid_nums){
    return valid_bitmap.GetValidIndexes(tid_nums);
}

void PagePivotSortIndex::SetMaxSorts(const std::vector<PivotIndexPair>& max_sorts_){
    assert(max_sorts_.size() == tid_nums_);
    for(int i = 0;i < tid_nums_;i++) max_sorts[i] = max_sorts_[i];
    sort(max_sorts,max_sorts+tid_nums_);
}

void PagePivotSortIndex::SetMinSorts(const std::vector<PivotIndexPair>& min_sorts_){
    assert(min_sorts_.size() == tid_nums_);
    for(int i = 0;i < tid_nums_;i++) min_sorts[i] = min_sorts_[i];
    sort(min_sorts,min_sorts+tid_nums_);
}

uint16_t PagePivotSortIndex::GetMinValidIndexStart(const float& radius,const float& dist,const float& weight_sum){
    // find the first one which is less or equal to dist + r
    int l = 0,r = tid_nums_-1;
    float dest = dist + radius;
    while(l < r){
        int mid = (l+r+1)/2;
        if(min_sorts[mid].first * weight_sum <= dest){
            l = mid;
        }else{
            r = mid - 1;
        }
    }
    return min_sorts[l].first <= dest?l:INVALID_INDEX;
}

void PagePivotSortIndex::SetTidNums(int tid_nums){
    tid_nums_ = tid_nums;
}

uint16_t PagePivotSortIndex::GetMaxValidIndexStart(const float& radius,const float& dist,const float& weight_sum){
    // find the first one which is greater or equal to dist - r
    int l = 0,r = tid_nums_-1;
    float dest = dist - radius;
    while(l < r){
        int mid = (l+r)/2;
        if(max_sorts[mid].first * weight_sum >= dest){
            r = mid;
        }else{
            l = mid + 1;
        }
    }
    return max_sorts[l].first >= dest?l:INVALID_INDEX;
}

uint16_t AuxiliarySortPage::GetMinValidIndexStart(PagePivotSortIndex& page_pivot_sort,const float& radius,const float& dist,const float& weight_sum) const{
    return page_pivot_sort.GetMinValidIndexStart(radius,dist,weight_sum);
}

uint16_t AuxiliarySortPage::GetMaxValidIndexStart(PagePivotSortIndex& page_pivot_sort,const float& radius,const float& dist,const float& weight_sum) const{
    return page_pivot_sort.GetMaxValidIndexStart(radius,dist,weight_sum);
}

void AuxiliarySortPage::SetPageSortIndexTidNums(int page_idx,int tid_nums){
    sorts[page_idx].SetTidNums(tid_nums);
}

void AuxiliarySortPage::SetMaxSorts(int page_index,const std::vector<PivotIndexPair>& max_sorts){
    sorts[page_index].SetMaxSorts(max_sorts);
}

void AuxiliarySortPage::SetMinSorts(int page_index,const std::vector<PivotIndexPair>& min_sorts){
    sorts[page_index].SetMinSorts(min_sorts);
}
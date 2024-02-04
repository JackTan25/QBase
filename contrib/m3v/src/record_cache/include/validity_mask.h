#pragma once

#include <cstdint>
using validity_t = uint64_t;
//! a saner size_t for loop indices etc
typedef uint64_t idx_t;

class ValidityMask{
	//! Constants for fast offset calculations in the bitmask
	static constexpr idx_t BASE[] = {0x00000000FFFFFFFF, 0x0000FFFF, 0x00FF, 0x0F, 0x3, 0x1};
	static constexpr uint8_t SHIFT[] = {32, 16, 8, 4, 2, 1};
	static constexpr const int BITS_PER_VALUE = sizeof(validity_t) * 8;
	public:
		ValidityMask(validity_t* mask_,uint32_t size_):mask(mask_),size(size_){
		}
		// get the first empty record slot.
		idx_t GetOffset(int allocated_record_counts);
		bool is_valid(idx_t row_idx);
		void set_invalid(idx_t row_idx);
    private:
        validity_t* mask;
		// size: the slots number of memory buffer
        uint32_t size;
};

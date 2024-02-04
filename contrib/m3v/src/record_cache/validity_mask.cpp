#include "validity_mask.h"
#include <cassert> 

idx_t ValidityMask::GetOffset(int allocated_record_counts){
    assert(mask!=nullptr);
    if(is_valid(allocated_record_counts)){
        return allocated_record_counts;
    }
    for (idx_t entry_idx = 0; entry_idx < size; entry_idx++) {
		// get an entry with free bits
		if (mask[entry_idx] == 0) {
			continue;
		}

		// find the position of the free bit
		auto entry = mask[entry_idx];
		idx_t first_valid_bit = 0;

		// this loop finds the position of the rightmost set bit in entry and stores it
		// in first_valid_bit
		for (idx_t i = 0; i < 6; i++) {
			// set the left half of the bits of this level to zero and test if the entry is still not zero
			if (entry & BASE[i]) {
				// first valid bit is in the rightmost s[i] bits
				// permanently set the left half of the bits to zero
				entry &= BASE[i];
			} else {
				// first valid bit is in the leftmost s[i] bits
				// shift by s[i] for the next iteration and add s[i] to the position of the rightmost set bit
				entry >>= SHIFT[i];
				first_valid_bit += SHIFT[i];
			}
		}
		assert(entry!=0);

		auto prev_bits = entry_idx * sizeof(validity_t) * 8;
		assert(is_valid(prev_bits + first_valid_bit));
		set_invalid(prev_bits + first_valid_bit);
		return (prev_bits + first_valid_bit);
	}
    // can't arrive here.
	assert(false);
}

bool ValidityMask::is_valid(idx_t row_idx){
    idx_t entry_idx = row_idx / BITS_PER_VALUE;
    idx_t idx_in_entry = row_idx % BITS_PER_VALUE;
    assert(entry_idx < size);
	// zero means is not mapped.
    return (mask[entry_idx]&(1<<idx_in_entry)) == 0;
}

void ValidityMask::set_invalid(idx_t row_idx){
    idx_t entry_idx = row_idx / BITS_PER_VALUE;
    idx_t idx_in_entry = row_idx % BITS_PER_VALUE;
    assert(entry_idx < size);
    mask[entry_idx] = (mask[entry_idx]|(1<<idx_in_entry));
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>

#include "util/bitmap.h"

namespace doris {

// Bit-vector representing the selection status of each row in a row block.
//
// It is used to scan data with column predicate.
// the bit in the selection vector is used to indicate whether the given row is live.
class SelectionVector {
public:
    // Construct a new vector. The bits are initially in an indeterminate state.
    // Call set_all_true() if you require all rows to be initially selected.
    explicit SelectionVector(size_t row_capacity)
            : _n_rows(row_capacity),
              _n_bytes(BitmapSize(row_capacity)),
              _bitmap(new uint8_t[_n_bytes]) {
        CHECK_GT(_n_bytes, 0);
        set_all_false();
    }

    // return the number of selected rows.
    size_t count_selected() const { return Bits::Count(_bitmap.get(), _n_bytes); }

    // Return true if any rows are selected, or false
    // This is equivalent to (count_selected() > 0), but faster.
    bool any_selected() const;

    bool is_row_selected(size_t row) const {
        DCHECK_LT(row, _n_rows);
        return BitmapTest(_bitmap.get(), row);
    }

    void set_row_selected(size_t row) {
        DCHECK_LT(row, _n_rows);
        BitmapSet(_bitmap.get(), row);
    }

    void set_all_true() {
        memset(_bitmap.get(), 0xff, _n_bytes);
        pad_extra_bits_wit_zeroes();
    }

    void set_all_false() { memset(_bitmap.get(), 0, _n_bytes); }

    void clear_bit(size_t row) {
        DCHECK_LT(row, _n_rows);
        return BitmapClear(_bitmap.get(), row);
    }

    uint8_t* mutable_bitmap() { return _bitmap.get(); }

    const uint8_t* bitmap() const { return _bitmap.get(); }

    size_t nrows() const { return _n_rows; }

    std::string to_string() const { return BitmapToString(_bitmap.get(), _n_rows); }

private:
    // Pads any non-byte-aligned bits at the end of the SelectionVector with
    // zeroes.
    //
    // To improve performance, CountSelected() and AnySelected() evaluate the
    // SelectionVector's bitmap in terms of bytes. As such, they consider all of
    // the trailing bits, even if the bitmap's bit length is not byte-aligned and
    // some trailing bits aren't part of the bitmap.
    //
    // To address this without sacrificing performance, we need to zero out all
    // trailing bits at construction time, or after any operation that sets all
    // bytes in bulk.

    void pad_extra_bits_wit_zeroes() {
        size_t bits_in_last_byte = _n_rows & 7;
        if (bits_in_last_byte > 0) {
            BitmapChangeBits(_bitmap.get(), _n_rows, 8 - bits_in_last_byte, false);
        }
    }

private:
    // row capacity
    size_t _n_rows;
    size_t _n_bytes;
    std::unique_ptr<uint8_t[]> _bitmap;
    DISALLOW_COPY_AND_ASSIGN(SelectionVector);
};

inline bool SelectionVector::any_selected() const {
    size_t rem = _n_bytes;
    const uint32_t* p32 = reinterpret_cast<const uint32_t*>(_bitmap.get());
    while (rem >= 4) {
        if (*p32 != 0) {
            return true;
        }
        ++p32;
        rem -= 4;
    }

    const uint8_t* p8 = reinterpret_cast<const uint8_t*>(p32);
    while (rem > 0) {
        if (*p8 != 0) {
            return true;
        }
        ++p8;
        --rem;
    }
    return false;
}

// A SelectionVectorView keeps track of where in the selection vector a given
// batch will start from. After processing a batch, Advance() should be called
// and the view will move forward by the appropriate amount. In this way, the
// underlying selection vector can easily be updated batch-by-batch.
class SelectionVectorView {
public:
    // Constructs a new SelectionVectorView.
    //
    // The 'sel_vec' object must outlive this SelectionVectorView.
    explicit SelectionVectorView(SelectionVector* sel_vec) : _sel_vec(sel_vec), _row_offset(0) {}
    void advance(size_t skip) {
        DCHECK_LE(skip, _sel_vec->nrows() - _row_offset);
        _row_offset += skip;
    }
    void set_bit(size_t row_idx) {
        DCHECK_LE(row_idx, _sel_vec->nrows() - _row_offset);
        BitmapSet(_sel_vec->mutable_bitmap(), _row_offset + row_idx);
    }
    void clear_bit(size_t row_idx) {
        DCHECK_LE(row_idx, _sel_vec->nrows() - _row_offset);
        BitmapClear(_sel_vec->mutable_bitmap(), _row_offset + row_idx);
    }
    bool test_bit(size_t row_idx) {
        DCHECK_LE(row_idx, _sel_vec->nrows() - _row_offset);
        return BitmapTest(_sel_vec->bitmap(), _row_offset + row_idx);
    }
    void clear_bits(size_t nrows) {
        DCHECK_LE(nrows, _sel_vec->nrows() - _row_offset);
        BitmapChangeBits(_sel_vec->mutable_bitmap(), _row_offset, nrows, false);
    }

private:
    SelectionVector* _sel_vec;
    size_t _row_offset;
};

} // namespace doris

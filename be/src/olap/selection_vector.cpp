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

#include "olap/selection_vector.h"

#include "common/logging.h"
#include "gutil/bits.h"
#include "util/bitmap.h"

namespace doris {

SelectionVector::SelectionVector(size_t row_capacity)
    : _n_rows(row_capacity),
      _n_bytes(BitmapSize(row_capacity)),
      _bitmap(new uint8_t[_n_bytes]) {
    CHECK_GT(_n_bytes, 0);
    pad_extra_bits_wit_zeroes();
}

size_t SelectionVector::count_selected() const {
    return Bits::Count(_bitmap.get(), _n_bytes);
}

bool SelectionVector::any_selected() const {
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

bool SelectionVector::is_row_selected(size_t row) const {
    DCHECK_LT(row, _n_rows);
    return BitmapTest(_bitmap.get(), row);
}

void SelectionVector::clear_bit(size_t row) {
    DCHECK_LT(row, _n_rows);
    return BitmapClear(_bitmap.get(), row);
}

}

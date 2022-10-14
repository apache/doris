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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnConst.cpp
// and modified by Doris

#include "vec/columns/column_const.h"

#include "runtime/raw_value.h"
#include "vec/columns/columns_common.h"
#include "vec/common/pod_array.h"
#include "vec/common/sip_hash.h"
#include "vec/common/typeid_cast.h"

namespace doris::vectorized {

ColumnConst::ColumnConst(const ColumnPtr& data_, size_t s_) : data(data_), s(s_) {
    /// Squash Const of Const.
    while (const ColumnConst* const_data = typeid_cast<const ColumnConst*>(data.get())) {
        data = const_data->get_data_column_ptr();
    }

    if (data->size() != 1) {
        LOG(FATAL) << fmt::format(
                "Incorrect size of nested column in constructor of ColumnConst: {}, must be 1.");
    }
}

ColumnPtr ColumnConst::convert_to_full_column() const {
    return data->replicate(Offsets(1, s));
}

ColumnPtr ColumnConst::remove_low_cardinality() const {
    return ColumnConst::create(data->convert_to_full_column_if_low_cardinality(), s);
}

ColumnPtr ColumnConst::filter(const Filter& filt, ssize_t /*result_size_hint*/) const {
    if (s != filt.size()) {
        LOG(FATAL) << fmt::format("Size of filter ({}) doesn't match size of column ({})",
                                  filt.size(), s);
    }

    return ColumnConst::create(data, count_bytes_in_filter(filt));
}

ColumnPtr ColumnConst::replicate(const Offsets& offsets) const {
    if (s != offsets.size()) {
        LOG(FATAL) << fmt::format("Size of offsets ({}) doesn't match size of column ({})",
                                  offsets.size(), s);
    }

    size_t replicated_size = 0 == s ? 0 : offsets.back();
    return ColumnConst::create(data, replicated_size);
}

void ColumnConst::replicate(const uint32_t* counts, size_t target_size, IColumn& column,
                            size_t begin, int count_sz) const {
    if (s == 0) return;
    auto& res = reinterpret_cast<ColumnConst&>(column);
    res.s = s;
}

ColumnPtr ColumnConst::permute(const Permutation& perm, size_t limit) const {
    if (limit == 0) {
        limit = s;
    } else {
        limit = std::min(s, limit);
    }

    if (perm.size() < limit) {
        LOG(FATAL) << fmt::format("Size of permutation ({}) is less than required ({})",
                                  perm.size(), limit);
    }

    return ColumnConst::create(data, limit);
}

void ColumnConst::update_hashes_with_value(std::vector<SipHash>& hashes,
                                           const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    DCHECK(hashes.size() == size());
    auto real_data = data->get_data_at(0);
    if (real_data.data == nullptr) {
        for (auto& hash : hashes) {
            hash.update(0);
        }
    } else {
        for (auto& hash : hashes) {
            hash.update(real_data.data, real_data.size);
        }
    }
}

void ColumnConst::update_crcs_with_value(std::vector<uint64_t>& hashes, doris::PrimitiveType type,
                                         const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    DCHECK(hashes.size() == size());
    auto real_data = data->get_data_at(0);
    if (real_data.data == nullptr) {
        for (int i = 0; i < hashes.size(); ++i) {
            hashes[i] = HashUtil::zlib_crc_hash_null(hashes[i]);
        }
    } else {
        for (int i = 0; i < hashes.size(); ++i) {
            hashes[i] = RawValue::zlib_crc32(real_data.data, real_data.size, TypeDescriptor {type},
                                             hashes[i]);
        }
    }
}

void ColumnConst::update_hashes_with_value(uint64_t* __restrict hashes,
                                           const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    auto real_data = data->get_data_at(0);
    auto real_size = size();
    if (real_data.data == nullptr) {
        for (int i = 0; i < real_size; ++i) {
            hashes[i] = HashUtil::xxHash64NullWithSeed(hashes[i]);
        }
    } else {
        for (int i = 0; i < real_size; ++i) {
            hashes[i] = HashUtil::xxHash64WithSeed(real_data.data, real_data.size, hashes[i]);
        }
    }
}

MutableColumns ColumnConst::scatter(ColumnIndex num_columns, const Selector& selector) const {
    if (s != selector.size()) {
        LOG(FATAL) << fmt::format("Size of selector ({}) doesn't match size of column ({})",
                                  selector.size(), s);
    }

    std::vector<size_t> counts = count_columns_size_in_selector(num_columns, selector);

    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        res[i] = clone_resized(counts[i]);
    }

    return res;
}

void ColumnConst::get_permutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/,
                                  Permutation& res) const {
    res.resize(s);
    for (size_t i = 0; i < s; ++i) {
        res[i] = i;
    }
}

} // namespace doris::vectorized

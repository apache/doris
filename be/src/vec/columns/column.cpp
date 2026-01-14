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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/IColumn.cpp
// and modified by Doris

#include "vec/columns/column.h"

#include "util/simd/bits.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

std::string IColumn::dump_structure() const {
    std::stringstream res;
    res << get_name() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr& subcolumn) {
        res << ", " << subcolumn->dump_structure();
    };

    // simply read using for_each_subcolumn without modification; const_cast can be used.
    const_cast<IColumn*>(this)->for_each_subcolumn(callback);

    res << ")";
    return res.str();
}

int IColumn::count_const_column() const {
    int count = is_column_const(*this) ? 1 : 0;
    ColumnCallback callback = [&](ColumnPtr& subcolumn) {
        count += subcolumn->count_const_column();
    };
    // simply read using for_each_subcolumn without modification; const_cast can be used.
    const_cast<IColumn*>(this)->for_each_subcolumn(callback);
    return count;
}

bool IColumn::const_nested_check() const {
    auto const_cnt = count_const_column();
    if (const_cnt == 0) {
        return true;
    }
    // A const column is not allowed to be nested; it may only appear as the outermost (top-level) column.
    return const_cnt == 1 && is_column_const(*this);
}

bool IColumn::column_boolean_check() const {
    if (const auto* col_nullable = check_and_get_column<ColumnNullable>(*this)) {
        // for column nullable, we need to skip null values check
        const auto& nested_col = col_nullable->get_nested_column();
        const auto& null_map = col_nullable->get_null_map_data();
        Filter not_null_filter;
        not_null_filter.reserve(nested_col.size());
        size_t result_size_hint = 0;
        for (size_t i = 0; i < null_map.size(); ++i) {
            not_null_filter.push_back(null_map[i] == 0);
            if (null_map[i] == 0) {
                ++result_size_hint;
            }
        }
        auto nested_col_skip_null = nested_col.filter(not_null_filter, result_size_hint);
        return nested_col_skip_null->column_boolean_check();
    }

    auto check_boolean_is_zero_or_one = [&](const IColumn& subcolumn) {
        if (const auto* column_boolean = check_and_get_column<ColumnBool>(subcolumn)) {
            for (size_t i = 0; i < column_boolean->size(); ++i) {
                auto val = column_boolean->get_element(i);
                if (val != 0 && val != 1) {
                    LOG_WARNING("column boolean check failed at index {} with value {}", i, val)
                            .tag("column structure", subcolumn.dump_structure());
                    return false;
                }
            }
        }
        return true;
    };

    bool is_valid = check_boolean_is_zero_or_one(*this);
    ColumnCallback callback = [&](ColumnPtr& subcolumn) {
        if (!subcolumn->column_boolean_check()) {
            is_valid = false;
        }
    };
    // simply read using for_each_subcolumn without modification; const_cast can be used.
    const_cast<IColumn*>(this)->for_each_subcolumn(callback);
    return is_valid;
}

bool IColumn::null_map_check() const {
    auto check_null_map_is_zero_or_one = [&](const IColumn& subcolumn) {
        if (is_column_nullable(subcolumn)) {
            const auto& nullable_col = assert_cast<const ColumnNullable&>(subcolumn);
            const auto& null_map = nullable_col.get_null_map_data();
            for (size_t i = 0; i < null_map.size(); ++i) {
                if (null_map[i] != 0 && null_map[i] != 1) {
                    LOG_WARNING("null map check failed at index {} with value {}", i, null_map[i])
                            .tag("column structure", subcolumn.dump_structure());
                    return false;
                }
            }
        }
        return true;
    };

    bool is_valid = check_null_map_is_zero_or_one(*this);
    ColumnCallback callback = [&](ColumnPtr& subcolumn) {
        if (!subcolumn->null_map_check()) {
            is_valid = false;
        }
    };
    // simply read using for_each_subcolumn without modification; const_cast can be used.
    const_cast<IColumn*>(this)->for_each_subcolumn(callback);
    return is_valid;
}

Status IColumn::column_self_check() const {
#ifndef NDEBUG
    // check const nested
    if (!const_nested_check()) {
        return Status::InternalError("const nested check failed for column: {} , {}", get_name(),
                                     dump_structure());
    }
    // check null map
    if (!null_map_check()) {
        return Status::InternalError("null map check failed for column: {}", get_name());
    }
    // check boolean column
    if (!column_boolean_check()) {
        return Status::InternalError("boolean column check failed for column: {}", get_name());
    }
#endif
    return Status::OK();
}

void IColumn::insert_from(const IColumn& src, size_t n) {
    insert(src[n]);
}

void IColumn::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                          IColumn::Permutation& perms, EqualRange& range, bool last_column) const {
    sorter->sort_column(static_cast<const IColumn&>(*this), flags, perms, range, last_column);
}

void IColumn::compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                               int direction, std::vector<uint8_t>& cmp_res,
                               uint8_t* __restrict filter) const {
    auto sz = this->size();
    DCHECK(cmp_res.size() == sz);
    size_t begin = simd::find_zero(cmp_res, 0);
    while (begin < sz) {
        size_t end = simd::find_one(cmp_res, begin + 1);
        for (size_t row_id = begin; row_id < end; row_id++) {
            int res = this->compare_at(row_id, rhs_row_id, rhs, nan_direction_hint);
            if (res * direction < 0) {
                filter[row_id] = 1;
                cmp_res[row_id] = 1;
            } else if (res * direction > 0) {
                cmp_res[row_id] = 1;
            }
        }
        begin = simd::find_zero(cmp_res, end + 1);
    }
}

void IColumn::serialize_with_nullable(StringRef* keys, size_t num_rows, const bool has_null,
                                      const uint8_t* __restrict null_map) const {
    if (has_null) {
        for (size_t i = 0; i < num_rows; ++i) {
            char* dest = const_cast<char*>(keys[i].data + keys[i].size);
            if (null_map[i]) {
                // is null
                *dest = true;
                keys[i].size += sizeof(UInt8);
                continue;
            }
            // not null
            *dest = false;
            keys[i].size += sizeof(UInt8) + serialize_impl(dest + sizeof(UInt8), i);
        }
    } else {
        for (size_t i = 0; i < num_rows; ++i) {
            char* dest = const_cast<char*>(keys[i].data + keys[i].size);
            *dest = false;
            keys[i].size += sizeof(UInt8) + serialize_impl(dest + sizeof(UInt8), i);
        }
    }
}

void IColumn::deserialize_with_nullable(StringRef* keys, const size_t num_rows,
                                        PaddedPODArray<UInt8>& null_map) {
    for (size_t i = 0; i != num_rows; ++i) {
        UInt8 is_null = *reinterpret_cast<const UInt8*>(keys[i].data);
        null_map.push_back(is_null);
        keys[i].data += sizeof(UInt8);
        keys[i].size -= sizeof(UInt8);
        if (is_null) {
            insert_default();
            continue;
        }
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

bool is_column_nullable(const IColumn& column) {
    return is_column<ColumnNullable>(column);
}

bool is_column_const(const IColumn& column) {
    return is_column<ColumnConst>(column);
}

} // namespace doris::vectorized

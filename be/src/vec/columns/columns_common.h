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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnsCommon.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <stdint.h>
#include <sys/types.h>

#include <ostream>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/types.h"

/// Common helper methods for implementation of different columns.

namespace doris::vectorized {

/// Counts how many bytes of `filt` are greater than zero.
size_t count_bytes_in_filter(const IColumn::Filter& filt);

/// Returns vector with num_columns elements. vector[i] is the count of i values in selector.
/// Selector must contain values from 0 to num_columns - 1. NOTE: this is not checked.
std::vector<size_t> count_columns_size_in_selector(IColumn::ColumnIndex num_columns,
                                                   const IColumn::Selector& selector);

/// The general implementation of `filter` function for ColumnArray and ColumnString.
template <typename T, typename OT>
void filter_arrays_impl(const PaddedPODArray<T>& src_elems, const PaddedPODArray<OT>& src_offsets,
                        PaddedPODArray<T>& res_elems, PaddedPODArray<OT>& res_offsets,
                        const IColumn::Filter& filt, ssize_t result_size_hint);

/// Same as above, but not fills res_offsets.
template <typename T, typename OT>
void filter_arrays_impl_only_data(const PaddedPODArray<T>& src_elems,
                                  const PaddedPODArray<OT>& src_offsets,
                                  PaddedPODArray<T>& res_elems, const IColumn::Filter& filt,
                                  ssize_t result_size_hint);

template <typename T, typename OT>
size_t filter_arrays_impl(PaddedPODArray<T>& data, PaddedPODArray<OT>& offsets,
                          const IColumn::Filter& filter);

template <typename T, typename OT>
size_t filter_arrays_impl_only_data(PaddedPODArray<T>& data, PaddedPODArray<OT>& offsets,
                                    const IColumn::Filter& filter);

inline void column_match_offsets_size(size_t size, size_t offsets_size) {
    if (size != offsets_size) {
        throw doris::Exception(
                ErrorCode::COLUMN_NO_MATCH_OFFSETS_SIZE,
                "Size of offsets doesn't match size of column: size={}, offsets.size={}", size,
                offsets_size);
    }
}

inline void column_match_filter_size(size_t size, size_t filter_size) {
    if (size != filter_size) {
        throw doris::Exception(
                ErrorCode::COLUMN_NO_MATCH_FILTER_SIZE,
                "Size of filter doesn't match size of column: size={}, filter.size={}", size,
                filter_size);
    }
}

} // namespace doris::vectorized

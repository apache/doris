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

#include "vec/exec/format/table/parquet_utils.h"

#include <fmt/format.h>

#include "util/string_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/unaligned.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/parquet/parquet_column_convert.h"

namespace doris::vectorized::parquet_utils {
#define MERGE_STATS_CASE(ParquetType)                                                     \
    case ParquetType: {                                                                   \
        auto typed_left_stat = std::static_pointer_cast<                                  \
                ::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(left);  \
        auto typed_right_stat = std::static_pointer_cast<                                 \
                ::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(right); \
        typed_left_stat->Merge(*typed_right_stat);                                        \
        return;                                                                           \
    }

void merge_stats(const std::shared_ptr<::parquet::Statistics>& left,
                 const std::shared_ptr<::parquet::Statistics>& right) {
    if (left == nullptr || right == nullptr) {
        return;
    }
    DCHECK(left->physical_type() == right->physical_type());

    switch (left->physical_type()) {
        MERGE_STATS_CASE(::parquet::Type::BOOLEAN);
        MERGE_STATS_CASE(::parquet::Type::INT32);
        MERGE_STATS_CASE(::parquet::Type::INT64);
        MERGE_STATS_CASE(::parquet::Type::INT96);
        MERGE_STATS_CASE(::parquet::Type::FLOAT);
        MERGE_STATS_CASE(::parquet::Type::DOUBLE);
        MERGE_STATS_CASE(::parquet::Type::BYTE_ARRAY);
        MERGE_STATS_CASE(::parquet::Type::FIXED_LEN_BYTE_ARRAY);
    default:
        LOG(WARNING) << "Unsupported parquet type for statistics merge: "
                     << static_cast<int>(left->physical_type());
        break;
    }
}

} // namespace doris::vectorized::parquet_utils

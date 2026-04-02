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

#include "core/data_type_serde/data_type_date_serde.h"

namespace doris {

/// Concrete SerDe for TYPE_DATETIME (DateTime v1). Corresponds to DataTypeDateTime on the DataType side.
/// Inherits from shared DataTypeDateLikeV1SerDe<TYPE_DATETIME> (sibling to DataTypeDateSerDe).
class DataTypeDateTimeSerDe final : public DataTypeDateLikeV1SerDe<PrimitiveType::TYPE_DATETIME> {
public:
    DataTypeDateTimeSerDe(int nesting_level = 1)
            : DataTypeDateLikeV1SerDe<PrimitiveType::TYPE_DATETIME>(nesting_level) {};

    // all from_{XXX} use DataTypeDateLikeV1SerDe's with template check of PrimitiveType T

    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;

    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;

    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;
    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               uint64_t* num_deserialized,
                                               const FormatOptions& options) const override;
    Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;
};

} // namespace doris

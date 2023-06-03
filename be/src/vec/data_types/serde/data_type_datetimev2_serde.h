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

#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <ostream>
#include <string>

#include "common/status.h"
#include "data_type_number_serde.h"
#include "olap/olap_common.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"

namespace doris {
class JsonbOutStream;

namespace vectorized {
class Arena;

class DataTypeDateTimeV2SerDe : public DataTypeNumberSerDe<UInt64> {
public:
    DataTypeDateTimeV2SerDe(int scale) : scale(scale) {};
    void write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                               arrow::ArrayBuilder* array_builder, int start,
                               int end) const override;
    void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                int end, const cctz::time_zone& ctz) const override {
        LOG(FATAL) << "not support read arrow array to uint64 column";
    }
    Status write_column_to_mysql(const IColumn& column, bool return_object_data_as_binary,
                                 std::vector<MysqlRowBuffer<false>>& result, int row_idx, int start,
                                 int end, bool col_const) const override {
        return _write_column_to_mysql(column, return_object_data_as_binary, result, row_idx, start,
                                      end, col_const);
    }

    Status write_column_to_mysql(const IColumn& column, bool return_object_data_as_binary,
                                 std::vector<MysqlRowBuffer<true>>& result, int row_idx, int start,
                                 int end, bool col_const) const override {
        return _write_column_to_mysql(column, return_object_data_as_binary, result, row_idx, start,
                                      end, col_const);
    }

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, bool return_object_data_as_binary,
                                  std::vector<MysqlRowBuffer<is_binary_format>>& result,
                                  int row_idx, int start, int end, bool col_const) const;
    int scale;
};
} // namespace vectorized
} // namespace doris
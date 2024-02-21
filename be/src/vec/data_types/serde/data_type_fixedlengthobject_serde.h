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

#include <glog/logging.h>

#include <ostream>

#include "common/status.h"
#include "data_type_serde.h"
#include "util/jsonb_writer.h"
#include "vec/data_types/serde/data_type_string_serde.h"

namespace doris {
class PValues;
class JsonbValue;

namespace vectorized {
class IColumn;
class Arena;

class DataTypeFixedLengthObjectSerDe : public DataTypeStringSerDe {
public:
    DataTypeFixedLengthObjectSerDe(int nesting_level = 1) : DataTypeStringSerDe(nesting_level) {};
    void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                               arrow::ArrayBuilder* array_builder, int start,
                               int end) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "write_column_to_arrow with type " + column.get_name());
    }
    void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                int end, const cctz::time_zone& ctz) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "read_column_from_arrow with type " + column.get_name());
    }

private:
    bool _is_fixed() const override { return true; }
};
} // namespace vectorized
} // namespace doris

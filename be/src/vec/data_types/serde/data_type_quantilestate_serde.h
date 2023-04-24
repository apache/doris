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
#include <stddef.h>
#include <stdint.h>

#include "common/status.h"
#include "data_type_serde.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "util/quantile_state.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/common/arena.h"
#include "vec/common/string_ref.h"

namespace doris {

namespace vectorized {

template <typename T>
class DataTypeQuantileStateSerDe : public DataTypeSerDe {
public:
    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;

    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena* mem_pool,
                                 int32_t col_id, int row_num) const override;

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;
};

template <typename T>
Status DataTypeQuantileStateSerDe<T>::write_column_to_pb(const IColumn& column, PValues& result,
                                                         int start, int end) const {
    result.mutable_bytes_value()->Reserve(end - start);
    for (size_t row_num = start; row_num < end; ++row_num) {
        StringRef data = column.get_data_at(row_num);
        result.add_bytes_value(data.to_string());
    }
    return Status::OK();
}

template <typename T>
Status DataTypeQuantileStateSerDe<T>::read_column_from_pb(IColumn& column,
                                                          const PValues& arg) const {
    column.reserve(arg.bytes_value_size());
    for (int i = 0; i < arg.bytes_value_size(); ++i) {
        column.insert_data(arg.bytes_value(i).c_str(), arg.bytes_value(i).size());
    }
    return Status::OK();
}

template <typename T>
void DataTypeQuantileStateSerDe<T>::write_one_cell_to_jsonb(const IColumn& column,
                                                            JsonbWriter& result, Arena* mem_pool,
                                                            int32_t col_id, int row_num) const {
    auto& col = reinterpret_cast<const ColumnQuantileState<T>&>(column);
    auto& val = const_cast<QuantileState<T>&>(col.get_element(row_num));
    size_t actual_size = val.get_serialized_size();
    auto ptr = mem_pool->alloc(actual_size);
    result.writeKey(col_id);
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(ptr), actual_size);
    result.writeEndBinary();
}

template <typename T>
void DataTypeQuantileStateSerDe<T>::read_one_cell_from_jsonb(IColumn& column,
                                                             const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnQuantileState<T>&>(column);
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    QuantileState<T> val;
    val.deserialize(Slice(blob->getBlob()));
    col.insert_value(val);
}

} // namespace vectorized
} // namespace doris

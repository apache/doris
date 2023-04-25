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

#include "data_type_string_serde.h"

#include <assert.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>

#include "util/jsonb_document.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"

namespace doris {
namespace vectorized {
class Arena;

Status DataTypeStringSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                               int end) const {
    result.mutable_bytes_value()->Reserve(end - start);
    for (size_t row_num = start; row_num < end; ++row_num) {
        StringRef data = column.get_data_at(row_num);
        result.add_string_value(data.to_string());
    }
    return Status::OK();
}
Status DataTypeStringSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col = reinterpret_cast<ColumnString&>(column);
    col.reserve(arg.string_value_size());
    for (int i = 0; i < arg.string_value_size(); ++i) {
        column.insert_data(arg.string_value(i).c_str(), arg.string_value(i).size());
    }
    return Status::OK();
}

void DataTypeStringSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                  Arena* mem_pool, int32_t col_id,
                                                  int row_num) const {
    result.writeKey(col_id);
    const auto& data_ref = column.get_data_at(row_num);
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(data_ref.data), data_ref.size);
    result.writeEndBinary();
}
void DataTypeStringSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    assert(arg->isBinary());
    auto& col = reinterpret_cast<ColumnString&>(column);
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    col.insert_data(blob->getBlob(), blob->getBlobLen());
}
} // namespace vectorized
} // namespace doris
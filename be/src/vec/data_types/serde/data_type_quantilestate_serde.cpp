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

#include "data_type_quantilestate_serde.h"

#include "util/jsonb_writer.h"

namespace doris::vectorized {

void DataTypeQuantileStateSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                         Arena& arena, int32_t col_id,
                                                         int64_t row_num) const {
    const auto& col = reinterpret_cast<const ColumnQuantileState&>(column);
    auto& val = col.get_element(row_num);
    size_t actual_size = val.get_serialized_size();
    auto* ptr = arena.alloc(actual_size);
    val.serialize((uint8_t*)ptr);
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(ptr), actual_size);
    result.writeEndBinary();
}

void DataTypeQuantileStateSerDe::read_one_cell_from_jsonb(IColumn& column,
                                                          const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnQuantileState&>(column);
    const auto* blob = arg->unpack<JsonbBinaryVal>();
    QuantileState val;
    val.deserialize(Slice(blob->getBlob(), blob->getBlobLen()));
    col.insert_value(val);
}
} // namespace doris::vectorized

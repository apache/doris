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

bool DataTypeQuantileStateSerDe::write_column_to_mysql_text(const IColumn& column,
                                                            BufferWritable& bw,
                                                            int64_t row_idx) const {
    const auto& data_column = reinterpret_cast<const ColumnQuantileState&>(column);

    if (_return_object_as_string) {
        const auto& quantile_value = data_column.get_element(row_idx);
        size_t size = quantile_value.get_serialized_size();
        std::unique_ptr<char[]> buf = std::make_unique_for_overwrite<char[]>(size);
        quantile_value.serialize((uint8_t*)buf.get());
        bw.write(buf.get(), size);
        return true;
    } else {
        return false;
    }
}

} // namespace doris::vectorized

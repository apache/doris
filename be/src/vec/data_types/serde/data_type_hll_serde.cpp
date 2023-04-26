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

#include "data_type_hll_serde.h"

#include <gen_cpp/types.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <string>

#include "olap/hll.h"
#include "util/jsonb_document.h"
#include "util/slice.h"
#include "vec/columns/column_complex.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"

namespace doris {

namespace vectorized {
class IColumn;

Status DataTypeHLLSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                            int end) const {
    auto ptype = result.mutable_type();
    ptype->set_id(PGenericType::HLL);
    auto& data_column = assert_cast<const ColumnHLL&>(column);
    int row_count = end - start;
    result.mutable_bytes_value()->Reserve(row_count);
    for (size_t row_num = start; row_num < end; ++row_num) {
        auto& value = const_cast<HyperLogLog&>(data_column.get_element(row_num));
        std::string memory_buffer(value.max_serialized_size(), '0');
        value.serialize((uint8_t*)memory_buffer.data());
        result.add_bytes_value(memory_buffer.data(), memory_buffer.size());
    }
    return Status::OK();
}
Status DataTypeHLLSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col = reinterpret_cast<ColumnHLL&>(column);
    for (int i = 0; i < arg.bytes_value_size(); ++i) {
        HyperLogLog value;
        value.deserialize(Slice(arg.bytes_value(i)));
        col.insert_value(value);
    }
    return Status::OK();
}

void DataTypeHLLSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                               Arena* mem_pool, int32_t col_id, int row_num) const {
    result.writeKey(col_id);
    auto& data_column = assert_cast<const ColumnHLL&>(column);
    auto& hll_value = const_cast<HyperLogLog&>(data_column.get_element(row_num));
    auto size = hll_value.max_serialized_size();
    auto ptr = reinterpret_cast<char*>(mem_pool->alloc(size));
    size_t actual_size = hll_value.serialize((uint8_t*)ptr);
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(ptr), actual_size);
    result.writeEndBinary();
}
void DataTypeHLLSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnHLL&>(column);
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    HyperLogLog hyper_log_log(Slice(blob->getBlob()));
    col.insert_value(hyper_log_log);
}

} // namespace vectorized
} // namespace doris
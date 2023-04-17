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

#include "vec/columns/column_complex.h"

namespace doris {

namespace vectorized {

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

} // namespace vectorized
} // namespace doris
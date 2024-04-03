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

#include "vec/data_types/data_type_quantilestate.h"

#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
// binary: <size array> | <quantilestate array>
//  <size array>: row num | quantilestate1 size | quantilestate2 size | ...
//  <quantilestate array>: quantilestate1 | quantilestate2 | ...
int64_t DataTypeQuantileState::get_uncompressed_serialized_bytes(const IColumn& column,
                                                                 int be_exec_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnQuantileState&>(*ptr);

    auto allocate_len_size = sizeof(size_t) * (column.size() + 1);
    size_t allocate_content_size = 0;
    for (size_t i = 0; i < column.size(); ++i) {
        auto& quantile_state = const_cast<QuantileState&>(data_column.get_element(i));
        allocate_content_size += quantile_state.get_serialized_size();
    }

    return allocate_len_size + allocate_content_size;
}

char* DataTypeQuantileState::serialize(const IColumn& column, char* buf,
                                       int be_exec_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnQuantileState&>(*ptr);

    // serialize the quantile_state size array, row num saves at index 0
    size_t* meta_ptr = (size_t*)buf;
    meta_ptr[0] = column.size();
    for (size_t i = 0; i < meta_ptr[0]; ++i) {
        auto& quantile_state = const_cast<QuantileState&>(data_column.get_element(i));
        meta_ptr[i + 1] = quantile_state.get_serialized_size();
    }

    // serialize each quantile_state
    char* data_ptr = buf + sizeof(size_t) * (meta_ptr[0] + 1);
    for (size_t i = 0; i < meta_ptr[0]; ++i) {
        auto& quantile_state = const_cast<QuantileState&>(data_column.get_element(i));
        quantile_state.serialize((uint8_t*)data_ptr);
        data_ptr += meta_ptr[i + 1];
    }

    return data_ptr;
}

const char* DataTypeQuantileState::deserialize(const char* buf, IColumn* column,
                                               int be_exec_version) const {
    auto& data_column = assert_cast<ColumnQuantileState&>(*column);
    auto& data = data_column.get_data();

    // deserialize the quantile_state size array
    const size_t* meta_ptr = reinterpret_cast<const size_t*>(buf);

    // deserialize each quantile_state
    data.resize(meta_ptr[0]);
    const char* data_ptr = buf + sizeof(size_t) * (meta_ptr[0] + 1);
    for (size_t i = 0; i < meta_ptr[0]; ++i) {
        Slice slice(data_ptr, meta_ptr[i + 1]);
        data[i].deserialize(slice);
        data_ptr += meta_ptr[i + 1];
    }

    return data_ptr;
}

MutableColumnPtr DataTypeQuantileState::create_column() const {
    return ColumnQuantileState::create();
}

void DataTypeQuantileState::serialize_as_stream(const QuantileState& cvalue, BufferWritable& buf) {
    auto& value = const_cast<QuantileState&>(cvalue);
    std::string memory_buffer;
    int bytesize = value.get_serialized_size();
    memory_buffer.resize(bytesize);
    value.serialize(const_cast<uint8_t*>(reinterpret_cast<uint8_t*>(memory_buffer.data())));
    write_string_binary(memory_buffer, buf);
}

void DataTypeQuantileState::deserialize_as_stream(QuantileState& value, BufferReadable& buf) {
    StringRef ref;
    read_string_binary(ref, buf);
    value.deserialize(ref.to_slice());
}

void DataTypeQuantileState::to_string(const class doris::vectorized::IColumn& column,
                                      size_t row_num,
                                      doris::vectorized::BufferWritable& ostr) const {
    auto& data = const_cast<QuantileState&>(
            assert_cast<const ColumnQuantileState&>(column).get_element(row_num));
    std::string result(data.get_serialized_size(), '0');
    data.serialize((uint8_t*)result.data());
    ostr.write(result.data(), result.size());
}

} // namespace doris::vectorized
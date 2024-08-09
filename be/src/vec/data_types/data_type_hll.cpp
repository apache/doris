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

#include "vec/data_types/data_type_hll.h"

#include <string.h>

#include <utility>

#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

// Two part of binary: <row num > + <size array> | <hll data array>
// first: row num | hll1 size | hll2 size | ...
// second: hll1 | hll2 | ...
char* DataTypeHLL::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnHLL&>(*ptr);

    size_t row_num = column.size();
    std::vector<size_t> hll_size_array(row_num + 1);
    hll_size_array[0] = row_num;

    auto allocate_len_size = sizeof(size_t) * (row_num + 1);
    char* buf_start = buf;
    buf += allocate_len_size;

    for (size_t i = 0; i < row_num; ++i) {
        auto& hll = const_cast<HyperLogLog&>(data_column.get_element(i));
        size_t actual_size = hll.serialize(reinterpret_cast<uint8_t*>(buf));
        hll_size_array[i + 1] = actual_size;
        buf += actual_size;
    }

    memcpy(buf_start, hll_size_array.data(), allocate_len_size);
    return buf;
}

// Two part of binary: <row num > + <size array> | <hll data array>
// first: row num | hll1 size | hll2 size | ...
// second: hll1 | hll2 | ...
const char* DataTypeHLL::deserialize(const char* buf, IColumn* column, int be_exec_version) const {
    auto& data_column = assert_cast<ColumnHLL&>(*column);
    auto& data = data_column.get_data();

    size_t row_num = *reinterpret_cast<const size_t*>(buf);
    buf += sizeof(size_t);
    std::vector<size_t> hll_size_array(row_num);
    memcpy(hll_size_array.data(), buf, sizeof(size_t) * row_num);
    buf += sizeof(size_t) * row_num;

    data.resize(row_num);
    for (int i = 0; i < row_num; ++i) {
        data[i].deserialize(Slice(buf, hll_size_array[i]));
        buf += hll_size_array[i];
    }

    return buf;
}

int64_t DataTypeHLL::get_uncompressed_serialized_bytes(const IColumn& column,
                                                       int be_exec_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnHLL&>(*ptr);

    auto allocate_len_size = sizeof(size_t) * (column.size() + 1);
    size_t allocate_content_size = 0;
    for (size_t i = 0; i < column.size(); ++i) {
        auto& hll = const_cast<HyperLogLog&>(data_column.get_element(i));
        allocate_content_size += hll.max_serialized_size();
    }

    return allocate_len_size + allocate_content_size;
}

MutableColumnPtr DataTypeHLL::create_column() const {
    return ColumnHLL::create();
}

void DataTypeHLL::serialize_as_stream(const HyperLogLog& cvalue, BufferWritable& buf) {
    auto& value = const_cast<HyperLogLog&>(cvalue);
    std::string memory_buffer(value.max_serialized_size(), '0');
    size_t actual_size = value.serialize((uint8_t*)memory_buffer.data());
    memory_buffer.resize(actual_size);
    write_string_binary(memory_buffer, buf);
}

void DataTypeHLL::deserialize_as_stream(HyperLogLog& value, BufferReadable& buf) {
    std::string str;
    read_string_binary(str, buf);
    value.deserialize(Slice(str));
}

void DataTypeHLL::to_string(const class doris::vectorized::IColumn& column, size_t row_num,
                            doris::vectorized::BufferWritable& ostr) const {
    auto col_row = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = col_row.first;
    row_num = col_row.second;

    auto& data = const_cast<HyperLogLog&>(assert_cast<const ColumnHLL&>(*ptr).get_element(row_num));

    std::string result(data.max_serialized_size(), '0');
    size_t actual_size = data.serialize((uint8_t*)result.data());
    result.resize(actual_size);
    ostr.write(result.c_str(), result.size());
}

Status DataTypeHLL::from_string(ReadBuffer& rb, IColumn* column) const {
    auto& data_column = assert_cast<ColumnHLL&>(*column);
    auto& data = data_column.get_data();

    HyperLogLog hll;
    if (!hll.deserialize(Slice(rb.to_string()))) {
        return Status::InternalError("deserialize hll from string fail!");
    }
    data.push_back(std::move(hll));
    return Status::OK();
}

} // namespace doris::vectorized

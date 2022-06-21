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

#include "data_type_json.h"

#include "gen_cpp/data.pb.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_json.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/io/io_helper.h"

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace doris::vectorized {

template <typename Reader>
static inline void read(IColumn& column, Reader&& reader) {
    ColumnJson& column_json = assert_cast<ColumnJson&>(column);
    ColumnJson::Chars& data = column_json.get_chars();
    ColumnJson::Offsets& offsets = column_json.get_offsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try {
        reader(data);
        data.push_back(0);
        offsets.push_back(data.size());
    } catch (...) {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}

std::string DataTypeJson::to_string(const IColumn& column, size_t row_num) const {
    const StringRef& s =
            reinterpret_cast<const ColumnJson&>(*column.convert_to_full_column_if_const().get())
                    .get_data_at(row_num);
    return s.json_to_string();
}

void DataTypeJson::to_string(const class doris::vectorized::IColumn& column, size_t row_num,
                             class doris::vectorized::BufferWritable& ostr) const {
    const StringRef& s =
            reinterpret_cast<const ColumnJson&>(*column.convert_to_full_column_if_const().get())
                    .get_data_at(row_num);
    std::string str = s.json_to_string();
    ostr.write(str.c_str(), str.size());
}

MutableColumnPtr DataTypeJson::create_column() const {
    return ColumnJson::create();
}

bool DataTypeJson::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

int64_t DataTypeJson::get_uncompressed_serialized_bytes(const IColumn& column) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnJson&>(*ptr.get());
    return sizeof(IColumn::Offset) * (column.size() + 1) + sizeof(uint64_t) +
           data_column.get_chars().size();
}

char* DataTypeJson::serialize(const IColumn& column, char* buf) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnJson&>(*ptr.get());

    // row num
    *reinterpret_cast<IColumn::Offset*>(buf) = column.size();
    buf += sizeof(IColumn::Offset);
    // offsets
    memcpy(buf, data_column.get_offsets().data(), column.size() * sizeof(IColumn::Offset));
    buf += column.size() * sizeof(IColumn::Offset);
    // total length
    uint64_t value_len = data_column.get_chars().size();
    *reinterpret_cast<uint64_t*>(buf) = value_len;
    buf += sizeof(uint64_t);
    // values
    memcpy(buf, data_column.get_chars().data(), value_len);
    buf += value_len;

    return buf;
}

const char* DataTypeJson::deserialize(const char* buf, IColumn* column) const {
    ColumnJson* column_json = assert_cast<ColumnJson*>(column);
    ColumnJson::Chars& data = column_json->get_chars();
    ColumnJson::Offsets& offsets = column_json->get_offsets();

    // row num
    uint32_t row_num = *reinterpret_cast<const IColumn::Offset*>(buf);
    buf += sizeof(IColumn::Offset);
    // offsets
    offsets.resize(row_num);
    memcpy(offsets.data(), buf, sizeof(IColumn::Offset) * row_num);
    buf += sizeof(IColumn::Offset) * row_num;
    // total length
    uint64_t value_len = *reinterpret_cast<const uint64_t*>(buf);
    buf += sizeof(uint64_t);
    // values
    data.resize(value_len);
    memcpy(data.data(), buf, value_len);
    buf += value_len;

    return buf;
}

} // namespace doris::vectorized

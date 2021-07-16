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

#include "vec/data_types/data_type_string.h"

#include "gen_cpp/data.pb.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/io/io_helper.h"

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace doris::vectorized {

template <typename Reader>
static inline void read(IColumn& column, Reader&& reader) {
    ColumnString& column_string = assert_cast<ColumnString&>(column);
    ColumnString::Chars& data = column_string.get_chars();
    ColumnString::Offsets& offsets = column_string.get_offsets();
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

std::string DataTypeString::to_string(const IColumn& column, size_t row_num) const {
    const StringRef& s =
            assert_cast<const ColumnString&>(*column.convert_to_full_column_if_const().get())
                    .get_data_at(row_num);
    return s.to_string();
}

Field DataTypeString::get_default() const {
    return String();
}

MutableColumnPtr DataTypeString::create_column() const {
    return ColumnString::create();
}

bool DataTypeString::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

size_t DataTypeString::serialize(const IColumn& column, PColumn* pcolumn) const {
    auto ptr = column.convert_to_full_column_if_const();
    const auto& data_column = assert_cast<const ColumnString&>(*ptr.get());

    // compute the mem need to be allocate
    auto allocate_len_size = sizeof(uint32_t) * (column.size() + 1);
    auto allocate_content_size = data_column.get_chars().size();
    pcolumn->mutable_binary()->resize(allocate_len_size + allocate_content_size);
    auto* data = pcolumn->mutable_binary()->data();

    // serialize the string size array
    *reinterpret_cast<uint32_t*>(data) = column.size();
    data += sizeof(uint32_t);
    memcpy(data, data_column.get_offsets().data(), column.size() * sizeof(uint32_t));
    data += column.size() * sizeof(uint32_t);

    // serialize the string content array
    memcpy(data, data_column.get_chars().data(), data_column.get_chars().size());

    return compress_binary(pcolumn);
}

void DataTypeString::deserialize(const PColumn& pcolumn, IColumn* column) const {
    ColumnString* column_string = assert_cast<ColumnString*>(column);
    ColumnString::Chars& data = column_string->get_chars();
    ColumnString::Offsets& offsets = column_string->get_offsets();
    std::string uncompressed;
    read_binary(pcolumn, &uncompressed);

    // deserialize the string size array
    auto* origin_data = uncompressed.data();
    uint32_t column_len = *reinterpret_cast<uint32_t*>(origin_data);
    origin_data += sizeof(uint32_t);
    offsets.resize(column_len);
    memcpy(offsets.data(), origin_data, sizeof(uint32_t) * column_len);
    origin_data += sizeof(uint32_t) * column_len;

    // deserialize the string content array
    uint32_t content_len = uncompressed.size() - sizeof(uint32_t) * (column_len + 1);
    data.resize(content_len);
    memcpy(data.data(), origin_data, content_len);
}
} // namespace doris::vectorized

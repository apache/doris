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

#include "vec/data_types/data_type_bitmap.h"

#include "vec/columns/column_complex.h"
#include "vec/common/assert_cast.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

size_t DataTypeBitMap::serialize(const IColumn& column, PColumn* pcolumn) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnBitmap&>(*ptr);

    auto allocate_len_size = sizeof(size_t) * (column.size() + 1);
    auto allocate_content_size = 0;
    size_t bitmap_size_array[column.size() + 1];
    bitmap_size_array[0] = column.size();

    // compute each bitmap size and save
    for (size_t i = 0; i < column.size(); ++i) {
        auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
        bitmap_size_array[i + 1] = bitmap.getSizeInBytes();
        allocate_content_size += bitmap_size_array[i + 1];
    }
    // serialize the bitmap size array
    pcolumn->mutable_binary()->resize(allocate_len_size + allocate_content_size);
    auto* data = pcolumn->mutable_binary()->data();
    memcpy(data, bitmap_size_array, allocate_len_size);
    data += allocate_len_size;
    // serialize each bitmap
    for (size_t i = 0; i < column.size(); ++i) {
        auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
        bitmap.write(data);
        data += bitmap_size_array[i + 1];
    }

    return compress_binary(pcolumn);
}

void DataTypeBitMap::deserialize(const PColumn& pcolumn, IColumn* column) const {
    auto& data_column = assert_cast<ColumnBitmap&>(*column);
    auto& data = data_column.get_data();

    std::string uncompressed;
    read_binary(pcolumn, &uncompressed);

    auto bitmap_size_array_size = *reinterpret_cast<size_t*>(uncompressed.data());
    size_t bitmap_size_array[bitmap_size_array_size];
    memcpy(bitmap_size_array, uncompressed.data() + sizeof(size_t), sizeof(size_t) * bitmap_size_array_size);
    auto bitmap_content_ptr = uncompressed.data() + sizeof(size_t) * (bitmap_size_array_size + 1);

    data.resize(bitmap_size_array_size);
    for (int i = 0; i < bitmap_size_array_size; ++i) {
        data[i].deserialize(bitmap_content_ptr);
        bitmap_content_ptr += bitmap_size_array[i];
    }
}

MutableColumnPtr DataTypeBitMap::create_column() const {
    return ColumnBitmap::create();
}

void DataTypeBitMap::serialize_as_stream(const BitmapValue& cvalue, BufferWritable& buf) {
    auto& value = const_cast<BitmapValue&>(cvalue);
    std::string memory_buffer;
    int bytesize = value.getSizeInBytes();
    memory_buffer.resize(bytesize);
    value.write(const_cast<char*>(memory_buffer.data()));
    write_string_binary(memory_buffer, buf);
}

void DataTypeBitMap::deserialize_as_stream(BitmapValue& value, BufferReadable& buf) {
    StringRef ref;
    read_string_binary(ref, buf);
    value.deserialize(ref.data);
}

void DataTypeBitMap::to_string(const class doris::vectorized::IColumn& column, size_t row_num,
        doris::vectorized::BufferWritable& ostr) const {
    auto& data = const_cast<BitmapValue&>(assert_cast<const ColumnBitmap&>(column).get_element(row_num));
    std::string result(data.getSizeInBytes(), '0');
    data.write((char*)result.data());

    ostr.write(result.data(), result.size());
}
} // namespace doris::vectorized

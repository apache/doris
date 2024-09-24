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

#include <utility>

#include "agent/be_exec_version_manager.h"
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

// binary: const flag| row num | size array | bitmap array
// <size array>:   bitmap1 size | bitmap2 size | ...
// <bitmap array>: bitmap1 | bitmap2 | ...
int64_t DataTypeBitMap::get_uncompressed_serialized_bytes(const IColumn& column,
                                                          int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        auto size = sizeof(bool) + sizeof(uint32_t);
        bool is_const_column = is_column_const(column);
        auto real_need_copy_num = is_const_column ? 1 : column.size();

        const IColumn* bitmap_column = &column;
        if (is_const_column) {
            const auto& const_column = assert_cast<const ColumnConst&>(column);
            bitmap_column = &(const_column.get_data_column());
        }
        const auto& data_column = assert_cast<const ColumnBitmap&>(*bitmap_column);
        auto allocate_len_size = sizeof(size_t) * (real_need_copy_num + 1);
        size_t allocate_content_size = 0;
        for (size_t i = 0; i < real_need_copy_num; ++i) {
            auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
            allocate_content_size += bitmap.getSizeInBytes();
        }

        return size + allocate_len_size + allocate_content_size;
    } else {
        auto ptr = column.convert_to_full_column_if_const();
        const auto& data_column = assert_cast<const ColumnBitmap&>(*ptr);

        auto allocate_len_size = sizeof(size_t) * (column.size() + 1);
        size_t allocate_content_size = 0;
        for (size_t i = 0; i < column.size(); ++i) {
            auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
            allocate_content_size += bitmap.getSizeInBytes();
        }

        return allocate_len_size + allocate_content_size;
    }
}

char* DataTypeBitMap::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnBitmap&>(*ptr);

    // serialize the bitmap size array, row num saves at index 0
    size_t* meta_ptr = (size_t*)buf;
    meta_ptr[0] = column.size();
    for (size_t i = 0; i < meta_ptr[0]; ++i) {
        auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
        meta_ptr[i + 1] = bitmap.getSizeInBytes();
    }

    // serialize each bitmap
    char* data_ptr = buf + sizeof(size_t) * (meta_ptr[0] + 1);
    for (size_t i = 0; i < meta_ptr[0]; ++i) {
        auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
        bitmap.write_to(data_ptr);
        data_ptr += meta_ptr[i + 1];
    }

    return data_ptr;
}

const char* DataTypeBitMap::deserialize(const char* buf, IColumn* column,
                                        int be_exec_version) const {
    auto& data_column = assert_cast<ColumnBitmap&>(*column);
    auto& data = data_column.get_data();

    // deserialize the bitmap size array
    const size_t* meta_ptr = reinterpret_cast<const size_t*>(buf);

    // deserialize each bitmap
    data.resize(meta_ptr[0]);
    const char* data_ptr = buf + sizeof(size_t) * (meta_ptr[0] + 1);
    for (size_t i = 0; i < meta_ptr[0]; ++i) {
        data[i].deserialize(data_ptr);
        data_ptr += meta_ptr[i + 1];
    }

    return data_ptr;
}

char* DataTypeBitMap::serialize2(const IColumn& column, char* buf, int be_exec_version) const {
    // const flag
    bool is_const_column = is_column_const(column);
    *reinterpret_cast<bool*>(buf) = is_const_column;
    buf += sizeof(bool);

    const IColumn* bitmap_column = &column;
    if (is_const_column) {
        const auto& const_column = assert_cast<const ColumnConst&>(column);
        bitmap_column = &(const_column.get_data_column());
    }
    const auto& data_column = assert_cast<const ColumnBitmap&>(*bitmap_column);

    // serialize the bitmap size array, row num saves at index 0
    size_t* meta_ptr = (size_t*)buf;
    // row num
    const auto row_num = column.size();
    meta_ptr[0] = row_num;
    auto real_need_copy_num = is_const_column ? 1 : row_num;
    for (size_t i = 0; i < real_need_copy_num; ++i) {
        auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
        meta_ptr[i + 1] = bitmap.getSizeInBytes();
    }

    // serialize each bitmap
    char* data_ptr = buf + sizeof(size_t) * (real_need_copy_num + 1);
    for (size_t i = 0; i < real_need_copy_num; ++i) {
        auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
        bitmap.write_to(data_ptr);
        data_ptr += meta_ptr[i + 1];
    }

    return data_ptr;

    // // row num
    // const auto row_num = column.size();
    // *reinterpret_cast<uint32_t*>(buf) = row_num;
    // buf += sizeof(uint32_t);
    // auto real_need_copy_num = is_const_column ? 1 : row_num;

    // const IColumn* bitmap_column = &column;
    // if (is_const_column) {
    //     const auto& const_column = assert_cast<const ColumnConst&>(column);
    //     bitmap_column = &(const_column.get_data_column());
    // }
    // const auto& data_column = assert_cast<const ColumnBitmap&>(*bitmap_column);

    // size_t* meta_ptr = (size_t*)buf;
    // for (size_t i = 0; i < real_need_copy_num; ++i) {
    //     auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
    //     meta_ptr[i] = bitmap.getSizeInBytes();
    // }
    // // serialize each bitmap
    // char* data_ptr = buf + sizeof(size_t) * real_need_copy_num;
    // for (size_t i = 0; i < real_need_copy_num; ++i) {
    //     auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
    //     bitmap.write_to(data_ptr);
    //     data_ptr += meta_ptr[i];
    // }

    // return data_ptr;
}
const char* DataTypeBitMap::deserialize2(const char* buf, MutableColumnPtr* column,
                                         int be_exec_version) const {
    //const flag
    bool is_const_column = *reinterpret_cast<const bool*>(buf);
    buf += sizeof(bool);

    // deserialize the bitmap size array
    const size_t* meta_ptr = reinterpret_cast<const size_t*>(buf);
    auto row_num = meta_ptr[0];
    auto real_copy_num = is_const_column ? 1 : row_num;
    auto& data_column = assert_cast<ColumnBitmap&>(*(column->get()));
    auto& data = data_column.get_data();
    // deserialize each bitmap
    data.resize(real_copy_num);
    const char* data_ptr = buf + sizeof(size_t) * (real_copy_num + 1);
    for (size_t i = 0; i < real_copy_num; ++i) {
        data[i].deserialize(data_ptr);
        data_ptr += meta_ptr[i + 1];
    }

    if (is_const_column) {
        auto const_column = ColumnConst::create((*column)->get_ptr(), row_num);
        *column = const_column->get_ptr();
    }
    return data_ptr;
}

MutableColumnPtr DataTypeBitMap::create_column() const {
    return ColumnBitmap::create();
}

void DataTypeBitMap::serialize_as_stream(const BitmapValue& cvalue, BufferWritable& buf) {
    auto& value = const_cast<BitmapValue&>(cvalue);
    std::string memory_buffer;
    int bytesize = value.getSizeInBytes();
    memory_buffer.resize(bytesize);
    value.write_to(const_cast<char*>(memory_buffer.data()));
    write_string_binary(memory_buffer, buf);
}

void DataTypeBitMap::deserialize_as_stream(BitmapValue& value, BufferReadable& buf) {
    StringRef ref;
    read_string_binary(ref, buf);
    value.deserialize(ref.data);
}

void DataTypeBitMap::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    auto& data =
            const_cast<BitmapValue&>(assert_cast<const ColumnBitmap&>(*ptr).get_element(row_num));
    std::string buffer(data.getSizeInBytes(), '0');
    data.write_to(const_cast<char*>(buffer.data()));
    ostr.write(buffer.c_str(), buffer.size());
}

Status DataTypeBitMap::from_string(ReadBuffer& rb, IColumn* column) const {
    auto& data_column = assert_cast<ColumnBitmap&>(*column);
    auto& data = data_column.get_data();

    BitmapValue value;
    if (!value.deserialize(rb.to_string().c_str())) {
        return Status::InternalError("deserialize BITMAP from string fail!");
    }
    data.push_back(std::move(value));
    return Status::OK();
}
} // namespace doris::vectorized

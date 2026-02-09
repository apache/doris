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

#include "vec/data_types/data_type_fixed_length_object.h"

#include <glog/logging.h>
#include <streamvbyte.h>
#include <string.h>

#include <ostream>

#include "agent/be_exec_version_manager.h"
#include "common/cast_set.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

char* DataTypeFixedLengthObject::serialize(const IColumn& column, char* buf,
                                           int be_exec_version) const {
    if (be_exec_version >= USE_NEW_FIXED_OBJECT_SERIALIZATION_VERSION) {
        // New serialization with streamvbyte encoding for large data
        const auto* data_column = &column;
        size_t real_need_copy_num = 0;
        buf = serialize_const_flag_and_row_num(&data_column, buf, &real_need_copy_num);

        const auto& src_col = assert_cast<const ColumnType&>(*data_column);
        DCHECK(src_col.item_size() > 0)
                << "[serialize]item size of DataTypeFixedLengthObject should be greater than 0";

        // item size
        unaligned_store<size_t>(buf, src_col.item_size());
        buf += sizeof(size_t);

        auto mem_size = real_need_copy_num * src_col.item_size();
        const auto* origin_data = src_col.get_data().data();

        // column data
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(buf, origin_data, mem_size);
            return buf + mem_size;
        } else {
            // Throw exception if mem_size is large than UINT32_MAX
            auto encode_size = streamvbyte_encode(reinterpret_cast<const uint32_t*>(origin_data),
                                                  cast_set<UInt32>(upper_int32(mem_size)),
                                                  (uint8_t*)(buf + sizeof(size_t)));
            unaligned_store<size_t>(buf, encode_size);
            buf += sizeof(size_t);
            return buf + encode_size;
        }
    }

    // Old serialization: const flag | row num | item size | data (memcpy)
    bool is_const_column = is_column_const(column);
    unaligned_store<bool>(buf, is_const_column);
    buf += sizeof(bool);

    const auto row_num = column.size();
    unaligned_store<size_t>(buf, row_num);
    buf += sizeof(size_t);
    auto real_need_copy_num = is_const_column ? 1 : row_num;

    const IColumn* data_column = &column;
    if (is_const_column) {
        const auto& const_column = assert_cast<const ColumnConst&>(column);
        data_column = &(const_column.get_data_column());
    }
    const auto& src_col = assert_cast<const ColumnType&>(*data_column);
    DCHECK(src_col.item_size() > 0)
            << "[serialize]item size of DataTypeFixedLengthObject should be greater than 0";

    unaligned_store<size_t>(buf, src_col.item_size());
    buf += sizeof(size_t);

    const auto* origin_data = src_col.get_data().data();
    memcpy(buf, origin_data, real_need_copy_num * src_col.item_size());
    buf += real_need_copy_num * src_col.item_size();
    return buf;
}

const char* DataTypeFixedLengthObject::deserialize(const char* buf, MutableColumnPtr* column,
                                                   int be_exec_version) const {
    if (be_exec_version >= USE_NEW_FIXED_OBJECT_SERIALIZATION_VERSION) {
        // New deserialization with streamvbyte decoding for large data
        size_t real_have_saved_num = 0;
        buf = deserialize_const_flag_and_row_num(buf, column, &real_have_saved_num);

        auto& dst_col = assert_cast<ColumnType&>(*(column->get()));
        auto item_size = unaligned_load<size_t>(buf);
        buf += sizeof(size_t);
        dst_col.set_item_size(item_size);

        auto mem_size = real_have_saved_num * item_size;
        dst_col.resize(real_have_saved_num);
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            memcpy(dst_col.get_data().data(), buf, mem_size);
            buf = buf + mem_size;
        } else {
            auto encode_size = unaligned_load<size_t>(buf);
            buf += sizeof(size_t);
            streamvbyte_decode((const uint8_t*)buf, (uint32_t*)(dst_col.get_data().data()),
                               cast_set<UInt32>(upper_int32(mem_size)));
            buf = buf + encode_size;
        }
        return buf;
    }

    // Old deserialization
    bool is_const_column = unaligned_load<bool>(buf);
    buf += sizeof(bool);

    size_t row_num = unaligned_load<size_t>(buf);
    buf += sizeof(size_t);

    size_t item_size = unaligned_load<size_t>(buf);
    buf += sizeof(size_t);

    DCHECK(item_size > 0)
            << "[deserialize]item size of DataTypeFixedLengthObject should be greater than 0";

    auto& dst_col = static_cast<ColumnType&>(*(column->get()));
    dst_col.set_item_size(item_size);

    auto real_copy_num = is_const_column ? 1 : row_num;
    dst_col.resize(real_copy_num);
    memcpy(dst_col.get_data().data(), buf, real_copy_num * item_size);
    buf += real_copy_num * item_size;

    if (is_const_column) {
        auto const_column = ColumnConst::create((*column)->get_ptr(), row_num);
        *column = const_column->get_ptr();
    }
    return buf;
}

// binary: const flag | row num | item size| data
// data  : item data1 | item data2...
int64_t DataTypeFixedLengthObject::get_uncompressed_serialized_bytes(const IColumn& column,
                                                                     int be_exec_version) const {
    if (be_exec_version >= USE_NEW_FIXED_OBJECT_SERIALIZATION_VERSION) {
        // New format size calculation with streamvbyte
        auto size = sizeof(bool) + sizeof(size_t) + sizeof(size_t) + sizeof(size_t);
        auto real_need_copy_num = is_column_const(column) ? 1 : column.size();
        const auto& src_col = assert_cast<const ColumnType&>(column);
        auto mem_size = src_col.item_size() * real_need_copy_num;
        if (mem_size <= SERIALIZED_MEM_SIZE_LIMIT) {
            return size + mem_size;
        } else {
            // Throw exception if mem_size is large than UINT32_MAX
            return size + sizeof(size_t) +
                   std::max(mem_size, streamvbyte_max_compressedbytes(
                                              cast_set<UInt32>(upper_int32(mem_size))));
        }
    }

    // Old format size calculation
    auto size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
    const IColumn* data_column = &column;
    if (is_column_const(column)) {
        const auto& const_column = assert_cast<const ColumnConst&>(column);
        data_column = &(const_column.get_data_column());
    }
    const auto& src_col = assert_cast<const ColumnType&>(*data_column);
    return static_cast<const ColumnType&>(src_col).byte_size() + size;
}

MutableColumnPtr DataTypeFixedLengthObject::create_column() const {
    return ColumnType::create(0);
}

Status DataTypeFixedLengthObject::check_column(const IColumn& column) const {
    return check_column_non_nested_type<ColumnType>(column);
}

} // namespace doris::vectorized
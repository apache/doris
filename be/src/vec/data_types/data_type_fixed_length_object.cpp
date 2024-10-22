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
#include <string.h>

#include <ostream>

#include "agent/be_exec_version_manager.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"

namespace doris::vectorized {

char* DataTypeFixedLengthObject::serialize(const IColumn& column, char* buf,
                                           int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        // const flag
        bool is_const_column = is_column_const(column);
        *reinterpret_cast<bool*>(buf) = is_const_column;
        buf += sizeof(bool);

        // row num
        const auto row_num = column.size();
        *reinterpret_cast<size_t*>(buf) = row_num;
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

        // item size
        *reinterpret_cast<size_t*>(buf) = src_col.item_size();
        buf += sizeof(size_t);
        // column data
        const auto* origin_data = src_col.get_data().data();
        memcpy(buf, origin_data, real_need_copy_num * src_col.item_size());
        buf += real_need_copy_num * src_col.item_size();

        return buf;
    } else {
        // row num
        const auto row_num = column.size();
        *reinterpret_cast<uint32_t*>(buf) = row_num;
        buf += sizeof(uint32_t);
        // column data
        auto ptr = column.convert_to_full_column_if_const();
        const auto& src_col = assert_cast<const ColumnType&>(*ptr.get());
        DCHECK(src_col.item_size() > 0)
                << "[serialize]item size of DataTypeFixedLengthObject should be greater than 0";
        *reinterpret_cast<size_t*>(buf) = src_col.item_size();
        buf += sizeof(size_t);
        const auto* origin_data = src_col.get_data().data();
        memcpy(buf, origin_data, row_num * src_col.item_size());
        buf += row_num * src_col.item_size();

        return buf;
    }
}

const char* DataTypeFixedLengthObject::deserialize(const char* buf, MutableColumnPtr* column,
                                                   int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        //const flag
        bool is_const_column = *reinterpret_cast<const bool*>(buf);
        buf += sizeof(bool);
        //row num
        size_t row_num = *reinterpret_cast<const size_t*>(buf);
        buf += sizeof(size_t);
        //item size
        size_t item_size = *reinterpret_cast<const size_t*>(buf);
        buf += sizeof(size_t);

        DCHECK(item_size > 0)
                << "[deserialize]item size of DataTypeFixedLengthObject should be greater than 0";

        auto& dst_col = static_cast<ColumnType&>(*(column->get()));
        dst_col.set_item_size(item_size);
        // column data
        auto real_copy_num = is_const_column ? 1 : row_num;
        dst_col.resize(real_copy_num);
        memcpy(dst_col.get_data().data(), buf, real_copy_num * item_size);
        buf += real_copy_num * item_size;
        if (is_const_column) {
            auto const_column = ColumnConst::create((*column)->get_ptr(), row_num);
            *column = const_column->get_ptr();
        }
        return buf;
    } else {
        // row num
        uint32_t row_num = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        size_t item_size = *reinterpret_cast<const size_t*>(buf);
        buf += sizeof(size_t);

        DCHECK(item_size > 0)
                << "[deserialize]item size of DataTypeFixedLengthObject should be greater than 0";

        auto& dst_col = static_cast<ColumnType&>(*(column->get()));
        dst_col.set_item_size(item_size);
        // column data
        dst_col.resize(row_num);
        memcpy(dst_col.get_data().data(), buf, row_num * item_size);
        buf += row_num * item_size;

        return buf;
    }
}

// binary: const flag | row num | item size| data
// data  : item data1 | item data2...
int64_t DataTypeFixedLengthObject::get_uncompressed_serialized_bytes(const IColumn& column,
                                                                     int be_exec_version) const {
    if (be_exec_version >= USE_CONST_SERDE) {
        auto size = sizeof(bool) + sizeof(size_t) + sizeof(size_t);
        const IColumn* data_column = &column;
        if (is_column_const(column)) {
            const auto& const_column = assert_cast<const ColumnConst&>(column);
            data_column = &(const_column.get_data_column());
        }
        const auto& src_col = assert_cast<const ColumnType&>(*data_column);
        return static_cast<const ColumnType&>(src_col).byte_size() + size;

    } else {
        return static_cast<const ColumnType&>(column).byte_size() + sizeof(uint32_t) +
               sizeof(size_t);
    }
}

MutableColumnPtr DataTypeFixedLengthObject::create_column() const {
    return ColumnType::create(0);
}

} // namespace doris::vectorized
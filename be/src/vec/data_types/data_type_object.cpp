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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeObject.cpp
// and modified by Doris

#include "vec/data_types/data_type_object.h"

#include <gen_cpp/data.pb.h>
#include <string.h>
#include <util/string_util.h>

#include <cassert>
#include <memory>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "vec/columns/column_object.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/json/path_in_data.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

DataTypeObject::DataTypeObject(const String& schema_format_, bool is_nullable_)
        : schema_format(to_lower(schema_format_)), is_nullable(is_nullable_) {}
bool DataTypeObject::equals(const IDataType& rhs) const {
    return typeid_cast<const DataTypeObject*>(&rhs) != nullptr;
}

int64_t DataTypeObject::get_uncompressed_serialized_bytes(const IColumn& column,
                                                          int be_exec_version) const {
    const auto& column_object = assert_cast<const ColumnObject&>(column);
    if (!column_object.is_finalized()) {
        const_cast<ColumnObject&>(column_object).finalize();
    }

    const auto& subcolumns = column_object.get_subcolumns();
    size_t size = 0;

    size += sizeof(uint32_t);
    for (const auto& entry : subcolumns) {
        auto type = entry->data.get_least_common_type();
        if (is_nothing(type)) {
            continue;
        }
        PColumnMeta column_meta_pb;
        column_meta_pb.set_name(entry->path.get_path());
        type->to_pb_column_meta(&column_meta_pb);
        std::string meta_binary;
        column_meta_pb.SerializeToString(&meta_binary);
        size += sizeof(uint32_t);
        size += meta_binary.size();

        size += type->get_uncompressed_serialized_bytes(entry->data.get_finalized_column(),
                                                        be_exec_version);
    }
    // serialize num of rows, only take effect when subcolumns empty
    if (be_exec_version >= VARIANT_SERDE) {
        size += sizeof(uint32_t);
    }

    return size;
}

char* DataTypeObject::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    const auto& column_object = assert_cast<const ColumnObject&>(column);
    if (!column_object.is_finalized()) {
        const_cast<ColumnObject&>(column_object).finalize();
    }
#ifndef NDEBUG
    // DCHECK size
    column_object.check_consistency();
#endif

    const auto& subcolumns = column_object.get_subcolumns();

    char* size_pos = buf;
    buf += sizeof(uint32_t);

    size_t num_of_columns = 0;
    // 2. serialize each subcolumn in a loop
    for (const auto& entry : subcolumns) {
        // 2.1 serialize subcolumn column meta pb (path and type)
        auto type = entry->data.get_least_common_type();
        if (is_nothing(type)) {
            continue;
        }
        ++num_of_columns;
        PColumnMeta column_meta_pb;
        column_meta_pb.set_name(entry->path.get_path());
        type->to_pb_column_meta(&column_meta_pb);
        std::string meta_binary;
        column_meta_pb.SerializeToString(&meta_binary);
        *reinterpret_cast<uint32_t*>(buf) = meta_binary.size();
        buf += sizeof(uint32_t);
        memcpy(buf, meta_binary.data(), meta_binary.size());
        buf += meta_binary.size();

        // 2.2 serialize subcolumn
        buf = type->serialize(entry->data.get_finalized_column(), buf, be_exec_version);
    }
    // serialize num of subcolumns
    *reinterpret_cast<uint32_t*>(size_pos) = num_of_columns;
    // serialize num of rows, only take effect when subcolumns empty
    if (be_exec_version >= VARIANT_SERDE) {
        *reinterpret_cast<uint32_t*>(buf) = column_object.rows();
        buf += sizeof(uint32_t);
    }

    return buf;
}

const char* DataTypeObject::deserialize(const char* buf, IColumn* column,
                                        int be_exec_version) const {
    auto column_object = assert_cast<ColumnObject*>(column);

    // 1. deserialize num of subcolumns
    uint32_t num_subcolumns = *reinterpret_cast<const uint32_t*>(buf);
    buf += sizeof(uint32_t);

    // 2. deserialize each subcolumn in a loop
    for (uint32_t i = 0; i < num_subcolumns; i++) {
        // 2.1 deserialize subcolumn column path (str size + str data)
        uint32_t size = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        std::string meta_binary {buf, size};
        buf += size;
        PColumnMeta column_meta_pb;
        column_meta_pb.ParseFromString(meta_binary);

        // 2.2 deserialize subcolumn
        auto type = DataTypeFactory::instance().create_data_type(column_meta_pb);
        MutableColumnPtr sub_column = type->create_column();
        buf = type->deserialize(buf, sub_column.get(), be_exec_version);

        // add subcolumn to column_object
        PathInData key;
        if (!column_meta_pb.name().empty()) {
            key = PathInData {column_meta_pb.name()};
        }
        column_object->add_sub_column(key, std::move(sub_column), type);
    }
    size_t num_rows = 0;
    // serialize num of rows, only take effect when subcolumns empty
    if (be_exec_version >= VARIANT_SERDE) {
        num_rows = *reinterpret_cast<const uint32_t*>(buf);
        column_object->set_num_rows(num_rows);
        buf += sizeof(uint32_t);
    }

    column_object->finalize();
#ifndef NDEBUG
    // DCHECK size
    column_object->check_consistency();
#endif
    return buf;
}

std::string DataTypeObject::to_string(const IColumn& column, size_t row_num) const {
    const auto& variant = assert_cast<const ColumnObject&>(column);
    std::string res;
    static_cast<void>(variant.serialize_one_row_to_string(row_num, &res));
    return res;
}

void DataTypeObject::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    const auto& variant = assert_cast<const ColumnObject&>(column);
    static_cast<void>(variant.serialize_one_row_to_string(row_num, ostr));
}

} // namespace doris::vectorized

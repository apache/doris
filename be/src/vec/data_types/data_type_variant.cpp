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

#include "vec/data_types/data_type_variant.h"

#include <gen_cpp/data.pb.h>
#include <string.h>
#include <util/string_util.h>

#include <cassert>
#include <memory>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "vec/columns/column.h"
#include "vec/columns/column_variant.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/json/path_in_data.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

DataTypeVariant::DataTypeVariant(int32_t max_subcolumns_count)
        : _max_subcolumns_count(max_subcolumns_count) {
    name = fmt::format("Variant(max subcolumns count = {})", max_subcolumns_count);
}
bool DataTypeVariant::equals(const IDataType& rhs) const {
    auto rhs_type = typeid_cast<const DataTypeVariant*>(&rhs);
    if (rhs_type && _max_subcolumns_count != rhs_type->variant_max_subcolumns_count()) {
        VLOG_DEBUG << "_max_subcolumns_count is" << _max_subcolumns_count
                   << "rhs_type->variant_max_subcolumns_count()"
                   << rhs_type->variant_max_subcolumns_count();
        return false;
    }
    return rhs_type && _max_subcolumns_count == rhs_type->variant_max_subcolumns_count();
}

int64_t DataTypeVariant::get_uncompressed_serialized_bytes(const IColumn& column,
                                                           int be_exec_version) const {
    const auto& column_variant = assert_cast<const ColumnVariant&>(column);
    if (!column_variant.is_finalized()) {
        // Icolumn originates from MutablePtr or block, and therefore can be modified.
        const_cast<ColumnVariant&>(column_variant).finalize();
    }

    const auto& subcolumns = column_variant.get_subcolumns();
    size_t size = 0;

    size += sizeof(uint32_t);
    for (const auto& entry : subcolumns) {
        auto type = entry->data.get_least_common_type();
        if (type->get_primitive_type() == INVALID_TYPE) {
            continue;
        }
        PColumnMeta column_meta_pb;
        column_meta_pb.set_name(entry->path.get_path());
        entry->path.to_protobuf(column_meta_pb.mutable_column_path(), -1 /*not used here*/);
        type->to_pb_column_meta(&column_meta_pb);
        std::string meta_binary;
        column_meta_pb.SerializeToString(&meta_binary);
        size += sizeof(uint32_t);
        size += meta_binary.size();

        size += type->get_uncompressed_serialized_bytes(entry->data.get_finalized_column(),
                                                        be_exec_version);
    }
    // serialize num of rows, only take effect when subcolumns empty
    size += sizeof(uint32_t);

    // sparse column
    // TODO make compability with sparse column
    size += ColumnVariant::get_sparse_column_type()->get_uncompressed_serialized_bytes(
            *column_variant.get_sparse_column(), be_exec_version);

    return size;
}

char* DataTypeVariant::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    const auto& column_variant = assert_cast<const ColumnVariant&>(column);
    if (!column_variant.is_finalized()) {
        // Icolumn originates from block, and therefore can be modified.
        const_cast<ColumnVariant&>(column_variant).finalize();
    }
#ifndef NDEBUG
    // DCHECK size
    column_variant.check_consistency();
#endif

    const auto& subcolumns = column_variant.get_subcolumns();

    char* size_pos = buf;
    buf += sizeof(uint32_t);

    size_t num_of_columns = 0;
    // 2. serialize each subcolumn in a loop
    for (const auto& entry : subcolumns) {
        // 2.1 serialize subcolumn column meta pb (path and type)
        auto type = entry->data.get_least_common_type();
        if (type->get_primitive_type() == INVALID_TYPE) {
            continue;
        }
        ++num_of_columns;
        PColumnMeta column_meta_pb;
        column_meta_pb.set_name(entry->path.get_path());
        entry->path.to_protobuf(column_meta_pb.mutable_column_path(), -1 /*not used here*/);
        type->to_pb_column_meta(&column_meta_pb);
        std::string meta_binary;
        column_meta_pb.SerializeToString(&meta_binary);
        // Safe cast
        unaligned_store<uint32_t>(buf, static_cast<UInt32>(meta_binary.size()));
        buf += sizeof(uint32_t);
        memcpy(buf, meta_binary.data(), meta_binary.size());
        buf += meta_binary.size();

        // 2.2 serialize subcolumn
        buf = type->serialize(entry->data.get_finalized_column(), buf, be_exec_version);
    }
    // serialize num of subcolumns
    // Safe case
    unaligned_store<uint32_t>(size_pos, static_cast<UInt32>(num_of_columns));
    // serialize num of rows, only take effect when subcolumns empty
    unaligned_store<uint32_t>(buf, static_cast<UInt32>(column_variant.rows()));
    buf += sizeof(uint32_t);

    // serialize sparse column
    // TODO make compability with sparse column
    buf = ColumnVariant::get_sparse_column_type()->serialize(*column_variant.get_sparse_column(),
                                                             buf, be_exec_version);

    return buf;
}

Field DataTypeVariant::get_field(const TExprNode& node) const {
    if (node.__isset.string_literal) {
        return Field::create_field<TYPE_STRING>(node.string_literal.value);
    }
    if (node.node_type == TExprNodeType::NULL_LITERAL) {
        return {};
    }
    std::stringstream error_string;
    node.printTo(error_string);
    throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Unkown literal {}", error_string.str());
    return {};
}

const char* DataTypeVariant::deserialize(const char* buf, MutableColumnPtr* column,
                                         int be_exec_version) const {
    auto column_variant = assert_cast<ColumnVariant*>(column->get());

    // 1. deserialize num of subcolumns
    uint32_t num_subcolumns = unaligned_load<uint32_t>(buf);
    buf += sizeof(uint32_t);
    // 2. deserialize each subcolumn in a loop
    for (uint32_t i = 0; i < num_subcolumns; i++) {
        // 2.1 deserialize subcolumn column path (str size + str data)
        auto size = unaligned_load<uint32_t>(buf);
        buf += sizeof(uint32_t);
        std::string meta_binary {buf, size};
        buf += size;
        PColumnMeta column_meta_pb;
        column_meta_pb.ParseFromString(meta_binary);

        // 2.2 deserialize subcolumn
        auto type = DataTypeFactory::instance().create_data_type(column_meta_pb);
        MutableColumnPtr sub_column = type->create_column();
        buf = type->deserialize(buf, &sub_column, be_exec_version);

        PathInData key;
        if (column_meta_pb.has_column_path()) {
            // init from path pb
            key.from_protobuf(column_meta_pb.column_path());
        } else if (!column_meta_pb.name().empty()) {
            // init from name for compatible
            key = PathInData {column_meta_pb.name()};
        }
        // add subcolumn to column_variant
        column_variant->add_sub_column(key, std::move(sub_column), type);
    }
    size_t num_rows = 0;
    // serialize num of rows, only take effect when subcolumns empty
    num_rows = unaligned_load<uint32_t>(buf);
    buf += sizeof(uint32_t);

    // deserialize sparse column
    MutableColumnPtr sparse_column = ColumnVariant::get_sparse_column_type()->create_column();
    buf = ColumnVariant::get_sparse_column_type()->deserialize(buf, &sparse_column,
                                                               be_exec_version);
    column_variant->set_sparse_column(std::move(sparse_column));

    if (column_variant->get_subcolumn({})) {
        column_variant->get_subcolumn({})->resize(num_rows);
    }

    column_variant->set_num_rows(num_rows);

    column_variant->finalize();
#ifndef NDEBUG
    // DCHECK size
    column_variant->check_consistency();
#endif
    return buf;
}

std::string DataTypeVariant::to_string(const IColumn& column, size_t row_num) const {
    const auto& variant = assert_cast<const ColumnVariant&>(column);
    std::string res;
    variant.serialize_one_row_to_string(cast_set<Int64>(row_num), &res);
    return res;
}

void DataTypeVariant::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    const auto& variant = assert_cast<const ColumnVariant&>(column);
    variant.serialize_one_row_to_string(cast_set<Int64>(row_num), ostr);
}

void DataTypeVariant::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->set_variant_max_subcolumns_count(_max_subcolumns_count);
}

MutableColumnPtr DataTypeVariant::create_column() const {
    return ColumnVariant::create(_max_subcolumns_count);
}

} // namespace doris::vectorized
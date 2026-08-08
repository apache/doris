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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/ColumnWithTypeAndName.cpp
// and modified by Doris

#include "core/block/column_with_type_and_name.h"

#include <gen_cpp/data.pb.h>
#include <stddef.h>

#include <memory>
#include <sstream>
#include <string>

#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nothing.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/types.h"

namespace doris {

ColumnWithTypeAndName ColumnWithTypeAndName::clone_empty() const {
    ColumnWithTypeAndName res;

    res.name = name;
    res.type = type;
    if (column) {
        res.column = column->clone_empty();
    } else {
        res.column = nullptr;
    }

    return res;
}

bool ColumnWithTypeAndName::operator==(const ColumnWithTypeAndName& other) const {
    return name == other.name &&
           ((!type && !other.type) || (type && other.type && type->equals(*other.type))) &&
           ((!column && !other.column) ||
            (column && other.column && column->get_name() == other.column->get_name()));
}

void ColumnWithTypeAndName::dump_structure(std::ostream& out) const {
    if (name.empty()) {
        out << "[Anonymous Column]";
    } else {
        out << name;
    }

    if (type) {
        out << " " << type->get_name();
    } else {
        out << " nullptr";
    }

    if (column) {
        out << ' ' << column->dump_structure() << "(use_count=" << column->use_count() << ')';
    } else {
        out << " nullptr";
    }
}

String ColumnWithTypeAndName::dump_structure() const {
    std::stringstream out;
    dump_structure(out);
    return out.str();
}

std::string ColumnWithTypeAndName::to_string(
        size_t row_num, const DataTypeSerDe::FormatOptions& format_options) const {
    return type->to_string(*column->convert_to_full_column_if_const().get(), row_num,
                           format_options);
}

#ifdef BE_TEST
std::string ColumnWithTypeAndName::to_string(size_t row_num) const {
    auto format_options = DataTypeSerDe::get_default_format_options();
    auto timezone = cctz::utc_time_zone();
    format_options.timezone = &timezone;
    return type->to_string(*column->convert_to_full_column_if_const().get(), row_num,
                           format_options);
}
#endif

void ColumnWithTypeAndName::to_pb_column_meta(PColumnMeta* col_meta) const {
    col_meta->set_name(name);
    type->to_pb_column_meta(col_meta);
}

const ColumnNullable& ColumnWithTypeAndName::get_nullable_column() const {
    DCHECK(type->is_nullable());
    DCHECK(column);
    const auto& [physical_column, _] = unpack_if_const(column);
    return assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*physical_column);
}

const ColumnUInt8::Ptr& ColumnWithTypeAndName::get_nullable_null_map_column() const {
    return get_nullable_column().get_null_map_column_ptr();
}

NullableColumnInfo ColumnWithTypeAndName::get_nullable_column_info() const {
    DCHECK(type->is_nullable());
    DCHECK(column);

    const auto [has_null, only_null] = get_nullable_column().get_null_map_state();
    return {.has_null = has_null,
            .only_null = only_null,
            .is_const = is_column_const(*column),
            .is_nullable = true};
}

ColumnWithTypeAndName ColumnWithTypeAndName::unnest_nullable(
        bool replace_null_data_to_default) const {
    NullableColumnInfo info;
    if (type->is_nullable()) {
        info = get_nullable_column_info();
    }
    return unnest_nullable(info, replace_null_data_to_default);
}

ColumnWithTypeAndName ColumnWithTypeAndName::unnest_nullable(
        const NullableColumnInfo& info, bool replace_null_data_to_default) const {
    if (!type->is_nullable()) {
        return {column, type, ""};
    }
    DCHECK(info.is_nullable);

    const auto& nullable_column = get_nullable_column();
    const auto get_nested_column = [&]() -> ColumnPtr {
        const auto& nested_column = nullable_column.get_nested_column_ptr();
        if (info.is_const) {
            return ColumnConst::create(nested_column, column->size());
        }
        return nested_column;
    };

    auto nested_type = assert_cast<const DataTypeNullable*, TypeCheckOnRelease::DISABLE>(type.get())
                               ->get_nested_type();
    if (replace_null_data_to_default && info.has_null) {
        if (column->try_replace_null_payload_with_default_without_cow()) {
            return {get_nested_column(), nested_type, ""};
        }

        // Only copy the nested column because the original nullable column must remain unchanged.
        const auto nested_column = get_nested_column();
        auto mutable_nested_col = nested_column->clone_resized(nested_column->size());
        mutable_nested_col->replace_column_null_data(nullable_column.get_null_map_data().data());
        return {std::move(mutable_nested_col), nested_type, ""};
    }
    return {get_nested_column(), nested_type, ""};
}

Status ColumnWithTypeAndName::check_type_and_column_match() const {
    if (!type) {
        return Status::InternalError("ColumnWithTypeAndName type is nullptr");
    }
    if (!column) {
        return Status::InternalError("ColumnWithTypeAndName column is nullptr");
    }

    if (check_and_get_column<ColumnNothing>(column.get())) {
        return Status::OK();
    }

    auto st = type->check_column(*column);
    if (!st.ok()) {
        return Status::InternalError(
                "ColumnWithTypeAndName check column type failed, column name: {}, type: {},  "
                "column: {} , error: {}",
                name, type->get_name(), column->get_name(), st.to_string());
    }
    return Status::OK();
}
} // namespace doris

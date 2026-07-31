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

#pragma once

#include <algorithm>
#include <optional>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "storage/predicate/predicate_creator.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {

inline std::shared_ptr<ColumnPredicate> string_equals(int32_t column_id, std::string column_name,
                                                      std::string value) {
    return create_comparison_predicate<PredicateType::EQ>(
            column_id, std::move(column_name), std::make_shared<DataTypeString>(),
            Field::create_field<TYPE_STRING>(std::move(value)), false);
}

inline DataTypePtr nullable_string_target_type() {
    return make_nullable(std::make_shared<DataTypeString>());
}

inline bool has_doc_value_column(const IndexRowsetProbe& probe) {
    return std::any_of(probe.segments.begin(), probe.segments.end(), [](const auto& segment) {
        return std::any_of(segment.variant_columns.begin(), segment.variant_columns.end(),
                           [](const auto& column) { return column.is_doc_value_column; });
    });
}

inline bool has_variant_layout(const IndexRowsetProbe& probe, int32_t parent_unique_id,
                               std::string_view relative_path) {
    return std::any_of(probe.segments.begin(), probe.segments.end(), [&](const auto& segment) {
        return std::any_of(segment.variant_columns.begin(), segment.variant_columns.end(),
                           [&](const auto& column) {
                               return column.parent_unique_id == parent_unique_id &&
                                      column.relative_path == relative_path;
                           });
    });
}

inline bool has_variant_parent(const IndexRowsetProbe& probe, int32_t parent_unique_id) {
    return std::any_of(probe.segments.begin(), probe.segments.end(), [&](const auto& segment) {
        return std::any_of(
                segment.variant_columns.begin(), segment.variant_columns.end(),
                [&](const auto& column) { return column.parent_unique_id == parent_unique_id; });
    });
}

inline bool has_sparse_path_stat(const IndexRowsetProbe& probe, std::string_view relative_path) {
    return std::any_of(probe.segments.begin(), probe.segments.end(), [&](const auto& segment) {
        return std::any_of(segment.variant_columns.begin(), segment.variant_columns.end(),
                           [&](const auto& column) {
                               auto it =
                                       column.sparse_non_null_size.find(std::string(relative_path));
                               return it != column.sparse_non_null_size.end() && it->second > 0;
                           });
    });
}

inline bool schema_has_variant_path(const TabletSchema& schema, int32_t parent_unique_id,
                                    std::string_view relative_path) {
    if (!schema.has_column_unique_id(parent_unique_id)) {
        return false;
    }
    const auto& parent = schema.column_by_uid(parent_unique_id);
    for (const auto& subcolumn : parent.get_sub_columns()) {
        if (subcolumn == nullptr) {
            continue;
        }
        if (subcolumn->name() == relative_path ||
            (subcolumn->has_path_info() &&
             subcolumn->path_info_ptr()->copy_pop_front().get_path() == relative_path)) {
            return true;
        }
    }
    return false;
}

inline bool schema_has_extracted_variant_path(const TabletSchema& schema, int32_t parent_unique_id,
                                              std::string_view relative_path) {
    for (int32_t column_id = 0; column_id < schema.num_columns(); ++column_id) {
        const auto& column = schema.column(column_id);
        if (!column.is_extracted_column() || column.parent_unique_id() != parent_unique_id ||
            !column.has_path_info()) {
            continue;
        }
        if (column.path_info_ptr()->copy_pop_front().get_path() == relative_path) {
            return true;
        }
    }
    return false;
}

inline size_t sparse_stat_entry_count(const IndexRowsetProbe& probe) {
    size_t count = 0;
    for (const auto& segment : probe.segments) {
        for (const auto& column : segment.variant_columns) {
            count += column.sparse_non_null_size.size();
        }
    }
    return count;
}

inline std::string describe_optional_string_values(
        const std::vector<std::optional<std::string>>& values) {
    std::ostringstream out;
    for (const auto& value : values) {
        out << (value.has_value() ? value.value() : "NULL") << '\n';
    }
    return out.str();
}

} // namespace doris::index_storage_test

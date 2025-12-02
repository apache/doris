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

#include "olap/rowset/segment_v2/variant/variant_util.h"

#include <glog/logging.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/json/parse2column.h"

namespace doris::segment_v2::variant_util {

namespace {

Status _parse_variant_columns(vectorized::Block& block, const std::vector<uint32_t>& variant_pos,
                              const std::vector<vectorized::ParseConfig>& configs) {
    for (size_t i = 0; i < variant_pos.size(); ++i) {
        auto column_ref = block.get_by_position(variant_pos[i]).column;
        bool is_nullable = column_ref->is_nullable();
        vectorized::MutableColumnPtr var_column = column_ref->assume_mutable();
        if (is_nullable) {
            const auto& nullable = assert_cast<const vectorized::ColumnNullable&>(*column_ref);
            var_column = nullable.get_nested_column_ptr()->assume_mutable();
        }
        auto& var = assert_cast<vectorized::ColumnVariant&>(*var_column);
        var_column->finalize();

        vectorized::MutableColumnPtr variant_column;
        if (var.is_doc_snapshot_mode()) {
            // doc snapshot mode, we need to parse the doc snapshot column
            vectorized::parse_binary_to_variant(var);
            continue;
        }
        if (!var.is_scalar_variant()) {
            // already parsed
            continue;
        }

        VLOG_DEBUG << "parse scalar variant column: " << var.get_root_type()->get_name();
        vectorized::ColumnPtr scalar_root_column;
        if (var.get_root_type()->get_primitive_type() == TYPE_JSONB) {
            // TODO more efficient way to parse jsonb type, currently we just convert jsonb to
            // json str and parse them into variant
            RETURN_IF_ERROR(vectorized::schema_util::cast_column(
                    {var.get_root(), var.get_root_type(), ""},
                    var.get_root()->is_nullable()
                            ? make_nullable(std::make_shared<vectorized::DataTypeString>())
                            : std::make_shared<vectorized::DataTypeString>(),
                    &scalar_root_column));
            if (scalar_root_column->is_nullable()) {
                scalar_root_column =
                        assert_cast<const vectorized::ColumnNullable*>(scalar_root_column.get())
                                ->get_nested_column_ptr();
            }
        } else {
            const auto& root = *var.get_root();
            scalar_root_column = root.is_nullable()
                                         ? assert_cast<const vectorized::ColumnNullable&>(root)
                                                   .get_nested_column_ptr()
                                         : var.get_root();
        }

        if (scalar_root_column->is_column_string()) {
            variant_column = vectorized::ColumnVariant::create(0);
            vectorized::parse_json_to_variant(
                    *variant_column.get(),
                    assert_cast<const vectorized::ColumnString&>(*scalar_root_column), configs[i]);
        } else {
            // Root maybe other types rather than string like ColumnVariant(Int32).
            // In this case, we should finlize the root and cast to JSON type
            auto expected_root_type =
                    make_nullable(std::make_shared<vectorized::ColumnVariant::MostCommonType>());
            var.ensure_root_node_type(expected_root_type);
            variant_column = var.assume_mutable();
        }

        // Wrap variant with nullmap if it is nullable
        vectorized::ColumnPtr result = variant_column->get_ptr();
        if (is_nullable) {
            const auto& null_map = assert_cast<const vectorized::ColumnNullable&>(*column_ref)
                                           .get_null_map_column_ptr();
            result = vectorized::ColumnNullable::create(result, null_map);
        }
        block.get_by_position(variant_pos[i]).column = result;
    }
    return Status::OK();
}

} // namespace

Status parse_variant_columns(vectorized::Block& block, const std::vector<uint32_t>& variant_pos,
                             const std::vector<vectorized::ParseConfig>& configs) {
    RETURN_IF_CATCH_EXCEPTION({ return _parse_variant_columns(block, variant_pos, configs); });
}

Status parse_variant_columns(vectorized::Block& block, const TabletSchema& tablet_schema,
                             const std::vector<uint32_t>& column_pos) {
    std::vector<uint32_t> variant_column_pos;
    for (const auto& pos : column_pos) {
        const auto& column = tablet_schema.column(pos);
        if (column.is_variant_type()) {
            variant_column_pos.push_back(pos);
        }
    }

    if (variant_column_pos.empty()) {
        return Status::OK();
    }

    std::vector<vectorized::ParseConfig> configs(variant_column_pos.size());
    for (size_t i = 0; i < variant_column_pos.size(); ++i) {
        configs[i].enable_flatten_nested = tablet_schema.variant_flatten_nested();
        const auto& column = tablet_schema.column(variant_column_pos[i]);
        if (column.is_variant_type()) {
            // enable doc snapshot mode
            if (column.variant_enable_doc_snapshot_mode()) {
                // if has schema template, no need to parse to doc snapshot, when writing data, we
                // will parse to doc snapshot
                if (column.get_sub_columns().empty()) {
                    configs[i].parse_to_doc_snapshot = true;
                } else {
                    configs[i].parse_to_subcolumns = false;
                }

                // if min rows is greater than 0, no need to parse to subcolumns
                // when compaction row size is greater than min rows, parse to subcolumns
                if (column.variant_doc_snapshot_min_rows() > 0) {
                    configs[i].parse_to_subcolumns = false;
                } else {
                    configs[i].parse_to_subcolumns = true;
                }
            } else {
                // default: only parse to subcolumns
                configs[i].parse_to_subcolumns = true;
                configs[i].parse_to_doc_snapshot = false;
            }
        } else {
            return Status::InternalError("column is not variant type, column name: {}",
                                         column.name());
        }
    }

    RETURN_IF_ERROR(doris::segment_v2::variant_util::parse_variant_columns(
            block, variant_column_pos, configs));
    return Status::OK();
}

} // namespace doris::segment_v2::variant_util

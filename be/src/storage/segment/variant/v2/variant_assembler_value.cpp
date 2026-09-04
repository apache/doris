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

#include <limits>
#include <utility>

#include "common/check.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_column_utils.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/typeid_cast.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "storage/segment/variant/v2/variant_assembler_internal.h"

namespace doris::segment_v2::variant_v2 {
namespace {

void append_materialized_scalar(const variant_assembler_detail::PreparedMaterializedColumn& column,
                                size_t row, VariantBatchBuilder::Row& output, uint32_t depth) {
    if (column.primitive == TYPE_JSONB) {
        jsonb_to_variant(assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*column.data)
                                 .get_data_at(row),
                         output, depth);
        return;
    }
    dispatch_variant_typed_column(
            *column.data, column.primitive, [&]<PrimitiveType Type>(const auto& typed) {
                with_variant_typed_scalar<Type>(
                        typed, row, column.scale,
                        [&](const VariantScalarRef& scalar) { output.add_scalar(scalar); });
            });
}

} // namespace

namespace variant_assembler_detail {

PreparedMaterializedColumn prepare_materialized_column(const DataTypePtr& type,
                                                       const IColumn* column, size_t rows) {
    DORIS_CHECK(type != nullptr);
    DORIS_CHECK(column != nullptr);
    DORIS_CHECK_EQ(column->size(), rows);
    const bool nullable_type = type->is_nullable();
    const IColumn* data = column;
    const uint8_t* nulls = nullptr;
    if (nullable_type) {
        const auto* nullable = check_and_get_column<ColumnNullable>(column);
        DORIS_CHECK(nullable != nullptr);
        data = &nullable->get_nested_column();
        nulls = nullable->get_null_map_data().data();
    } else {
        DORIS_CHECK(check_and_get_column<ColumnNullable>(column) == nullptr);
    }

    const DataTypePtr base = remove_nullable(type);
    const PrimitiveType primitive = base->get_primitive_type();
    DORIS_CHECK(primitive == TYPE_ARRAY || primitive == TYPE_JSONB ||
                primitive == TYPE_DECIMAL256 || is_supported_variant_typed_identity(primitive));
    DORIS_CHECK(base->check_column(*data).ok());

    PreparedMaterializedColumn output;
    output.data = data;
    output.nulls = nulls;
    output.primitive = primitive;
    const int scale = base->get_scale();
    DORIS_CHECK_GE(scale, 0);
    DORIS_CHECK(!std::cmp_greater(scale, std::numeric_limits<uint8_t>::max()));
    DORIS_CHECK((primitive != TYPE_DATETIMEV2 && primitive != TYPE_TIMESTAMPTZ) || scale <= 6);
    output.scale = static_cast<uint8_t>(scale);
    if (primitive == TYPE_ARRAY) {
        output.array = assert_cast<const ColumnArray*>(data);
        const auto* array_type = typeid_cast<const DataTypeArray*>(base.get());
        DORIS_CHECK(array_type != nullptr);
        output.nested = std::make_unique<PreparedMaterializedColumn>(prepare_materialized_column(
                array_type->get_nested_type(), &output.array->get_data(),
                output.array->get_data().size()));
    }
    return output;
}

bool is_materialized_value_visible(const PreparedMaterializedColumn& column, size_t row,
                                   bool preserve_direct_subtree_value) {
    DCHECK_LT(row, column.data->size());
    if (column.is_null_at(row)) {
        return false;
    }
    if (preserve_direct_subtree_value) {
        return true;
    }
    if (column.primitive != TYPE_ARRAY) {
        return true;
    }
    const auto row_number = static_cast<ssize_t>(row);
    const size_t begin = column.array->offset_at(row_number);
    const size_t end = begin + column.array->size_at(row_number);
    DCHECK_LE(end, column.nested->data->size());
    // An absent legacy array subcolumn is represented as [], while [null] is an explicit value.
    return begin != end;
}

Status append_materialized_value(const PreparedMaterializedColumn& column, size_t row,
                                 VariantBatchBuilder::Row& output, uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        return Status::Corruption("Variant value exceeds maximum nesting depth {}",
                                  VARIANT_MAX_NESTING_DEPTH);
    }
    if (column.is_null_at(row)) {
        output.add_null();
        return Status::OK();
    }
    if (column.primitive == TYPE_DECIMAL256) {
        return Status::NotSupported(
                "Conversion from Decimal256 materialized storage column to Variant V2 is not "
                "supported");
    }
    if (column.primitive != TYPE_ARRAY) {
        append_materialized_scalar(column, row, output, depth);
        return Status::OK();
    }
    if (depth >= VARIANT_MAX_NESTING_DEPTH) {
        return Status::Corruption("Variant materialized container exceeds maximum depth {}",
                                  VARIANT_MAX_NESTING_DEPTH);
    }
    const auto row_number = static_cast<ssize_t>(row);
    const size_t begin = column.array->offset_at(row_number);
    const size_t end = begin + column.array->size_at(row_number);
    DCHECK_LE(end, column.nested->data->size());
    auto array = output.start_array();
    for (size_t element = begin; element < end; ++element) {
        RETURN_IF_ERROR(append_materialized_value(*column.nested, element, output, depth + 1));
    }
    array.finish();
    return Status::OK();
}

} // namespace variant_assembler_detail
} // namespace doris::segment_v2::variant_v2

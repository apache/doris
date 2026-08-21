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

#include <algorithm>
#include <memory>
#include <utility>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2_internal.h"

namespace doris::CastWrapper::variant_v2_internal {
namespace {

struct CollectedArrayNode {
    DataTypePtr type;
    DorisVector<NullMap::value_type> nulls;
    DorisVector<ColumnArray::Offset64> offsets;
    DorisVector<VariantRef> values;
    std::unique_ptr<CollectedArrayNode> child;

    size_t size() const noexcept { return nulls.size(); }
};

std::unique_ptr<CollectedArrayNode> make_collected_node(const DataTypePtr& type) {
    auto result = std::make_unique<CollectedArrayNode>();
    result->type = remove_nullable(type);
    if (result->type->get_primitive_type() == TYPE_ARRAY) {
        const auto& array_type = assert_cast<const DataTypeArray&>(*result->type);
        result->child = make_collected_node(array_type.get_nested_type());
    }
    return result;
}

void append_collected_value(CollectedArrayNode* node, VariantRef value, bool forced_null) {
    if (node->child == nullptr) {
        node->values.push_back(value);
        node->nulls.push_back(forced_null);
        return;
    }
    if (forced_null || value.is_null() || value.basic_type() != VariantBasicType::ARRAY) {
        node->nulls.push_back(1);
        node->offsets.push_back(node->child->size());
        return;
    }
    node->nulls.push_back(0);
    const uint32_t elements = value.num_elements();
    for (uint32_t element = 0; element < elements; ++element) {
        append_collected_value(node->child.get(), value.array_at(element), false);
    }
    node->offsets.push_back(node->child->size());
}

bool array_dimensions_match(VariantRef value, const CollectedArrayNode& target) {
    if (value.is_null()) {
        // Doris permits null-literal ARRAY leaves to adapt to a deeper target dimension.
        return true;
    }
    if (target.child == nullptr) {
        // ARRAY<VariantV2> deliberately keeps arbitrary Variant values as leaves.
        return target.type->get_primitive_type() == TYPE_VARIANT ||
               value.basic_type() != VariantBasicType::ARRAY;
    }
    if (value.basic_type() != VariantBasicType::ARRAY) {
        return false;
    }
    for (uint32_t element = 0; element < value.num_elements(); ++element) {
        if (!array_dimensions_match(value.array_at(element), *target.child)) {
            return false;
        }
    }
    return true;
}

ColumnPtr variant_column_from_refs(std::span<const VariantRef> values, ForcedNulls nulls) {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = values.size()});
    for (size_t row_index = 0; row_index < values.size(); ++row_index) {
        auto row = builder.begin_row();
        if (!nulls.empty() && nulls[row_index] != 0) {
            row.add_null();
        } else {
            row.add_value(values[row_index]);
        }
        row.finish();
    }
    VariantBatchBuilder block = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

Status finalize_collected_node(FunctionContext* context, const CollectedArrayNode& node,
                               ColumnPtr* output) {
    const ForcedNulls nulls {node.nulls.data(), node.nulls.size()};
    if (node.child != nullptr) {
        ColumnPtr child;
        RETURN_IF_ERROR(finalize_collected_node(context, *node.child, &child));
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->get_data().insert(node.offsets.begin(), node.offsets.end());
        MutableColumnPtr mutable_child = IColumn::mutate(std::move(child));
        auto array = ColumnArray::create(std::move(mutable_child), std::move(offsets));
        auto outer_nulls = ColumnUInt8::create();
        outer_nulls->get_data().insert(node.nulls.begin(), node.nulls.end());
        *output = ColumnNullable::create(std::move(array), std::move(outer_nulls));
        return Status::OK();
    }

    const PrimitiveType primitive = node.type->get_primitive_type();
    if (primitive == TYPE_VARIANT) {
        if (dynamic_cast<const DataTypeVariantV2*>(node.type.get()) == nullptr) {
            return Status::InvalidArgument(
                    "Variant V2 ARRAY CAST does not support legacy Variant targets");
        }
        ColumnPtr encoded = variant_column_from_refs(node.values, nulls);
        return apply_forced_nulls(std::move(encoded), nulls, output);
    }
    if (primitive == TYPE_STRING || primitive == TYPE_CHAR || primitive == TYPE_VARCHAR ||
        primitive == TYPE_JSONB) {
        if (primitive == TYPE_JSONB) {
            return cast_variant_refs_to_jsonb(context, node.values, nulls, output);
        }
        return cast_variant_refs_to_string(context, node.values, nulls, output);
    }
    if (is_supported_scalar_target(node.type)) {
        return cast_variant_refs_to_scalar(context, node.values, node.type, nulls, output);
    }
    *output = make_all_null_column(node.type, node.values.size());
    return Status::OK();
}

} // namespace

Status cast_variant_to_array(FunctionContext* context, const ColumnVariantV2& source,
                             const DataTypePtr& target_type, size_t rows, ForcedNulls forced_nulls,
                             ColumnPtr* output) {
    if (source.size() != rows || target_type->get_primitive_type() != TYPE_ARRAY ||
        (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid Variant V2 input shape for ARRAY CAST");
    }
    if (source.is_typed()) {
        if (context == nullptr) {
            // vexplode_v2 requests only already-encoded arrays. A typed Variant has no array
            // representation to expose on that internal path.
            *output = make_all_null_column(target_type, rows);
            return Status::OK();
        }
        // The typed state already owns a concrete Doris column. Reuse the same non-strict CAST
        // executor as scalar targets so typed strings such as "[]" retain the V1 String->Array
        // behavior.
        return cast_typed_variant_to_scalar(context, source, target_type, rows, forced_nulls,
                                            output);
    }
    std::unique_ptr<CollectedArrayNode> root = make_collected_node(target_type);
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        const VariantRef value = source.get_value_ref(row_index);
        const bool row_is_null = (!forced_nulls.empty() && forced_nulls[row_index] != 0) ||
                                 !array_dimensions_match(value, *root);
        append_collected_value(root.get(), value, row_is_null);
    }
    ColumnPtr direct;
    RETURN_IF_ERROR(finalize_collected_node(context, *root, &direct));

    // An encoded Variant string is allowed to contain the textual representation of an array.
    // V1 delegates that case to the ordinary non-strict String->Array CAST. Parse only those
    // rows through the shared executor; native Variant arrays keep the direct recursive path
    // above, which is also required for ARRAY<VariantV2>.
    if (context == nullptr) {
        *output = std::move(direct);
        return Status::OK();
    }
    DorisVector<size_t> string_rows;
    DorisVector<VariantRef> string_values;
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        if (!forced_nulls.empty() && forced_nulls[row_index] != 0) {
            continue;
        }
        const VariantRef value = source.get_value_ref(row_index);
        const VariantBasicType basic_type = value.basic_type();
        if (basic_type == VariantBasicType::SHORT_STRING ||
            (basic_type == VariantBasicType::PRIMITIVE &&
             value.primitive_id() == VariantPrimitiveId::STRING)) {
            string_rows.push_back(row_index);
            string_values.push_back(value);
        }
    }
    if (string_rows.empty()) {
        *output = std::move(direct);
        return Status::OK();
    }

    ColumnPtr parsed_strings;
    RETURN_IF_ERROR(
            cast_variant_refs_to_scalar(context, string_values, target_type, {}, &parsed_strings));
    MutableColumnPtr merged = make_nullable(target_type)->create_column();
    merged->reserve(rows);
    size_t string_index = 0;
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        if (string_index < string_rows.size() && string_rows[string_index] == row_index) {
            merged->insert_from(*parsed_strings, string_index++);
        } else {
            merged->insert_from(*direct, row_index);
        }
    }
    DCHECK_EQ(string_index, string_rows.size());
    *output = std::move(merged);
    return Status::OK();
}

} // namespace doris::CastWrapper::variant_v2_internal

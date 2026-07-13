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
#include "core/column/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/cast/cast_variant_v2_internal.h"
#include "util/variant/variant_block_builder.h"

namespace doris::CastWrapper::variant_v2_internal {
namespace {

struct ArrayEncodePlan {
    DataTypePtr type;
    const ColumnArray* array = nullptr;
    DorisVector<NullMap::value_type> effective_nulls;
    std::unique_ptr<ArrayEncodePlan> child;
    ColumnPtr encoded_owner;
    const ColumnVariantV2* encoded_leaf = nullptr;
};

Status validate_local_nulls(const IColumn& source, const NullMap* nulls) {
    if (nulls != nullptr && nulls->size() != source.size()) {
        return Status::InvalidArgument("Array element null map has {} rows, expected {}",
                                       nulls->size(), source.size());
    }
    return Status::OK();
}

Status mask_and_encode_typed_variant(const ColumnVariantV2& source, ForcedNulls effective_nulls,
                                     ColumnPtr* output) {
    const auto& typed = assert_cast<const ColumnNullable&>(source.typed_column());
    auto combined_nulls = ColumnUInt8::create();
    combined_nulls->get_data().resize(source.size());
    for (size_t row = 0; row < source.size(); ++row) {
        combined_nulls->get_data()[row] = typed.get_null_map_data()[row] |
                                          (!effective_nulls.empty() && effective_nulls[row] != 0);
    }
    ColumnPtr masked =
            ColumnNullable::create(typed.get_nested_column_ptr(), std::move(combined_nulls));
    ColumnPtr masked_variant =
            ColumnVariantV2::create_typed_from_cast(std::move(masked), source.typed_type());
    return clone_as_encoded(assert_cast<const ColumnVariantV2&>(*masked_variant), output);
}

Status build_array_encode_plan(const ColumnPtr& source, const DataTypePtr& source_type,
                               const NullMap* local_nulls, ForcedNulls inherited_nulls,
                               ArrayEncodePlan* plan);

Status populate_effective_nulls(const IColumn& source, const NullMap* local_nulls,
                                ForcedNulls inherited_nulls, ArrayEncodePlan* plan) {
    RETURN_IF_ERROR(validate_local_nulls(source, local_nulls));
    if (!inherited_nulls.empty() && inherited_nulls.size() != source.size()) {
        return Status::InvalidArgument("Array ancestor null map has {} rows, expected {}",
                                       inherited_nulls.size(), source.size());
    }
    if (local_nulls == nullptr && inherited_nulls.empty()) {
        return Status::OK();
    }
    plan->effective_nulls.resize(source.size());
    for (size_t row = 0; row < source.size(); ++row) {
        const bool local_null = local_nulls != nullptr && (*local_nulls)[row] != 0;
        const bool ancestor_null = !inherited_nulls.empty() && inherited_nulls[row] != 0;
        plan->effective_nulls[row] = local_null || ancestor_null;
    }
    return Status::OK();
}

Status build_array_node_plan(const ColumnPtr& source, const DataTypePtr& source_type,
                             ArrayEncodePlan* plan) {
    plan->array = check_and_get_column<ColumnArray>(source.get());
    if (plan->array == nullptr) {
        return Status::InvalidArgument("Array Variant V2 CAST expected ColumnArray, got {}",
                                       source->get_name());
    }
    const auto& elements = assert_cast<const ColumnNullable&>(plan->array->get_data());
    const auto& array_type = assert_cast<const DataTypeArray&>(*source_type);
    plan->child = std::make_unique<ArrayEncodePlan>();
    DorisVector<NullMap::value_type> child_ancestor_nulls;
    if (!plan->effective_nulls.empty()) {
        child_ancestor_nulls.resize(elements.size());
        for (size_t row = 0; row < plan->array->size(); ++row) {
            if (plan->effective_nulls[row] == 0) {
                continue;
            }
            const size_t begin = plan->array->offset_at(row);
            const size_t end = plan->array->get_offsets()[row];
            std::fill(child_ancestor_nulls.begin() + begin, child_ancestor_nulls.begin() + end, 1);
        }
    }
    return build_array_encode_plan(
            elements.get_nested_column_ptr(), remove_nullable(array_type.get_nested_type()),
            &elements.get_null_map_data(), child_ancestor_nulls, plan->child.get());
}

Status build_array_leaf_plan(const ColumnPtr& source, PrimitiveType primitive,
                             ArrayEncodePlan* plan) {
    ColumnPtr typed_or_encoded;
    const ForcedNulls element_nulls {plan->effective_nulls.data(), plan->effective_nulls.size()};
    if (primitive == INVALID_TYPE && source->empty()) {
        plan->encoded_owner = ColumnVariantV2::create();
    } else if (primitive == TYPE_VARIANT) {
        const auto* variant = check_and_get_column<ColumnVariantV2>(source.get());
        if (variant == nullptr) {
            return Status::InvalidArgument("Array Variant V2 CAST received a legacy Variant leaf");
        }
        if (variant->is_typed()) {
            RETURN_IF_ERROR(
                    mask_and_encode_typed_variant(*variant, element_nulls, &plan->encoded_owner));
        } else {
            RETURN_IF_ERROR(clone_as_encoded(*variant, &plan->encoded_owner));
        }
    } else if (primitive == TYPE_JSONB) {
        RETURN_IF_ERROR(
                cast_jsonb_to_variant(source, source->size(), element_nulls, &typed_or_encoded));
        plan->encoded_owner = std::move(typed_or_encoded);
    } else if (is_supported_scalar_source(plan->type)) {
        RETURN_IF_ERROR(cast_scalar_to_variant(source, plan->type, source->size(), element_nulls,
                                               &typed_or_encoded));
        const auto& typed = assert_cast<const ColumnVariantV2&>(*typed_or_encoded);
        RETURN_IF_ERROR(clone_as_encoded(typed, &plan->encoded_owner));
    } else {
        return Status::InvalidArgument("Array element type {} cannot be cast to Variant V2",
                                       plan->type->get_name());
    }
    plan->encoded_leaf = assert_cast<const ColumnVariantV2*>(plan->encoded_owner.get());
    return Status::OK();
}

Status build_array_encode_plan(const ColumnPtr& source, const DataTypePtr& source_type,
                               const NullMap* local_nulls, ForcedNulls inherited_nulls,
                               ArrayEncodePlan* plan) {
    if (!source || !source_type) {
        return Status::InvalidArgument("Array Variant V2 CAST received an empty source");
    }
    plan->type = remove_nullable(source_type);
    RETURN_IF_ERROR(populate_effective_nulls(*source, local_nulls, inherited_nulls, plan));
    const PrimitiveType primitive = plan->type->get_primitive_type();
    if (primitive == TYPE_ARRAY) {
        return build_array_node_plan(source, plan->type, plan);
    }
    return build_array_leaf_plan(source, primitive, plan);
}

void append_array_value(const ArrayEncodePlan& plan, size_t index, VariantBlockBuilder::Row* row) {
    if (!plan.effective_nulls.empty() && plan.effective_nulls[index] != 0) {
        row->add_null();
        return;
    }
    if (plan.array == nullptr) {
        row->add_value(plan.encoded_leaf->get_value_ref(index));
        return;
    }
    auto array = row->start_array();
    const size_t begin = plan.array->offset_at(index);
    const size_t end = plan.array->get_offsets()[index];
    for (size_t child = begin; child < end; ++child) {
        append_array_value(*plan.child, child, row);
    }
    array.finish();
}

struct CollectedArrayNode {
    DataTypePtr type;
    DorisVector<NullMap::value_type> nulls;
    DorisVector<ColumnArray::Offset64> offsets;
    DorisVector<VariantValueRef> values;
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

void append_collected_value(CollectedArrayNode* node, VariantValueRef value, bool forced_null) {
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

ColumnPtr encoded_refs(std::span<const VariantValueRef> values, ForcedNulls nulls) {
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = values.size()});
    for (size_t row_index = 0; row_index < values.size(); ++row_index) {
        auto row = builder.begin_row();
        if (!nulls.empty() && nulls[row_index] != 0) {
            row.add_null();
        } else {
            row.add_value(values[row_index]);
        }
        row.finish();
    }
    VariantEncodedBlock block = builder.finish_block();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_block(block.view());
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
        ColumnPtr encoded = encoded_refs(node.values, nulls);
        return apply_forced_nulls(std::move(encoded), nulls, output);
    }
    if (primitive == TYPE_STRING || primitive == TYPE_CHAR || primitive == TYPE_VARCHAR ||
        primitive == TYPE_JSONB) {
        ColumnPtr encoded = encoded_refs(node.values, nulls);
        const auto& variant = assert_cast<const ColumnVariantV2&>(*encoded);
        if (primitive == TYPE_JSONB) {
            return cast_variant_to_jsonb(context, variant, node.values.size(), nulls, output);
        }
        return cast_variant_to_string(context, variant, node.values.size(), nulls, output);
    }
    if (is_supported_scalar_target(node.type)) {
        return cast_variant_refs_to_scalar(context, node.values, node.type, nulls, output);
    }
    *output = make_all_null_column(node.type, node.values.size());
    return Status::OK();
}

} // namespace

Status cast_array_to_variant(const ColumnPtr& source, const DataTypePtr& source_type, size_t rows,
                             ForcedNulls forced_nulls, ColumnPtr* output) {
    if (!source || source->size() != rows ||
        (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid ARRAY input shape for Variant V2 CAST");
    }
    ArrayEncodePlan plan;
    RETURN_IF_ERROR(build_array_encode_plan(source, source_type, nullptr, forced_nulls, &plan));
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = rows});
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        auto row = builder.begin_row();
        if (!forced_nulls.empty() && forced_nulls[row_index] != 0) {
            row.add_null();
        } else {
            append_array_value(plan, row_index, &row);
        }
        row.finish();
    }
    VariantEncodedBlock block = builder.finish_block();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_block(block.view());
    *output = std::move(result);
    return Status::OK();
}

Status cast_variant_to_array(FunctionContext* context, const ColumnVariantV2& source,
                             const DataTypePtr& target_type, size_t rows, ForcedNulls forced_nulls,
                             ColumnPtr* output) {
    if (source.size() != rows || target_type->get_primitive_type() != TYPE_ARRAY ||
        (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid Variant V2 input shape for ARRAY CAST");
    }
    if (source.is_typed()) {
        *output = make_all_null_column(target_type, rows);
        return Status::OK();
    }
    std::unique_ptr<CollectedArrayNode> root = make_collected_node(target_type);
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        append_collected_value(root.get(), source.get_value_ref(row_index),
                               !forced_nulls.empty() && forced_nulls[row_index] != 0);
    }
    return finalize_collected_node(context, *root, output);
}

} // namespace doris::CastWrapper::variant_v2_internal

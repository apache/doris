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
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2_internal.h"
#include "exprs/function/parse/variant_jsonb_parse.h"

namespace doris::CastWrapper::variant_v2_internal {
namespace {

struct ArrayEncodePlan {
    using ScalarAppender = void (*)(const IColumn&, size_t, uint8_t, VariantBatchBuilder::Row*);

    DataTypePtr type;
    const ColumnArray* array = nullptr;
    ForcedNulls effective_nulls;
    DorisVector<NullMap::value_type> owned_effective_nulls;
    std::unique_ptr<ArrayEncodePlan> child;
    const ColumnVariantV2* encoded_leaf = nullptr;
    const ColumnString* jsonb_leaf = nullptr;
    const IColumn* scalar_leaf = nullptr;
    const NullMap* scalar_nulls = nullptr;
    ScalarAppender append_scalar = nullptr;
    uint8_t scalar_scale = 0;
};

Status validate_local_nulls(const IColumn& source, const NullMap* nulls) {
    if (nulls != nullptr && nulls->size() != source.size()) {
        return Status::InvalidArgument("Array element null map has {} rows, expected {}",
                                       nulls->size(), source.size());
    }
    return Status::OK();
}

template <PrimitiveType Type, typename Column>
void append_scalar_value(const IColumn& source, size_t row, uint8_t scale,
                         VariantBatchBuilder::Row* output) {
    const auto& column = assert_cast<const Column&>(source);
    column_variant_v2_internal::with_typed_scalar<Type>(
            column, row, scale,
            [&](auto&& physical_factory, auto&&) { output->add_scalar(physical_factory()); });
}

void configure_scalar_leaf(const IColumn& source, const DataTypePtr& type, ArrayEncodePlan* plan) {
    const uint32_t scale = type->get_scale();
    DORIS_CHECK_LE(scale, static_cast<uint32_t>(std::numeric_limits<uint8_t>::max()));
    column_variant_v2_internal::dispatch_scalar_column(
            source, type->get_primitive_type(), [&]<PrimitiveType Type>(const auto& column) {
                using Column = std::remove_cvref_t<decltype(column)>;
                plan->scalar_leaf = &column;
                plan->append_scalar = &append_scalar_value<Type, Column>;
                plan->scalar_scale = static_cast<uint8_t>(scale);
            });
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
    const ForcedNulls local = local_nulls == nullptr
                                      ? ForcedNulls {}
                                      : ForcedNulls {local_nulls->data(), local_nulls->size()};
    const bool has_local_null =
            std::ranges::any_of(local, [](uint8_t value) { return value != 0; });
    const bool has_inherited_null =
            std::ranges::any_of(inherited_nulls, [](uint8_t value) { return value != 0; });
    if (!has_local_null && !has_inherited_null) {
        return Status::OK();
    }
    if (!has_inherited_null) {
        plan->effective_nulls = local;
        return Status::OK();
    }
    if (!has_local_null) {
        plan->effective_nulls = inherited_nulls;
        return Status::OK();
    }
    plan->owned_effective_nulls.resize(source.size());
    for (size_t row = 0; row < source.size(); ++row) {
        plan->owned_effective_nulls[row] = local[row] | inherited_nulls[row];
    }
    plan->effective_nulls = {plan->owned_effective_nulls.data(),
                             plan->owned_effective_nulls.size()};
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
    return build_array_encode_plan(elements.get_nested_column_ptr(),
                                   remove_nullable(array_type.get_nested_type()),
                                   &elements.get_null_map_data(), {}, plan->child.get());
}

Status build_array_leaf_plan(const ColumnPtr& source, PrimitiveType primitive,
                             ArrayEncodePlan* plan) {
    if (primitive == INVALID_TYPE && source->empty()) {
        return Status::OK();
    } else if (primitive == TYPE_VARIANT) {
        const auto* variant = check_and_get_column<ColumnVariantV2>(source.get());
        if (variant == nullptr) {
            return Status::InvalidArgument("Array Variant V2 CAST received a legacy Variant leaf");
        }
        if (variant->is_typed()) {
            const auto& typed = assert_cast<const ColumnNullable&>(variant->typed_column());
            plan->scalar_nulls = &typed.get_null_map_data();
            configure_scalar_leaf(typed.get_nested_column(), variant->typed_type(), plan);
        } else {
            plan->encoded_leaf = variant;
        }
    } else if (primitive == TYPE_JSONB) {
        plan->jsonb_leaf = check_and_get_column<ColumnString>(source.get());
        if (plan->jsonb_leaf == nullptr) {
            return Status::InvalidArgument("Array JSONB leaf expected ColumnString, got {}",
                                           source->get_name());
        }
    } else if (is_supported_scalar_source(plan->type)) {
        configure_scalar_leaf(*source, plan->type, plan);
    } else {
        return Status::InvalidArgument("Array element type {} cannot be cast to Variant V2",
                                       plan->type->get_name());
    }
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

void append_array_value(const ArrayEncodePlan& plan, size_t index, VariantBatchBuilder::Row* row) {
    if (!plan.effective_nulls.empty() && plan.effective_nulls[index] != 0) {
        row->add_null();
        return;
    }
    if (plan.array == nullptr) {
        if (plan.scalar_nulls != nullptr && (*plan.scalar_nulls)[index] != 0) {
            row->add_null();
        } else if (plan.append_scalar != nullptr) {
            plan.append_scalar(*plan.scalar_leaf, index, plan.scalar_scale, row);
        } else if (plan.encoded_leaf != nullptr) {
            row->add_value(plan.encoded_leaf->get_value_ref(index));
        } else if (plan.jsonb_leaf != nullptr) {
            jsonb_to_variant(plan.jsonb_leaf->get_data_at(index), *row);
        } else {
            DORIS_CHECK(false) << "empty Array leaf unexpectedly contains a value";
        }
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

} // namespace

Status cast_array_to_variant(const ColumnPtr& source, const DataTypePtr& source_type, size_t rows,
                             ForcedNulls forced_nulls, ColumnPtr* output) {
    if (!source || source->size() != rows ||
        (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid ARRAY input shape for Variant V2 CAST");
    }
    ArrayEncodePlan plan;
    RETURN_IF_ERROR(build_array_encode_plan(source, source_type, nullptr, forced_nulls, &plan));
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = rows});
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        auto row = builder.begin_row();
        append_array_value(plan, row_index, &row);
        row.finish();
    }
    VariantBatchBuilder block = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    *output = std::move(result);
    return Status::OK();
}

} // namespace doris::CastWrapper::variant_v2_internal

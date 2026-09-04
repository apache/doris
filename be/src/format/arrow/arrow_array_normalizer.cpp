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

#include "format/arrow/arrow_array_normalizer.h"

#include <arrow/array/array_base.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/compute/cast.h>
#include <arrow/type.h>

#include <limits>
#include <memory>

#include "common/check.h"
#include "common/status.h"

namespace doris {

namespace {

// The accepted counterpart of an encoding-only variant. Null when neither this type nor any child
// needs normalization.
std::shared_ptr<arrow::DataType> target_type_for(const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
    case arrow::Type::LARGE_STRING:
    case arrow::Type::STRING_VIEW:
        return arrow::utf8();
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::BINARY_VIEW:
        return arrow::binary();
    case arrow::Type::DICTIONARY: {
        const auto& dictionary = static_cast<const arrow::DictionaryType&>(*type);
        auto nested_target = target_type_for(dictionary.value_type());
        return nested_target != nullptr ? nested_target : dictionary.value_type();
    }
    case arrow::Type::RUN_END_ENCODED: {
        const auto& encoded = static_cast<const arrow::RunEndEncodedType&>(*type);
        auto nested_target = target_type_for(encoded.value_type());
        return nested_target != nullptr ? nested_target : encoded.value_type();
    }
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST: {
        const auto& list = static_cast<const arrow::BaseListType&>(*type);
        auto child_target = target_type_for(list.value_type());
        if (child_target == nullptr) {
            return nullptr;
        }
        auto child = list.value_field()->WithType(std::move(child_target));
        if (type->id() == arrow::Type::LIST) {
            return arrow::list(std::move(child));
        }
        if (type->id() == arrow::Type::LARGE_LIST) {
            return arrow::large_list(std::move(child));
        }
        const auto& fixed = static_cast<const arrow::FixedSizeListType&>(*type);
        return arrow::fixed_size_list(std::move(child), fixed.list_size());
    }
    case arrow::Type::STRUCT: {
        arrow::FieldVector fields;
        fields.reserve(type->num_fields());
        bool changed = false;
        for (const auto& field : type->fields()) {
            auto child_target = target_type_for(field->type());
            changed |= child_target != nullptr;
            fields.push_back(child_target != nullptr ? field->WithType(std::move(child_target))
                                                     : field);
        }
        return changed ? arrow::struct_(fields) : nullptr;
    }
    case arrow::Type::MAP: {
        const auto& map = static_cast<const arrow::MapType&>(*type);
        auto key_target = target_type_for(map.key_type());
        auto item_target = target_type_for(map.item_type());
        if (key_target == nullptr && item_target == nullptr) {
            return nullptr;
        }
        auto key = key_target != nullptr ? map.key_field()->WithType(std::move(key_target))
                                         : map.key_field();
        auto item = item_target != nullptr ? map.item_field()->WithType(std::move(item_target))
                                           : map.item_field();
        return std::make_shared<arrow::MapType>(std::move(key), std::move(item), map.keys_sorted());
    }
    default:
        return nullptr;
    }
}

bool contains_list_view(const arrow::DataType& type) {
    switch (type.id()) {
    case arrow::Type::LIST_VIEW:
    case arrow::Type::LARGE_LIST_VIEW:
        return true;
    case arrow::Type::DICTIONARY:
        return contains_list_view(*static_cast<const arrow::DictionaryType&>(type).value_type());
    case arrow::Type::RUN_END_ENCODED:
        return contains_list_view(*static_cast<const arrow::RunEndEncodedType&>(type).value_type());
    case arrow::Type::EXTENSION:
        return contains_list_view(*static_cast<const arrow::ExtensionType&>(type).storage_type());
    default:
        for (const auto& field : type.fields()) {
            if (contains_list_view(*field->type())) {
                return true;
            }
        }
        return false;
    }
}

template <typename OffsetType, typename ViewArray, typename ListBuilder, typename ListArray>
arrow::Result<std::shared_ptr<arrow::Array>> canonicalize_list_view(const ViewArray& source,
                                                                    arrow::MemoryPool* pool) {
    // Imported C stream arrays are not guaranteed to have validated ranges; copying an invalid
    // range before this check could read beyond the child array.
    auto validation = source.ValidateFull();
    if (!validation.ok()) {
        return validation;
    }

    // Preflight the full expansion before any builder mutation. Nested view builders and Arrow's
    // NullBuilder can otherwise overflow signed lengths before their parent reports capacity.
    if (contains_list_view(*source.value_type())) {
        return arrow::Status::Invalid("nested list view canonicalization is not supported");
    }
    int64_t logical_value_count = 0;
    constexpr int64_t max_value_count = std::numeric_limits<OffsetType>::max();
    for (int64_t i = 0; i < source.length(); ++i) {
        if (source.IsNull(i)) {
            continue;
        }
        const int64_t value_length = source.value_length(i);
        if (value_length > max_value_count - logical_value_count) {
            return arrow::Status::CapacityError("list view logical values exceed output capacity");
        }
        logical_value_count += value_length;
    }

    auto child_builder_result = arrow::MakeBuilder(source.value_type(), pool);
    if (!child_builder_result.ok()) {
        return child_builder_result.status();
    }
    std::shared_ptr<arrow::ArrayBuilder> child_builder(child_builder_result.MoveValueUnsafe());
    ListBuilder builder(pool, child_builder);
    auto reserve_status = builder.Reserve(source.length());
    if (!reserve_status.ok()) {
        return reserve_status;
    }
    reserve_status = child_builder->Reserve(logical_value_count);
    if (!reserve_status.ok()) {
        return reserve_status;
    }

    arrow::ArraySpan values(*source.values()->data());
    for (int64_t i = 0; i < source.length(); ++i) {
        if (source.IsNull(i)) {
            auto append_status = builder.AppendNull();
            if (!append_status.ok()) {
                return append_status;
            }
            continue;
        }
        auto append_status = builder.Append();
        if (!append_status.ok()) {
            return append_status;
        }
        append_status = child_builder->AppendArraySlice(values, source.value_offset(i),
                                                        source.value_length(i));
        if (!append_status.ok()) {
            return append_status;
        }
    }

    std::shared_ptr<ListArray> out;
    auto finish_status = builder.Finish(&out);
    if (!finish_status.ok()) {
        return finish_status;
    }
    return std::static_pointer_cast<arrow::Array>(out);
}

} // namespace

bool is_serde_acceptable_arrow_type(const arrow::DataType& type) {
    switch (type.id()) {
    // Encoding-only variants: convertible to an accepted type.
    case arrow::Type::LARGE_STRING:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::STRING_VIEW:
    case arrow::Type::BINARY_VIEW:
    case arrow::Type::DICTIONARY:
    case arrow::Type::RUN_END_ENCODED:
    case arrow::Type::LIST_VIEW:
    case arrow::Type::LARGE_LIST_VIEW:
        return false;
    // No Doris column can hold these, so they must not reach a serde either.
    case arrow::Type::INTERVAL_MONTHS:
    case arrow::Type::INTERVAL_DAY_TIME:
    case arrow::Type::INTERVAL_MONTH_DAY_NANO:
    case arrow::Type::DURATION:
    case arrow::Type::SPARSE_UNION:
    case arrow::Type::DENSE_UNION:
        return false;
    default:
        for (const auto& field : type.fields()) {
            if (!is_serde_acceptable_arrow_type(*field->type())) {
                return false;
            }
        }
        return true;
    }
}

Status normalize_arrow_array(const std::shared_ptr<arrow::Array>& arr, arrow::MemoryPool* pool,
                             std::shared_ptr<arrow::Array>* out) {
    DORIS_CHECK(arr != nullptr);
    DORIS_CHECK(pool != nullptr);
    DORIS_CHECK(out != nullptr);

    std::shared_ptr<arrow::Array> current = arr;
    // dictionary<int32, large_utf8> decodes to large_utf8, which still needs converting. The bound
    // stops a driver from turning a malformed type into an endless loop.
    constexpr int kMaxPasses = 4;
    for (int pass = 0; pass < kMaxPasses; ++pass) {
        const auto& type = *current->type();
        if (is_serde_acceptable_arrow_type(type)) {
            *out = std::move(current);
            return Status::OK();
        }

        // List views may share or reorder value ranges, so rebuild canonical offsets instead of
        // exposing their buffers to a serde that requires contiguous list values.
        if (type.id() == arrow::Type::LIST_VIEW) {
            auto converted = canonicalize_list_view<int32_t, arrow::ListViewArray,
                                                    arrow::ListBuilder, arrow::ListArray>(
                    static_cast<const arrow::ListViewArray&>(*current), pool);
            if (!converted.ok()) {
                return Status::InternalError("ADBC: failed to normalize arrow type '{}': {}",
                                             type.ToString(), converted.status().ToString());
            }
            current = converted.MoveValueUnsafe();
            continue;
        }
        if (type.id() == arrow::Type::LARGE_LIST_VIEW) {
            auto converted = canonicalize_list_view<int64_t, arrow::LargeListViewArray,
                                                    arrow::LargeListBuilder, arrow::LargeListArray>(
                    static_cast<const arrow::LargeListViewArray&>(*current), pool);
            if (!converted.ok()) {
                return Status::InternalError("ADBC: failed to normalize arrow type '{}': {}",
                                             type.ToString(), converted.status().ToString());
            }
            current = converted.MoveValueUnsafe();
            continue;
        }

        auto target = target_type_for(current->type());
        if (target == nullptr) {
            return Status::NotSupported(
                    "ADBC: arrow type '{}' cannot be materialized into a Doris column",
                    type.ToString());
        }

        arrow::compute::ExecContext exec_context(pool);
        auto casted = arrow::compute::Cast(*current, target, arrow::compute::CastOptions::Safe(),
                                           &exec_context);
        if (!casted.ok()) {
            return Status::InternalError("ADBC: failed to normalize arrow type '{}' to '{}': {}",
                                         type.ToString(), target->ToString(),
                                         casted.status().ToString());
        }
        current = casted.MoveValueUnsafe();
    }
    return Status::InternalError(
            "ADBC: arrow type '{}' is still not materializable after {} "
            "normalization passes",
            arr->type()->ToString(), kMaxPasses);
}

} // namespace doris

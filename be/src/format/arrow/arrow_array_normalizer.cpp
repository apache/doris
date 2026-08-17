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
#include <arrow/compute/cast.h>
#include <arrow/type.h>

#include <memory>

#include "common/check.h"
#include "common/status.h"

namespace doris {

namespace {

// The accepted counterpart of an encoding-only variant. Null when there is none.
std::shared_ptr<arrow::DataType> target_type_for(const arrow::DataType& type) {
    switch (type.id()) {
    case arrow::Type::LARGE_STRING:
    case arrow::Type::STRING_VIEW:
        return arrow::utf8();
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::BINARY_VIEW:
        return arrow::binary();
    case arrow::Type::DICTIONARY:
        return static_cast<const arrow::DictionaryType&>(type).value_type();
    case arrow::Type::RUN_END_ENCODED:
        return static_cast<const arrow::RunEndEncodedType&>(type).value_type();
    default:
        return nullptr;
    }
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
        return true;
    }
}

Status normalize_arrow_array(const std::shared_ptr<arrow::Array>& arr,
                             std::shared_ptr<arrow::Array>* out) {
    DORIS_CHECK(arr != nullptr);
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
            auto converted = arrow::ListArray::FromListView(
                    static_cast<const arrow::ListViewArray&>(*current),
                    arrow::default_memory_pool());
            if (!converted.ok()) {
                return Status::InternalError("ADBC: failed to normalize arrow type '{}': {}",
                                             type.ToString(), converted.status().ToString());
            }
            current = converted.MoveValueUnsafe();
            continue;
        }
        if (type.id() == arrow::Type::LARGE_LIST_VIEW) {
            auto converted = arrow::LargeListArray::FromListView(
                    static_cast<const arrow::LargeListViewArray&>(*current),
                    arrow::default_memory_pool());
            if (!converted.ok()) {
                return Status::InternalError("ADBC: failed to normalize arrow type '{}': {}",
                                             type.ToString(), converted.status().ToString());
            }
            current = converted.MoveValueUnsafe();
            continue;
        }

        auto target = target_type_for(type);
        if (target == nullptr) {
            return Status::NotSupported(
                    "ADBC: arrow type '{}' cannot be materialized into a Doris column",
                    type.ToString());
        }

        auto casted = arrow::compute::Cast(*current, target);
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

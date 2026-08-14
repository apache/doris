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

#include "storage/segment/variant/v2/variant_column_reader.h"

#include <utility>

#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"

namespace doris::segment_v2::variant_v2 {

ColumnVariantV2* try_get_variant_v2_destination(IColumn& column) {
    if (auto* nullable = check_and_get_column<ColumnNullable>(&column)) {
        return check_and_get_column<ColumnVariantV2>(&nullable->get_nested_column());
    }
    return check_and_get_column<ColumnVariantV2>(&column);
}

Status append_assembled_variant(MutableColumnPtr& dst, ColumnNullable::MutablePtr&& assembled) {
    if (!dst || !assembled) {
        return Status::InvalidArgument("Variant V2 assembled output or destination is null");
    }
    const auto* assembled_values =
            check_and_get_column<ColumnVariantV2>(&assembled->get_nested_column());
    if (assembled_values == nullptr) {
        return Status::InvalidArgument(
                "Variant V2 assembled output must be Nullable<ColumnVariantV2>");
    }

    if (try_get_variant_v2_destination(*dst) == nullptr) {
        return Status::InvalidArgument("Variant V2 reader requires a ColumnVariantV2 destination");
    }
    if (is_column_nullable(*dst)) {
        if (dst->empty()) {
            dst = std::move(assembled);
            return Status::OK();
        }
        dst = IColumn::mutate(std::move(dst));
        auto* nullable = assert_cast<ColumnNullable*>(dst.get());
        nullable->insert_range_from(*assembled, 0, assembled->size());
        return Status::OK();
    }

    if (assembled->has_null()) {
        return Status::Corruption(
                "Variant storage returned SQL NULL for a non-nullable Variant V2 destination");
    }
    if (dst->empty()) {
        ColumnPtr values = static_cast<const ColumnNullable&>(*assembled).get_nested_column_ptr();
        assembled.reset();
        dst = IColumn::mutate(std::move(values));
    } else {
        dst = IColumn::mutate(std::move(dst));
        auto& values = assert_cast<ColumnVariantV2&>(*dst);
        values.insert_range_from(*assembled_values, 0, assembled_values->size());
    }
    return Status::OK();
}

} // namespace doris::segment_v2::variant_v2

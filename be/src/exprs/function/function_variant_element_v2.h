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

#include <cstdint>
#include <memory>
#include <span>

#include "common/status.h"
#include "core/column/column.h"
#include "core/string_ref.h"

namespace doris {

class ColumnVariantV2;

class VariantElementV2PathSegment {
public:
    enum class Kind : uint8_t { OBJECT_KEY, ARRAY_INDEX };

    static VariantElementV2PathSegment object_key(StringRef key) {
        return {Kind::OBJECT_KEY, key, 0};
    }

    // Non-negative indexes are zero-based. Negative indexes address elements from the end, so -1
    // selects the last element.
    static VariantElementV2PathSegment array_index(int64_t index) {
        return {Kind::ARRAY_INDEX, {}, index};
    }

    Kind kind() const noexcept { return _kind; }
    StringRef key() const noexcept { return _key; }
    int64_t index() const noexcept { return _index; }

private:
    VariantElementV2PathSegment(Kind kind, StringRef key, int64_t index)
            : _kind(kind), _key(key), _index(index) {}

    Kind _kind;
    StringRef _key;
    int64_t _index;
};

// Owns an explicit, already-tokenized path. It intentionally does not parse dotted strings: the
// runtime SQL adapter contract remains deferred until the coordinated Variant V2 cutover.
class ResolvedVariantElementV2Path {
public:
    ~ResolvedVariantElementV2Path();
    ResolvedVariantElementV2Path(ResolvedVariantElementV2Path&&) noexcept;
    ResolvedVariantElementV2Path& operator=(ResolvedVariantElementV2Path&&) noexcept;

    ResolvedVariantElementV2Path(const ResolvedVariantElementV2Path&) = delete;
    ResolvedVariantElementV2Path& operator=(const ResolvedVariantElementV2Path&) = delete;

    size_t size() const noexcept;
    VariantElementV2PathSegment::Kind kind_at(size_t position) const;
    StringRef object_key_at(size_t position) const;
    int64_t array_index_at(size_t position) const;

private:
    struct Impl;
    explicit ResolvedVariantElementV2Path(std::unique_ptr<Impl> impl);

    friend Status resolve_variant_element_v2_path(
            std::span<const VariantElementV2PathSegment> segments,
            std::unique_ptr<ResolvedVariantElementV2Path>* output);

    std::unique_ptr<Impl> _impl;
};

Status resolve_variant_element_v2_path(std::span<const VariantElementV2PathSegment> segments,
                                       std::unique_ptr<ResolvedVariantElementV2Path>* output);

// The source must be materialized by IFunction routing, which also owns ColumnConst expansion and
// supplies the outer SQL-null map. The output is always Nullable(ColumnVariantV2).
Status extract_variant_element_v2(const ColumnVariantV2& source,
                                  const ResolvedVariantElementV2Path& path,
                                  std::span<const uint8_t> outer_nulls, ColumnPtr* output);

} // namespace doris
#include "core/column/column_variant_v2.h"
#include "exprs/function/function_variant_element_v2.h"

namespace doris::variant_element_v2_internal {

Status extract_encoded_variant_element(const ColumnVariantV2& source,
                                       const ResolvedVariantElementV2Path& path,
                                       std::span<const uint8_t> outer_nulls, ColumnPtr* output);

Status make_all_null_variant_element_result(size_t rows, ColumnPtr* output);

} // namespace doris::variant_element_v2_internal

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
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/string_ref.h"

namespace doris {

class ColumnVariantV2;
class PathInData;

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

// Owns an explicit, already-tokenized path and its immutable derived object-path metadata. It
// intentionally does not parse dotted strings: the runtime SQL adapter contract remains deferred
// until the coordinated Variant V2 cutover.
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
    size_t object_key_count() const noexcept;
    const PathInData* object_path() const noexcept;

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

// Immutable selector plan used by prepared consecutive element_at expressions. Object keys are
// owned by the resolved path, so executing a new Block never reparses literals or copies tokens.
class VariantElementV2PathPlan {
public:
    VariantElementV2PathPlan(const VariantElementV2PathPlan&) = delete;
    VariantElementV2PathPlan& operator=(const VariantElementV2PathPlan&) = delete;

private:
    VariantElementV2PathPlan(std::shared_ptr<const ResolvedVariantElementV2Path> resolved_path,
                             bool always_null)
            : _resolved_path(std::move(resolved_path)), _always_null(always_null) {}

    friend Status build_variant_element_v2_path_plan(
            std::span<const ColumnWithTypeAndName> selectors,
            std::shared_ptr<const VariantElementV2PathPlan>* output);
    friend Status try_extract_variant_element_v2_path(const ColumnPtr& source,
                                                      const VariantElementV2PathPlan& plan,
                                                      ColumnPtr* output, bool* applied);

    std::shared_ptr<const ResolvedVariantElementV2Path> _resolved_path;
    bool _always_null = false;
};

Status build_variant_element_v2_path_plan(std::span<const ColumnWithTypeAndName> selectors,
                                          std::shared_ptr<const VariantElementV2PathPlan>* output);

// The source must be materialized by IFunction routing, which also owns ColumnConst expansion and
// supplies the outer SQL-null map. An input-independent all-NULL result may be Const over a
// one-row Nullable(ColumnVariantV2); other outputs are materialized Nullable(ColumnVariantV2).
Status extract_variant_element_v2(const ColumnVariantV2& source,
                                  const ResolvedVariantElementV2Path& path,
                                  std::span<const uint8_t> outer_nulls, ColumnPtr* output);

// Runtime adapter shared by the ordinary element_at function and the BE-local consecutive-path
// fast path. It applies only when source materializes as ColumnVariantV2; applied remains false for
// legacy Variant so the caller can execute the original expression chain. Every selector must be a
// constant string/integer/bool column. Object keys stay separate tokens and are never dotted-string
// concatenated.
Status try_extract_variant_element_v2_path(const ColumnPtr& source,
                                           std::span<const ColumnWithTypeAndName> selectors,
                                           ColumnPtr* output, bool* applied);

// Executes a selector plan compiled during expression preparation.
Status try_extract_variant_element_v2_path(const ColumnPtr& source,
                                           const VariantElementV2PathPlan& plan, ColumnPtr* output,
                                           bool* applied);

#ifdef BE_TEST
struct VariantElementV2TestAccess {
    static void reset_shredded_path_inspections();
    static size_t shredded_path_inspections();
    static bool has_exact_shredded_path(const ColumnVariantV2& source,
                                        const PathInData& requested_path);
};
#endif

} // namespace doris

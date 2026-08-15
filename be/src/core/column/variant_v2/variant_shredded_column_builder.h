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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>

#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "util/json/path_in_data.h"

namespace doris {

struct VariantShreddedLayoutEntry {
    PathInData path;
    DataTypePtr scalar_type;
};

// Builds one output-owned shredded column from a fixed, Block-local path layout. The source is
// never mutated or retained. Only exact primitive scalar leaves are extracted in this first-stage
// builder; structural conflicts remain wholly in the residual.
class VariantShreddedColumnBuilder {
public:
    // Collects one shredded batch directly from row-local reader values. Selected scalar leaves
    // are copied into owned shredded field children as they arrive; structural values stay in the
    // caller's residual row. The builder retains neither source columns nor borrowed Variant refs.
    class Batch {
    public:
        ~Batch();

        Batch(const Batch&) = delete;
        Batch& operator=(const Batch&) = delete;
        Batch(Batch&&) noexcept;
        Batch& operator=(Batch&&) noexcept;

        std::optional<size_t> find_path(const PathInData& path) const noexcept;
        std::optional<size_t> find_raw_path(StringRef path, uint32_t depth) const noexcept;

        // Returns false without mutating the field for structural values, which stay in residual.
        // Exact scalar type conflicts promote only this child from typed to encoded.
        bool append_value(size_t path_index, VariantRef value);

        // Binds one dense native source to a planned field. The optional owner is consumed by
        // finish() and transferred into the typed child; without it, finish() performs one bulk
        // insert_indices_from. Row routing still decides presence and conflict precedence.
        void bind_materialized_source(size_t path_index, const IColumn& values,
                                      const uint8_t* nulls, MutableColumnPtr owner = {});
        void append_materialized(size_t path_index, size_t source_row);

        // Extracts all matching scalar leaves from root and appends its residual projection.
        // extract=false preserves root unchanged and records all shredded fields as missing.
        void append_root(VariantRef root, VariantBatchBuilder::Row& residual, bool extract);
        void finish_row();
        ColumnVariantV2::MutablePtr finish(ColumnVariantV2::MutablePtr residual);

    private:
        friend class VariantShreddedColumnBuilder;
        Batch(const DorisVector<VariantShreddedLayoutEntry>& layout,
              const DorisVector<size_t>& raw_path_order, size_t rows);

        struct Impl;
        std::unique_ptr<Impl> _impl;
    };

    explicit VariantShreddedColumnBuilder(DorisVector<VariantShreddedLayoutEntry> layout);

    const DorisVector<VariantShreddedLayoutEntry>& layout() const noexcept { return _layout; }

    ColumnVariantV2::MutablePtr build(const ColumnVariantV2& encoded,
                                      std::span<const NullMap::value_type> outer_nulls = {}) const;

    Batch begin_batch(size_t rows) const;

#ifdef BE_TEST
    size_t test_encoded_source_builds() const noexcept { return _test_encoded_source_builds; }
    size_t test_direct_batches() const noexcept { return _test_direct_batches; }
#endif

private:
    static ColumnVariantV2::MutablePtr publish_validated(ColumnVariantV2::MutablePtr residual,
                                                         ColumnVariantV2::ShreddedFields fields);

    DorisVector<VariantShreddedLayoutEntry> _layout;
    DorisVector<size_t> _raw_path_order;
#ifdef BE_TEST
    mutable size_t _test_encoded_source_builds = 0;
    mutable size_t _test_direct_batches = 0;
#endif
};

} // namespace doris

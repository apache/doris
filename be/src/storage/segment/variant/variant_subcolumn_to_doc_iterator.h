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

#include <string>
#include <utility>
#include <vector>

#include "common/compile_check_begin.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_nullable.h"
#include "storage/segment/column_reader.h"

namespace doris::segment_v2 {

/// Converts subcolumn data to doc-value format during compaction.
///
/// When a segment stores variant data in subcolumn format (due to dynamic
/// downgrade) but the compaction output schema expects doc-value buckets,
/// this iterator reads the relevant subcolumns and serializes them into
/// the ColumnMap<String, String> format expected by doc-value columns.
class SubcolumnToDocCompactIterator : public ColumnIterator {
public:
    struct LeafEntry {
        std::string path;
        std::shared_ptr<ColumnReader> column_reader;
        ColumnIteratorUPtr iterator;
        DataTypePtr type; // file_column_type from SubcolumnMeta
    };

    explicit SubcolumnToDocCompactIterator(std::vector<LeafEntry>&& entries)
            : _entries(std::move(entries)) {}

    Status init(const ColumnIteratorOptions& opts) override {
        _leaf_columns.reserve(_entries.size());
        _nested_serdes.reserve(_entries.size());
        for (auto& entry : _entries) {
            RETURN_IF_ERROR(entry.column_reader->new_iterator(&entry.iterator, nullptr));
            RETURN_IF_ERROR(entry.iterator->init(opts));
            // Pre-allocate reusable columns and cache serdes
            _leaf_columns.push_back(entry.type->create_column());
            auto nullable_serde =
                    std::static_pointer_cast<DataTypeNullableSerDe>(entry.type->get_serde());
            _nested_serdes.push_back(nullable_serde->get_nested_serde());
        }
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        for (auto& entry : _entries) {
            RETURN_IF_ERROR(entry.iterator->seek_to_ordinal(ord));
        }
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        // Step 1: Read each leaf subcolumn (reuse pre-allocated columns)
        for (size_t i = 0; i < _entries.size(); ++i) {
            _leaf_columns[i]->clear();
            bool leaf_has_null = false;
            RETURN_IF_ERROR(_entries[i].iterator->next_batch(n, _leaf_columns[i], &leaf_has_null));
        }

        // Step 2: Serialize subcolumns into ColumnMap<String, String> (doc-value format)
        return _serialize_to_doc_value(dst, *n);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        // Step 1: Read each leaf by rowids (reuse pre-allocated columns)
        for (size_t i = 0; i < _entries.size(); ++i) {
            _leaf_columns[i]->clear();
            RETURN_IF_ERROR(_entries[i].iterator->read_by_rowids(rowids, count, _leaf_columns[i]));
        }

        // Step 2: Serialize
        return _serialize_to_doc_value(dst, count);
    }

    ordinal_t get_current_ordinal() const override {
        if (_entries.empty()) {
            return 0;
        }
        return _entries[0].iterator->get_current_ordinal();
    }

private:
    Status _serialize_to_doc_value(MutableColumnPtr& dst, size_t count) const {
        // Create doc-value ColumnMap<String, String>
        // Format: for each row, a list of (path, jsonb_binary_value) pairs
        auto doc_value_column = ColumnVariant::create_binary_column_fn();
        auto* map_column = assert_cast<ColumnMap*>(doc_value_column.get());
        auto& map_keys = assert_cast<ColumnString&>(map_column->get_keys());
        auto& map_values = assert_cast<ColumnString&>(map_column->get_values());
        auto& map_offsets = map_column->get_offsets();

        for (size_t row = 0; row < count; ++row) {
            for (size_t col_idx = 0; col_idx < _entries.size(); ++col_idx) {
                const auto& entry = _entries[col_idx];
                const auto& leaf_col = _leaf_columns[col_idx];

                // Subcolumn leaves are always nullable
                const auto& nullable =
                        assert_cast<const ColumnNullable&>(*leaf_col);
                if (nullable.is_null_at(row)) {
                    continue; // skip null values
                }

                // Insert path as key
                map_keys.insert_data(entry.path.data(), entry.path.size());

                // Insert serialized JSONB binary as value
                ColumnString::Chars& chars = map_values.get_chars();
                _nested_serdes[col_idx]->write_one_cell_to_binary(
                        nullable.get_nested_column(), chars, row);
                map_values.get_offsets().push_back(chars.size());
            }
            map_offsets.push_back(map_keys.size());
        }

        // Step 3: Set doc-value into ColumnVariant
        auto& variant = assert_cast<ColumnVariant&>(*dst);
        MutableColumnPtr container = ColumnVariant::create(variant.max_subcolumns_count(), count);
        auto& container_variant = assert_cast<ColumnVariant&>(*container);
        container_variant.set_doc_value_column(std::move(doc_value_column));
        variant.insert_range_from(container_variant, 0, count);
        return Status::OK();
    }

    std::vector<LeafEntry> _entries;
    std::vector<MutableColumnPtr> _leaf_columns;        // pre-allocated, reused per batch
    std::vector<DataTypeSerDeSPtr> _nested_serdes;       // cached nested serdes
};

#include "common/compile_check_end.h"

} // namespace doris::segment_v2

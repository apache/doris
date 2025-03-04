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

#include <parallel_hashmap/phmap.h>

#include <memory>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "io/io_common.h"
#include "olap/field.h"
#include "olap/iterators.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/stream_reader.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_object.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_helpers.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

// Reader for hierarchical data for variant, merge with root(sparse encoded columns)
class HierarchicalDataReader : public ColumnIterator {
public:
    // Currently two types of read, merge sparse columns with root columns, or read directly
    enum class ReadType { MERGE_ROOT, READ_DIRECT };

    HierarchicalDataReader(const vectorized::PathInData& path) : _path(path) {}

    static Status create(ColumnIterator** reader, vectorized::PathInData path,
                         const SubcolumnColumnReaders::Node* target_node,
                         const SubcolumnColumnReaders::Node* root, ReadType read_type,
                         std::unique_ptr<ColumnIterator>&& sparse_reader);

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override;

    Status add_stream(const SubcolumnColumnReaders::Node* node);

    void set_root(std::unique_ptr<SubstreamIterator>&& root) { _root_reader = std::move(root); }

private:
    SubstreamReaderTree _substream_reader;
    std::unique_ptr<SubstreamIterator> _root_reader;
    std::unique_ptr<SubstreamIterator> _sparse_column_reader;
    size_t _rows_read = 0;
    vectorized::PathInData _path;

    template <typename NodeFunction>
    Status tranverse(NodeFunction&& node_func) {
        for (auto& entry : _substream_reader) {
            RETURN_IF_ERROR(node_func(*entry));
        }
        return Status::OK();
    }

    Status _process_sub_columns(vectorized::ColumnObject& container_variant,
                                const vectorized::PathsWithColumnAndType& non_nested_subcolumns);

    Status _process_nested_columns(
            vectorized::ColumnObject& container_variant,
            const std::map<vectorized::PathInData, vectorized::PathsWithColumnAndType>&
                    nested_subcolumns,
            size_t nrows);

    Status _process_sparse_column(vectorized::ColumnObject& container_variant, size_t nrows);

    // 1. add root column
    // 2. collect path for subcolumns and nested subcolumns
    // 3. init container with subcolumns
    // 4. init container with nested subcolumns
    // 5. init container with sparse column
    Status _init_container(vectorized::MutableColumnPtr& container, size_t nrows,
                           int max_subcolumns_count);

    // clear all subcolumns's column data for next batch read
    // set null map for nullable column
    Status _init_null_map_and_clear_columns(vectorized::MutableColumnPtr& container,
                                            vectorized::MutableColumnPtr& dst, size_t nrows);

    // process read
    template <typename ReadFunction>
    Status process_read(ReadFunction&& read_func, vectorized::MutableColumnPtr& dst, size_t nrows) {
        using namespace vectorized;
        // // Read all sub columns, and merge with root column
        ColumnNullable* nullable_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = assert_cast<ColumnNullable*>(dst.get());
        }
        auto& variant = nullable_column == nullptr
                                ? assert_cast<ColumnObject&>(*dst)
                                : assert_cast<ColumnObject&>(nullable_column->get_nested_column());

        // read data
        // read root first if it is not read before
        if (_root_reader) {
            RETURN_IF_ERROR(read_func(*_root_reader, {}, _root_reader->type));
        }

        // read container columns
        RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
            RETURN_IF_ERROR(read_func(node.data, node.path, node.data.type));
            return Status::OK();
        }));

        // read sparse column
        if (_sparse_column_reader) {
            RETURN_IF_ERROR(read_func(*_sparse_column_reader, {}, nullptr));
        }

        MutableColumnPtr container;
        RETURN_IF_ERROR(_init_container(container, nrows, variant.max_subcolumns_count()));
        auto& container_variant = assert_cast<ColumnObject&>(*container);
        variant.insert_range_from(container_variant, 0, nrows);

        _rows_read += nrows;
        variant.finalize();
        RETURN_IF_ERROR(_init_null_map_and_clear_columns(container, dst, nrows));
#ifndef NDEBUG
        variant.check_consistency();
#endif

        return Status::OK();
    }
};

// Base class for sparse column processors with common functionality
class BaseSparseColumnProcessor : public ColumnIterator {
protected:
    vectorized::MutableColumnPtr _sparse_column;
    StorageReadOptions* _read_opts; // Shared cache pointer
    std::unique_ptr<ColumnIterator> _sparse_column_reader;

    // Pure virtual method for data processing when encounter existing sparse columns(to be implemented by subclasses)
    virtual void _process_data_with_existing_sparse_column(vectorized::MutableColumnPtr& dst,
                                                           size_t num_rows) = 0;

    // Pure virtual method for data processing when no sparse columns(to be implemented by subclasses)
    virtual void _process_data_without_sparse_column(vectorized::MutableColumnPtr& dst,
                                                     size_t num_rows) = 0;

public:
    BaseSparseColumnProcessor(std::unique_ptr<ColumnIterator>&& reader, StorageReadOptions* opts)
            : _read_opts(opts), _sparse_column_reader(std::move(reader)) {
        _sparse_column = vectorized::ColumnObject::create_sparse_column_fn();
    }

    // Common initialization for all processors
    Status init(const ColumnIteratorOptions& opts) override {
        return _sparse_column_reader->init(opts);
    }

    // Standard seek implementations
    Status seek_to_first() override { return _sparse_column_reader->seek_to_first(); }

    Status seek_to_ordinal(ordinal_t ord) override {
        return _sparse_column_reader->seek_to_ordinal(ord);
    }

    ordinal_t get_current_ordinal() const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "not implement");
    }

    // Template method pattern for batch processing
    template <typename ReadMethod>
    Status _process_batch(ReadMethod&& read_method, size_t nrows,
                          vectorized::MutableColumnPtr& dst) {
        // Cache check and population logic
        if (_read_opts && _read_opts->sparse_column_cache &&
            ColumnReader::is_compaction_reader_type(_read_opts->io_ctx.reader_type)) {
            _sparse_column = _read_opts->sparse_column_cache->assume_mutable();
        } else {
            _sparse_column->clear();
            RETURN_IF_ERROR(read_method());

            if (_read_opts) {
                _read_opts->sparse_column_cache = _sparse_column->assume_mutable();
            }
        }

        const auto& offsets =
                assert_cast<const vectorized::ColumnMap&>(*_sparse_column).get_offsets();
        if (offsets.back() == offsets[-1]) {
            // no sparse column in this batch
            _process_data_without_sparse_column(dst, nrows);
        } else {
            // merge subcolumns to existing sparse columns
            _process_data_with_existing_sparse_column(dst, nrows);
        }
        return Status::OK();
    }

    // Batch processing using template method
    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override {
        return _process_batch(
                [&]() { return _sparse_column_reader->next_batch(n, _sparse_column, has_null); },
                *n, dst);
    }

    // RowID-based read using template method
    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override {
        return _process_batch(
                [&]() {
                    return _sparse_column_reader->read_by_rowids(rowids, count, _sparse_column);
                },
                count, dst);
    }
};

// Implementation for path extraction processor
class SparseColumnExtractReader : public BaseSparseColumnProcessor {
public:
    SparseColumnExtractReader(std::string_view path, std::unique_ptr<ColumnIterator> reader,
                              StorageReadOptions* opts)
            : BaseSparseColumnProcessor(std::move(reader), opts), _path(path) {}

private:
    std::string _path;

    // Fill column by finding path in sparse column
    void _process_data_with_existing_sparse_column(vectorized::MutableColumnPtr& dst,
                                                   size_t num_rows) override {
        _fill_path_column(dst);
    }

    void _fill_path_column(vectorized::MutableColumnPtr& dst);

    void _process_data_without_sparse_column(vectorized::MutableColumnPtr& dst,
                                             size_t num_rows) override {
        dst->insert_many_defaults(num_rows);
    }
};

// Implementation for merge processor
class SparseColumnMergeReader : public BaseSparseColumnProcessor {
public:
    SparseColumnMergeReader(const TabletSchema::PathSet& path_map,
                            std::unique_ptr<ColumnIterator>&& sparse_column_reader,
                            SubstreamReaderTree&& src_subcolumns_for_sparse,
                            StorageReadOptions* opts)
            : BaseSparseColumnProcessor(std::move(sparse_column_reader), opts),
              _src_subcolumn_map(path_map),
              _src_subcolumns_for_sparse(src_subcolumns_for_sparse) {}
    Status init(const ColumnIteratorOptions& opts) override;

    // Batch processing using template method
    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override {
        // read subcolumns first
        RETURN_IF_ERROR(_read_subcolumns([&](SubstreamReaderTree::Node* entry) {
            bool has_null = false;
            return entry->data.iterator->next_batch(n, entry->data.column, &has_null);
        }));
        // then read sparse column
        return _process_batch(
                [&]() { return _sparse_column_reader->next_batch(n, _sparse_column, has_null); },
                *n, dst);
    }

    // RowID-based read using template method
    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override {
        // read subcolumns first
        RETURN_IF_ERROR(_read_subcolumns([&](SubstreamReaderTree::Node* entry) {
            return entry->data.iterator->read_by_rowids(rowids, count, entry->data.column);
        }));
        // then read sparse column
        return _process_batch(
                [&]() {
                    return _sparse_column_reader->read_by_rowids(rowids, count, _sparse_column);
                },
                count, dst);
    }

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

private:
    template <typename ReadFunction>
    Status _read_subcolumns(ReadFunction&& read_func) {
        // clear previous data
        for (auto& entry : _src_subcolumns_for_sparse) {
            entry->data.column->clear();
        }
        // read subcolumns
        for (auto& entry : _src_subcolumns_for_sparse) {
            RETURN_IF_ERROR(read_func(entry.get()));
        }
        return Status::OK();
    }

    // subcolumns in src tablet schema, which will be filtered
    const TabletSchema::PathSet& _src_subcolumn_map;
    // subcolumns to merge to sparse column
    SubstreamReaderTree _src_subcolumns_for_sparse;
    std::vector<std::pair<StringRef, std::shared_ptr<SubstreamReaderTree::Node>>>
            _sorted_src_subcolumn_for_sparse;

    // Path filtering implementation
    void _process_data_with_existing_sparse_column(vectorized::MutableColumnPtr& dst,
                                                   size_t num_rows) override {
        _merge_to(dst);
    }

    void _merge_to(vectorized::MutableColumnPtr& dst);

    void _process_data_without_sparse_column(vectorized::MutableColumnPtr& dst,
                                             size_t num_rows) override;

    void _serialize_nullable_column_to_sparse(const SubstreamReaderTree::Node* src_subcolumn,
                                              vectorized::ColumnString& dst_sparse_column_paths,
                                              vectorized::ColumnString& dst_sparse_column_values,
                                              const StringRef& src_path, size_t row);
};

} // namespace doris::segment_v2

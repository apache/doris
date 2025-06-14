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
#include "vec/columns/column_variant.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/functions/function_helpers.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

// Base class for sparse column processors with common functionality
class BaseSparseColumnProcessor : public ColumnIterator {
protected:
    vectorized::MutableColumnPtr _sparse_column;
    StorageReadOptions* _read_opts; // Shared cache pointer
    std::unique_ptr<ColumnIterator> _sparse_column_reader;
    const TabletColumn& _col;
    // Pure virtual method for data processing when encounter existing sparse columns(to be implemented by subclasses)
    virtual void _process_data_with_existing_sparse_column(vectorized::MutableColumnPtr& dst,
                                                           size_t num_rows) = 0;

    // Pure virtual method for data processing when no sparse columns(to be implemented by subclasses)
    virtual void _process_data_without_sparse_column(vectorized::MutableColumnPtr& dst,
                                                     size_t num_rows) = 0;

public:
    BaseSparseColumnProcessor(std::unique_ptr<ColumnIterator>&& reader, StorageReadOptions* opts,
                              const TabletColumn& col)
            : _read_opts(opts), _sparse_column_reader(std::move(reader)), _col(col) {
        _sparse_column = vectorized::ColumnVariant::create_sparse_column_fn();
    }

    // Common initialization for all processors
    Status init(const ColumnIteratorOptions& opts) override {
        return _sparse_column_reader->init(opts);
    }

    // When performing compaction, multiple columns are extracted from the sparse columns,
    // and the sparse columns only need to be read once.
    // So we need to cache the sparse column and reuse it.
    // The cache is only used when the compaction reader is used.
    bool has_sparse_column_cache() const {
        return _read_opts && _read_opts->sparse_column_cache[_col.parent_unique_id()] &&
               ColumnReader::is_compaction_reader_type(_read_opts->io_ctx.reader_type);
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        if (has_sparse_column_cache()) {
            return Status::OK();
        }
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
        if (has_sparse_column_cache()) {
            _sparse_column =
                    _read_opts->sparse_column_cache[_col.parent_unique_id()]->assume_mutable();
        } else {
            _sparse_column->clear();
            RETURN_IF_ERROR(read_method());

            // cache the sparse column
            if (_read_opts) {
                _read_opts->sparse_column_cache[_col.parent_unique_id()] =
                        _sparse_column->get_ptr();
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
};

// Implementation for path extraction processor
class SparseColumnExtractIterator : public BaseSparseColumnProcessor {
public:
    SparseColumnExtractIterator(std::string_view path, std::unique_ptr<ColumnIterator> reader,
                                StorageReadOptions* opts, const TabletColumn& col)
            : BaseSparseColumnProcessor(std::move(reader), opts, col), _path(path) {}

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

private:
    std::string _path;

    // Fill column by finding path in sparse column
    void _process_data_with_existing_sparse_column(vectorized::MutableColumnPtr& dst,
                                                   size_t num_rows) override {
        _fill_path_column(dst);
    }

    void _fill_path_column(vectorized::MutableColumnPtr& dst) {
        vectorized::ColumnNullable* nullable_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = assert_cast<vectorized::ColumnNullable*>(dst.get());
        }
        vectorized::ColumnVariant& var = nullable_column != nullptr
                                                 ? assert_cast<vectorized::ColumnVariant&>(
                                                           nullable_column->get_nested_column())
                                                 : assert_cast<vectorized::ColumnVariant&>(*dst);
        if (var.is_null_root()) {
            var.add_sub_column({}, dst->size());
        }
        vectorized::NullMap* null_map =
                nullable_column ? &nullable_column->get_null_map_data() : nullptr;
        vectorized::ColumnVariant::fill_path_column_from_sparse_data(
                *var.get_subcolumn({}) /*root*/, null_map, StringRef {_path.data(), _path.size()},
                _sparse_column->get_ptr(), 0, _sparse_column->size());
        var.incr_num_rows(_sparse_column->size());
        var.get_sparse_column()->assume_mutable()->resize(var.rows());
        ENABLE_CHECK_CONSISTENCY(&var);
    }

    void _process_data_without_sparse_column(vectorized::MutableColumnPtr& dst,
                                             size_t num_rows) override {
        dst->insert_many_defaults(num_rows);
    }
};

} // namespace doris::segment_v2

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

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_map.h"
#include "runtime/runtime_profile.h"
#include "storage/iterators.h"
#include "storage/segment/variant/variant_column_reader.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

// Base class for sparse column processors with common functionality
class BaseBinaryColumnProcessor : public ColumnIterator {
protected:
    const StorageReadOptions* _read_opts;
    BinaryColumnCacheSPtr _sparse_column_cache;
    virtual void _process_data_with_existing_sparse_column(MutableColumnPtr& dst,
                                                           size_t num_rows) = 0;

    virtual void _process_data_without_sparse_column(MutableColumnPtr& dst, size_t num_rows) = 0;

public:
    BaseBinaryColumnProcessor(BinaryColumnCacheSPtr sparse_column_cache,
                              const StorageReadOptions* opts)
            : _read_opts(opts), _sparse_column_cache(std::move(sparse_column_cache)) {}

    Status init(const ColumnIteratorOptions& opts) override {
        return _sparse_column_cache->init(opts);
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        return _sparse_column_cache->seek_to_ordinal(ord);
    }

    ordinal_t get_current_ordinal() const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "not implement");
    }

    template <typename ReadMethod>
    Status _process_batch(ReadMethod&& read_method, MutableColumnPtr& dst) {
        {
            SCOPED_RAW_TIMER(&_read_opts->stats->variant_scan_sparse_column_timer_ns);
            int64_t before_size = _read_opts->stats->uncompressed_bytes_read;
            RETURN_IF_ERROR(read_method());
            _read_opts->stats->variant_scan_sparse_column_bytes +=
                    _read_opts->stats->uncompressed_bytes_read - before_size;
        }

        SCOPED_RAW_TIMER(&_read_opts->stats->variant_fill_path_from_sparse_column_timer_ns);
        const size_t nrows = _sparse_column_cache->binary_column->size();
        const auto& offsets =
                assert_cast<const ColumnMap&>(*_sparse_column_cache->binary_column).get_offsets();
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
class BinaryColumnExtractIterator : public BaseBinaryColumnProcessor {
public:
    BinaryColumnExtractIterator(std::string_view path, BinaryColumnCacheSPtr sparse_column_cache,
                                const StorageReadOptions* opts, bool use_variant_v2);

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override;

private:
    std::string _path;
    bool _use_variant_v2;

    Status _validate_destination(IColumn& dst) const;
    Status _finish_variant_v2_batch(size_t num_rows, MutableColumnPtr& dst, bool* has_null);
    Status _fill_variant_v2_path(MutableColumnPtr& dst, size_t num_rows, bool* has_null);
    void _process_data_with_existing_sparse_column(MutableColumnPtr& dst, size_t num_rows) override;
    void _fill_path_column(MutableColumnPtr& dst);
    void _process_data_without_sparse_column(MutableColumnPtr& dst, size_t num_rows) override;
};

#include "common/compile_check_end.h"

} // namespace doris::segment_v2

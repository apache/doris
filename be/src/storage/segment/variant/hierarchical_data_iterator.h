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
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_variant.h"
#include "core/column/subcolumn_tree.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/types.h"
#include "exprs/function/function_helpers.h"
#include "io/io_common.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/stream_reader.h"
#include "storage/segment/variant/v2/variant_column_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

class ColumnReaderCache;

namespace variant_v2 {
class VariantAssembler;
}

struct PathWithColumnAndType {
    PathInData path;
    ColumnPtr column;
    DataTypePtr type;
};

using PathsWithColumnAndType = std::vector<PathWithColumnAndType>;

// Reader for hierarchical data for variant, merge with root(sparse encoded columns)
class HierarchicalDataIterator : public ColumnIterator {
public:
    enum class ReadType {
        SUBCOLUMNS_AND_SPARSE = 0,
        DOC_VALUE_COLUMN = 1,
    };
    static Status create(ColumnIteratorUPtr* reader, int32_t col_uid, PathInData path,
                         const SubcolumnColumnMetaInfo::Node* target_node,
                         std::unique_ptr<SubstreamIterator>&& sparse_reader,
                         std::unique_ptr<SubstreamIterator>&& root_column_reader,
                         ColumnReaderCache* column_reader_cache, OlapReaderStatistics* stats,
                         ReadType read_type, bool use_variant_v2 = false,
                         const io::IOContext* io_ctx = nullptr);

    ~HierarchicalDataIterator() override;

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_ordinal(ordinal_t ord) override;

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override;

    Status add_stream(int32_t col_uid, const SubcolumnColumnMetaInfo::Node* node,
                      ColumnReaderCache* column_reader_cache, OlapReaderStatistics* stats,
                      const io::IOContext* io_ctx = nullptr);

    Status init_prefetcher(const SegmentPrefetchParams& params) override;
    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override;

private:
    SubstreamReaderTree _substream_reader;
    std::unique_ptr<SubstreamIterator> _root_reader;
    std::unique_ptr<SubstreamIterator> _binary_column_reader;
    size_t _rows_read = 0;
    PathInData _path;
    OlapReaderStatistics* _stats = nullptr;
    ReadType _read_type = ReadType::SUBCOLUMNS_AND_SPARSE;
    std::unique_ptr<variant_v2::VariantAssembler> _variant_v2_assembler;
    HierarchicalDataIterator(const PathInData& path, ReadType read_type);

    template <typename NodeFunction>
    Status tranverse(NodeFunction&& node_func) {
        for (auto& entry : _substream_reader) {
            RETURN_IF_ERROR(node_func(*entry));
        }
        return Status::OK();
    }

    Status _process_sub_columns(ColumnVariant& container_variant,
                                const PathsWithColumnAndType& non_nested_subcolumns);

    Status _process_nested_columns(
            ColumnVariant& container_variant,
            const std::map<PathInData, PathsWithColumnAndType>& nested_subcolumns, size_t nrows);

    Status _process_binary_column(ColumnVariant& container_variant, size_t nrows);

    // 1. add root column
    // 2. collect path for subcolumns and nested subcolumns
    // 3. init container with subcolumns
    // 4. init container with nested subcolumns
    // 5. init container with sparse column
    Status _init_container(MutableColumnPtr& container, size_t nrows, int max_subcolumns_count,
                           bool enable_doc_mode);

    // clear all subcolumns's column data for next batch read
    // set null map for nullable column
    Status _init_null_map_and_clear_columns(MutableColumnPtr& container, MutableColumnPtr& dst,
                                            size_t nrows);

    Status _assemble_variant_v2(MutableColumnPtr& dst, size_t nrows, bool* has_null);
    void _clear_read_columns();

    // process read
    template <typename ReadFunction>
    Status process_read(ReadFunction&& read_func, MutableColumnPtr& dst, size_t requested_rows,
                        bool allow_short_read, size_t* actual_rows, bool* has_null) {
        const bool use_variant_v2 = _variant_v2_assembler != nullptr;
        if (use_variant_v2) {
            if (variant_v2::try_get_variant_v2_destination(*dst) == nullptr) {
                return Status::InvalidArgument(
                        "Variant V2 reader requires a ColumnVariantV2 destination");
            }
        } else {
            IColumn* values = dst.get();
            if (auto* nullable = check_and_get_column<ColumnNullable>(values)) {
                values = &nullable->get_nested_column();
            }
            if (check_and_get_column<ColumnVariant>(values) == nullptr) {
                return Status::InvalidArgument(
                        "Variant V1 reader requires a ColumnVariant destination");
            }
        }

        dst = IColumn::mutate(std::move(dst));
        // Read all sub columns, and merge with root column.
        ColumnNullable* nullable_column = nullptr;
        if (!use_variant_v2 && is_column_nullable(*dst)) {
            nullable_column = assert_cast<ColumnNullable*>(dst.get());
        }
        ColumnVariant* variant = nullptr;
        if (!use_variant_v2) {
            variant = nullable_column == nullptr
                              ? &assert_cast<ColumnVariant&>(*dst)
                              : &assert_cast<ColumnVariant&>(nullable_column->get_nested_column());
        }

        size_t observed_rows = 0;
        bool has_observed_rows = false;
        auto read_stream = [&](SubstreamIterator& reader, const PathInData& path,
                               const DataTypePtr& type) -> Status {
            bool ignored_physical_has_null = false;
            RETURN_IF_ERROR(read_func(reader, path, type, &ignored_physical_has_null));
            const size_t stream_rows = reader.column->size();
            if (stream_rows > requested_rows ||
                (!allow_short_read && stream_rows != requested_rows)) {
                return Status::Corruption("Variant stream {} returned {} rows, expected {}",
                                          path.get_path(), stream_rows, requested_rows);
            }
            if (has_observed_rows && stream_rows != observed_rows) {
                return Status::Corruption(
                        "Variant stream {} returned {} rows, previous streams returned {}",
                        path.get_path(), stream_rows, observed_rows);
            }
            observed_rows = stream_rows;
            has_observed_rows = true;
            reader.rows_read += stream_rows;
            return Status::OK();
        };

        // Read root first if it is not read before.
        if (_root_reader) {
            RETURN_IF_ERROR(read_stream(*_root_reader, {}, _root_reader->type));
        }

        // Read container columns.
        RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
            RETURN_IF_ERROR(read_stream(node.data, node.path, node.data.type));
            return Status::OK();
        }));

        // Read sparse column.
        if (_binary_column_reader) {
            SCOPED_RAW_TIMER(&_stats->variant_scan_sparse_column_timer_ns);
            int64_t curr_size = _binary_column_reader->column->byte_size();
            RETURN_IF_ERROR(read_stream(*_binary_column_reader, {}, nullptr));
            _stats->variant_scan_sparse_column_bytes +=
                    _binary_column_reader->column->byte_size() - curr_size;
        }
        if (!has_observed_rows) {
            return Status::InternalError("Variant hierarchical reader has no physical streams");
        }

        *actual_rows = observed_rows;
        const size_t nrows = observed_rows;

        if (use_variant_v2) {
            const Status assemble_status = _assemble_variant_v2(dst, nrows, has_null);
            _clear_read_columns();
            RETURN_IF_ERROR(assemble_status);
            _rows_read += nrows;
            return Status::OK();
        }

        MutableColumnPtr container;
        RETURN_IF_ERROR(_init_container(container, nrows, variant->max_subcolumns_count(),
                                        variant->enable_doc_mode()));
        auto& container_variant = assert_cast<ColumnVariant&>(*container);
        variant->insert_range_from(container_variant, 0, nrows);

        _rows_read += nrows;
        variant->finalize();
        RETURN_IF_ERROR(_init_null_map_and_clear_columns(container, dst, nrows));
        if (has_null != nullptr) {
            *has_null = dst->has_null(dst->size() - nrows, dst->size());
        }
#ifndef NDEBUG
        variant->check_consistency();
#endif

        return Status::OK();
    }
};

} // namespace doris::segment_v2

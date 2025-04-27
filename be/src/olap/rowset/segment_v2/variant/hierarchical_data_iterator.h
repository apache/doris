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

struct PathWithColumnAndType {
    vectorized::PathInData path;
    vectorized::ColumnPtr column;
    vectorized::DataTypePtr type;
};

using PathsWithColumnAndType = std::vector<PathWithColumnAndType>;

// Reader for hierarchical data for variant, merge with root(sparse encoded columns)
class HierarchicalDataIterator : public ColumnIterator {
public:
    // Currently two types of read, merge sparse columns with root columns, or read directly
    enum class ReadType { MERGE_ROOT, READ_DIRECT };

    HierarchicalDataIterator(const vectorized::PathInData& path) : _path(path) {}

    static Status create(ColumnIterator** reader, vectorized::PathInData path,
                         const SubcolumnColumnReaders::Node* target_node,
                         const SubcolumnColumnReaders::Node* root, ReadType read_type,
                         std::unique_ptr<ColumnIterator>&& sparse_reader);

    Status init(const ColumnIteratorOptions& opts) override;

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

    Status _process_sub_columns(vectorized::ColumnVariant& container_variant,
                                const PathsWithColumnAndType& non_nested_subcolumns);

    Status _process_nested_columns(
            vectorized::ColumnVariant& container_variant,
            const std::map<vectorized::PathInData, PathsWithColumnAndType>& nested_subcolumns,
            size_t nrows);

    Status _process_sparse_column(vectorized::ColumnVariant& container_variant, size_t nrows);

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
                                ? assert_cast<ColumnVariant&>(*dst)
                                : assert_cast<ColumnVariant&>(nullable_column->get_nested_column());

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
        auto& container_variant = assert_cast<ColumnVariant&>(*container);
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

} // namespace doris::segment_v2

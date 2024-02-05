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
#include <unordered_map>

#include "io/io_common.h"
#include "olap/field.h"
#include "olap/iterators.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_object.h"
#include "vec/data_types/data_type_string.h"
#include "vec/json/path_in_data.h"

namespace doris {
namespace segment_v2 {

struct StreamReader {
    vectorized::MutableColumnPtr column;
    std::unique_ptr<ColumnIterator> iterator;
    std::shared_ptr<const vectorized::IDataType> type;
    bool inited = false;
    size_t rows_read = 0;
    StreamReader() = default;
    StreamReader(vectorized::MutableColumnPtr&& col, std::unique_ptr<ColumnIterator>&& it,
                 std::shared_ptr<const vectorized::IDataType> t)
            : column(std::move(col)), iterator(std::move(it)), type(t) {}
};

// path -> StreamReader
using SubstreamReaderTree = vectorized::SubcolumnsTree<StreamReader>;

// path -> SubcolumnReader
struct SubcolumnReader {
    std::unique_ptr<ColumnReader> reader;
    std::shared_ptr<const vectorized::IDataType> file_column_type;
};
using SubcolumnColumnReaders = vectorized::SubcolumnsTree<SubcolumnReader>;

// Reader for hierarchical data for variant, merge with root(sparse encoded columns)
class HierarchicalDataReader : public ColumnIterator {
public:
    HierarchicalDataReader(const vectorized::PathInData& path, bool output_as_raw_json = false)
            : _path(path), _output_as_raw_json(output_as_raw_json) {}

    static Status create(std::unique_ptr<ColumnIterator>* reader, vectorized::PathInData path,
                         const SubcolumnColumnReaders::Node* target_node,
                         const SubcolumnColumnReaders::Node* root, bool output_as_raw_json = false);

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override;

    Status add_stream(const SubcolumnColumnReaders::Node* node);

    void set_root(std::unique_ptr<StreamReader>&& root) { _root_reader = std::move(root); }

private:
    SubstreamReaderTree _substream_reader;
    std::unique_ptr<StreamReader> _root_reader;
    size_t _rows_read = 0;
    vectorized::PathInData _path;
    bool _output_as_raw_json = false;

    template <typename NodeFunction>
    Status tranverse(NodeFunction&& node_func) {
        for (auto& entry : _substream_reader) {
            RETURN_IF_ERROR(node_func(*entry));
        }
        return Status::OK();
    }
    // process read
    template <typename ReadFunction>
    Status process_read(ReadFunction&& read_func, vectorized::MutableColumnPtr& dst, size_t nrows) {
        // // Read all sub columns, and merge with root column
        vectorized::ColumnNullable* nullable_column = nullptr;
        if (dst->is_nullable()) {
            nullable_column = assert_cast<vectorized::ColumnNullable*>(dst.get());
        }
        auto& variant = nullable_column == nullptr ? assert_cast<vectorized::ColumnObject&>(*dst)
                                                   : assert_cast<vectorized::ColumnObject&>(
                                                             nullable_column->get_nested_column());

        // read data
        // read root first if it is not read before
        RETURN_IF_ERROR(read_func(*_root_reader, {}, _root_reader->type));

        // read container columns
        RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
            RETURN_IF_ERROR(read_func(node.data, node.path, node.data.type));
            return Status::OK();
        }));

        // build variant as container
        auto container = vectorized::ColumnObject::create(true, false);
        auto& container_variant = assert_cast<vectorized::ColumnObject&>(*container);

        // add root first
        if (_path.get_parts().size() == 1) {
            auto& root_var =
                    _root_reader->column->is_nullable()
                            ? assert_cast<vectorized::ColumnObject&>(
                                      assert_cast<vectorized::ColumnNullable&>(
                                              *_root_reader->column)
                                              .get_nested_column())
                            : assert_cast<vectorized::ColumnObject&>(*_root_reader->column);
            auto column = root_var.get_root();
            auto type = root_var.get_root_type();
            container_variant.add_sub_column({}, std::move(column), type);
        }

        RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
            vectorized::MutableColumnPtr column = node.data.column->get_ptr();
            bool add = container_variant.add_sub_column(
                    node.path.copy_pop_nfront(_path.get_parts().size()), std::move(column),
                    node.data.type);
            if (!add) {
                return Status::InternalError("Duplicated {}, type {}", node.path.get_path(),
                                             node.data.type->get_name());
            }
            return Status::OK();
        }));

        if (_output_as_raw_json) {
            auto col_to = vectorized::ColumnString::create();
            col_to->reserve(nrows * 2);
            vectorized::VectorBufferWriter write_buffer(*col_to.get());
            auto type = std::make_shared<vectorized::DataTypeObject>();
            for (size_t i = 0; i < nrows; ++i) {
                type->to_string(container_variant, i, write_buffer);
                write_buffer.commit();
            }
            CHECK(variant.empty());
            variant.create_root(std::make_shared<vectorized::DataTypeString>(), std::move(col_to));
        } else {
            // TODO select v:b -> v.b / v.b.c but v.d maybe in v
            // copy container variant to dst variant, todo avoid copy
            variant.insert_range_from(container_variant, 0, nrows);
        }

        // variant.set_num_rows(nrows);
        _rows_read += nrows;
        variant.finalize();
#ifndef NDEBUG
        variant.check_consistency();
#endif
        // clear data in nodes
        RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
            node.data.column->clear();
            return Status::OK();
        }));
        container->clear();
        if (_root_reader->column->is_nullable()) {
            // fill nullmap
            DCHECK(dst->is_nullable());
            vectorized::ColumnUInt8& dst_null_map =
                    assert_cast<vectorized::ColumnNullable&>(*dst).get_null_map_column();
            vectorized::ColumnUInt8& src_null_map =
                    assert_cast<vectorized::ColumnNullable&>(*_root_reader->column)
                            .get_null_map_column();
            dst_null_map.insert_range_from(src_null_map, 0, src_null_map.size());
            // clear nullmap and inner data
            src_null_map.clear();
            assert_cast<vectorized::ColumnObject&>(
                    assert_cast<vectorized::ColumnNullable&>(*_root_reader->column)
                            .get_nested_column())
                    .clear_subcolumns_data();
        } else {
            vectorized::ColumnObject& root_column =
                    assert_cast<vectorized::ColumnObject&>(*_root_reader->column);
            root_column.clear_subcolumns_data();
        }
        return Status::OK();
    }
};

// Extract from root column of variant, since root column of variant
// encodes sparse columns that are not materialized
class ExtractReader : public ColumnIterator {
public:
    ExtractReader(const TabletColumn& col, std::unique_ptr<StreamReader>&& root_reader)
            : _col(col), _root_reader(std::move(root_reader)) {}

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override;

private:
    Status extract_to(vectorized::MutableColumnPtr& dst, size_t nrows);

    TabletColumn _col;
    // may shared among different column iterators
    std::unique_ptr<StreamReader> _root_reader;
};

} // namespace segment_v2
} // namespace doris

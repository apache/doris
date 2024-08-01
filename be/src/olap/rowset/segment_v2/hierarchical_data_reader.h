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
#include "olap/rowset/segment_v2/stream_reader.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/assert_cast.h"
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
    HierarchicalDataReader(const vectorized::PathInData& path) : _path(path) {}

    static Status create(std::unique_ptr<ColumnIterator>* reader, vectorized::PathInData path,
                         const SubcolumnColumnReaders::Node* target_node,
                         const SubcolumnColumnReaders::Node* root);

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
        RETURN_IF_ERROR(read_func(*_root_reader, {}, _root_reader->type));

        // read container columns
        RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
            RETURN_IF_ERROR(read_func(node.data, node.path, node.data.type));
            return Status::OK();
        }));

        // build variant as container
        auto container = ColumnObject::create(true, false);
        auto& container_variant = assert_cast<ColumnObject&>(*container);

        // add root first
        if (_path.get_parts().size() == 1) {
            auto& root_var = _root_reader->column->is_nullable()
                                     ? assert_cast<ColumnObject&>(
                                               assert_cast<ColumnNullable&>(*_root_reader->column)
                                                       .get_nested_column())
                                     : assert_cast<ColumnObject&>(*_root_reader->column);
            auto column = root_var.get_root();
            auto type = root_var.get_root_type();
            container_variant.add_sub_column({}, std::move(column), type);
        }
        bool nested = false;
        RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
            MutableColumnPtr column = node.data.column->get_ptr();
            PathInData real_path = node.path.copy_pop_nfront(_path.get_parts().size());
            bool add =
                    container_variant.add_sub_column(real_path, std::move(column), node.data.type);
            if (!add) {
                return Status::InternalError("Duplicated {}, type {}", node.path.get_path(),
                                             node.data.type->get_name());
            }
            if (node.parent->is_nested()) {
                nested = true;
            }
            return Status::OK();
        }));

        if (nested) {
            container_variant.finalize_if_not();
            MutableColumnPtr nested_object = ColumnObject::create(true, false);
            MutableColumnPtr offset =
                    check_and_get_column<ColumnArray>(
                            *remove_nullable(container_variant.get_subcolumns()
                                                     .get_leaves()[0]
                                                     ->data.get_finalized_column_ptr()))
                            ->get_offsets_ptr()
                            ->assume_mutable();
            auto* nested_object_ptr = assert_cast<ColumnObject*>(nested_object.get());
            // flatten nested arrays
            for (const auto& entry : container_variant.get_subcolumns()) {
                auto& column = entry->data.get_finalized_column_ptr();
                const auto& type = entry->data.get_least_common_type();
                if (!remove_nullable(column)->is_column_array()) {
                    return Status::InvalidArgument(
                            "Meet none array column when flatten nested array, path {}, type {}",
                            entry->path.get_path(), entry->data.get_finalized_column().get_name());
                }
                MutableColumnPtr flattend_column =
                        check_and_get_column<ColumnArray>(
                                remove_nullable(entry->data.get_finalized_column_ptr()).get())
                                ->get_data_ptr()
                                ->assume_mutable();
                DataTypePtr flattend_type =
                        check_and_get_data_type<DataTypeArray>(remove_nullable(type).get())
                                ->get_nested_type();
                nested_object_ptr->add_sub_column(entry->path, std::move(flattend_column),
                                                  std::move(flattend_type));
            }
            nested_object = make_nullable(nested_object->get_ptr())->assume_mutable();
            auto array =
                    make_nullable(ColumnArray::create(std::move(nested_object), std::move(offset)));
            container_variant.clear();
            container_variant.create_root(ColumnObject::NESTED_TYPE, array->assume_mutable());
        }

        // TODO select v:b -> v.b / v.b.c but v.d maybe in v
        // copy container variant to dst variant, todo avoid copy
        variant.insert_range_from(container_variant, 0, nrows);

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
            ColumnUInt8& dst_null_map = assert_cast<ColumnNullable&>(*dst).get_null_map_column();
            ColumnUInt8& src_null_map =
                    assert_cast<ColumnNullable&>(*_root_reader->column).get_null_map_column();
            dst_null_map.insert_range_from(src_null_map, 0, src_null_map.size());
            // clear nullmap and inner data
            src_null_map.clear();
            assert_cast<ColumnObject&>(
                    assert_cast<ColumnNullable&>(*_root_reader->column).get_nested_column())
                    .clear_subcolumns_data();
        } else {
            ColumnObject& root_column = assert_cast<ColumnObject&>(*_root_reader->column);
            root_column.clear_subcolumns_data();
        }
        return Status::OK();
    }
};

// Extract from root column of variant, since root column of variant
// encodes sparse columns that are not materialized
class ExtractReader : public ColumnIterator {
public:
    ExtractReader(const TabletColumn& col, std::unique_ptr<StreamReader>&& root_reader,
                  vectorized::DataTypePtr target_type_hint)
            : _col(col),
              _root_reader(std::move(root_reader)),
              _target_type_hint(target_type_hint) {}

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
    vectorized::DataTypePtr _target_type_hint;
};

} // namespace doris::segment_v2

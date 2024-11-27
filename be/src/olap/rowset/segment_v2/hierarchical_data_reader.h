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
    enum class ReadType { MERGE_SPARSE, READ_DIRECT };

    HierarchicalDataReader(const vectorized::PathInData& path) : _path(path) {}

    static Status create(std::unique_ptr<ColumnIterator>* reader, vectorized::PathInData path,
                         const SubcolumnColumnReaders::Node* target_node,
                         const SubcolumnColumnReaders::Node* root, ReadType read_type);

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
        if (_root_reader) {
            RETURN_IF_ERROR(read_func(*_root_reader, {}, _root_reader->type));
        }

        // read container columns
        RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
            RETURN_IF_ERROR(read_func(node.data, node.path, node.data.type));
            return Status::OK();
        }));

        // build variant as container
        auto container = ColumnObject::create(true, false);
        auto& container_variant = assert_cast<ColumnObject&>(*container);

        // add root first
        if (_path.get_parts().empty() && _root_reader) {
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
        // parent path -> subcolumns
        std::map<PathInData, PathsWithColumnAndType> nested_subcolumns;
        PathsWithColumnAndType non_nested_subcolumns;
        RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
            MutableColumnPtr column = node.data.column->get_ptr();
            PathInData relative_path = node.path.copy_pop_nfront(_path.get_parts().size());

            if (node.path.has_nested_part()) {
                CHECK_EQ(getTypeName(remove_nullable(node.data.type)->get_type_id()),
                         getTypeName(TypeIndex::Array));
                PathInData parent_path = node.path.get_nested_prefix_path().copy_pop_nfront(
                        _path.get_parts().size());
                nested_subcolumns[parent_path].emplace_back(relative_path, column->get_ptr(),
                                                            node.data.type);
            } else {
                non_nested_subcolumns.emplace_back(relative_path, column->get_ptr(),
                                                   node.data.type);
            }
            return Status::OK();
        }));

        for (auto& entry : non_nested_subcolumns) {
            DCHECK(!entry.path.has_nested_part());
            bool add = container_variant.add_sub_column(entry.path, entry.column->assume_mutable(),
                                                        entry.type);
            if (!add) {
                return Status::InternalError("Duplicated {}, type {}", entry.path.get_path(),
                                             entry.type->get_name());
            }
        }
        // Iterate nested subcolumns and flatten them, the entry contains the nested subcolumns of the same nested parent
        // first we pick the first subcolumn as base array and using it's offset info. Then we flatten all nested subcolumns
        // into a new object column and wrap it with array column using the first element offsets.The wrapped array column
        // will type the type of ColumnObject::NESTED_TYPE, whih is Nullable<ColumnArray<NULLABLE(ColumnObject)>>.
        for (auto& entry : nested_subcolumns) {
            MutableColumnPtr nested_object = ColumnObject::create(true, false);
            const auto* base_array =
                    check_and_get_column<ColumnArray>(remove_nullable(entry.second[0].column));
            MutableColumnPtr offset = base_array->get_offsets_ptr()->assume_mutable();
            auto* nested_object_ptr = assert_cast<ColumnObject*>(nested_object.get());
            // flatten nested arrays
            for (const auto& subcolumn : entry.second) {
                const auto& column = subcolumn.column;
                const auto& type = subcolumn.type;
                if (!remove_nullable(column)->is_column_array()) {
                    return Status::InvalidArgument(
                            "Meet none array column when flatten nested array, path {}, type {}",
                            subcolumn.path.get_path(), subcolumn.type->get_name());
                }
                const auto* target_array =
                        check_and_get_column<ColumnArray>(remove_nullable(subcolumn.column).get());
#ifndef NDEBUG
                if (!base_array->has_equal_offsets(*target_array)) {
                    return Status::InvalidArgument(
                            "Meet none equal offsets array when flatten nested array, path {}, "
                            "type {}",
                            subcolumn.path.get_path(), subcolumn.type->get_name());
                }
#endif
                MutableColumnPtr flattend_column = target_array->get_data_ptr()->assume_mutable();
                DataTypePtr flattend_type =
                        check_and_get_data_type<DataTypeArray>(remove_nullable(type).get())
                                ->get_nested_type();
                // add sub path without parent prefix
                nested_object_ptr->add_sub_column(
                        subcolumn.path.copy_pop_nfront(entry.first.get_parts().size()),
                        std::move(flattend_column), std::move(flattend_type));
            }
            nested_object = make_nullable(nested_object->get_ptr())->assume_mutable();
            auto array =
                    make_nullable(ColumnArray::create(std::move(nested_object), std::move(offset)));
            PathInDataBuilder builder;
            // add parent prefix
            builder.append(entry.first.get_parts(), false);
            PathInData parent_path = builder.build();
            // unset nested parts
            parent_path.unset_nested();
            DCHECK(!parent_path.has_nested_part());
            container_variant.add_sub_column(parent_path, array->assume_mutable(),
                                             ColumnObject::NESTED_TYPE);
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
        if (_root_reader) {
            if (_root_reader->column->is_nullable()) {
                // fill nullmap
                DCHECK(dst->is_nullable());
                ColumnUInt8& dst_null_map =
                        assert_cast<ColumnNullable&>(*dst).get_null_map_column();
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
        } else {
            if (dst->is_nullable()) {
                // No nullable info exist in hirearchical data, fill nullmap with all none null
                ColumnUInt8& dst_null_map =
                        assert_cast<ColumnNullable&>(*dst).get_null_map_column();
                auto fake_nullable_column = ColumnUInt8::create(nrows, 0);
                dst_null_map.insert_range_from(*fake_nullable_column, 0, nrows);
            }
        }

        return Status::OK();
    }
};

// Extract from root column of variant, since root column of variant
// encodes sparse columns that are not materialized
class ExtractReader : public ColumnIterator {
public:
    ExtractReader(const TabletColumn& col, std::unique_ptr<SubstreamIterator>&& root_reader,
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
    std::unique_ptr<SubstreamIterator> _root_reader;
    vectorized::DataTypePtr _target_type_hint;
};

} // namespace doris::segment_v2

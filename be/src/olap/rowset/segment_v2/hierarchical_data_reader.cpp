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

#include "olap/rowset/segment_v2/hierarchical_data_reader.h"

#include <memory>

#include "common/status.h"
#include "io/io_common.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "vec/columns/column.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_nothing.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

Status HierarchicalDataReader::create(ColumnIterator** reader, vectorized::PathInData path,
                                      const SubcolumnColumnReaders::Node* node,
                                      const SubcolumnColumnReaders::Node* root, ReadType read_type,
                                      std::unique_ptr<ColumnIterator>&& sparse_reader) {
    // None leave node need merge with root
    auto* stream_iter = new HierarchicalDataReader(path);
    if (node != nullptr) {
        std::vector<const SubcolumnColumnReaders::Node*> leaves;
        vectorized::PathsInData leaves_paths;
        SubcolumnColumnReaders::get_leaves_of_node(node, leaves, leaves_paths);
        for (size_t i = 0; i < leaves_paths.size(); ++i) {
            if (leaves_paths[i].empty()) {
                // use set_root to share instead
                continue;
            }
            RETURN_IF_ERROR(stream_iter->add_stream(leaves[i]));
        }
        // Make sure the root node is in strem_cache, so that child can merge data with root
        // Eg. {"a" : "b" : {"c" : 1}}, access the `a.b` path and merge with root path so that
        // we could make sure the data could be fully merged, since some column may not be extracted but remains in root
        // like {"a" : "b" : {"e" : 1.1}} in jsonb format
        if (read_type == ReadType::MERGE_ROOT) {
            // ColumnIterator* it;
            // RETURN_IF_ERROR(root->data.reader->new_iterator(&it));
            stream_iter->set_root(std::make_unique<SubstreamIterator>(
                    root->data.file_column_type->create_column(),
                    std::unique_ptr<ColumnIterator>(
                            new FileColumnIterator(root->data.reader.get())),
                    root->data.file_column_type));
        }
    }

    // need read from sparse column
    if (sparse_reader) {
        vectorized::MutableColumnPtr sparse_column =
                vectorized::ColumnObject::create_sparse_column_fn();
        stream_iter->_sparse_column_reader = std::make_unique<SubstreamIterator>(
                std::move(sparse_column), std::move(sparse_reader), nullptr);
    };
    *reader = stream_iter;

    return Status::OK();
}

Status HierarchicalDataReader::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->init(opts));
        node.data.inited = true;
        return Status::OK();
    }));
    if (_root_reader && !_root_reader->inited) {
        RETURN_IF_ERROR(_root_reader->iterator->init(opts));
        _root_reader->inited = true;
    }
    if (_sparse_column_reader && !_sparse_column_reader->inited) {
        RETURN_IF_ERROR(_sparse_column_reader->iterator->init(opts));
        _sparse_column_reader->inited = true;
    }
    return Status::OK();
}

Status HierarchicalDataReader::seek_to_first() {
    throw Exception(Status::FatalError("Not implemented"));
}

Status HierarchicalDataReader::seek_to_ordinal(ordinal_t ord) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->seek_to_ordinal(ord));
        return Status::OK();
    }));
    if (_root_reader) {
        DCHECK(_root_reader->inited);
        RETURN_IF_ERROR(_root_reader->iterator->seek_to_ordinal(ord));
    }
    if (_sparse_column_reader) {
        DCHECK(_sparse_column_reader->inited);
        RETURN_IF_ERROR(_sparse_column_reader->iterator->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status HierarchicalDataReader::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                          bool* has_null) {
    return process_read(
            [&](SubstreamIterator& reader, const vectorized::PathInData& path,
                const vectorized::DataTypePtr& type) {
                CHECK(reader.inited);
                RETURN_IF_ERROR(reader.iterator->next_batch(n, reader.column, has_null));
                VLOG_DEBUG << fmt::format("{} next_batch {} rows, type={}", path.get_path(), *n,
                                          type ? type->get_name() : "null");
                reader.rows_read += *n;
                return Status::OK();
            },
            dst, *n);
}

Status HierarchicalDataReader::read_by_rowids(const rowid_t* rowids, const size_t count,
                                              vectorized::MutableColumnPtr& dst) {
    return process_read(
            [&](SubstreamIterator& reader, const vectorized::PathInData& path,
                const vectorized::DataTypePtr& type) {
                CHECK(reader.inited);
                RETURN_IF_ERROR(reader.iterator->read_by_rowids(rowids, count, reader.column));
                VLOG_DEBUG << fmt::format("{} read_by_rowids {} rows, type={}", path.get_path(),
                                          count, type ? type->get_name() : "null");
                reader.rows_read += count;
                return Status::OK();
            },
            dst, count);
}

Status HierarchicalDataReader::add_stream(const SubcolumnColumnReaders::Node* node) {
    if (_substream_reader.find_leaf(node->path)) {
        VLOG_DEBUG << "Already exist sub column " << node->path.get_path();
        return Status::OK();
    }
    CHECK(node);
    ColumnIterator* it;
    RETURN_IF_ERROR(node->data.reader->new_iterator(&it));
    std::unique_ptr<ColumnIterator> it_ptr;
    it_ptr.reset(it);
    SubstreamIterator reader(node->data.file_column_type->create_column(), std::move(it_ptr),
                             node->data.file_column_type);
    bool added = _substream_reader.add(node->path, std::move(reader));
    if (!added) {
        return Status::InternalError("Failed to add node path {}", node->path.get_path());
    }
    VLOG_DEBUG << fmt::format("Add substream {} for {}", node->path.get_path(), _path.get_path());
    return Status::OK();
}

ordinal_t HierarchicalDataReader::get_current_ordinal() const {
    return (*_substream_reader.begin())->data.iterator->get_current_ordinal();
}

Status HierarchicalDataReader::_process_sub_columns(
        vectorized::ColumnObject& container_variant,
        const vectorized::PathsWithColumnAndType& non_nested_subcolumns) {
    for (const auto& entry : non_nested_subcolumns) {
        DCHECK(!entry.path.has_nested_part());
        bool add = container_variant.add_sub_column(entry.path, entry.column->assume_mutable(),
                                                    entry.type);
        if (!add) {
            return Status::InternalError("Duplicated {}, type {}", entry.path.get_path(),
                                         entry.type->get_name());
        }
    }
    return Status::OK();
}

Status HierarchicalDataReader::_process_nested_columns(
        vectorized::ColumnObject& container_variant,
        const std::map<vectorized::PathInData, vectorized::PathsWithColumnAndType>&
                nested_subcolumns,
        size_t nrows) {
    using namespace vectorized;
    // Iterate nested subcolumns and flatten them, the entry contains the nested subcolumns of the same nested parent
    // first we pick the first subcolumn as base array and using it's offset info. Then we flatten all nested subcolumns
    // into a new object column and wrap it with array column using the first element offsets.The wrapped array column
    // will type the type of ColumnObject::NESTED_TYPE, whih is Nullable<ColumnArray<NULLABLE(ColumnObject)>>.
    for (const auto& entry : nested_subcolumns) {
        const auto* base_array =
                check_and_get_column<ColumnArray>(*remove_nullable(entry.second[0].column));
        MutableColumnPtr nested_object = ColumnObject::create(
                container_variant.max_subcolumns_count(), base_array->get_data().size());
        MutableColumnPtr offset = base_array->get_offsets_ptr()->assume_mutable();
        auto* nested_object_ptr = assert_cast<ColumnObject*>(nested_object.get());
        // flatten nested arrays
        for (const auto& subcolumn : entry.second) {
            const auto& column = subcolumn.column;
            const auto& type = subcolumn.type;
            if (!check_and_get_column<ColumnArray>(remove_nullable(column).get())) {
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
                                         container_variant.NESTED_TYPE);
    }
    return Status::OK();
}

Status HierarchicalDataReader::_init_container(vectorized::MutableColumnPtr& container,
                                               size_t nrows, int32_t max_subcolumns_count) {
    using namespace vectorized;

    // build variant as container
    // add root first
    if (_path.get_parts().empty() && _root_reader) {
        // auto& root_var =
        //         _root_reader->column->is_nullable()
        //                 ? assert_cast<vectorized::ColumnObject&>(
        //                           assert_cast<vectorized::ColumnNullable&>(*_root_reader->column)
        //                                   .get_nested_column())
        //                 : assert_cast<vectorized::ColumnObject&>(*_root_reader->column);
        // auto column = root_var.get_root();
        // auto type = root_var.get_root_type();

        MutableColumnPtr column = _root_reader->column->get_ptr();
        // container_variant.add_sub_column({}, std::move(column), _root_reader->type);
        DCHECK(column->size() == nrows);
        container =
                ColumnObject::create(max_subcolumns_count, _root_reader->type, std::move(column));
    } else {
        auto root_type =
                vectorized::DataTypeFactory::instance().create_data_type(TypeIndex::Nothing, false);
        auto column = vectorized::ColumnNothing::create(nrows);
        container = ColumnObject::create(max_subcolumns_count, root_type, std::move(column));
    }

    auto& container_variant = assert_cast<ColumnObject&>(*container);

    // parent path -> subcolumns
    std::map<PathInData, PathsWithColumnAndType> nested_subcolumns;
    PathsWithColumnAndType non_nested_subcolumns;
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        MutableColumnPtr column = node.data.column->get_ptr();
        PathInData relative_path = node.path.copy_pop_nfront(_path.get_parts().size());
        DCHECK(column->size() == nrows);
        if (node.path.has_nested_part()) {
            CHECK_EQ(getTypeName(remove_nullable(node.data.type)->get_type_id()),
                     getTypeName(TypeIndex::Array));
            PathInData parent_path =
                    node.path.get_nested_prefix_path().copy_pop_nfront(_path.get_parts().size());
            nested_subcolumns[parent_path].emplace_back(relative_path, column->get_ptr(),
                                                        node.data.type);
        } else {
            non_nested_subcolumns.emplace_back(relative_path, column->get_ptr(), node.data.type);
        }
        return Status::OK();
    }));

    RETURN_IF_ERROR(_process_sub_columns(container_variant, non_nested_subcolumns));

    RETURN_IF_ERROR(_process_nested_columns(container_variant, nested_subcolumns, nrows));

    RETURN_IF_ERROR(_process_sparse_column(container_variant, nrows));
    container_variant.set_num_rows(nrows);
    return Status::OK();
}

// Return sub-path by specified prefix.
// For example, for prefix a.b:
// a.b.c.d -> c.d, a.b.c -> c
static std::string_view get_sub_path(const std::string_view& path, const std::string_view& prefix) {
    return path.substr(prefix.size() + 1);
}

Status HierarchicalDataReader::_process_sparse_column(vectorized::ColumnObject& container_variant,
                                                      size_t nrows) {
    using namespace vectorized;
    container_variant.clear_sparse_column();
    if (!_sparse_column_reader) {
        container_variant.get_sparse_column()->assume_mutable()->insert_many_defaults(nrows);
        return Status::OK();
    }
    // process sparse column
    if (_path.get_parts().empty()) {
        // directly use sparse column if access root
        container_variant.set_sparse_column(_sparse_column_reader->column->get_ptr());
    } else {
        const auto& offsets =
                assert_cast<const ColumnMap&>(*_sparse_column_reader->column).get_offsets();
        /// Check if there is no data in shared data in current range.
        if (offsets.back() == offsets[-1]) {
            container_variant.get_sparse_column()->assume_mutable()->insert_many_defaults(nrows);
        } else {
            // Read for variant sparse column
            // Example path: a.b
            // data: a.b.c : int|123
            //       a.b.d : string|"456"
            //       a.e.d : string|"789"
            // then the extracted sparse column will be:
            // c : int|123
            // d : string|"456"
            const auto& sparse_data_map =
                    assert_cast<const ColumnMap&>(*_sparse_column_reader->column);
            const auto& src_sparse_data_offsets = sparse_data_map.get_offsets();
            const auto& src_sparse_data_paths =
                    assert_cast<const ColumnString&>(sparse_data_map.get_keys());
            const auto& src_sparse_data_values =
                    assert_cast<const ColumnString&>(sparse_data_map.get_values());

            auto& sparse_data_offsets =
                    assert_cast<ColumnMap&>(
                            *container_variant.get_sparse_column()->assume_mutable())
                            .get_offsets();
            auto [sparse_data_paths, sparse_data_values] =
                    container_variant.get_sparse_data_paths_and_values();
            StringRef prefix_ref(_path.get_path());
            std::string_view path_prefix(prefix_ref.data, prefix_ref.size);
            for (size_t i = 0; i != src_sparse_data_offsets.size(); ++i) {
                size_t start = src_sparse_data_offsets[ssize_t(i) - 1];
                size_t end = src_sparse_data_offsets[ssize_t(i)];
                size_t lower_bound_index =
                        vectorized::ColumnObject::find_path_lower_bound_in_sparse_data(
                                prefix_ref, src_sparse_data_paths, start, end);
                for (; lower_bound_index != end; ++lower_bound_index) {
                    auto path_ref = src_sparse_data_paths.get_data_at(lower_bound_index);
                    std::string_view path(path_ref.data, path_ref.size);
                    if (!path.starts_with(path_prefix)) {
                        break;
                    }
                    // Don't include path that is equal to the prefix.
                    if (path.size() != path_prefix.size()) {
                        auto sub_path = get_sub_path(path, path_prefix);
                        sparse_data_paths->insert_data(sub_path.data(), sub_path.size());
                        sparse_data_values->insert_from(src_sparse_data_values, lower_bound_index);
                    } else {
                        // insert into root column, example:  access v['b'] and b is in sparse column
                        // data example:
                        // {"b" : 123}
                        // {"b" : {"c" : 456}}
                        // b maybe in sparse column, and b.c is in subolumn, put `b` into root column to distinguish
                        // from "" which is empty path and root
                        if (container_variant.is_null_root()) {
                            // root was created with nrows with Nothing type, resize it to fit the size of sparse column
                            container_variant.get_root()->resize(sparse_data_offsets.size());
                            // bool added = container_variant.add_sub_column({}, sparse_data_offsets.size());
                            // if (!added) {
                            //     return Status::InternalError("Failed to add subcolumn for sparse column");
                            // }
                        }
                        const auto& data = ColumnObject::deserialize_from_sparse_column(
                                &src_sparse_data_values, lower_bound_index);
                        container_variant.get_subcolumn({})->insert(data.first, data.second);
                    }
                }
                // if root was created, and not seen in sparse data, insert default
                if (!container_variant.is_null_root() &&
                    container_variant.get_subcolumn({})->size() == sparse_data_offsets.size()) {
                    container_variant.get_subcolumn({})->insert_default();
                }
                sparse_data_offsets.push_back(sparse_data_paths->size());
            }
        }
    }
    return Status::OK();
}

Status HierarchicalDataReader::_init_null_map_and_clear_columns(
        vectorized::MutableColumnPtr& container, vectorized::MutableColumnPtr& dst, size_t nrows) {
    using namespace vectorized;
    // clear data in nodes
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        node.data.column->clear();
        return Status::OK();
    }));
    container->clear();
    if (_sparse_column_reader) {
        _sparse_column_reader->column->clear();
    }
    if (_root_reader) {
        if (_root_reader->column->is_nullable()) {
            // fill nullmap
            DCHECK(dst->is_nullable());
            ColumnUInt8& dst_null_map = assert_cast<ColumnNullable&>(*dst).get_null_map_column();
            ColumnUInt8& src_null_map =
                    assert_cast<ColumnNullable&>(*_root_reader->column).get_null_map_column();
            dst_null_map.insert_range_from(src_null_map, 0, src_null_map.size());
            // clear nullmap and inner data
            src_null_map.clear();
        }
        _root_reader->column->clear();
    } else {
        if (dst->is_nullable()) {
            // No nullable info exist in hirearchical data, fill nullmap with all none null
            ColumnUInt8& dst_null_map = assert_cast<ColumnNullable&>(*dst).get_null_map_column();
            auto fake_nullable_column = ColumnUInt8::create(nrows, 0);
            dst_null_map.insert_range_from(*fake_nullable_column, 0, nrows);
        }
    }
    return Status::OK();
}

bool is_compaction_type(ReaderType type) {
    return type == ReaderType::READER_BASE_COMPACTION ||
           type == ReaderType::READER_CUMULATIVE_COMPACTION ||
           type == ReaderType::READER_COLD_DATA_COMPACTION ||
           type == ReaderType::READER_SEGMENT_COMPACTION ||
           type == ReaderType::READER_FULL_COMPACTION;
}

void SparseColumnExtractReader::_fill_path_column(vectorized::MutableColumnPtr& dst) {
    vectorized::ColumnNullable* nullable_column = nullptr;
    if (dst->is_nullable()) {
        nullable_column = assert_cast<vectorized::ColumnNullable*>(dst.get());
    }
    vectorized::ColumnObject& var =
            nullable_column != nullptr
                    ? assert_cast<vectorized::ColumnObject&>(nullable_column->get_nested_column())
                    : assert_cast<vectorized::ColumnObject&>(*dst);
    if (var.is_null_root()) {
        var.add_sub_column({}, dst->size());
    }
    vectorized::NullMap* null_map =
            nullable_column ? &nullable_column->get_null_map_data() : nullptr;
    vectorized::ColumnObject::fill_path_column_from_sparse_data(
            *var.get_subcolumn({}) /*root*/, null_map, StringRef {_path.data(), _path.size()},
            _sparse_column->get_ptr(), 0, _sparse_column->size());
    var.incr_num_rows(_sparse_column->size());
    var.get_sparse_column()->assume_mutable()->insert_many_defaults(_sparse_column->size());
#ifndef NDEBUG
    var.check_consistency();
#endif
    // _sparse_column->clear();
}

Status SparseColumnMergeReader::seek_to_first() {
    RETURN_IF_ERROR(_sparse_column_reader->seek_to_first());
    for (auto& entry : _src_subcolumns_for_sparse) {
        RETURN_IF_ERROR(entry->data.iterator->seek_to_first());
    }
    return Status::OK();
}

Status SparseColumnMergeReader::seek_to_ordinal(ordinal_t ord) {
    RETURN_IF_ERROR(_sparse_column_reader->seek_to_ordinal(ord));
    for (auto& entry : _src_subcolumns_for_sparse) {
        RETURN_IF_ERROR(entry->data.iterator->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status SparseColumnMergeReader::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(_sparse_column_reader->init(opts));
    for (auto& entry : _src_subcolumns_for_sparse) {
        entry->data.serde = entry->data.type->get_serde();
        RETURN_IF_ERROR(entry->data.iterator->init(opts));
        const auto& path = entry->path.get_path();
        _sorted_src_subcolumn_for_sparse.emplace_back(StringRef(path.data(), path.size()), entry);
    }

    // sort src subcolumns by path
    std::sort(
            _sorted_src_subcolumn_for_sparse.begin(), _sorted_src_subcolumn_for_sparse.end(),
            [](const auto& lhsItem, const auto& rhsItem) { return lhsItem.first < rhsItem.first; });
    return Status::OK();
}

void SparseColumnMergeReader::_serialize_nullable_column_to_sparse(
        const SubstreamReaderTree::Node* src_subcolumn,
        vectorized::ColumnString& dst_sparse_column_paths,
        vectorized::ColumnString& dst_sparse_column_values, const StringRef& src_path, size_t row) {
    // every subcolumn is always Nullable
    const auto& nullable_serde =
            assert_cast<vectorized::DataTypeNullableSerDe&>(*src_subcolumn->data.serde);
    const auto& nullable_col =
            assert_cast<const vectorized::ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                    *src_subcolumn->data.column);
    if (nullable_col.is_null_at(row)) {
        return;
    }
    // insert key
    dst_sparse_column_paths.insert_data(src_path.data, src_path.size);
    // insert value
    vectorized::ColumnString::Chars& chars = dst_sparse_column_values.get_chars();
    nullable_serde.get_nested_serde()->write_one_cell_to_binary(nullable_col.get_nested_column(),
                                                                chars, row);
    dst_sparse_column_values.get_offsets().push_back(chars.size());
}

void SparseColumnMergeReader::_process_data_without_sparse_column(vectorized::MutableColumnPtr& dst,
                                                                  size_t num_rows) {
    if (_src_subcolumns_for_sparse.empty()) {
        dst->insert_many_defaults(num_rows);
    } else {
        // merge subcolumns to sparse column
        // Otherwise insert required src dense columns into sparse column.
        auto& map_column = assert_cast<vectorized::ColumnMap&>(*dst);
        auto& sparse_column_keys = assert_cast<vectorized::ColumnString&>(map_column.get_keys());
        auto& sparse_column_values =
                assert_cast<vectorized::ColumnString&>(map_column.get_values());
        auto& sparse_column_offsets = map_column.get_offsets();
        for (size_t i = 0; i != num_rows; ++i) {
            // Paths in sorted_src_subcolumn_for_sparse_column are already sorted.
            for (const auto& entry : _sorted_src_subcolumn_for_sparse) {
                const auto& path = entry.first;
                _serialize_nullable_column_to_sparse(entry.second.get(), sparse_column_keys,
                                                     sparse_column_values, path, i);
            }
            sparse_column_offsets.push_back(sparse_column_keys.size());
        }
    }
}

void SparseColumnMergeReader::_merge_to(vectorized::MutableColumnPtr& dst) {
    auto& column_map = assert_cast<vectorized::ColumnMap&>(*dst);
    auto& dst_sparse_column_paths = assert_cast<vectorized::ColumnString&>(column_map.get_keys());
    auto& dst_sparse_column_values =
            assert_cast<vectorized::ColumnString&>(column_map.get_values());
    auto& dst_sparse_column_offsets = column_map.get_offsets();

    const auto& src_column_map = assert_cast<const vectorized::ColumnMap&>(*_sparse_column);
    const auto& src_sparse_column_paths =
            assert_cast<const vectorized::ColumnString&>(*src_column_map.get_keys_ptr());
    const auto& src_sparse_column_values =
            assert_cast<const vectorized::ColumnString&>(*src_column_map.get_values_ptr());
    const auto& src_serialized_sparse_column_offsets = src_column_map.get_offsets();
    DCHECK_EQ(src_sparse_column_paths.size(), src_sparse_column_values.size());
    // Src object column contains some paths in serialized sparse column in specified range.
    // Iterate over this range and insert all required paths into serialized sparse column or subcolumns.
    for (size_t row = 0; row != _sparse_column->size(); ++row) {
        // Use separate index to iterate over sorted sorted_src_subcolumn_for_sparse_column.
        size_t sorted_src_subcolumn_for_sparse_column_idx = 0;
        size_t sorted_src_subcolumn_for_sparse_column_size = _src_subcolumns_for_sparse.size();

        size_t offset = src_serialized_sparse_column_offsets[row - 1];
        size_t end = src_serialized_sparse_column_offsets[row];
        // Iterator over [path, binary value]
        for (size_t i = offset; i != end; ++i) {
            const StringRef src_sparse_path_string = src_sparse_column_paths.get_data_at(i);
            // Check if we have this path in subcolumns. This path already materialized in subcolumns.
            // So we don't need to insert it into sparse column.
            if (!_src_subcolumn_map.contains(src_sparse_path_string)) {
                // Before inserting this path into sparse column check if we need to
                // insert subcolumns from sorted_src_subcolumn_for_sparse_column before.
                while (sorted_src_subcolumn_for_sparse_column_idx <
                               sorted_src_subcolumn_for_sparse_column_size &&
                       _sorted_src_subcolumn_for_sparse[sorted_src_subcolumn_for_sparse_column_idx]
                                       .first < src_sparse_path_string) {
                    auto& [src_path, src_subcolumn] = _sorted_src_subcolumn_for_sparse
                            [sorted_src_subcolumn_for_sparse_column_idx++];
                    _serialize_nullable_column_to_sparse(src_subcolumn.get(),
                                                         dst_sparse_column_paths,
                                                         dst_sparse_column_values, src_path, row);
                }

                /// Insert path and value from src sparse column to our sparse column.
                dst_sparse_column_paths.insert_from(src_sparse_column_paths, i);
                dst_sparse_column_values.insert_from(src_sparse_column_values, i);
            }
        }

        // Insert remaining dynamic paths from src_dynamic_paths_for_sparse_data.
        while (sorted_src_subcolumn_for_sparse_column_idx <
               sorted_src_subcolumn_for_sparse_column_size) {
            auto& [src_path, src_subcolumn] =
                    _sorted_src_subcolumn_for_sparse[sorted_src_subcolumn_for_sparse_column_idx++];
            _serialize_nullable_column_to_sparse(src_subcolumn.get(), dst_sparse_column_paths,
                                                 dst_sparse_column_values, src_path, row);
        }

        // All the sparse columns in this row are null.
        dst_sparse_column_offsets.push_back(dst_sparse_column_paths.size());
    }
}

} // namespace doris::segment_v2

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

#include "olap/rowset/segment_v2/variant/variant_column_reader.h"

#include <gen_cpp/segment_v2.pb.h>

#include <memory>
#include <set>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/page_handle.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/variant/hierarchical_data_iterator.h"
#include "olap/rowset/segment_v2/variant/sparse_column_extract_iterator.h"
#include "olap/rowset/segment_v2/variant/sparse_column_merge_iterator.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_variant.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

const SubcolumnColumnReaders::Node* VariantColumnReader::get_reader_by_path(
        const vectorized::PathInData& relative_path) const {
    const auto* node = _subcolumn_readers->find_leaf(relative_path);
    if (node) {
        return node;
    }
    // try rebuild path with hierarchical
    // example path(['a.b']) -> path(['a', 'b'])
    auto path = vectorized::PathInData(relative_path.get_path());
    node = _subcolumn_readers->find_leaf(path);
    return node;
}

bool VariantColumnReader::exist_in_sparse_column(
        const vectorized::PathInData& relative_path) const {
    // Check if path exist in sparse column
    bool existed_in_sparse_column =
            !_statistics->sparse_column_non_null_size.empty() &&
            _statistics->sparse_column_non_null_size.find(relative_path.get_path()) !=
                    _statistics->sparse_column_non_null_size.end();
    const std::string& prefix = relative_path.get_path() + ".";
    bool prefix_existed_in_sparse_column =
            !_statistics->sparse_column_non_null_size.empty() &&
            (_statistics->sparse_column_non_null_size.lower_bound(prefix) !=
             _statistics->sparse_column_non_null_size.end()) &&
            _statistics->sparse_column_non_null_size.lower_bound(prefix)->first.starts_with(prefix);
    return existed_in_sparse_column || prefix_existed_in_sparse_column;
}

bool VariantColumnReader::is_exceeded_sparse_column_limit() const {
    return !_statistics->sparse_column_non_null_size.empty() &&
           _statistics->sparse_column_non_null_size.size() >=
                   config::variant_max_sparse_column_statistics_size;
}

int64_t VariantColumnReader::get_metadata_size() const {
    int64_t size = ColumnReader::get_metadata_size();
    if (_statistics) {
        for (const auto& [path, _] : _statistics->subcolumns_non_null_size) {
            size += path.size() + sizeof(size_t);
        }
        for (const auto& [path, _] : _statistics->sparse_column_non_null_size) {
            size += path.size() + sizeof(size_t);
        }
    }

    for (const auto& reader : *_subcolumn_readers) {
        size += reader->data.reader->get_metadata_size();
        size += reader->path.get_path().size();
    }
    return size;
}

Status VariantColumnReader::_create_hierarchical_reader(ColumnIterator** reader,
                                                        vectorized::PathInData path,
                                                        const SubcolumnColumnReaders::Node* node,
                                                        const SubcolumnColumnReaders::Node* root) {
    // Node contains column with children columns or has correspoding sparse columns
    // Create reader with hirachical data.
    std::unique_ptr<ColumnIterator> sparse_iter;
    if (_statistics && !_statistics->sparse_column_non_null_size.empty()) {
        // Sparse column exists or reached sparse size limit, read sparse column
        ColumnIterator* iter;
        RETURN_IF_ERROR(_sparse_column_reader->new_iterator(&iter, nullptr));
        sparse_iter.reset(iter);
    }
    // If read the full path of variant read in MERGE_ROOT, otherwise READ_DIRECT
    HierarchicalDataIterator::ReadType read_type =
            (path == root->path) ? HierarchicalDataIterator::ReadType::MERGE_ROOT
                                 : HierarchicalDataIterator::ReadType::READ_DIRECT;
    RETURN_IF_ERROR(HierarchicalDataIterator::create(reader, path, node, root, read_type,
                                                     std::move(sparse_iter)));
    return Status::OK();
}

Status VariantColumnReader::_create_sparse_merge_reader(ColumnIterator** iterator,
                                                        const StorageReadOptions* opts,
                                                        const TabletColumn& target_col,
                                                        ColumnIterator* inner_iter) {
    // Get subcolumns path set from tablet schema
    const auto& path_set_info = opts->tablet_schema->path_set_info(target_col.parent_unique_id());

    // Build substream reader tree for merging subcolumns into sparse column
    SubstreamReaderTree src_subcolumns_for_sparse;
    for (const auto& subcolumn_reader : *_subcolumn_readers) {
        const auto& path = subcolumn_reader->path.get_path();
        if (path_set_info.sparse_path_set.find(StringRef(path)) ==
            path_set_info.sparse_path_set.end()) {
            // The subcolumn is not a sparse column, skip it
            continue;
        }
        // Create subcolumn iterator
        ColumnIterator* it;
        RETURN_IF_ERROR(subcolumn_reader->data.reader->new_iterator(&it, nullptr));
        std::unique_ptr<ColumnIterator> it_ptr(it);

        // Create substream reader and add to tree
        SubstreamIterator reader(subcolumn_reader->data.file_column_type->create_column(),
                                 std::move(it_ptr), subcolumn_reader->data.file_column_type);
        if (!src_subcolumns_for_sparse.add(subcolumn_reader->path, std::move(reader))) {
            return Status::InternalError("Failed to add node path {}", path);
        }
    }
    VLOG_DEBUG << "subcolumns to merge " << src_subcolumns_for_sparse.size();
    // Create sparse column merge reader
    *iterator = new SparseColumnMergeIterator(path_set_info,
                                              std::unique_ptr<ColumnIterator>(inner_iter),
                                              std::move(src_subcolumns_for_sparse),
                                              const_cast<StorageReadOptions*>(opts), target_col);
    return Status::OK();
}

Status VariantColumnReader::_new_default_iter_with_same_nested(ColumnIterator** iterator,
                                                               const TabletColumn& tablet_column) {
    auto relative_path = tablet_column.path_info_ptr()->copy_pop_front();
    // We find node that represents the same Nested type as path.
    const auto* parent = _subcolumn_readers->find_best_match(relative_path);
    VLOG_DEBUG << "find with path " << tablet_column.path_info_ptr()->get_path() << " parent "
               << (parent ? parent->path.get_path() : "nullptr") << ", type "
               << ", parent is nested " << (parent ? parent->is_nested() : false) << ", "
               << TabletColumn::get_string_by_field_type(tablet_column.type()) << ", relative_path "
               << relative_path.get_path();
    // find it's common parent with nested part
    // why not use parent->path->has_nested_part? because parent may not be a leaf node
    // none leaf node may not contain path info
    // Example:
    // {"payload" : {"commits" : [{"issue" : {"id" : 123, "email" : "a@b"}}]}}
    // nested node path          : payload.commits(NESTED)
    // tablet_column path_info   : payload.commits.issue.id(SCALAR)
    // parent path node          : payload.commits.issue(TUPLE)
    // leaf path_info            : payload.commits.issue.email(SCALAR)
    if (parent && SubcolumnColumnReaders::find_parent(
                          parent, [](const auto& node) { return node.is_nested(); })) {
        /// Find any leaf of Nested subcolumn.
        const auto* leaf = SubcolumnColumnReaders::find_leaf(
                parent, [](const auto& node) { return node.path.has_nested_part(); });
        assert(leaf);
        std::unique_ptr<ColumnIterator> sibling_iter;
        ColumnIterator* sibling_iter_ptr;
        RETURN_IF_ERROR(leaf->data.reader->new_iterator(&sibling_iter_ptr, nullptr));
        sibling_iter.reset(sibling_iter_ptr);
        *iterator = new DefaultNestedColumnIterator(std::move(sibling_iter),
                                                    leaf->data.file_column_type);
    } else {
        *iterator = new DefaultNestedColumnIterator(nullptr, nullptr);
    }
    return Status::OK();
}

Status VariantColumnReader::_new_iterator_with_flat_leaves(ColumnIterator** iterator,
                                                           const TabletColumn& target_col,
                                                           const StorageReadOptions* opts,
                                                           bool exceeded_sparse_column_limit,
                                                           bool existed_in_sparse_column) {
    DCHECK(opts != nullptr);
    auto relative_path = target_col.path_info_ptr()->copy_pop_front();
    // compaction need to read flat leaves nodes data to prevent from amplification
    const auto* node =
            target_col.has_path_info() ? _subcolumn_readers->find_leaf(relative_path) : nullptr;
    if (!node) {
        if (relative_path.get_path() == SPARSE_COLUMN_PATH) {
            // read sparse column and filter extracted columns in subcolumn_path_map
            ColumnIterator* inner_iter;
            RETURN_IF_ERROR(_sparse_column_reader->new_iterator(&inner_iter, nullptr));
            // get subcolumns in sparse path set which will be merged into sparse column
            RETURN_IF_ERROR(_create_sparse_merge_reader(iterator, opts, target_col, inner_iter));
            return Status::OK();
        }

        if (target_col.is_nested_subcolumn()) {
            // using the sibling of the nested column to fill the target nested column
            RETURN_IF_ERROR(_new_default_iter_with_same_nested(iterator, target_col));
            return Status::OK();
        }

        // If the path is typed, it means the path is not a sparse column, so we can't read the sparse column
        // even if the sparse column size is reached limit
        if (existed_in_sparse_column || exceeded_sparse_column_limit) {
            // Sparse column exists or reached sparse size limit, read sparse column
            ColumnIterator* inner_iter;
            RETURN_IF_ERROR(_sparse_column_reader->new_iterator(&inner_iter, nullptr));
            DCHECK(opts);
            *iterator = new SparseColumnExtractIterator(
                    relative_path.get_path(), std::unique_ptr<ColumnIterator>(inner_iter),
                    // need to modify sparse_column_cache, so use const_cast here
                    const_cast<StorageReadOptions*>(opts), target_col);
            return Status::OK();
        }

        VLOG_DEBUG << "new_default_iter: " << target_col.path_info_ptr()->get_path();
        std::unique_ptr<ColumnIterator> it;
        RETURN_IF_ERROR(Segment::new_default_iterator(target_col, &it));
        *iterator = it.release();
        return Status::OK();
    }
    if (relative_path.empty()) {
        // root path, use VariantRootColumnIterator
        *iterator = new VariantRootColumnIterator(new FileColumnIterator(node->data.reader.get()));
        return Status::OK();
    }
    VLOG_DEBUG << "new iterator: " << target_col.path_info_ptr()->get_path();
    RETURN_IF_ERROR(node->data.reader->new_iterator(iterator, nullptr));
    return Status::OK();
}

Status VariantColumnReader::new_iterator(ColumnIterator** iterator, const TabletColumn* target_col,
                                         const StorageReadOptions* opt) {
    // root column use unique id, leaf column use parent_unique_id
    auto relative_path = target_col->path_info_ptr()->copy_pop_front();
    const auto* root = _subcolumn_readers->get_root();
    const auto* node =
            target_col->has_path_info() ? _subcolumn_readers->find_exact(relative_path) : nullptr;

    // try rebuild path with hierarchical
    // example path(['a.b']) -> path(['a', 'b'])
    if (node == nullptr) {
        relative_path = vectorized::PathInData(relative_path.get_path());
        node = _subcolumn_readers->find_exact(relative_path);
    }

    // Check if path exist in sparse column
    bool existed_in_sparse_column =
            !_statistics->sparse_column_non_null_size.empty() &&
            _statistics->sparse_column_non_null_size.find(relative_path.get_path()) !=
                    _statistics->sparse_column_non_null_size.end();

    DBUG_EXECUTE_IF("exist_in_sparse_column_must_be_false", {
        if (existed_in_sparse_column) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "exist_in_sparse_column_must_be_false, relative_path: {}",
                    relative_path.get_path());
        }
    })

    // Otherwise the prefix is not exist and the sparse column size is reached limit
    // which means the path maybe exist in sparse_column
    bool exceeded_sparse_column_limit = !_statistics->sparse_column_non_null_size.empty() &&
                                        _statistics->sparse_column_non_null_size.size() >=
                                                config::variant_max_sparse_column_statistics_size;

    // If the variant column has extracted columns and is a compaction reader, then read flat leaves
    // Otherwise read hierarchical data, since the variant subcolumns are flattened in schema_util::VariantCompactionUtil::get_extended_compaction_schema
    // when config::enable_vertical_compact_variant_subcolumns is true
    auto has_extracted_columns_in_compaction = [](const StorageReadOptions* opts) {
        return opts != nullptr && opts->tablet_schema != nullptr &&
               std::ranges::any_of(
                       opts->tablet_schema->columns(),
                       [](const auto& column) { return column->is_extracted_column(); }) &&
               is_compaction_reader_type(opts->io_ctx.reader_type);
    };
    if (has_extracted_columns_in_compaction(opt)) {
        // original path, compaction with wide schema
        return _new_iterator_with_flat_leaves(
                iterator, *target_col, opt, exceeded_sparse_column_limit, existed_in_sparse_column);
    }

    // Check if path is prefix, example sparse columns path: a.b.c, a.b.e, access prefix: a.b.
    // then we must read the sparse columns
    const std::string& prefix = relative_path.get_path() + ".";
    bool prefix_existed_in_sparse_column =
            !_statistics->sparse_column_non_null_size.empty() &&
            (_statistics->sparse_column_non_null_size.lower_bound(prefix) !=
             _statistics->sparse_column_non_null_size.end()) &&
            _statistics->sparse_column_non_null_size.lower_bound(prefix)->first.starts_with(prefix);
    // if prefix exists in sparse column, read sparse column with hierarchical reader
    if (prefix_existed_in_sparse_column || exceeded_sparse_column_limit) {
        // Example {"b" : {"c":456,"e":7.111}}
        // b.c is sparse column, b.e is subcolumn, so b is both the prefix of sparse column and subcolumn
        return _create_hierarchical_reader(iterator, relative_path, node, root);
    }

    // if path exists in sparse column, read sparse column with extract reader
    if (existed_in_sparse_column && !node) {
        // node should be nullptr, example
        // {"b" : {"c":456}}   b.c in subcolumn
        // {"b" : 123}         b in sparse column
        // Then we should use hierarchical reader to read b
        ColumnIterator* inner_iter;
        RETURN_IF_ERROR(_sparse_column_reader->new_iterator(&inner_iter, nullptr));
        DCHECK(opt);
        // Sparse column exists or reached sparse size limit, read sparse column
        *iterator = new SparseColumnExtractIterator(relative_path.get_path(),
                                                    std::unique_ptr<ColumnIterator>(inner_iter),
                                                    nullptr, *target_col);
        return Status::OK();
    }

    if (node != nullptr) {
        // relative_path means the root node, should always use HierarchicalDataIterator
        if (node->is_leaf_node() && !relative_path.empty()) {
            // Node contains column without any child sub columns and no corresponding sparse columns
            // Direct read extracted columns
            RETURN_IF_ERROR(node->data.reader->new_iterator(iterator, nullptr));
        } else {
            RETURN_IF_ERROR(_create_hierarchical_reader(iterator, relative_path, node, root));
        }
    } else {
        // Sparse column not exists and not reached stats limit, then the target path is not exist, get a default iterator
        std::unique_ptr<ColumnIterator> iter;
        RETURN_IF_ERROR(Segment::new_default_iterator(*target_col, &iter));
        *iterator = iter.release();
    }
    return Status::OK();
}

Status VariantColumnReader::init(const ColumnReaderOptions& opts, const SegmentFooterPB& footer,
                                 uint32_t column_id, uint64_t num_rows,
                                 io::FileReaderSPtr file_reader) {
    // init sub columns
    _subcolumn_readers = std::make_unique<SubcolumnColumnReaders>();
    _statistics = std::make_unique<VariantStatistics>();
    const ColumnMetaPB& self_column_pb = footer.columns(column_id);
    const auto& parent_index = opts.tablet_schema->inverted_indexs(self_column_pb.unique_id());
    for (const ColumnMetaPB& column_pb : footer.columns()) {
        // Find all columns belonging to the current variant column
        // 1. not the variant column
        if (!column_pb.has_column_path_info()) {
            continue;
        }

        // 2. other variant root columns
        if (column_pb.type() == (int)FieldType::OLAP_FIELD_TYPE_VARIANT &&
            column_pb.unique_id() != self_column_pb.unique_id()) {
            continue;
        }

        // 3. other variant's subcolumns
        if (column_pb.type() != (int)FieldType::OLAP_FIELD_TYPE_VARIANT &&
            column_pb.column_path_info().parrent_column_unique_id() != self_column_pb.unique_id()) {
            continue;
        }
        DCHECK(column_pb.has_column_path_info());
        std::unique_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(
                ColumnReader::create(opts, column_pb, footer.num_rows(), file_reader, &reader));
        vectorized::PathInData path;
        path.from_protobuf(column_pb.column_path_info());

        // init sparse column
        if (path.copy_pop_front().get_path() == SPARSE_COLUMN_PATH) {
            DCHECK(column_pb.has_variant_statistics()) << column_pb.DebugString();
            const auto& variant_stats = column_pb.variant_statistics();
            for (const auto& [subpath, size] : variant_stats.sparse_column_non_null_size()) {
                _statistics->sparse_column_non_null_size.emplace(subpath, size);
            }
            RETURN_IF_ERROR(ColumnReader::create(opts, column_pb, footer.num_rows(), file_reader,
                                                 &_sparse_column_reader));
            continue;
        }

        // init subcolumns
        auto relative_path = path.copy_pop_front();
        auto get_data_type_fn = [&]() {
            // root subcolumn is ColumnVariant::MostCommonType which is jsonb
            if (relative_path.empty()) {
                return self_column_pb.is_nullable()
                               ? make_nullable(std::make_unique<
                                               vectorized::ColumnVariant::MostCommonType>())
                               : std::make_unique<vectorized::ColumnVariant::MostCommonType>();
            }
            return vectorized::DataTypeFactory::instance().create_data_type(column_pb);
        };
        // init subcolumns
        if (_subcolumn_readers->get_root() == nullptr) {
            _subcolumn_readers->create_root(SubcolumnReader {nullptr, nullptr});
        }
        if (relative_path.empty()) {
            // root column
            _subcolumn_readers->get_mutable_root()->modify_to_scalar(
                    SubcolumnReader {std::move(reader), get_data_type_fn()});
        } else {
            // check the root is already a leaf node
            if (column_pb.has_none_null_size()) {
                _statistics->subcolumns_non_null_size.emplace(relative_path.get_path(),
                                                              column_pb.none_null_size());
            }
            _subcolumn_readers->add(relative_path,
                                    SubcolumnReader {std::move(reader), get_data_type_fn()});
            TabletSchema::SubColumnInfo sub_column_info;
            // if subcolumn has index, add index to _variant_subcolumns_indexes
            if (vectorized::schema_util::generate_sub_column_info(
                        *opts.tablet_schema, self_column_pb.unique_id(), relative_path.get_path(),
                        &sub_column_info) &&
                !sub_column_info.indexes.empty()) {
                _variant_subcolumns_indexes[path.get_path()] = std::move(sub_column_info.indexes);
            }
            // if parent column has index, add index to _variant_subcolumns_indexes
            else if (!parent_index.empty()) {
                vectorized::schema_util::inherit_index(
                        parent_index, _variant_subcolumns_indexes[path.get_path()], column_pb);
            }
        }
    }

    // init sparse column set in stats
    if (self_column_pb.has_variant_statistics()) {
        _statistics = std::make_unique<VariantStatistics>();
        const auto& variant_stats = self_column_pb.variant_statistics();
        for (const auto& [path, size] : variant_stats.sparse_column_non_null_size()) {
            _statistics->sparse_column_non_null_size.emplace(path, size);
        }
    }
    return Status::OK();
}

std::vector<const TabletIndex*> VariantColumnReader::find_subcolumn_tablet_indexes(
        const std::string& path) {
    auto it = _variant_subcolumns_indexes.find(path);
    std::vector<const TabletIndex*> indexes;
    if (it != _variant_subcolumns_indexes.end()) {
        for (const auto& index : it->second) {
            indexes.push_back(index.get());
        }
    }
    return indexes;
}

void VariantColumnReader::get_subcolumns_types(
        std::unordered_map<vectorized::PathInData, vectorized::DataTypes,
                           vectorized::PathInData::Hash>* subcolumns_types) const {
    for (const auto& subcolumn_reader : *_subcolumn_readers) {
        auto& path_types = (*subcolumns_types)[subcolumn_reader->path];
        path_types.push_back(subcolumn_reader->data.file_column_type);
    }
}

void VariantColumnReader::get_typed_paths(std::unordered_set<std::string>* typed_paths) const {
    for (const auto& entry : *_subcolumn_readers) {
        if (entry->path.get_is_typed()) {
            typed_paths->insert(entry->path.get_path());
        }
    }
}

void VariantColumnReader::get_nested_paths(
        std::unordered_set<vectorized::PathInData, vectorized::PathInData::Hash>* nested_paths)
        const {
    for (const auto& entry : *_subcolumn_readers) {
        if (entry->path.has_nested_part()) {
            nested_paths->insert(entry->path);
        }
    }
}

Status VariantRootColumnIterator::_process_root_column(
        vectorized::MutableColumnPtr& dst, vectorized::MutableColumnPtr& root_column,
        const vectorized::DataTypePtr& most_common_type) {
    auto& obj =
            dst->is_nullable()
                    ? assert_cast<vectorized::ColumnVariant&>(
                              assert_cast<vectorized::ColumnNullable&>(*dst).get_nested_column())
                    : assert_cast<vectorized::ColumnVariant&>(*dst);

    // fill nullmap
    if (root_column->is_nullable() && dst->is_nullable()) {
        vectorized::ColumnUInt8& dst_null_map =
                assert_cast<vectorized::ColumnNullable&>(*dst).get_null_map_column();
        vectorized::ColumnUInt8& src_null_map =
                assert_cast<vectorized::ColumnNullable&>(*root_column).get_null_map_column();
        dst_null_map.insert_range_from(src_null_map, 0, src_null_map.size());
    }

    // add root column to a tmp object column
    auto tmp = vectorized::ColumnVariant::create(0, root_column->size());
    auto& tmp_obj = assert_cast<vectorized::ColumnVariant&>(*tmp);
    tmp_obj.add_sub_column({}, std::move(root_column), most_common_type);
    // tmp_obj.get_sparse_column()->assume_mutable()->insert_many_defaults(root_column->size());

    // merge tmp object column to dst
    obj.insert_range_from(*tmp, 0, tmp_obj.rows());

    // finalize object if needed
    if (!obj.is_finalized()) {
        obj.finalize();
    }

#ifndef NDEBUG
    obj.check_consistency();
#endif

    return Status::OK();
}

Status VariantRootColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                             bool* has_null) {
    // read root column
    auto& obj =
            dst->is_nullable()
                    ? assert_cast<vectorized::ColumnVariant&>(
                              assert_cast<vectorized::ColumnNullable&>(*dst).get_nested_column())
                    : assert_cast<vectorized::ColumnVariant&>(*dst);

    auto most_common_type = obj.get_most_common_type();
    auto root_column = most_common_type->create_column();
    RETURN_IF_ERROR(_inner_iter->next_batch(n, root_column, has_null));

    return _process_root_column(dst, root_column, most_common_type);
}

Status VariantRootColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                 vectorized::MutableColumnPtr& dst) {
    // read root column
    auto& obj =
            dst->is_nullable()
                    ? assert_cast<vectorized::ColumnVariant&>(
                              assert_cast<vectorized::ColumnNullable&>(*dst).get_nested_column())
                    : assert_cast<vectorized::ColumnVariant&>(*dst);

    auto most_common_type = obj.get_most_common_type();
    auto root_column = most_common_type->create_column();
    RETURN_IF_ERROR(_inner_iter->read_by_rowids(rowids, count, root_column));

    return _process_root_column(dst, root_column, most_common_type);
}

static void fill_nested_with_defaults(vectorized::MutableColumnPtr& dst,
                                      vectorized::MutableColumnPtr& sibling_column, size_t nrows) {
    const auto* sibling_array = vectorized::check_and_get_column<vectorized::ColumnArray>(
            remove_nullable(sibling_column->get_ptr()).get());
    const auto* dst_array = vectorized::check_and_get_column<vectorized::ColumnArray>(
            remove_nullable(dst->get_ptr()).get());
    if (!dst_array || !sibling_array) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Expected array column, but met {} and {}", dst->get_name(),
                               sibling_column->get_name());
    }
    auto new_nested =
            dst_array->get_data_ptr()->clone_resized(sibling_array->get_data_ptr()->size());
    auto new_array = make_nullable(vectorized::ColumnArray::create(
            new_nested->assume_mutable(), sibling_array->get_offsets_ptr()->assume_mutable()));
    dst->insert_range_from(*new_array, 0, new_array->size());
#ifndef NDEBUG
    if (!dst_array->has_equal_offsets(*sibling_array)) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Expected same array offsets");
    }
#endif
}

Status DefaultNestedColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
    bool has_null = false;
    return next_batch(n, dst, &has_null);
}

Status DefaultNestedColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                               bool* has_null) {
    if (_sibling_iter) {
        vectorized::MutableColumnPtr sibling_column = _file_column_type->create_column();
        RETURN_IF_ERROR(_sibling_iter->next_batch(n, sibling_column, has_null));
        fill_nested_with_defaults(dst, sibling_column, *n);
    } else {
        dst->insert_many_defaults(*n);
    }
    return Status::OK();
}

Status DefaultNestedColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                   vectorized::MutableColumnPtr& dst) {
    if (_sibling_iter) {
        vectorized::MutableColumnPtr sibling_column = _file_column_type->create_column();
        RETURN_IF_ERROR(_sibling_iter->read_by_rowids(rowids, count, sibling_column));
        fill_nested_with_defaults(dst, sibling_column, count);
    } else {
        dst->insert_many_defaults(count);
    }
    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
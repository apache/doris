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
#include "olap/rowset/segment_v2/column_reader_cache.h"
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

const SubcolumnColumnMetaInfo::Node* VariantColumnReader::get_subcolumn_meta_by_path(
        const vectorized::PathInData& relative_path) const {
    const auto* node = _subcolumns_meta_info->find_leaf(relative_path);
    if (node) {
        return node;
    }
    // try rebuild path with hierarchical
    // example path(['a.b']) -> path(['a', 'b'])
    auto path = vectorized::PathInData(relative_path.get_path());
    node = _subcolumns_meta_info->find_leaf(path);
    return node;
}

// Combine multiple bucket sparse iterators into one logical sparse iterator by row-wise merging.
class CombineBucketsSparseIterator : public ColumnIterator {
public:
    explicit CombineBucketsSparseIterator(std::vector<std::unique_ptr<ColumnIterator>>&& iters)
            : _iters(std::move(iters)) {}

    Status init(const ColumnIteratorOptions& opts) override {
        for (auto& it : _iters) {
            RETURN_IF_ERROR(it->init(opts));
        }
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        for (auto& it : _iters) {
            RETURN_IF_ERROR(it->seek_to_ordinal(ord_idx));
        }
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override {
        // read each bucket into temp maps
        _sparse_data_buckets.clear();
        _sparse_data_buckets.reserve(_iters.size());
        for (auto& it : _iters) {
            vectorized::MutableColumnPtr m = vectorized::ColumnVariant::create_sparse_column_fn();
            RETURN_IF_ERROR(it->next_batch(n, m, has_null));
            _sparse_data_buckets.emplace_back(std::move(m));
        }
        _collect_sparse_data_from_buckets(*dst);
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override {
        _sparse_data_buckets.clear();
        _sparse_data_buckets.reserve(_iters.size());
        for (auto& it : _iters) {
            vectorized::MutableColumnPtr m = vectorized::ColumnVariant::create_sparse_column_fn();
            RETURN_IF_ERROR(it->read_by_rowids(rowids, count, m));
            _sparse_data_buckets.emplace_back(std::move(m));
        }
        _collect_sparse_data_from_buckets(*dst);
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override {
        return _iters.empty() ? 0 : _iters.front()->get_current_ordinal();
    }

private:
    void _collect_sparse_data_from_buckets(vectorized::IColumn& sparse_data_column) {
        using namespace vectorized;

        // get path, value, offset from all buckets
        auto& column_map = assert_cast<ColumnMap&>(sparse_data_column);
        auto& dst_sparse_data_paths = assert_cast<vectorized::ColumnString&>(column_map.get_keys());
        auto& dst_sparse_data_values =
                assert_cast<vectorized::ColumnString&>(column_map.get_values());
        auto& dst_sparse_data_offsets =
                assert_cast<vectorized::ColumnArray::Offsets64&>(column_map.get_offsets());
        std::vector<const ColumnString*> src_sparse_data_paths_buckets(_sparse_data_buckets.size());
        std::vector<const ColumnString*> src_sparse_data_values_buckets(
                _sparse_data_buckets.size());
        std::vector<const ColumnArray::Offsets64*> src_sparse_data_offsets_buckets(
                _sparse_data_buckets.size());
        for (size_t i = 0; i != _sparse_data_buckets.size(); ++i) {
            const auto& src_map =
                    assert_cast<const vectorized::ColumnMap&>(*_sparse_data_buckets[i]);
            src_sparse_data_paths_buckets[i] =
                    assert_cast<const vectorized::ColumnString*>(&src_map.get_keys());
            src_sparse_data_values_buckets[i] =
                    assert_cast<const vectorized::ColumnString*>(&src_map.get_values());
            src_sparse_data_offsets_buckets[i] =
                    assert_cast<const vectorized::ColumnArray::Offsets64*>(&src_map.get_offsets());
        }

        size_t num_rows = _sparse_data_buckets[0]->size();
        for (size_t i = 0; i != num_rows; ++i) {
            // Sparse data contains paths in sorted order in each row.
            // Collect all paths from all buckets in this row and sort them.
            // Save each path bucket and index to be able find corresponding value later.
            std::vector<std::tuple<std::string_view, size_t, size_t>> all_paths;
            for (size_t bucket = 0; bucket != _sparse_data_buckets.size(); ++bucket) {
                size_t offset_start = (*src_sparse_data_offsets_buckets[bucket])[ssize_t(i) - 1];
                size_t offset_end = (*src_sparse_data_offsets_buckets[bucket])[ssize_t(i)];

                // collect all paths.
                for (size_t j = offset_start; j != offset_end; ++j) {
                    auto path =
                            src_sparse_data_paths_buckets[bucket]->get_data_at(j).to_string_view();
                    all_paths.emplace_back(path, bucket, j);
                }
            }

            std::sort(all_paths.begin(), all_paths.end());
            for (const auto& [path, bucket, offset] : all_paths) {
                dst_sparse_data_paths.insert_data(path.data(), path.size());
                dst_sparse_data_values.insert_from(*src_sparse_data_values_buckets[bucket], offset);
            }

            dst_sparse_data_offsets.push_back(dst_sparse_data_paths.size());
        }
    }

    std::vector<std::unique_ptr<ColumnIterator>> _iters;
    std::vector<vectorized::MutableColumnPtr> _sparse_data_buckets;
};

// Implement UnifiedSparseColumnReader helpers declared in header
Status UnifiedSparseColumnReader::new_sparse_iterator(ColumnIteratorUPtr* iter) const {
    if (has_buckets()) {
        std::vector<std::unique_ptr<ColumnIterator>> iters;
        iters.reserve(_buckets.size());
        for (const auto& br : _buckets) {
            if (!br) continue;
            ColumnIteratorUPtr it;
            RETURN_IF_ERROR(br->new_iterator(&it, nullptr));
            iters.emplace_back(std::move(it));
        }
        *iter = std::make_unique<CombineBucketsSparseIterator>(std::move(iters));
        return Status::OK();
    }
    if (_single) {
        return _single->new_iterator(iter, nullptr);
    }
    return Status::NotFound("No sparse readers available");
}

std::pair<std::shared_ptr<ColumnReader>, std::string>
UnifiedSparseColumnReader::select_reader_and_cache_key(const std::string& relative_path) const {
    if (has_buckets()) {
        uint32_t N = static_cast<uint32_t>(_buckets.size());
        uint32_t bucket_index = vectorized::schema_util::variant_sparse_shard_of(
                StringRef {relative_path.data(), relative_path.size()}, N);
        DCHECK(bucket_index < _buckets.size());
        std::string key = std::string(SPARSE_COLUMN_PATH) + ".b" + std::to_string(bucket_index);
        return {_buckets[bucket_index], key};
    }
    return {_single, std::string(SPARSE_COLUMN_PATH)};
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
    bool exceeded_sparse_column_limit = !_statistics->sparse_column_non_null_size.empty() &&
                                        _statistics->sparse_column_non_null_size.size() >=
                                                _variant_sparse_column_statistics_size;
    DBUG_EXECUTE_IF("exceeded_sparse_column_limit_must_be_false", {
        if (exceeded_sparse_column_limit) {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "exceeded_sparse_column_limit_must_be_false, sparse_column_non_null_size: {} : "
                    " _variant_sparse_column_statistics_size: {}",
                    _statistics->sparse_column_non_null_size.size(),
                    _variant_sparse_column_statistics_size);
        }
    })
    return exceeded_sparse_column_limit;
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

    for (const auto& reader : *_subcolumns_meta_info) {
        size += reader->path.get_path().size();
        size += sizeof(SubcolumnMeta);
    }
    return size;
}

Status VariantColumnReader::_create_hierarchical_reader(ColumnIteratorUPtr* reader, int32_t col_uid,
                                                        vectorized::PathInData path,
                                                        const SubcolumnColumnMetaInfo::Node* node,
                                                        const SubcolumnColumnMetaInfo::Node* root,
                                                        ColumnReaderCache* column_reader_cache,
                                                        OlapReaderStatistics* stats) {
    stats->variant_subtree_hierarchical_iter_count++;
    // Node contains column with children columns or has correspoding sparse columns
    // Create reader with hirachical data.
    std::unique_ptr<SubstreamIterator> sparse_iter;
    if (_statistics && !_statistics->sparse_column_non_null_size.empty() &&
        !_sparse_reader.empty()) {
        ColumnIteratorUPtr iter;
        RETURN_IF_ERROR(_sparse_reader.new_sparse_iterator(&iter));
        sparse_iter = std::make_unique<SubstreamIterator>(
                vectorized::ColumnVariant::create_sparse_column_fn(), std::move(iter), nullptr);
    }
    // If read the full path of variant read in MERGE_ROOT, otherwise READ_DIRECT
    HierarchicalDataIterator::ReadType read_type =
            (path == root->path) ? HierarchicalDataIterator::ReadType::MERGE_ROOT
                                 : HierarchicalDataIterator::ReadType::READ_DIRECT;
    // Make sure the root node is in strem_cache, so that child can merge data with root
    // Eg. {"a" : "b" : {"c" : 1}}, access the `a.b` path and merge with root path so that
    // we could make sure the data could be fully merged, since some column may not be extracted but remains in root
    // like {"a" : "b" : {"e" : 1.1}} in jsonb format
    std::unique_ptr<SubstreamIterator> root_column_reader;
    if (read_type == HierarchicalDataIterator::ReadType::MERGE_ROOT) {
        root_column_reader = std::make_unique<SubstreamIterator>(
                root->data.file_column_type->create_column(),
                std::make_unique<FileColumnIterator>(_root_column_reader),
                root->data.file_column_type);
    }
    RETURN_IF_ERROR(HierarchicalDataIterator::create(
            reader, col_uid, path, node, std::move(sparse_iter), std::move(root_column_reader),
            column_reader_cache, stats));
    return Status::OK();
}

Status VariantColumnReader::_create_sparse_merge_reader(ColumnIteratorUPtr* iterator,
                                                        const StorageReadOptions* opts,
                                                        const TabletColumn& target_col,
                                                        SparseColumnCacheSPtr sparse_column_cache,
                                                        ColumnReaderCache* column_reader_cache,
                                                        std::optional<uint32_t> bucket_index) {
    // Get subcolumns path set from tablet schema
    const auto& path_set_info = opts->tablet_schema->path_set_info(target_col.parent_unique_id());

    // Build substream reader tree for merging subcolumns into sparse column
    SubstreamReaderTree src_subcolumns_for_sparse;
    for (const auto& subcolumn_reader : *_subcolumns_meta_info) {
        const auto& path = subcolumn_reader->path.get_path();
        if (path_set_info.sparse_path_set.find(StringRef(path)) ==
            path_set_info.sparse_path_set.end()) {
            // The subcolumn is not a sparse column, skip it
            continue;
        }
        // If bucketized sparse column is requested (per-bucket sparse output column),
        // only collect subcolumns that belong to this bucket to avoid extra IO.
        if (bucket_index.has_value() && _sparse_reader.has_buckets()) {
            uint32_t N = static_cast<uint32_t>(_sparse_reader.num_buckets());
            if (N > 1) {
                uint32_t b = vectorized::schema_util::variant_sparse_shard_of(
                        StringRef {path.data(), path.size()}, N);
                if (b != bucket_index.value()) {
                    continue; // prune subcolumns of other buckets early
                }
            }
        }
        // Create subcolumn iterator
        std::shared_ptr<ColumnReader> column_reader;
        RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
                target_col.parent_unique_id(), subcolumn_reader->path, &column_reader, opts->stats,
                subcolumn_reader.get()));
        ColumnIteratorUPtr it;
        RETURN_IF_ERROR(column_reader->new_iterator(&it, nullptr));
        // Create substream reader and add to tree
        SubstreamIterator reader(subcolumn_reader->data.file_column_type->create_column(),
                                 std::move(it), subcolumn_reader->data.file_column_type);
        if (!src_subcolumns_for_sparse.add(subcolumn_reader->path, std::move(reader))) {
            return Status::InternalError("Failed to add node path {}", path);
        }
    }
    VLOG_DEBUG << "subcolumns to merge " << src_subcolumns_for_sparse.size();
    // Create sparse column merge reader
    *iterator = std::make_unique<SparseColumnMergeIterator>(
            path_set_info, std::move(sparse_column_cache), std::move(src_subcolumns_for_sparse),
            opts);
    return Status::OK();
}

Status VariantColumnReader::_new_default_iter_with_same_nested(
        ColumnIteratorUPtr* iterator, const TabletColumn& tablet_column,
        const StorageReadOptions* opt, ColumnReaderCache* column_reader_cache) {
    auto relative_path = tablet_column.path_info_ptr()->copy_pop_front();
    // We find node that represents the same Nested type as path.
    const auto* parent = _subcolumns_meta_info->find_best_match(relative_path);
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
    if (parent && SubcolumnColumnMetaInfo::find_parent(
                          parent, [](const auto& node) { return node.is_nested(); })) {
        /// Find any leaf of Nested subcolumn.
        const auto* leaf = SubcolumnColumnMetaInfo::find_leaf(
                parent, [](const auto& node) { return node.path.has_nested_part(); });
        assert(leaf);
        std::unique_ptr<ColumnIterator> sibling_iter;
        std::shared_ptr<ColumnReader> column_reader;
        RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
                tablet_column.parent_unique_id(), leaf->path, &column_reader, opt->stats, leaf));
        RETURN_IF_ERROR(column_reader->new_iterator(&sibling_iter, nullptr));
        *iterator = std::make_unique<DefaultNestedColumnIterator>(std::move(sibling_iter),
                                                                  leaf->data.file_column_type);
    } else {
        *iterator = std::make_unique<DefaultNestedColumnIterator>(nullptr, nullptr);
    }
    return Status::OK();
}

Result<SparseColumnCacheSPtr> VariantColumnReader::_get_shared_column_cache(
        PathToSparseColumnCache* sparse_column_cache_ptr, const std::string& path,
        std::shared_ptr<ColumnReader> sparse_column_reader) {
    if (!sparse_column_cache_ptr || !sparse_column_cache_ptr->contains(path)) {
        ColumnIteratorUPtr inner_iter;
        RETURN_IF_ERROR_RESULT(sparse_column_reader->new_iterator(&inner_iter, nullptr));
        vectorized::MutableColumnPtr sparse_column =
                vectorized::ColumnVariant::create_sparse_column_fn();
        auto sparse_column_cache = std::make_shared<SparseColumnCache>(std::move(inner_iter),
                                                                       std::move(sparse_column));
        // if sparse_column_cache_ptr is nullptr, means the sparse column cache is not used
        if (sparse_column_cache_ptr) {
            sparse_column_cache_ptr->emplace(path, sparse_column_cache);
        }
        return sparse_column_cache;
    }
    return sparse_column_cache_ptr->at(path);
}

Status VariantColumnReader::_new_iterator_with_flat_leaves(
        ColumnIteratorUPtr* iterator, const TabletColumn& target_col,
        const StorageReadOptions* opts, bool exceeded_sparse_column_limit,
        bool existed_in_sparse_column, ColumnReaderCache* column_reader_cache,
        PathToSparseColumnCache* sparse_column_cache_ptr) {
    DCHECK(opts != nullptr);
    auto relative_path = target_col.path_info_ptr()->copy_pop_front();
    // compaction need to read flat leaves nodes data to prevent from amplification
    const auto* node =
            target_col.has_path_info() ? _subcolumns_meta_info->find_leaf(relative_path) : nullptr;
    if (!node) {
        // Handle sparse column reads in flat-leaf compaction.
        const std::string rel = relative_path.get_path();
        // Case 1: single sparse column path
        if (rel == SPARSE_COLUMN_PATH && !_sparse_reader.has_buckets() &&
            _sparse_reader.single() != nullptr) {
            // read sparse column and filter extracted columns in subcolumn_path_map
            SparseColumnCacheSPtr sparse_column_cache = DORIS_TRY(_get_shared_column_cache(
                    sparse_column_cache_ptr, SPARSE_COLUMN_PATH, _sparse_reader.single()));
            // get subcolumns in sparse path set which will be merged into sparse column
            RETURN_IF_ERROR(_create_sparse_merge_reader(iterator, opts, target_col,
                                                        sparse_column_cache, column_reader_cache));
            return Status::OK();
        }
        // Case 2: bucketized sparse column path: __DORIS_VARIANT_SPARSE__.b{i}
        if (rel.rfind(std::string(SPARSE_COLUMN_PATH) + ".b", 0) == 0 &&
            _sparse_reader.has_buckets()) {
            // parse bucket index
            uint32_t bucket_index = static_cast<uint32_t>(
                    atoi(rel.substr(std::string(SPARSE_COLUMN_PATH).size() + 2).c_str()));
            const auto& buckets = _sparse_reader.buckets();
            if (bucket_index >= buckets.size() || !buckets[bucket_index]) {
                return Status::NotFound("bucket sparse column reader not found: {}", rel);
            }
            std::string cache_key =
                    std::string(SPARSE_COLUMN_PATH) + ".b" + std::to_string(bucket_index);
            SparseColumnCacheSPtr sparse_column_cache = DORIS_TRY(_get_shared_column_cache(
                    sparse_column_cache_ptr, cache_key, buckets[bucket_index]));
            RETURN_IF_ERROR(_create_sparse_merge_reader(iterator, opts, target_col,
                                                        sparse_column_cache, column_reader_cache,
                                                        bucket_index));
            return Status::OK();
        }

        if (target_col.is_nested_subcolumn()) {
            // using the sibling of the nested column to fill the target nested column
            RETURN_IF_ERROR(_new_default_iter_with_same_nested(iterator, target_col, opts,
                                                               column_reader_cache));
            return Status::OK();
        }

        // If the path is typed, it means the path is not a sparse column, so we can't read the sparse column
        // even if the sparse column size is reached limit
        if (existed_in_sparse_column || exceeded_sparse_column_limit) {
            // Sparse column exists or reached sparse size limit, read sparse column
            auto [reader, cache_key] =
                    _sparse_reader.select_reader_and_cache_key(relative_path.get_path());
            DCHECK(reader != nullptr);
            SparseColumnCacheSPtr sparse_column_cache =
                    DORIS_TRY(_get_shared_column_cache(sparse_column_cache_ptr, cache_key, reader));
            DCHECK(opts);
            *iterator = std::make_unique<SparseColumnExtractIterator>(
                    relative_path.get_path(), std::move(sparse_column_cache), opts);
            return Status::OK();
        }

        VLOG_DEBUG << "new_default_iter: " << target_col.path_info_ptr()->get_path();
        RETURN_IF_ERROR(Segment::new_default_iterator(target_col, iterator));
        return Status::OK();
    }
    if (relative_path.empty()) {
        // root path, use VariantRootColumnIterator
        *iterator = std::make_unique<VariantRootColumnIterator>(
                std::make_unique<FileColumnIterator>(_root_column_reader));
        return Status::OK();
    }
    VLOG_DEBUG << "new iterator: " << target_col.path_info_ptr()->get_path();
    std::shared_ptr<ColumnReader> column_reader;
    RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
            target_col.parent_unique_id(), node->path, &column_reader, opts->stats, node));
    RETURN_IF_ERROR(column_reader->new_iterator(iterator, nullptr));
    return Status::OK();
}

Status VariantColumnReader::new_iterator(ColumnIteratorUPtr* iterator,
                                         const TabletColumn* target_col,
                                         const StorageReadOptions* opt) {
    // return new_iterator(iterator, target_col, opt, nullptr);
    return Status::NotSupported("Not implemented");
}

Status VariantColumnReader::new_iterator(ColumnIteratorUPtr* iterator,
                                         const TabletColumn* target_col,
                                         const StorageReadOptions* opt,
                                         ColumnReaderCache* column_reader_cache,
                                         PathToSparseColumnCache* sparse_column_cache_ptr) {
    int32_t col_uid =
            target_col->unique_id() >= 0 ? target_col->unique_id() : target_col->parent_unique_id();
    // root column use unique id, leaf column use parent_unique_id
    auto relative_path = target_col->path_info_ptr()->copy_pop_front();
    const auto* root = _subcolumns_meta_info->get_root();
    const auto* node = target_col->has_path_info()
                               ? _subcolumns_meta_info->find_exact(relative_path)
                               : nullptr;

    // try rebuild path with hierarchical
    // example path(['a.b']) -> path(['a', 'b'])
    if (node == nullptr) {
        relative_path = vectorized::PathInData(relative_path.get_path());
        node = _subcolumns_meta_info->find_exact(relative_path);
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
    bool exceeded_sparse_column_limit = is_exceeded_sparse_column_limit();

    // If the variant column has extracted columns and is a compaction reader, then read flat leaves
    // Otherwise read hierarchical data, since the variant subcolumns are flattened in schema_util::VariantCompactionUtil::get_extended_compaction_schema
    // when config::enable_vertical_compact_variant_subcolumns is true
    // For checksum reader, we need to read flat leaves to get the correct data if has extracted columns
    auto need_read_flat_leaves = [](const StorageReadOptions* opts) {
        return opts != nullptr && opts->tablet_schema != nullptr &&
               std::ranges::any_of(
                       opts->tablet_schema->columns(),
                       [](const auto& column) { return column->is_extracted_column(); }) &&
               (is_compaction_reader_type(opts->io_ctx.reader_type) ||
                opts->io_ctx.reader_type == ReaderType::READER_CHECKSUM);
    };

    if (need_read_flat_leaves(opt)) {
        // original path, compaction with wide schema
        return _new_iterator_with_flat_leaves(
                iterator, *target_col, opt, exceeded_sparse_column_limit, existed_in_sparse_column,
                column_reader_cache, sparse_column_cache_ptr);
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
        return _create_hierarchical_reader(iterator, col_uid, relative_path, node, root,
                                           column_reader_cache, opt->stats);
    }

    // if path exists in sparse column, read sparse column with extract reader
    if (existed_in_sparse_column && !node) {
        // node should be nullptr, example
        // {"b" : {"c":456}}   b.c in subcolumn
        // {"b" : 123}         b in sparse column
        // Then we should use hierarchical reader to read b
        auto [reader, cache_key] =
                _sparse_reader.select_reader_and_cache_key(relative_path.get_path());
        SparseColumnCacheSPtr sparse_column_cache =
                DORIS_TRY(_get_shared_column_cache(sparse_column_cache_ptr, cache_key, reader));
        DCHECK(opt);
        // Sparse column exists or reached sparse size limit, read sparse column
        *iterator = std::make_unique<SparseColumnExtractIterator>(
                relative_path.get_path(), std::move(sparse_column_cache), opt);
        opt->stats->variant_subtree_sparse_iter_count++;
        return Status::OK();
    }

    if (node != nullptr) {
        // relative_path means the root node, should always use HierarchicalDataIterator
        if (node->is_leaf_node() && !relative_path.empty()) {
            // Node contains column without any child sub columns and no corresponding sparse columns
            // Direct read extracted columns
            const auto* leaf_node = _subcolumns_meta_info->find_leaf(relative_path);
            std::shared_ptr<ColumnReader> leaf_column_reader;
            RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
                    col_uid, leaf_node->path, &leaf_column_reader, opt->stats, leaf_node));
            RETURN_IF_ERROR(leaf_column_reader->new_iterator(iterator, nullptr));
            opt->stats->variant_subtree_leaf_iter_count++;
        } else {
            RETURN_IF_ERROR(_create_hierarchical_reader(iterator, col_uid, relative_path, node,
                                                        root, column_reader_cache, opt->stats));
        }
    } else {
        // Sparse column not exists and not reached stats limit, then the target path is not exist, get a default iterator
        RETURN_IF_ERROR(Segment::new_default_iterator(*target_col, iterator));
        opt->stats->variant_subtree_default_iter_count++;
    }
    return Status::OK();
}

Status VariantColumnReader::init(const ColumnReaderOptions& opts, const SegmentFooterPB& footer,
                                 uint32_t column_id, uint64_t num_rows,
                                 io::FileReaderSPtr file_reader) {
    // init sub columns
    _subcolumns_meta_info = std::make_unique<SubcolumnColumnMetaInfo>();
    _statistics = std::make_unique<VariantStatistics>();
    const ColumnMetaPB& self_column_pb = footer.columns(column_id);
    const auto& parent_index = opts.tablet_schema->inverted_indexs(self_column_pb.unique_id());
    // record variant_sparse_column_statistics_size from parent column
    _variant_sparse_column_statistics_size =
            opts.tablet_schema->column_by_uid(self_column_pb.unique_id())
                    .variant_max_sparse_column_statistics_size();

    // collect bucketized sparse readers for this variant column
    std::map<int, std::shared_ptr<ColumnReader>> tmp_bucket_readers;
    std::map<std::string, int64_t> aggregated_bucket_stats;

    for (int32_t ordinal = 0; ordinal < footer.columns_size(); ++ordinal) {
        const ColumnMetaPB& column_pb = footer.columns(ordinal);
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
        vectorized::PathInData path;
        path.from_protobuf(column_pb.column_path_info());

        // init sparse column readers
        auto relative_sparse = path.copy_pop_front();
        auto rel_str = relative_sparse.get_path();
        if (rel_str == SPARSE_COLUMN_PATH) {
            DCHECK(column_pb.has_variant_statistics()) << column_pb.DebugString();
            const auto& variant_stats = column_pb.variant_statistics();
            for (const auto& [subpath, size] : variant_stats.sparse_column_non_null_size()) {
                _statistics->sparse_column_non_null_size.emplace(subpath, size);
            }
            std::shared_ptr<ColumnReader> single_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, column_pb, footer.num_rows(), file_reader,
                                                 &single_reader));
            _sparse_reader.set_single(std::move(single_reader));
            continue;
        } else {
            // detect bucket: __DORIS_VARIANT_SPARSE__.b{idx}
            std::string bucket_prefix = std::string(SPARSE_COLUMN_PATH) + ".b";
            if (rel_str.rfind(bucket_prefix, 0) == 0) {
                int idx = atoi(rel_str.substr(bucket_prefix.size()).c_str());
                DCHECK(column_pb.has_variant_statistics()) << column_pb.DebugString();
                const auto& variant_stats = column_pb.variant_statistics();
                for (const auto& [subpath, size] : variant_stats.sparse_column_non_null_size()) {
                    aggregated_bucket_stats[subpath] += size;
                }
                std::shared_ptr<ColumnReader> reader;
                RETURN_IF_ERROR(
                        ColumnReader::create(opts, column_pb, num_rows, file_reader, &reader));
                tmp_bucket_readers[idx] = reader;
                continue;
            }
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
        if (_subcolumns_meta_info->get_root() == nullptr) {
            _subcolumns_meta_info->create_root(SubcolumnMeta {});
        }
        if (relative_path.empty()) {
            // root column
            _subcolumns_meta_info->get_mutable_root()->modify_to_scalar(
                    SubcolumnMeta {get_data_type_fn(), ordinal});
            RETURN_IF_ERROR(ColumnReader::create(opts, column_pb, num_rows, file_reader,
                                                 &_root_column_reader));
        } else {
            // check the root is already a leaf node
            if (column_pb.has_none_null_size()) {
                _statistics->subcolumns_non_null_size.emplace(relative_path.get_path(),
                                                              column_pb.none_null_size());
            }
            _subcolumns_meta_info->add(relative_path, SubcolumnMeta {get_data_type_fn(), ordinal});
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

    // finalize bucket readers if any
    if (!tmp_bucket_readers.empty()) {
        for (auto& kv : tmp_bucket_readers) {
            _sparse_reader.add_bucket(kv.first, std::move(kv.second));
        }
        // set aggregated stats across buckets for existence/prefix checks
        for (const auto& [path, size] : aggregated_bucket_stats) {
            _statistics->sparse_column_non_null_size.emplace(path, size);
        }
    } else if (self_column_pb.has_variant_statistics()) {
        // single sparse column mode: use parent meta stats
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
    for (const auto& subcolumn_reader : *_subcolumns_meta_info) {
        auto& path_types = (*subcolumns_types)[subcolumn_reader->path];
        path_types.push_back(subcolumn_reader->data.file_column_type);
    }
}

void VariantColumnReader::get_typed_paths(std::unordered_set<std::string>* typed_paths) const {
    for (const auto& entry : *_subcolumns_meta_info) {
        if (entry->path.get_is_typed()) {
            typed_paths->insert(entry->path.get_path());
        }
    }
}

void VariantColumnReader::get_nested_paths(
        std::unordered_set<vectorized::PathInData, vectorized::PathInData::Hash>* nested_paths)
        const {
    for (const auto& entry : *_subcolumns_meta_info) {
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
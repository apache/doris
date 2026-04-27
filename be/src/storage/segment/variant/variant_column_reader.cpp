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

#include "storage/segment/variant/variant_column_reader.h"

#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <memory>
#include <ranges>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_variant.h"
#include "exec/common/variant_util.h"
#include "io/fs/file_reader.h"
#include "runtime/descriptors.h"
#include "storage/key_coder.h"
#include "storage/segment/column_meta_accessor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/binary_column_extract_iterator.h"
#include "storage/segment/variant/binary_column_reader.h"
#include "storage/segment/variant/hierarchical_data_iterator.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/sparse_column_merge_iterator.h"
#include "storage/segment/variant/variant_doc_snpashot_compact_iterator.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"
#include "util/json/path_in_data.h"
#include "util/string_util.h"

namespace doris::segment_v2 {

namespace {

bool is_compaction_or_checksum_reader(const StorageReadOptions* opts) {
    return opts != nullptr && (ColumnReader::is_compaction_reader_type(opts->io_ctx.reader_type) ||
                               opts->io_ctx.reader_type == ReaderType::READER_CHECKSUM);
}

} // namespace

const SubcolumnColumnMetaInfo::Node* VariantColumnReader::get_subcolumn_meta_by_path(
        const PathInData& relative_path) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    const auto* node = _subcolumns_meta_info->find_leaf(relative_path);
    if (node) {
        return node;
    }
    // try rebuild path with hierarchical
    // example path(['a.b']) -> path(['a', 'b'])
    auto path = PathInData(relative_path.get_path());
    node = _subcolumns_meta_info->find_leaf(path);
    return node;
}

bool VariantColumnReader::exist_in_sparse_column(const PathInData& relative_path) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    // Check if path exist in sparse column
    bool existed_in_sparse_column =
            !_statistics->sparse_column_non_null_size.empty() &&
            _statistics->sparse_column_non_null_size.contains(relative_path.get_path());
    const std::string& prefix = relative_path.get_path() + ".";
    bool prefix_existed_in_sparse_column =
            !_statistics->sparse_column_non_null_size.empty() &&
            (_statistics->sparse_column_non_null_size.lower_bound(prefix) !=
             _statistics->sparse_column_non_null_size.end()) &&
            _statistics->sparse_column_non_null_size.lower_bound(prefix)->first.starts_with(prefix);
    return existed_in_sparse_column || prefix_existed_in_sparse_column;
}

bool VariantColumnReader::is_exceeded_sparse_column_limit() const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    return _is_exceeded_sparse_column_limit_unlocked();
}

bool VariantColumnReader::_is_exceeded_sparse_column_limit_unlocked() const {
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
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
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

Status VariantColumnReader::_create_hierarchical_reader(
        ColumnIteratorUPtr* reader, int32_t col_uid, PathInData path,
        const SubcolumnColumnMetaInfo::Node* node, const SubcolumnColumnMetaInfo::Node* root,
        ColumnReaderCache* column_reader_cache, OlapReaderStatistics* stats,
        HierarchicalDataIterator::ReadType read_type) {
    // make sure external meta is loaded otherwise can't find any meta data for extracted columns
    // TODO(lhy): this will load all external meta if not loaded, and memory will be consumed.
    RETURN_IF_ERROR(load_external_meta_once());

    stats->variant_subtree_hierarchical_iter_count++;
    // After external meta is loaded, protect reads from `_statistics` and
    // `_subcolumns_meta_info` against concurrent writers.
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);

    // Node contains column with children columns or has correspoding sparse columns
    // Create reader with hirachical data.
    std::unique_ptr<SubstreamIterator> sparse_iter;
    ColumnIteratorUPtr iter;
    // if read from subcolumns, but the binary column reader is multiple doc value,
    // use dummy binary column reader to insert default values to binary column.
    if (read_type == HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE &&
        _binary_column_reader->get_type() == BinaryColumnType::MULTIPLE_DOC_VALUE) {
        DummyBinaryColumnReader dummy_binary_column_reader;
        RETURN_IF_ERROR(dummy_binary_column_reader.new_binary_column_iterator(&iter));
    } else {
        RETURN_IF_ERROR(_binary_column_reader->new_binary_column_iterator(&iter));
    }

    sparse_iter = std::make_unique<SubstreamIterator>(ColumnVariant::create_binary_column_fn(),
                                                      std::move(iter), nullptr);
    if (node == nullptr) {
        node = _subcolumns_meta_info->find_exact(path);
    }
    // Make sure the root node is in strem_cache, so that child can merge data with root
    // Eg. {"a" : "b" : {"c" : 1}}, access the `a.b` path and merge with root path so that
    // we could make sure the data could be fully merged, since some column may not be extracted but remains in root
    // like {"a" : "b" : {"e" : 1.1}} in jsonb format
    std::unique_ptr<SubstreamIterator> root_column_reader;
    if (path == root->path) {
        root_column_reader = std::make_unique<SubstreamIterator>(
                root->data.file_column_type->create_column(),
                std::make_unique<FileColumnIterator>(_root_column_reader),
                root->data.file_column_type);
    }
    RETURN_IF_ERROR(HierarchicalDataIterator::create(
            reader, col_uid, path, node, std::move(sparse_iter), std::move(root_column_reader),
            column_reader_cache, stats, read_type));
    return Status::OK();
}

Status VariantColumnReader::_create_sparse_merge_reader(ColumnIteratorUPtr* iterator,
                                                        const StorageReadOptions* opts,
                                                        const TabletColumn& target_col,
                                                        BinaryColumnCacheSPtr sparse_column_cache,
                                                        ColumnReaderCache* column_reader_cache,
                                                        std::optional<uint32_t> bucket_index) {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    // Get subcolumns path set from tablet schema
    const auto& path_set_info = opts->tablet_schema->path_set_info(target_col.parent_unique_id());

    // Build substream reader tree for merging subcolumns into sparse column
    SubstreamReaderTree src_subcolumns_for_sparse;
    for (const auto& subcolumn_reader : *_subcolumns_meta_info) {
        // NOTE: Skip the root node (empty parts). Do NOT skip "empty key" subcolumns where
        // path.get_path() may also be "" but parts are not empty. Otherwise v[''] data will be lost.
        if (subcolumn_reader->path.empty()) {
            continue;
        }
        const auto& path = subcolumn_reader->path.get_path();
        if (path_set_info.sparse_path_set.find(StringRef(path)) ==
            path_set_info.sparse_path_set.end()) {
            // The subcolumn is not a sparse column, skip it
            continue;
        }
        // If bucketized sparse column is requested (per-bucket sparse output column),
        // only collect subcolumns that belong to this bucket to avoid extra IO.
        if (bucket_index.has_value()) {
            CHECK(_binary_column_reader->get_type() == BinaryColumnType::MULTIPLE_SPARSE);
            uint32_t N = static_cast<uint32_t>(_binary_column_reader->num_buckets());
            if (N > 1) {
                uint32_t b = variant_util::variant_binary_shard_of(
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
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
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

Result<BinaryColumnCacheSPtr> VariantColumnReader::_get_binary_column_cache(
        PathToBinaryColumnCache* binary_column_cache_ptr, const std::string& path,
        std::shared_ptr<ColumnReader> binary_column_reader) {
    if (!binary_column_cache_ptr || !binary_column_cache_ptr->contains(path)) {
        ColumnIteratorUPtr inner_iter;
        RETURN_IF_ERROR_RESULT(binary_column_reader->new_iterator(&inner_iter, nullptr));
        MutableColumnPtr binary_column = ColumnVariant::create_binary_column_fn();
        auto binary_column_cache = std::make_shared<BinaryColumnCache>(std::move(inner_iter),
                                                                       std::move(binary_column));
        // if binary_column_cache_ptr is nullptr, means the binary column cache is not used
        if (binary_column_cache_ptr) {
            binary_column_cache_ptr->emplace(path, binary_column_cache);
        }
        return binary_column_cache;
    }
    return binary_column_cache_ptr->at(path);
}

DataTypePtr create_variant_type(const TabletColumn& target_col) {
    return target_col.is_nullable()
                   ? make_nullable(std::make_shared<DataTypeVariant>(
                             target_col.variant_max_subcolumns_count(),
                             target_col.variant_enable_doc_mode()))
                   : std::make_shared<DataTypeVariant>(target_col.variant_max_subcolumns_count(),
                                                       target_col.variant_enable_doc_mode());
}

Status VariantColumnReader::_build_read_plan_flat_leaves(
        ReadPlan* plan, const TabletColumn& target_col, const StorageReadOptions* opts,
        ColumnReaderCache* column_reader_cache, PathToBinaryColumnCache* binary_column_cache_ptr) {
    // make sure external meta is loaded otherwise can't find any meta data for extracted columns
    // TODO(lhy): this will load all external meta if not loaded, and memory will be consumed.
    RETURN_IF_ERROR(load_external_meta_once());

    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);

    DCHECK(opts != nullptr);
    int32_t col_uid =
            target_col.unique_id() >= 0 ? target_col.unique_id() : target_col.parent_unique_id();
    auto relative_path = target_col.path_info_ptr()->copy_pop_front();
    const auto* node =
            target_col.has_path_info() ? _subcolumns_meta_info->find_leaf(relative_path) : nullptr;
    if (!relative_path.empty() && _can_use_nested_group_read_path() &&
        _try_fill_nested_group_plan(plan, target_col, opts, col_uid, relative_path)) {
        return Status::OK();
    }

    // compaction need to read flat leaves nodes data to prevent from amplification
    if (!node) {
        // Handle sparse column reads in flat-leaf compaction.
        const std::string rel = relative_path.get_path();
        // Case 1: single sparse column path
        if (rel == SPARSE_COLUMN_PATH &&
            _binary_column_reader->get_type() == BinaryColumnType::SINGLE_SPARSE) {
            plan->kind = ReadKind::SPARSE_MERGE;
            plan->type = DataTypeFactory::instance().create_data_type(target_col);
            plan->relative_path = relative_path;
            plan->binary_column_reader = _binary_column_reader->select_reader(0);
            plan->binary_cache_key = SPARSE_COLUMN_PATH;
            plan->bucket_index.reset();
            return Status::OK();
        }
        // Case 2: bucketized sparse column path: __DORIS_VARIANT_SPARSE__.b{i}
        if (rel.rfind(std::string(SPARSE_COLUMN_PATH) + ".b", 0) == 0) {
            CHECK(_binary_column_reader->get_type() == BinaryColumnType::MULTIPLE_SPARSE);
            // parse bucket index
            uint32_t bucket_index = static_cast<uint32_t>(
                    atoi(rel.substr(std::string(SPARSE_COLUMN_PATH).size() + 2).c_str()));
            const auto& reader = _binary_column_reader->select_reader(bucket_index);
            if (!reader) {
                return Status::NotFound("bucket sparse column reader not found: {}", rel);
            }
            plan->kind = ReadKind::SPARSE_MERGE;
            plan->type = DataTypeFactory::instance().create_data_type(target_col);
            plan->relative_path = relative_path;
            plan->binary_column_reader = _binary_column_reader->select_reader(bucket_index);
            plan->binary_cache_key =
                    std::string(SPARSE_COLUMN_PATH) + ".b" + std::to_string(bucket_index);
            plan->bucket_index = bucket_index;
            return Status::OK();
        }

        // case 3: doc snapshot column
        if (rel.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) {
            CHECK(_binary_column_reader->get_type() == BinaryColumnType::MULTIPLE_DOC_VALUE);
            size_t bucket = rel.rfind('b');
            uint32_t bucket_value = static_cast<uint32_t>(std::stoul(rel.substr(bucket + 1)));
            plan->kind = ReadKind::DOC_COMPACT;
            plan->type = DataTypeFactory::instance().create_data_type(target_col);
            plan->binary_column_reader = _binary_column_reader->select_reader(bucket_value);
            return Status::OK();
        }

        if (target_col.is_nested_subcolumn()) {
            plan->kind = ReadKind::DEFAULT_NESTED;
            plan->type = DataTypeFactory::instance().create_data_type(target_col);
            plan->relative_path = relative_path;
            return Status::OK();
        }

        // If the path is typed, it means the path is not a sparse column, so we can't read the sparse column
        // even if the sparse column size is reached limit
        bool existed_in_sparse_column =
                _statistics->existed_in_sparse_column(relative_path.get_path());
        bool exceeded_sparse_column_limit = is_exceeded_sparse_column_limit();
        if (existed_in_sparse_column || exceeded_sparse_column_limit) {
            // Sparse column exists or reached sparse size limit, read sparse column
            auto [reader, cache_key] =
                    _binary_column_reader->select_reader_and_cache_key(relative_path.get_path());
            DCHECK(reader != nullptr);
            plan->kind = ReadKind::BINARY_EXTRACT;
            plan->type = create_variant_type(target_col);
            plan->relative_path = relative_path;
            plan->binary_column_reader = std::move(reader);
            plan->binary_cache_key = std::move(cache_key);
            plan->bucket_index.reset();
            return Status::OK();
        }

        VLOG_DEBUG << "new_default_iter: " << target_col.path_info_ptr()->get_path();
        plan->kind = ReadKind::DEFAULT_FILL;
        plan->type = DataTypeFactory::instance().create_data_type(target_col);
        plan->relative_path = relative_path;
        return Status::OK();
    }
    if (relative_path.empty()) {
        // root path, use VariantRootColumnIterator
        plan->kind = ReadKind::ROOT_FLAT;
        plan->type = create_variant_type(target_col);
        plan->relative_path = relative_path;
        plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
        return Status::OK();
    }
    VLOG_DEBUG << "new iterator: " << target_col.path_info_ptr()->get_path();
    std::shared_ptr<ColumnReader> column_reader;
    RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
            target_col.parent_unique_id(), node->path, &column_reader, opts->stats, node));
    plan->kind = ReadKind::LEAF;
    plan->type = column_reader->get_vec_data_type();
    plan->relative_path = relative_path;
    plan->leaf_column_reader = std::move(column_reader);
    return Status::OK();
}

bool VariantColumnReader::has_prefix_path(const PathInData& relative_path) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    return _has_prefix_path_unlocked(relative_path);
}

bool VariantColumnReader::_has_prefix_path_unlocked(const PathInData& relative_path) const {
    if (relative_path.empty()) {
        return true;
    }
    const std::string path = relative_path.get_path();
    const std::string dot_prefix = relative_path.get_path() + ".";

    // 1) exact node exists and has children.
    if (const auto* node = _subcolumns_meta_info->find_exact(relative_path)) {
        if (!node->children.empty()) {
            return true;
        }
    }

    // 2) Check sparse column stats: use lower_bound to test the `p.` prefix range
    // example sparse columns path: a.b.c, a.b.e, access prefix: a.b.
    // then we must read the sparse columns
    if (_statistics->has_prefix_path_in_sparse_column(dot_prefix)) {
        return true;
    }

    // 3) Check external meta store (if available).
    if (_ext_meta_reader && _ext_meta_reader->available()) {
        bool has = false;
        // Pass strict prefix `p.` to avoid false positives like `a.b` matching `a.bc`.
        if (_ext_meta_reader->has_prefix(dot_prefix, &has).ok() && has) {
            return true;
        }
    }

    return false;
}

bool VariantColumnReader::_need_read_flat_leaves(const StorageReadOptions* opts) {
    return opts != nullptr && opts->tablet_schema != nullptr &&
           std::ranges::any_of(opts->tablet_schema->columns(),
                               [](const auto& column) { return column->is_extracted_column(); }) &&
           is_compaction_or_checksum_reader(opts);
}

bool VariantColumnReader::_can_use_nested_group_read_path() const {
    return _nested_group_read_provider != nullptr &&
           _nested_group_read_provider->should_enable_nested_group_read_path();
}

bool VariantColumnReader::_needs_root_nested_group_merge(const PathInData& relative_path) const {
    return relative_path.empty() && _nested_group_read_provider != nullptr &&
           !_nested_group_readers.empty();
}

Status VariantColumnReader::_validate_access_paths_debug(const TabletColumn& target_col,
                                                         const StorageReadOptions* opt,
                                                         int32_t col_uid,
                                                         const PathInData& relative_path) const {
    DBUG_EXECUTE_IF("VariantColumnReader.build_read_plan.access_paths", {
        if (opt != nullptr && opt->io_ctx.reader_type == ReaderType::READER_QUERY) {
            auto split_csv = [](const std::string& s) {
                std::vector<std::string> out;
                out.reserve(8);
                size_t pos = 0;
                while (pos < s.size()) {
                    size_t comma = s.find(',', pos);
                    if (comma == std::string::npos) {
                        comma = s.size();
                    }
                    size_t l = pos;
                    size_t r = comma;
                    while (l < r && s[l] == ' ') {
                        ++l;
                    }
                    while (r > l && s[r - 1] == ' ') {
                        --r;
                    }
                    if (r > l) {
                        out.emplace_back(s.substr(l, r - l));
                    }
                    pos = comma + 1;
                }
                return out;
            };

            const std::string root_name = _tablet_schema->column_by_uid(col_uid).name();
            bool allow_all = false;
            std::unordered_set<std::string> rel_paths;
            auto dump_paths = [&]() -> std::string {
                std::string out;
                bool first = true;
                for (const auto& p : rel_paths) {
                    if (!first) {
                        out += ",";
                    }
                    first = false;
                    out += p;
                }
                return out;
            };

            auto collect = [&](const TColumnAccessPaths& access_paths) {
                for (const auto& access_path : access_paths) {
                    if (access_path.type != TAccessPathType::DATA ||
                        !access_path.__isset.data_access_path) {
                        continue;
                    }
                    const auto& parts = access_path.data_access_path.path;
                    if (parts.empty()) {
                        continue;
                    }
                    size_t start = 0;
                    if (StringCaseEqual()(parts[0], root_name)) {
                        start = 1;
                    }
                    if (start >= parts.size()) {
                        allow_all = true;
                        return;
                    }
                    for (size_t i = start; i < parts.size(); ++i) {
                        if (parts[i] == "*") {
                            allow_all = true;
                            return;
                        }
                    }
                    std::string rel = parts[start];
                    for (size_t i = start + 1; i < parts.size(); ++i) {
                        rel += ".";
                        rel += parts[i];
                    }
                    if (rel.empty()) {
                        allow_all = true;
                        return;
                    }
                    rel_paths.emplace(std::move(rel));
                }
            };

            if (auto it = opt->all_access_paths.find(col_uid); it != opt->all_access_paths.end()) {
                collect(it->second);
            }
            if (auto it = opt->predicate_access_paths.find(col_uid);
                it != opt->predicate_access_paths.end()) {
                collect(it->second);
            }

            auto require = split_csv(dp->param<std::string>("require", ""));
            auto forbid = split_csv(dp->param<std::string>("forbid", ""));
            const bool expect_allow_all = dp->param<bool>("expect_allow_all", false);

            if (expect_allow_all != allow_all) {
                return Status::InternalError(
                        "DebugPoint {} expect_allow_all={} but allow_all={} col_uid={} root={} "
                        "relative_path={} paths={}",
                        DP_NAME, expect_allow_all, allow_all, col_uid, root_name,
                        relative_path.get_path(), dump_paths());
            }

            if (!allow_all) {
                for (const auto& r : require) {
                    if (!r.empty() && !rel_paths.contains(r)) {
                        return Status::InternalError(
                                "DebugPoint {} missing required path {} col_uid={} root={} "
                                "paths={}",
                                DP_NAME, r, col_uid, root_name, dump_paths());
                    }
                }
                for (const auto& f : forbid) {
                    if (!f.empty() && rel_paths.contains(f)) {
                        return Status::InternalError(
                                "DebugPoint {} hit forbidden path {} col_uid={} root={} paths={}",
                                DP_NAME, f, col_uid, root_name, dump_paths());
                    }
                }
            }
        }
    });
    return Status::OK();
}

bool VariantColumnReader::_try_fill_nested_group_plan(ReadPlan* plan,
                                                      const TabletColumn& target_col,
                                                      const StorageReadOptions* opt,
                                                      int32_t col_uid,
                                                      const PathInData& relative_path) const {
    DCHECK(_nested_group_read_provider != nullptr);

    bool is_whole = false;
    DataTypePtr out_type;
    PathInData out_relative_path;
    std::string out_child_path;
    std::string out_pruned_path;
    std::vector<const NestedGroupReader*> out_chain;
    std::optional<NestedGroupPathFilter> out_path_filter;

    if (!_nested_group_read_provider->try_build_read_plan(
                _tablet_schema.get(), _nested_group_readers, target_col, opt, col_uid,
                relative_path, &is_whole, &out_type, &out_relative_path, &out_child_path,
                &out_pruned_path, &out_chain, &out_path_filter)) {
        return false;
    }
    plan->kind = is_whole ? ReadKind::NESTED_GROUP_WHOLE : ReadKind::NESTED_GROUP_CHILD;
    plan->type = std::move(out_type);
    plan->relative_path = std::move(out_relative_path);
    plan->nested_child_path = std::move(out_child_path);
    plan->nested_group_pruned_path = std::move(out_pruned_path);
    plan->nested_group_chain = std::move(out_chain);
    plan->nested_group_path_filter = std::move(out_path_filter);
    return true;
}

bool VariantColumnReader::_try_build_nested_group_plan(ReadPlan* plan,
                                                       const TabletColumn& target_col,
                                                       const StorageReadOptions* opt,
                                                       int32_t col_uid,
                                                       const PathInData& relative_path) const {
    const bool is_compaction_or_checksum = is_compaction_or_checksum_reader(opt);

    // Root path in compaction/checksum must reconstruct full Variant rows for re-write.
    // Query root reads can still use NestedGroup whole read for top-level array shape.
    if (relative_path.empty() && is_compaction_or_checksum) {
        return false;
    }
    if (!_can_use_nested_group_read_path()) {
        return false;
    }

    if (_need_read_flat_leaves(opt)) {
        return false;
    }
    return _try_fill_nested_group_plan(plan, target_col, opt, col_uid, relative_path);
}

Status VariantColumnReader::_try_build_leaf_plan(ReadPlan* plan, int32_t col_uid,
                                                 const PathInData& relative_path,
                                                 const SubcolumnColumnMetaInfo::Node* node,
                                                 ColumnReaderCache* column_reader_cache,
                                                 OlapReaderStatistics* stats) {
    if (node == nullptr) {
        return Status::OK();
    }

    DCHECK(node->is_leaf_node());
    const auto* leaf_node = _subcolumns_meta_info->find_leaf(relative_path);

    std::shared_ptr<ColumnReader> leaf_column_reader;
    RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(
            col_uid, leaf_node->path, &leaf_column_reader, stats, leaf_node));
    plan->kind = ReadKind::LEAF;
    plan->type = leaf_column_reader->get_vec_data_type();
    plan->relative_path = relative_path;
    plan->leaf_column_reader = std::move(leaf_column_reader);
    return Status::OK();
}

Status VariantColumnReader::_try_build_external_leaf_plan(ReadPlan* plan, int32_t col_uid,
                                                          const PathInData& relative_path,
                                                          ColumnReaderCache* column_reader_cache,
                                                          OlapReaderStatistics* stats) {
    if (!_ext_meta_reader || !_ext_meta_reader->available()) {
        return Status::OK();
    }

    std::shared_ptr<ColumnReader> leaf_column_reader;
    Status st = column_reader_cache->get_path_column_reader(col_uid, relative_path,
                                                            &leaf_column_reader, stats, nullptr);
    DCHECK(!_has_prefix_path_unlocked(relative_path));
    if (st.ok()) {
        plan->kind = ReadKind::LEAF;
        plan->type = leaf_column_reader->get_vec_data_type();
        plan->relative_path = relative_path;
        plan->leaf_column_reader = std::move(leaf_column_reader);
        return Status::OK();
    }
    if (!st.is<ErrorCode::NOT_FOUND>()) {
        return st;
    }
    return Status::OK();
}

Status VariantColumnReader::_build_read_plan(ReadPlan* plan, const TabletColumn& target_col,
                                             const StorageReadOptions* opt,
                                             ColumnReaderCache* column_reader_cache,
                                             PathToBinaryColumnCache* binary_column_cache_ptr) {
    // root column use unique id, leaf column use parent_unique_id
    int32_t col_uid =
            target_col.unique_id() >= 0 ? target_col.unique_id() : target_col.parent_unique_id();
    // root column use unique id, leaf column use parent_unique_id
    auto relative_path = target_col.path_info_ptr()->copy_pop_front();

    RETURN_IF_ERROR(_validate_access_paths_debug(target_col, opt, col_uid, relative_path));

    // If the variant column has extracted columns and is a compaction reader, then read flat leaves
    // Otherwise read hierarchical data, since the variant subcolumns are flattened in
    // variant_util::get_compaction_schema. For checksum reader, we need to read flat leaves to
    // get the correct data if has extracted columns.
    // Flat-leaf compaction/checksum mode: delegate to dedicated planner which handles locking
    // and external meta loading internally.
    if (_need_read_flat_leaves(opt)) {
        return _build_read_plan_flat_leaves(plan, target_col, opt, column_reader_cache,
                                            binary_column_cache_ptr);
    }

    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    const auto* root = _subcolumns_meta_info->get_root();
    const auto* node =
            target_col.has_path_info() ? _subcolumns_meta_info->find_exact(relative_path) : nullptr;

    // try rebuild path with hierarchical
    // example path(['a.b']) -> path(['a', 'b'])
    if (node == nullptr) {
        relative_path = PathInData(relative_path.get_path());
        node = _subcolumns_meta_info->find_exact(relative_path);
    }

    // NestedGroup path resolution must happen before doc/sparse/hierarchical fallbacks.
    // This keeps query/compaction behavior consistent for array<object> paths.
    if (_try_build_nested_group_plan(plan, target_col, opt, col_uid, relative_path)) {
        return Status::OK();
    }

    // read root: from doc value column
    if (root->path == relative_path && _statistics->has_doc_value_column_non_null_size()) {
        plan->kind = ReadKind::HIERARCHICAL_DOC;
        plan->type = create_variant_type(target_col);
        plan->relative_path = relative_path;
        plan->root = root;
        plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
        return Status::OK();
    }

    // Check if path exist in sparse column
    bool existed_in_sparse_column = _statistics->existed_in_sparse_column(relative_path.get_path());

    DBUG_EXECUTE_IF("exist_in_sparse_column_must_be_false", {
        if (existed_in_sparse_column) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "exist_in_sparse_column_must_be_false, relative_path: {}",
                    relative_path.get_path());
        }
    })

    // Otherwise the prefix is not exist and the sparse column size is reached limit
    // which means the path maybe exist in sparse_column
    bool exceeded_sparse_column_limit = _is_exceeded_sparse_column_limit_unlocked();

    // Check if path is prefix, example sparse columns path: a.b.c, a.b.e, access prefix: a.b.
    // Or access root path
    if (_has_prefix_path_unlocked(relative_path)) {
        // Example {"b" : {"c":456,"e":7.111}}
        // b.c is sparse column, b.e is subcolumn, so b is both the prefix of sparse column and
        // subcolumn.
        // Doc mode: prefer extracting hierarchy from doc_value column to preserve doc mode
        // invariant (root-only + doc_value). Non-doc mode: read from subcolumns + sparse.
        if (target_col.variant_enable_doc_mode()) {
            plan->kind = ReadKind::HIERARCHICAL_DOC;
        } else {
            plan->kind = ReadKind::HIERARCHICAL;
        }
        plan->type = create_variant_type(target_col);
        plan->relative_path = relative_path;
        plan->node = node;
        plan->root = root;
        plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
        return Status::OK();
    }

    // if path exists in sparse column, read sparse column with extract reader
    if (existed_in_sparse_column && !node) {
        // node should be nullptr, example
        // {"b" : {"c":456}}   b.c in subcolumn
        // {"b" : 123}         b in sparse column
        // Then we should use hierarchical reader to read b
        auto [reader, cache_key] =
                _binary_column_reader->select_reader_and_cache_key(relative_path.get_path());
        DCHECK(reader);
        plan->kind = ReadKind::BINARY_EXTRACT;
        plan->type = create_variant_type(target_col);
        plan->relative_path = relative_path;
        plan->binary_column_reader = std::move(reader);
        plan->binary_cache_key = std::move(cache_key);
        plan->bucket_index.reset();
        return Status::OK();
    }

    RETURN_IF_ERROR(_try_build_leaf_plan(plan, col_uid, relative_path, node, column_reader_cache,
                                         opt->stats));
    if (plan->kind == ReadKind::LEAF) {
        return Status::OK();
    }
    if (node == nullptr) {
        RETURN_IF_ERROR(_try_build_external_leaf_plan(plan, col_uid, relative_path,
                                                      column_reader_cache, opt->stats));
        if (plan->kind == ReadKind::LEAF) {
            return Status::OK();
        }

        const std::string dot_prefix = relative_path.get_path() + ".";
        bool has_prefix_in_doc_column =
                _statistics->has_prefix_path_in_doc_value_column(dot_prefix);
        if (has_prefix_in_doc_column) {
            plan->kind = ReadKind::HIERARCHICAL_DOC;
            plan->type = create_variant_type(target_col);
            plan->relative_path = relative_path;
            plan->root = root;
            plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
            return Status::OK();
        }

        // find if path exists in doc snapshot column
        bool existed_in_doc_column =
                _statistics->existed_in_doc_value_column(relative_path.get_path());
        if (existed_in_doc_column) {
            auto [reader, cache_key] =
                    _binary_column_reader->select_reader_and_cache_key(relative_path.get_path());
            DCHECK(reader);
            plan->kind = ReadKind::BINARY_EXTRACT;
            plan->type = create_variant_type(target_col);
            plan->relative_path = relative_path;
            plan->binary_column_reader = std::move(reader);
            plan->binary_cache_key = std::move(cache_key);
            return Status::OK();
        }

        if (exceeded_sparse_column_limit) {
            // maybe exist prefix path in sparse column
            plan->kind = ReadKind::HIERARCHICAL;
            plan->type = create_variant_type(target_col);
            plan->relative_path = relative_path;
            plan->node = node;
            plan->root = root;
            plan->needs_root_merge = _needs_root_nested_group_merge(relative_path);
            return Status::OK();
        }

        // Sparse column not exists and not reached stats limit, then the target path is not
        // exist, get a default iterator
        plan->kind = ReadKind::DEFAULT_FILL;
        plan->type = DataTypeFactory::instance().create_data_type(target_col);
        plan->relative_path = relative_path;
        return Status::OK();
    }
    return Status::OK();
}

Status VariantColumnReader::_create_iterator_from_plan(
        ColumnIteratorUPtr* iterator, const ReadPlan& plan, const TabletColumn& target_col,
        const StorageReadOptions* opt, ColumnReaderCache* column_reader_cache,
        PathToBinaryColumnCache* binary_column_cache_ptr) {
    switch (plan.kind) {
    case ReadKind::ROOT_FLAT: {
        // ROOT_FLAT reads the persisted root column itself. It does not rebuild root `v` from
        // regular extracted columns such as `v.keep` / `v.owner`; only the optional root-merge
        // wrapper below may fold NestedGroup data back into the root view.
        *iterator = std::make_unique<VariantRootColumnIterator>(
                std::make_unique<FileColumnIterator>(_root_column_reader));
        return _maybe_wrap_root_merge_iterator(iterator, plan, opt);
    }
    case ReadKind::HIERARCHICAL: {
        // HIERARCHICAL reconstructs the requested object from extracted subcolumns plus sparse
        // state. Reading root `v` through this branch may therefore read regular children such as
        // `v.keep` / `v.owner` and merge them into the final variant result.
        int32_t col_uid = target_col.unique_id() >= 0 ? target_col.unique_id()
                                                      : target_col.parent_unique_id();
        RETURN_IF_ERROR(_create_hierarchical_reader(
                iterator, col_uid, plan.relative_path, plan.node, plan.root, column_reader_cache,
                opt->stats, HierarchicalDataIterator::ReadType::SUBCOLUMNS_AND_SPARSE));
        return _maybe_wrap_root_merge_iterator(iterator, plan, opt);
    }
    case ReadKind::LEAF: {
        DCHECK(plan.leaf_column_reader != nullptr);
        RETURN_IF_ERROR(plan.leaf_column_reader->new_iterator(iterator, nullptr));
        if (opt && opt->stats) {
            opt->stats->variant_subtree_leaf_iter_count++;
        }
        return Status::OK();
    }
    case ReadKind::BINARY_EXTRACT: {
        DCHECK(plan.binary_column_reader != nullptr);
        BinaryColumnCacheSPtr binary_column_cache = DORIS_TRY(_get_binary_column_cache(
                binary_column_cache_ptr, plan.binary_cache_key, plan.binary_column_reader));
        *iterator = std::make_unique<BinaryColumnExtractIterator>(
                plan.relative_path.get_path(), std::move(binary_column_cache), opt);
        if (opt && opt->stats) {
            opt->stats->variant_subtree_sparse_iter_count++;
        }
        return Status::OK();
    }
    case ReadKind::SPARSE_MERGE: {
        DCHECK(plan.binary_column_reader != nullptr);
        BinaryColumnCacheSPtr sparse_column_cache = DORIS_TRY(_get_binary_column_cache(
                binary_column_cache_ptr, plan.binary_cache_key, plan.binary_column_reader));
        RETURN_IF_ERROR(_create_sparse_merge_reader(iterator, opt, target_col, sparse_column_cache,
                                                    column_reader_cache, plan.bucket_index));
        return Status::OK();
    }
    case ReadKind::DEFAULT_NESTED: {
        RETURN_IF_ERROR(
                _new_default_iter_with_same_nested(iterator, target_col, opt, column_reader_cache));
        return Status::OK();
    }
    case ReadKind::DEFAULT_FILL: {
        RETURN_IF_ERROR(Segment::new_default_iterator(target_col, iterator));
        if (opt && opt->stats) {
            opt->stats->variant_subtree_default_iter_count++;
        }
        return Status::OK();
    }
    case ReadKind::DOC_COMPACT: {
        DCHECK(plan.binary_column_reader);
        ColumnIteratorUPtr inner_iter;
        RETURN_IF_ERROR(plan.binary_column_reader->new_iterator(&inner_iter, nullptr));
        *iterator = std::make_unique<VariantDocValueCompactIterator>(std::move(inner_iter));
        return Status::OK();
    }
    case ReadKind::HIERARCHICAL_DOC: {
        int32_t col_uid = target_col.unique_id() >= 0 ? target_col.unique_id()
                                                      : target_col.parent_unique_id();
        RETURN_IF_ERROR(_create_hierarchical_reader(
                iterator, col_uid, plan.relative_path, plan.node, plan.root, column_reader_cache,
                opt->stats, HierarchicalDataIterator::ReadType::DOC_VALUE_COLUMN));
        if (opt && opt->stats) {
            opt->stats->variant_doc_value_column_iter_count++;
        }
        return _maybe_wrap_root_merge_iterator(iterator, plan, opt);
    }
    case ReadKind::NESTED_GROUP_WHOLE:
    case ReadKind::NESTED_GROUP_CHILD: {
        // Delegate iterator creation to the read provider.
        DCHECK(!plan.nested_group_chain.empty());
        bool is_whole = (plan.kind == ReadKind::NESTED_GROUP_WHOLE);
        DataTypePtr out_type;
        RETURN_IF_ERROR(_nested_group_read_provider->create_nested_group_iterator(
                is_whole, plan.nested_group_chain, plan.nested_child_path,
                plan.nested_group_pruned_path, plan.nested_group_path_filter, iterator, &out_type));

        DCHECK(plan.type->equals(*make_nullable(out_type)))
                << "Type mismatch in NESTED_GROUP: plan.type=" << plan.type->get_name()
                << ", iterator_type=" << make_nullable(out_type)->get_name();

        if (!is_whole && opt && opt->stats) {
            opt->stats->variant_subtree_leaf_iter_count++;
        }
        return Status::OK();
    }
    default:
        return Status::InternalError("unknown variant read kind");
    }
}

Status VariantColumnReader::_maybe_wrap_root_merge_iterator(ColumnIteratorUPtr* iterator,
                                                            const ReadPlan& plan,
                                                            const StorageReadOptions* opt) {
    if (!plan.needs_root_merge) {
        return Status::OK();
    }

    // The planner may reach this point through ROOT_FLAT, HIERARCHICAL or HIERARCHICAL_DOC.
    // Wrapping once here prevents those branches from duplicating the same root merge logic.
    ColumnIteratorUPtr merged_iterator;
    RETURN_IF_ERROR(_nested_group_read_provider->create_root_merge_iterator(
            std::move(*iterator), _nested_group_readers, opt, &merged_iterator));
    *iterator = std::move(merged_iterator);
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
                                         PathToBinaryColumnCache* binary_column_cache_ptr) {
    ReadPlan plan;
    RETURN_IF_ERROR(_build_read_plan(&plan, *target_col, opt, column_reader_cache,
                                     binary_column_cache_ptr));
    // Caller of this overload does not need the storage type; only iterator is used.
    return _create_iterator_from_plan(iterator, plan, *target_col, opt, column_reader_cache,
                                      binary_column_cache_ptr);
}

Status VariantColumnReader::init(const ColumnReaderOptions& opts, ColumnMetaAccessor* accessor,
                                 const std::shared_ptr<SegmentFooterPB>& footer, int32_t column_uid,
                                 uint64_t num_rows, io::FileReaderSPtr file_reader) {
    _nested_group_read_provider = create_nested_group_read_provider();

    // init sub columns
    _subcolumns_meta_info = std::make_unique<SubcolumnColumnMetaInfo>();
    _statistics = std::make_unique<VariantStatistics>();

    // Prefer external root ColumnMetaPB via ColumnMetaAccessor when available.
    ColumnMetaPB self_column_pb;
    {
        // Locate root column meta by unique id; this hides inline vs external CMO layout.
        RETURN_IF_ERROR(accessor->get_column_meta_by_uid(*footer, column_uid, &self_column_pb));
        // root column
        // root subcolumn is ColumnVariant::MostCommonType which is jsonb
        DataTypePtr root_type =
                self_column_pb.is_nullable()
                        ? make_nullable(std::make_unique<ColumnVariant::MostCommonType>())
                        : std::make_unique<ColumnVariant::MostCommonType>();
        int32_t root_footer_ordinal = -1;
        if (self_column_pb.has_column_id()) {
            // Narrow explicitly to avoid implicit narrowing warnings.
            root_footer_ordinal = static_cast<int32_t>(self_column_pb.column_id());
        }
        _subcolumns_meta_info->create_root(SubcolumnMeta {
                .file_column_type = root_type,
                // Use column_id from meta as footer ordinal when inline footer is available.
                .footer_ordinal = root_footer_ordinal});
        RETURN_IF_ERROR(ColumnReader::create(opts, self_column_pb, num_rows, file_reader,
                                             &_root_column_reader));
    }

    _data_type = DataTypeFactory::instance().create_data_type(self_column_pb);
    _root_unique_id = self_column_pb.unique_id();
    const bool should_record_path_stats = variant_util::should_record_variant_path_stats(
            opts.tablet_schema->column_by_uid(self_column_pb.unique_id()));
    const auto& parent_index = opts.tablet_schema->inverted_indexs(self_column_pb.unique_id());
    // record variant_sparse_column_statistics_size from parent column
    _variant_sparse_column_statistics_size =
            opts.tablet_schema->column_by_uid(self_column_pb.unique_id())
                    .variant_max_sparse_column_statistics_size();
    DCHECK(opts.tablet_schema != nullptr) << "tablet_schema is nullptr";
    _tablet_schema = opts.tablet_schema;

    // Only extract scalar flags from root stats; sparse/doc maps come from
    // their respective column metas via additive merge methods.
    if (self_column_pb.has_variant_statistics()) {
        _statistics->has_nested_group = self_column_pb.variant_statistics().has_nested_group();
    }

    // collect bucketized binary column readers for this variant column
    std::map<uint32_t, std::shared_ptr<ColumnReader>> tmp_sparse_readers;
    std::map<uint32_t, std::shared_ptr<ColumnReader>> tmp_doc_value_readers;

    // helper to handle sparse meta (single or bucket) from a ColumnMetaPB
    auto handle_sparse_meta = [&](const ColumnMetaPB& col, bool* handled) -> Status {
        *handled = false;
        if (!col.has_column_path_info()) {
            return Status::OK();
        }
        PathInData path;
        path.from_protobuf(col.column_path_info());
        auto relative = path.copy_pop_front();
        if (relative.empty()) {
            return Status::OK();
        }

        // case 1: single sparse column
        std::string rel_str = relative.get_path();
        if (rel_str == SPARSE_COLUMN_PATH) {
            DCHECK(col.has_variant_statistics()) << col.DebugString();
            if (should_record_path_stats) {
                // Always load sparse stats from the sparse column's own meta.
                // This is the authoritative source; root stats may duplicate these
                // but the sparse column meta is canonical.
                _statistics->merge_sparse_from_pb(col.variant_statistics());
            }
            std::shared_ptr<ColumnReader> single_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, col, footer->num_rows(), file_reader,
                                                 &single_reader));
            // only one sparse column
            if (_binary_column_reader) {
                return Status::AlreadyExist("single sparse column reader already exists");
            }
            _binary_column_reader = std::make_shared<SingleSparseColumnReader>();
            RETURN_IF_ERROR(
                    _binary_column_reader->add_binary_column_reader(std::move(single_reader), 0));
            *handled = true;
            return Status::OK();
        }

        // case 2: bucketized sparse column
        std::string bucket_prefix = std::string(SPARSE_COLUMN_PATH) + ".b";
        if (rel_str.starts_with(bucket_prefix)) {
            uint32_t idx =
                    static_cast<uint32_t>(atoi(rel_str.substr(bucket_prefix.size()).c_str()));
            DCHECK(col.has_variant_statistics()) << col.DebugString();
            if (should_record_path_stats) {
                // Additively merge per-bucket sparse stats into the unified statistics.
                _statistics->merge_sparse_from_pb(col.variant_statistics());
            }
            std::shared_ptr<ColumnReader> reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, col, num_rows, file_reader, &reader));
            tmp_sparse_readers[idx] = std::move(reader);
            *handled = true;
            return Status::OK();
        }

        // case 3: doc snapshot column
        if (rel_str.find(DOC_VALUE_COLUMN_PATH) != std::string::npos) {
            size_t bucket = rel_str.rfind('b');
            uint32_t bucket_value = static_cast<uint32_t>(std::stoi(rel_str.substr(bucket + 1)));
            std::shared_ptr<ColumnReader> column_reader;
            RETURN_IF_ERROR(ColumnReader::create(opts, col, num_rows, file_reader, &column_reader));
            tmp_doc_value_readers[bucket_value] = std::move(column_reader);
            if (should_record_path_stats) {
                // Additively merge per-bucket doc value stats into the unified statistics.
                _statistics->merge_doc_value_from_pb(col.variant_statistics());
            }
            *handled = true;
            return Status::OK();
        }
        return Status::OK();
    };

    // First try initialize sparse from root's embedded children_columns (new segments)
    if (self_column_pb.children_columns_size() > 0) {
        for (int i = 0; i < self_column_pb.children_columns_size(); ++i) {
            const ColumnMetaPB& child_pb = self_column_pb.children_columns(i);
            bool handled = false;
            RETURN_IF_ERROR(handle_sparse_meta(child_pb, &handled));
        }
    }

    // init from inline columns meta
    for (int32_t ordinal = 0; ordinal < footer->columns_size(); ++ordinal) {
        const ColumnMetaPB& column_pb = footer->columns(ordinal);
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
        PathInData path;
        path.from_protobuf(column_pb.column_path_info());

        // init sparse column readers
        auto relative_sparse = path.copy_pop_front();
        auto rel_str = relative_sparse.get_path();
        {
            bool handled = false;
            RETURN_IF_ERROR(handle_sparse_meta(column_pb, &handled));
            if (handled) {
                continue;
            }
        }

        // init subcolumns
        auto relative_path = path.copy_pop_front();
        if (relative_path.empty()) {
            continue;
        }
        // Skip NestedGroup subcolumns (columns with ___DOR_ng___. prefix in path).
        // NestedGroup columns only contain rows that have the nested array, not all rows.
        // They need special handling via NestedGroupWholeIterator, not regular subcolumns.
        const auto& leaf_path = relative_path.get_path();
        if (contains_nested_group_marker(leaf_path)) {
            VLOG_DEBUG << "Skipping NestedGroup subcolumn: " << leaf_path;
            continue;
        }
        // check the root is already a leaf node
        if (should_record_path_stats && column_pb.has_none_null_size()) {
            _statistics->subcolumns_non_null_size.emplace(relative_path.get_path(),
                                                          column_pb.none_null_size());
        }
        // 3.1.2 may store a flat JSON key like {"a.b": 1} as a single PathInData part.
        // New compaction schema and query path expect a dot-split multi-part shape.
        // Rebuild via the string constructor when the path has neither typed
        // nor nested metadata, so the tree matches the new shape.
        if (!relative_path.get_is_typed() && !relative_path.has_nested_part()) {
            relative_path = PathInData(relative_path.get_path());
        }
        _subcolumns_meta_info->add(
                relative_path,
                SubcolumnMeta {
                        .file_column_type = DataTypeFactory::instance().create_data_type(column_pb),
                        .footer_ordinal = ordinal});
    }

    // finalize bucket readers if any
    // Stats have already been merged additively as each bucket column was processed.
    if (!tmp_sparse_readers.empty()) {
        _binary_column_reader = std::make_shared<MultipleSparseColumnReader>();
        for (auto& [index, reader] : tmp_sparse_readers) {
            RETURN_IF_ERROR(
                    _binary_column_reader->add_binary_column_reader(std::move(reader), index));
        }
    } else if (!tmp_doc_value_readers.empty()) {
        _binary_column_reader = std::make_shared<MultipleDocColumnReader>();
        for (auto& [index, reader] : tmp_doc_value_readers) {
            RETURN_IF_ERROR(
                    _binary_column_reader->add_binary_column_reader(std::move(reader), index));
        }
    }

    // old version variant column without any binary data.
    // if no binary column reader, use dummy binary column reader
    if (_binary_column_reader == nullptr) {
        _binary_column_reader = std::make_shared<DummyBinaryColumnReader>();
    }
    _segment_file_reader = file_reader;
    _num_rows = num_rows;
    // try build external meta readers (optional)
    _ext_meta_reader = std::make_unique<VariantExternalMetaReader>();
    RETURN_IF_ERROR(_ext_meta_reader->init_from_footer(footer, file_reader, _root_unique_id));

    // NestedGroup initialization is provider-driven. Disabled providers keep fallback behavior,
    // while enabled providers populate nested group readers from segment footer.
    if (_can_use_nested_group_read_path()) {
        RETURN_IF_ERROR(_nested_group_read_provider->init_readers(opts, footer, file_reader,
                                                                  accessor, _root_unique_id,
                                                                  num_rows, _nested_group_readers));
    }

    return Status::OK();
}

Status VariantColumnReader::create_reader_from_external_meta(const std::string& path,
                                                             const ColumnReaderOptions& opts,
                                                             const io::FileReaderSPtr& file_reader,
                                                             uint64_t num_rows,
                                                             std::shared_ptr<ColumnReader>* out) {
    if (!_ext_meta_reader || !_ext_meta_reader->available()) {
        return Status::Error<ErrorCode::NOT_FOUND, false>("no external variant meta");
    }
    ColumnMetaPB meta;
    RETURN_IF_ERROR(_ext_meta_reader->lookup_meta_by_path(path, &meta));
    return ColumnReader::create(opts, meta, num_rows, file_reader, out);
}

Status VariantColumnReader::create_path_reader(const PathInData& relative_path,
                                               const ColumnReaderOptions& opts,
                                               ColumnMetaAccessor* accessor,
                                               const SegmentFooterPB& footer,
                                               const io::FileReaderSPtr& file_reader,
                                               uint64_t num_rows,
                                               std::shared_ptr<ColumnReader>* out) {
    // 1) Try inline subcolumn meta if available (footer_ordinal >= 0)
    const auto* node = get_subcolumn_meta_by_path(relative_path);
    if (node != nullptr && node->data.footer_ordinal >= 0) {
        // leaf node, get the column meta by footer ordinal
        const int32_t column_ordinal = node->data.footer_ordinal;
        ColumnMetaPB meta;
        RETURN_IF_ERROR(
                accessor->get_column_meta_by_column_ordinal_id(footer, column_ordinal, &meta));
        return ColumnReader::create(opts, meta, num_rows, file_reader, out);
    }

    // 2) Try external meta layout (if available)
    Status st = create_reader_from_external_meta(relative_path.get_path(), opts, file_reader,
                                                 num_rows, out);
    if (st.is<ErrorCode::NOT_FOUND>()) {
        // 3) Try nested group readers (array-of-objects / nested search paths).
        // relative_path is already popped of the variant root, so it can directly match
        // nested group child names (e.g. "msg", "title").
        auto [group_reader, child_path] = find_nested_group_for_path(relative_path.get_path());
        if (group_reader != nullptr && !child_path.empty()) {
            auto it = group_reader->child_readers.find(child_path);
            if (it != group_reader->child_readers.end() && it->second != nullptr) {
                *out = it->second;
                return Status::OK();
            }
        }
        *out = nullptr;
        return st;
    }
    return st;
}

Status VariantColumnReader::load_external_meta_once() {
    if (!_ext_meta_reader || !_ext_meta_reader->available()) {
        return Status::OK();
    }
    // Ensure only one writer can populate `_subcolumns_meta_info` / `_statistics`
    // while readers of these structures hold shared locks.
    std::unique_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    VariantStatistics* stats = variant_util::should_record_variant_path_stats(
                                       _tablet_schema->column_by_uid(_root_unique_id))
                                       ? _statistics.get()
                                       : nullptr;
    return _ext_meta_reader->load_all_once(_subcolumns_meta_info.get(), stats);
}

TabletIndexes VariantColumnReader::find_subcolumn_tablet_indexes(const TabletColumn& column,
                                                                 const DataTypePtr& data_type) {
    TabletSchema::SubColumnInfo sub_column_info;
    const auto& parent_index = _tablet_schema->inverted_indexs(column.parent_unique_id());
    auto relative_path = column.path_info_ptr()->copy_pop_front();
    // if subcolumn has index, add index to _variant_subcolumns_indexes
    if (variant_util::generate_sub_column_info(*_tablet_schema, column.parent_unique_id(),
                                               relative_path.get_path(), &sub_column_info) &&
        !sub_column_info.indexes.empty()) {
        return sub_column_info.indexes;
    }

    // Otherwise, inherit index from the VARIANT parent column.
    if (!parent_index.empty() && data_type->get_primitive_type() != PrimitiveType::TYPE_VARIANT &&
        data_type->get_primitive_type() != PrimitiveType::TYPE_MAP /*SPARSE COLUMN*/) {
        // type in column maynot be real type, so use data_type to get the real type
        PathInData index_path {*column.path_info_ptr()};
        DataTypePtr index_data_type = data_type;
        if (!relative_path.empty()) {
            auto [nested_reader, _] = find_nested_group_for_path(relative_path.get_path());
            const std::string root_path(kRootNestedGroupPath);

            if (nested_reader != nullptr) {
                const bool is_root_ng = nested_reader->array_path == root_path;
                if (!is_root_ng) {
                    // Named NG — use variant-relative path (consistent with write path)
                    index_path = relative_path;
                } else {
                    // $root NG — prefix path with __D0_root__
                    index_path = PathInData(root_path + "." + relative_path.get_path());
                }

                // Unwrap Nullable(Array(...)) → element type for NG subcolumns
                if (data_type->is_nullable()) {
                    auto base = variant_util::get_base_type_of_array(remove_nullable(data_type));
                    index_data_type = base->is_nullable() ? base : make_nullable(base);
                } else {
                    index_data_type = variant_util::get_base_type_of_array(data_type);
                }
            }
            // else: non-NG scalar field — keep index_path and index_data_type unchanged
        }
        TabletColumn target_column =
                variant_util::get_column_by_type(index_data_type, column.name(),
                                                 {.unique_id = -1,
                                                  .parent_unique_id = column.parent_unique_id(),
                                                  .path_info = index_path});
        variant_util::inherit_index(parent_index, sub_column_info.indexes, target_column);
    }
    // Return shared_ptr directly to maintain object lifetime
    return sub_column_info.indexes;
}

void VariantColumnReader::get_subcolumns_types(
        std::unordered_map<PathInData, DataTypes, PathInData::Hash>* subcolumns_types) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    for (const auto& subcolumn_reader : *_subcolumns_meta_info) {
        auto& path_types = (*subcolumns_types)[subcolumn_reader->path];
        path_types.push_back(subcolumn_reader->data.file_column_type);
    }
}

void VariantColumnReader::get_typed_paths(std::unordered_set<std::string>* typed_paths) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    for (const auto& entry : *_subcolumns_meta_info) {
        if (entry->path.get_is_typed()) {
            typed_paths->insert(entry->path.get_path());
        }
    }
}

void VariantColumnReader::get_nested_paths(
        std::unordered_set<PathInData, PathInData::Hash>* nested_paths) const {
    std::shared_lock<std::shared_mutex> lock(_subcolumns_meta_mutex);
    for (const auto& entry : *_subcolumns_meta_info) {
        if (entry->path.has_nested_part()) {
            nested_paths->insert(entry->path);
        }
    }
}

Status VariantColumnReader::infer_data_type_for_path(DataTypePtr* type, const TabletColumn& column,
                                                     const StorageReadOptions& opts,
                                                     ColumnReaderCache* column_reader_cache) {
    DCHECK(column.has_path_info());
    ReadPlan plan;
    RETURN_IF_ERROR(_build_read_plan(&plan, column, &opts, column_reader_cache, nullptr));
    *type = plan.type;
    return Status::OK();
}

Status VariantRootColumnIterator::_process_root_column(MutableColumnPtr& dst,
                                                       MutableColumnPtr& root_column,
                                                       const DataTypePtr& most_common_type) {
    auto& obj = dst->is_nullable() ? assert_cast<ColumnVariant&>(
                                             assert_cast<ColumnNullable&>(*dst).get_nested_column())
                                   : assert_cast<ColumnVariant&>(*dst);

    // fill nullmap
    if (root_column->is_nullable() && dst->is_nullable()) {
        ColumnUInt8& dst_null_map = assert_cast<ColumnNullable&>(*dst).get_null_map_column();
        ColumnUInt8& src_null_map =
                assert_cast<ColumnNullable&>(*root_column).get_null_map_column();
        dst_null_map.insert_range_from(src_null_map, 0, src_null_map.size());
    }

    // add root column to a tmp object column
    auto tmp = ColumnVariant::create(0, obj.enable_doc_mode(), root_column->size());
    auto& tmp_obj = assert_cast<ColumnVariant&>(*tmp);
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

Status VariantRootColumnIterator::next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) {
    // read root column
    auto& obj = dst->is_nullable() ? assert_cast<ColumnVariant&>(
                                             assert_cast<ColumnNullable&>(*dst).get_nested_column())
                                   : assert_cast<ColumnVariant&>(*dst);

    auto most_common_type =
            obj.get_most_common_type(); // NOLINT(readability-static-accessed-through-instance)
    auto root_column = most_common_type->create_column();
    RETURN_IF_ERROR(_inner_iter->next_batch(n, root_column, has_null));

    return _process_root_column(dst, root_column, most_common_type);
}

Status VariantRootColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                 MutableColumnPtr& dst) {
    // read root column
    auto& obj = dst->is_nullable() ? assert_cast<ColumnVariant&>(
                                             assert_cast<ColumnNullable&>(*dst).get_nested_column())
                                   : assert_cast<ColumnVariant&>(*dst);

    auto most_common_type =
            obj.get_most_common_type(); // NOLINT(readability-static-accessed-through-instance)
    auto root_column = most_common_type->create_column();
    RETURN_IF_ERROR(_inner_iter->read_by_rowids(rowids, count, root_column));

    return _process_root_column(dst, root_column, most_common_type);
}

Status VariantRootColumnIterator::init_prefetcher(const SegmentPrefetchParams& params) {
    return _inner_iter->init_prefetcher(params);
}

void VariantRootColumnIterator::collect_prefetchers(
        std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
        PrefetcherInitMethod init_method) {
    _inner_iter->collect_prefetchers(prefetchers, init_method);
}

static void fill_nested_with_defaults(MutableColumnPtr& dst, MutableColumnPtr& sibling_column,
                                      size_t nrows) {
    const auto* sibling_array =
            check_and_get_column<ColumnArray>(remove_nullable(sibling_column->get_ptr()).get());
    const auto* dst_array =
            check_and_get_column<ColumnArray>(remove_nullable(dst->get_ptr()).get());
    if (!dst_array || !sibling_array) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Expected array column, but met {} and {}", dst->get_name(),
                               sibling_column->get_name());
    }
    auto new_nested =
            dst_array->get_data_ptr()->clone_resized(sibling_array->get_data_ptr()->size());
    auto new_array = make_nullable(ColumnArray::create(
            new_nested->assume_mutable(), sibling_array->get_offsets_ptr()->assume_mutable()));
    dst->insert_range_from(*new_array, 0, new_array->size());
#ifndef NDEBUG
    if (!dst_array->has_equal_offsets(*sibling_array)) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Expected same array offsets");
    }
#endif
}

Status DefaultNestedColumnIterator::next_batch(size_t* n, MutableColumnPtr& dst) {
    bool has_null = false;
    return next_batch(n, dst, &has_null);
}

Status DefaultNestedColumnIterator::next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) {
    if (_sibling_iter) {
        MutableColumnPtr sibling_column = _file_column_type->create_column();
        RETURN_IF_ERROR(_sibling_iter->next_batch(n, sibling_column, has_null));
        fill_nested_with_defaults(dst, sibling_column, *n);
    } else {
        dst->insert_many_defaults(*n);
    }
    return Status::OK();
}
Status DefaultNestedColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                   MutableColumnPtr& dst) {
    if (_sibling_iter) {
        MutableColumnPtr sibling_column = _file_column_type->create_column();
        RETURN_IF_ERROR(_sibling_iter->read_by_rowids(rowids, count, sibling_column));
        fill_nested_with_defaults(dst, sibling_column, count);
    } else {
        dst->insert_many_defaults(count);
    }
    return Status::OK();
}

const NestedGroupReader* VariantColumnReader::get_nested_group_reader(
        const std::string& array_path) const {
    auto res = find_in_nested_groups(_nested_group_readers, array_path, false);
    return (res.found && res.child_path.empty()) ? res.reader : nullptr;
}

std::pair<const NestedGroupReader*, std::string> VariantColumnReader::find_nested_group_for_path(
        const std::string& path) const {
    auto res = find_in_nested_groups(_nested_group_readers, path, false);
    if (!res.found) {
        return {nullptr, ""};
    }
    if (res.child_path.empty()) {
        return {res.reader, ""};
    }
    if (res.reader && res.reader->child_readers.contains(res.child_path)) {
        return {res.reader, std::move(res.child_path)};
    }
    return {nullptr, ""};
}

std::tuple<bool, std::vector<const NestedGroupReader*>, std::string>
VariantColumnReader::collect_nested_group_chain(const std::string& path) const {
    auto res = find_in_nested_groups(_nested_group_readers, path, true);
    return {res.found, std::move(res.chain), std::move(res.child_path)};
}

} // namespace doris::segment_v2

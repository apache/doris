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

#include <gen_cpp/segment_v2.pb.h>
#include <sys/types.h>

#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/page_handle.h"
#include "olap/rowset/segment_v2/variant/variant_external_meta_reader.h"
#include "olap/rowset/segment_v2/variant_statistics.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column_object.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/json/path_in_data.h"

namespace doris {

class TabletIndex;
class StorageReadOptions;
class TabletSchema;

namespace segment_v2 {

class ColumnIterator;
class InvertedIndexIterator;
class InvertedIndexFileReader;
class ColumnReaderCache;
class ColumnMetaAccessor;

class VariantColumnReader : public ColumnReader {
public:
    VariantColumnReader() = default;

    Status init(const ColumnReaderOptions& opts, ColumnMetaAccessor* accessor,
                const std::shared_ptr<SegmentFooterPB>& footer, int32_t column_uid,
                uint64_t num_rows, io::FileReaderSPtr file_reader);

    Status new_iterator(ColumnIteratorUPtr* iterator, const TabletColumn* col,
                        const StorageReadOptions* opt) override;

    Status new_iterator(ColumnIteratorUPtr* iterator, const TabletColumn* col,
                        const StorageReadOptions* opt, ColumnReaderCache* column_reader_cache);

    virtual const SubcolumnColumnMetaInfo::Node* get_subcolumn_meta_by_path(
            const vectorized::PathInData& relative_path) const;

    ~VariantColumnReader() override = default;

    FieldType get_meta_type() override { return FieldType::OLAP_FIELD_TYPE_VARIANT; }

    const VariantStatistics* get_stats() const { return _statistics.get(); }

    int64_t get_metadata_size() const override;

    // Return shared_ptr to ensure the lifetime of TabletIndex objects
    TabletIndexes find_subcolumn_tablet_indexes(const TabletColumn& target_column,
                                                const vectorized::DataTypePtr& data_type);

    bool exist_in_sparse_column(const vectorized::PathInData& path) const;

    bool is_exceeded_sparse_column_limit() const;

    const SubcolumnColumnMetaInfo* get_subcolumns_meta_info() const {
        return _subcolumns_meta_info.get();
    }

    // Get the types of all subcolumns in the variant column.
    void get_subcolumns_types(
            std::unordered_map<vectorized::PathInData, vectorized::DataTypes,
                               vectorized::PathInData::Hash>* subcolumns_types) const;

    // Get the typed paths in the variant column.
    void get_typed_paths(std::unordered_set<std::string>* typed_paths) const;

    // Get the nested paths in the variant column.
    void get_nested_paths(std::unordered_set<vectorized::PathInData, vectorized::PathInData::Hash>*
                                  nested_paths) const;

    // Infer the storage data type for a variant subcolumn using full StorageReadOptions
    // (reader type, tablet schema, etc). This shares the same decision logic as
    // `new_iterator` via an internal read plan, but does not create any iterator.
    Status infer_data_type_for_path(vectorized::DataTypePtr* type, const TabletColumn& column,
                                    const StorageReadOptions& opts,
                                    ColumnReaderCache* column_reader_cache);

    // Create a ColumnReader for a sub-column identified by `relative_path`.
    // This method will first try inline footer.columns via footer_ordinal and then
    // fall back to external meta if available. Callers do not need to care about
    // the underlying layout (inline vs external).
    Status create_path_reader(const vectorized::PathInData& relative_path,
                              const ColumnReaderOptions& opts, ColumnMetaAccessor* accessor,
                              const SegmentFooterPB& footer, const io::FileReaderSPtr& file_reader,
                              uint64_t num_rows, std::shared_ptr<ColumnReader>* out);

    // Try create a ColumnReader from externalized meta (path -> ColumnMetaPB bytes) if present.
    // Only used internally by create_path_reader. External callers should not rely
    // on external meta details directly.
    Status create_reader_from_external_meta(const std::string& path,
                                            const ColumnReaderOptions& opts,
                                            const io::FileReaderSPtr& file_reader,
                                            uint64_t num_rows, std::shared_ptr<ColumnReader>* out);

    // Ensure external meta is loaded only once across concurrent callers.
    Status load_external_meta_once();

    // Determine whether `path` is a strict prefix of any existing subcolumn path.
    // Consider three sources:
    // 1) Extracted subcolumns in `_subcolumns_meta_info`
    // 2) Sparse column statistics in `_statistics->sparse_column_non_null_size`
    // 3) Externalized metas via `_ext_meta_reader`
    bool has_prefix_path(const vectorized::PathInData& relative_path) const;

private:
    // Describe how a variant sub-path should be read. This is a logical plan only and
    // does not create any concrete ColumnIterator.
    enum class ReadKind {
        ROOT_FLAT,      // root variant using `VariantRootColumnIterator`
        HIERARCHICAL,   // hierarchical merge (root + subcolumns + sparse)
        LEAF,           // direct leaf reader
        SPARSE_EXTRACT, // extract single path from sparse column
        SPARSE_MERGE,   // merge subcolumns into sparse column
        DEFAULT_NESTED, // fill nested subcolumn using sibling nested column
        DEFAULT_FILL    // default iterator when path not exist
    };

    struct ReadPlan {
        ReadKind kind {ReadKind::DEFAULT_FILL};
        vectorized::DataTypePtr type;

        // path & meta context
        vectorized::PathInData relative_path;

        std::shared_ptr<ColumnReader> leaf_column_reader = nullptr;
        const SubcolumnColumnMetaInfo::Node* node = nullptr;
        const SubcolumnColumnMetaInfo::Node* root = nullptr;
    };

    // init for compaction read
    Status _new_default_iter_with_same_nested(ColumnIteratorUPtr* iterator, const TabletColumn& col,
                                              const StorageReadOptions* opt,
                                              ColumnReaderCache* column_reader_cache);

    // Build read plan for flat-leaf (compaction/checksum) mode. Only decides the
    // resulting type and how to read, without creating iterators.
    Status _build_read_plan_flat_leaves(ReadPlan* plan, const TabletColumn& col,
                                        const StorageReadOptions* opts,
                                        ColumnReaderCache* column_reader_cache);

    // Build read plan for the general hierarchical reading mode.
    Status _build_read_plan(ReadPlan* plan, const TabletColumn& target_col,
                            const StorageReadOptions* opt, ColumnReaderCache* column_reader_cache);

    // Materialize a concrete ColumnIterator according to the previously built plan.
    Status _create_iterator_from_plan(ColumnIteratorUPtr* iterator, const ReadPlan& plan,
                                      const TabletColumn& target_col, const StorageReadOptions* opt,
                                      ColumnReaderCache* column_reader_cache);

    Status _new_iterator_with_flat_leaves(ColumnIteratorUPtr* iterator,
                                          vectorized::DataTypePtr* type, const TabletColumn& col,
                                          const StorageReadOptions* opts,
                                          bool exceeded_sparse_column_limit,
                                          bool existed_in_sparse_column,
                                          ColumnReaderCache* column_reader_cache);

    Status _create_hierarchical_reader(ColumnIteratorUPtr* reader, int32_t col_uid,
                                       vectorized::PathInData path,
                                       const SubcolumnColumnMetaInfo::Node* node,
                                       const SubcolumnColumnMetaInfo::Node* root,
                                       ColumnReaderCache* column_reader_cache,
                                       OlapReaderStatistics* stats);
    Status _create_sparse_merge_reader(ColumnIteratorUPtr* iterator, const StorageReadOptions* opts,
                                       const TabletColumn& target_col,
                                       ColumnIteratorUPtr inner_iter,
                                       ColumnReaderCache* column_reader_cache);

    // Protect `_subcolumns_meta_info` and `_statistics` when loading external meta.
    // english only in comments
    mutable std::shared_mutex _subcolumns_meta_mutex;

    std::unique_ptr<SubcolumnColumnMetaInfo> _subcolumns_meta_info;
    std::shared_ptr<ColumnReader> _sparse_column_reader;
    std::shared_ptr<ColumnReader> _root_column_reader;
    std::unique_ptr<VariantStatistics> _statistics;
    std::shared_ptr<TabletSchema> _tablet_schema;
    // variant_sparse_column_statistics_size
    size_t _variant_sparse_column_statistics_size =
            BeConsts::DEFAULT_VARIANT_MAX_SPARSE_COLUMN_STATS_SIZE;

    // Externalized meta reader (optional)
    std::unique_ptr<VariantExternalMetaReader> _ext_meta_reader;

    io::FileReaderSPtr _segment_file_reader;
    uint64_t _num_rows {0};
    uint32_t _root_unique_id {0};

    // call-once guard moved into VariantExternalMetaReader
};

class VariantRootColumnIterator : public ColumnIterator {
public:
    VariantRootColumnIterator() = delete;

    explicit VariantRootColumnIterator(FileColumnIteratorUPtr iter) {
        _inner_iter = std::move(iter);
    }

    ~VariantRootColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override { return _inner_iter->init(opts); }

    Status seek_to_first() override { return _inner_iter->seek_to_first(); }

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        return _inner_iter->seek_to_ordinal(ord_idx);
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override { return _inner_iter->get_current_ordinal(); }

private:
    Status _process_root_column(vectorized::MutableColumnPtr& dst,
                                vectorized::MutableColumnPtr& root_column,
                                const vectorized::DataTypePtr& most_common_type);
    std::unique_ptr<FileColumnIterator> _inner_iter;
};

class DefaultNestedColumnIterator : public ColumnIterator {
public:
    DefaultNestedColumnIterator(ColumnIteratorUPtr&& sibling, DataTypePtr file_column_type)
            : _sibling_iter(std::move(sibling)), _file_column_type(std::move(file_column_type)) {}

    Status init(const ColumnIteratorOptions& opts) override {
        if (_sibling_iter) {
            return _sibling_iter->init(opts);
        }
        return Status::OK();
    }

    Status seek_to_first() override {
        _current_rowid = 0;
        if (_sibling_iter) {
            return _sibling_iter->seek_to_first();
        }
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = ord_idx;
        if (_sibling_iter) {
            return _sibling_iter->seek_to_ordinal(ord_idx);
        }
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst);

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    Status next_batch_of_zone_map(size_t* n, vectorized::MutableColumnPtr& dst) override {
        return Status::NotSupported("Not supported next_batch_of_zone_map");
    }

    ordinal_t get_current_ordinal() const override {
        if (_sibling_iter) {
            return _sibling_iter->get_current_ordinal();
        }
        return _current_rowid;
    }

private:
    std::unique_ptr<ColumnIterator> _sibling_iter;
    std::shared_ptr<const vectorized::IDataType> _file_column_type;
    // current rowid
    ordinal_t _current_rowid = 0;
};

} // namespace segment_v2
} // namespace doris
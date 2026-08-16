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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <cstddef> // for size_t
#include <cstdint> // for uint32_t
#include <functional>
#include <map>
#include <memory> // for unique_ptr
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/compiler_util.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"            // for Status
#include "core/column/column_array.h" // ColumnArray
#include "core/data_type/data_type.h"
#include "core/data_type/storage_field_type.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/io_common.h"
#include "storage/index/index_reader.h"
#include "storage/index/ordinal_page_index.h" // for OrdinalPageIndexIterator
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/olap_common.h"
#include "storage/predicate/column_predicate.h"
#include "storage/segment/common.h"
#include "storage/segment/page_handle.h" // for PageHandle
#include "storage/segment/page_pointer.h"
#include "storage/segment/parsed_page.h" // for ParsedPage
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment_prefetcher.h"
#include "storage/segment/stream_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "storage/utils.h"
#include "util/once.h"

namespace doris {

class BlockCompressionCodec;
class AndBlockColumnPredicate;
class ColumnPredicate;
class TabletIndex;
class StorageReadOptions;

namespace io {
class FileReader;
} // namespace io
struct Slice;
struct StringRef;

using TColumnAccessPaths = std::vector<TColumnAccessPath>;

namespace segment_v2 {
class EncodingInfo;
class ColumnIterator;
class BloomFilterIndexReader;
class InvertedIndexIterator;
class InvertedIndexReader;
class IndexFileReader;
class PageDecoder;
class RowRanges;
class ZoneMapIndexReader;
class IndexIterator;
class ColumnMetaAccessor;

struct ColumnReaderOptions {
    // whether verify checksum when read page
    bool verify_checksum = true;
    // for in memory olap table, use DURABLE CachePriority in page cache
    bool kept_in_memory = false;

    int be_exec_version = -1;

    TabletSchemaSPtr tablet_schema = nullptr;

    // When set, ColumnReader::create returns a ConstantColumnReader carrying this value instead
    // of reading on-disk data. Used for read-time-filled constant columns (e.g.
    // __DORIS_COMMIT_TSO_COL__) on a single-version segment, whose on-disk value is only a
    // placeholder. The value is constant within a segment, so the resulting reader is cacheable.
    std::optional<Field> const_value = std::nullopt;
};

struct ColumnIteratorOptions {
    bool use_page_cache = false;
    bool is_predicate_column = false;
    // for page cache allocation
    // page types are divided into DATA_PAGE & INDEX_PAGE
    // INDEX_PAGE including index_page, dict_page and short_key_page
    PageTypePB type = PageTypePB::UNKNOWN_PAGE_TYPE;
    io::FileReader* file_reader = nullptr; // Ref
    // reader statistics
    OlapReaderStatistics* stats = nullptr; // Ref
    io::IOContext io_ctx;
    bool only_read_offsets = false;

    void sanity_check() const {
        CHECK_NOTNULL(file_reader);
        CHECK_NOTNULL(stats);
    }
};

class ColumnIterator;
class OffsetFileColumnIterator;
class FileColumnIterator;

using ColumnIteratorUPtr = std::unique_ptr<ColumnIterator>;
using OffsetFileColumnIteratorUPtr = std::unique_ptr<OffsetFileColumnIterator>;
using FileColumnIteratorUPtr = std::unique_ptr<FileColumnIterator>;
using ColumnIteratorSPtr = std::shared_ptr<ColumnIterator>;

// There will be concurrent users to read the same column. So
// we should do our best to reduce resource usage through share
// same information, such as OrdinalPageIndex and Page data.
// This will cache data shared by all reader
class ColumnReader : public MetadataAdder<ColumnReader>,
                     public std::enable_shared_from_this<ColumnReader> {
public:
    ColumnReader();
    // Create an initialized ColumnReader in *reader.
    // This should be a lightweight operation without I/O.
    static Status create(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                         uint64_t num_rows, const io::FileReaderSPtr& file_reader,
                         std::shared_ptr<ColumnReader>* reader);

    static Status create_array(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                               const io::FileReaderSPtr& file_reader,
                               std::shared_ptr<ColumnReader>* reader);
    static Status create_map(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                             const io::FileReaderSPtr& file_reader,
                             std::shared_ptr<ColumnReader>* reader);
    static Status create_struct(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                                uint64_t num_rows, const io::FileReaderSPtr& file_reader,
                                std::shared_ptr<ColumnReader>* reader);
    static Status create_agg_state(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                                   uint64_t num_rows, const io::FileReaderSPtr& file_reader,
                                   std::shared_ptr<ColumnReader>* reader);

    enum DictEncodingType { UNKNOWN_DICT_ENCODING, PARTIAL_DICT_ENCODING, ALL_DICT_ENCODING };

    static bool is_compaction_reader_type(ReaderType type);

    ~ColumnReader() override;

    // create a new column iterator. Client should delete returned iterator
    virtual Status new_iterator(ColumnIteratorUPtr* iterator, const TabletColumn* col,
                                const StorageReadOptions*);
    Status new_iterator(ColumnIteratorUPtr* iterator, const TabletColumn* tablet_column);
    Status new_array_iterator(ColumnIteratorUPtr* iterator, const TabletColumn* tablet_column);
    Status new_struct_iterator(ColumnIteratorUPtr* iterator, const TabletColumn* tablet_column);
    Status new_map_iterator(ColumnIteratorUPtr* iterator, const TabletColumn* tablet_column);
    Status new_agg_state_iterator(ColumnIteratorUPtr* iterator);

    Status new_index_iterator(const std::shared_ptr<IndexFileReader>& index_file_reader,
                              const TabletIndex* index_meta, const std::string& rowset_id,
                              uint32_t segment_id, size_t rows_of_segment,
                              std::unique_ptr<IndexIterator>* iterator);

    Status seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter,
                             const ColumnIteratorOptions& iter_opts);
    Status get_ordinal_index_reader(OrdinalIndexReader*& reader,
                                    OlapReaderStatistics* index_load_stats);

    // read a page from file into a page handle
    Status read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp,
                     PageHandle* handle, Slice* page_body, PageFooterPB* footer,
                     BlockCompressionCodec* codec) const;

    bool is_nullable() const { return _meta_is_nullable; }

    const EncodingInfo* encoding_info() const { return _encoding_info; }

    virtual bool has_zone_map() const { return _zone_map_index != nullptr; }
    bool has_bloom_filter_index(bool ngram) const;
    // Check if this column could match `cond' using segment zone map.
    // Since segment zone map is stored in metadata, this function is fast without I/O.
    // set matched to true if segment zone map is absent or `cond' could be satisfied, false otherwise.
    virtual Status match_condition(const AndBlockColumnPredicate* col_predicates,
                                   bool* matched) const;

    Status next_batch_of_zone_map(size_t* n, MutableColumnPtr& dst) const;

    // get row ranges with zone map
    // - cond_column is user's query predicate
    // - delete_condition is a delete predicate of one version
    Status get_row_ranges_by_zone_map(
            const AndBlockColumnPredicate* col_predicates,
            const std::vector<std::shared_ptr<const ColumnPredicate>>* delete_predicates,
            RowRanges* row_ranges, const ColumnIteratorOptions& iter_opts);

    // get row ranges with bloom filter index
    Status get_row_ranges_by_bloom_filter(const AndBlockColumnPredicate* col_predicates,
                                          RowRanges* row_ranges,
                                          const ColumnIteratorOptions& iter_opts);

    PagePointer get_dict_page_pointer() const { return _meta_dict_page; }

    bool is_empty() const { return _num_rows == 0; }

    Status prune_predicates_by_zone_map(std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                        const int column_id, bool* pruned) const;

    virtual Status get_segment_zone_map(segment_v2::ZoneMap* zone_map) const;
    Status get_page_zone_maps(const ColumnIteratorOptions& iter_opts,
                              const std::vector<ZoneMapPB>** zone_maps);
    Status get_row_range_for_page(uint32_t page_index, const ColumnIteratorOptions& iter_opts,
                                  RowRange* row_range);

    CompressionTypePB get_compression() const { return _meta_compression; }

    uint64_t num_rows() const { return _num_rows; }

    void set_dict_encoding_type(DictEncodingType type) {
        static_cast<void>(_set_dict_encoding_type_once.call([&] {
            _dict_encoding_type = type;
            return Status::OK();
        }));
    }

    DictEncodingType get_dict_encoding_type() { return _dict_encoding_type; }

    void disable_index_meta_cache() { _use_index_page_cache = false; }

    DataTypePtr get_vec_data_type() { return _data_type; }

    virtual FieldType get_meta_type() { return _meta_type; }

    int64_t get_metadata_size() const override;

#ifdef BE_TEST
    void check_data_by_zone_map_for_test(const MutableColumnPtr& dst) const;
#endif

private:
    friend class VariantColumnReader;
    friend class FileColumnIterator;
    friend class SegmentPrefetcher;

    ColumnReader(const ColumnReaderOptions& opts, const ColumnMetaPB& meta, uint64_t num_rows,
                 io::FileReaderSPtr file_reader);
    Status init(const ColumnMetaPB* meta);

    [[nodiscard]] Status _load_zone_map_index(bool use_page_cache, bool kept_in_memory,
                                              const ColumnIteratorOptions& iter_opts);
    [[nodiscard]] Status _load_ordinal_index(bool use_page_cache, bool kept_in_memory,
                                             const ColumnIteratorOptions& iter_opts);

    [[nodiscard]] Status _load_index(const std::shared_ptr<IndexFileReader>& index_file_reader,
                                     const TabletIndex* index_meta, const std::string& rowset_id,
                                     uint32_t segment_id, size_t rows_of_segment);
    [[nodiscard]] Status _load_bloom_filter_index(bool use_page_cache, bool kept_in_memory,
                                                  const ColumnIteratorOptions& iter_opts);

    bool _zone_map_match_condition(const segment_v2::ZoneMap& zone_map,
                                   const AndBlockColumnPredicate* col_predicates) const;

    Status _get_filtered_pages(
            const AndBlockColumnPredicate* col_predicates,
            const std::vector<std::shared_ptr<const ColumnPredicate>>* delete_predicates,
            std::vector<uint32_t>* page_indexes, const ColumnIteratorOptions& iter_opts);

    Status _calculate_row_ranges(const std::vector<uint32_t>& page_indexes, RowRanges* row_ranges,
                                 const ColumnIteratorOptions& iter_opts);

    int64_t _meta_length;
    FieldType _meta_type;
    FieldType _meta_children_column_type;
    bool _meta_is_nullable;
    bool _use_index_page_cache;
    int _be_exec_version = -1;

    PagePointer _meta_dict_page;
    CompressionTypePB _meta_compression;

    ColumnReaderOptions _opts;
    uint64_t _num_rows;

    io::FileReaderSPtr _file_reader;

    DictEncodingType _dict_encoding_type;

    DataTypePtr _data_type;

    FieldType _type =
            FieldType::OLAP_FIELD_TYPE_NONE; // initialized in init(), may changed by subclasses.
    const EncodingInfo* _encoding_info =
            nullptr; // initialized in init(), used for create PageDecoder

    // meta for various column indexes (null if the index is absent)
    std::unique_ptr<ZoneMapPB> _segment_zone_map;

    mutable std::shared_mutex _load_index_lock;
    std::unique_ptr<ZoneMapIndexReader> _zone_map_index;
    std::unique_ptr<OrdinalIndexReader> _ordinal_index;
    std::shared_ptr<BloomFilterIndexReader> _bloom_filter_index;

    std::unordered_map<int64_t, IndexReaderPtr> _index_readers;

    std::vector<std::shared_ptr<ColumnReader>> _sub_readers;

    DorisCallOnce<Status> _set_dict_encoding_type_once;
};

// Base iterator to read one column data
class ColumnIterator {
public:
    ColumnIterator() = default;
    virtual ~ColumnIterator() = default;

    virtual Status init(const ColumnIteratorOptions& opts) {
        _opts = opts;
        return Status::OK();
    }

    // Seek to the given ordinal entry in the column.
    // Entry 0 is the first entry written to the column.
    // If provided seek point is past the end of the file,
    // then returns false.
    virtual Status seek_to_ordinal(ordinal_t ord) = 0;

    Status next_batch(size_t* n, MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    virtual Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) {
        return Status::NotSupported("next_batch not implement");
    }

    virtual Status next_batch_of_zone_map(size_t* n, MutableColumnPtr& dst) {
        return Status::NotSupported("next_batch_of_zone_map not implement");
    }

    virtual Status read_by_rowids(const rowid_t* rowids, const size_t count,
                                  MutableColumnPtr& dst) {
        return Status::NotSupported("read_by_rowids not implement");
    }

    virtual ordinal_t get_current_ordinal() const = 0;

    virtual Status get_row_ranges_by_zone_map(
            const AndBlockColumnPredicate* col_predicates,
            const std::vector<std::shared_ptr<const ColumnPredicate>>* delete_predicates,
            RowRanges* row_ranges) {
        return Status::OK();
    }

    virtual Status get_row_ranges_by_bloom_filter(const AndBlockColumnPredicate* col_predicates,
                                                  RowRanges* row_ranges) {
        return Status::OK();
    }

    virtual Status get_row_ranges_by_dict(const AndBlockColumnPredicate* col_predicates,
                                          RowRanges* row_ranges) {
        return Status::OK();
    }

    virtual bool is_all_dict_encoding() const { return false; }

    virtual Status set_access_paths(const TColumnAccessPaths& all_access_paths,
                                    const TColumnAccessPaths& predicate_access_paths) {
        if (!predicate_access_paths.empty()) {
            set_read_requirement_self(ReadRequirement::PREDICATE);
        }
        return Status::OK();
    }

    void set_column_name(const std::string& column_name) { _column_name = column_name; }

    const std::string& column_name() const { return _column_name; }

    // Per-iterator read requirement derived from nested access paths.
    //
    // The ordering is intentional and used by set_read_requirement_self(): requirements are
    // monotonic and a weaker requirement must not downgrade a stronger one.
    // - NORMAL: no pruning decision has been made yet.
    // - SKIP: this iterator should not be read.
    // - LAZY_OUTPUT: materialize this iterator in the lazy phase after predicate filtering.
    // - PREDICATE: read this iterator in the predicate phase. This must stay stronger than
    //   LAZY_OUTPUT because parents may mark children as LAZY_OUTPUT after child set_access_paths()
    //   has already promoted predicate-only children to PREDICATE.
    enum class ReadRequirement : int { NORMAL, SKIP, LAZY_OUTPUT, PREDICATE };

    // Set the read requirement on this iterator and all nested child iterators.
    virtual void set_read_requirement(ReadRequirement requirement) {
        set_read_requirement_self(requirement);
    }

    ReadRequirement read_requirement() const { return _read_requirement; }

    virtual void set_lazy_output_requirement() {
        set_read_requirement(ReadRequirement::LAZY_OUTPUT);
    }

    virtual void remove_pruned_sub_iterators() {};

    virtual Status init_prefetcher(const SegmentPrefetchParams& params) { return Status::OK(); }

    virtual void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) {}

    static constexpr const char* ACCESS_OFFSET = "OFFSET";
    static constexpr const char* ACCESS_ALL = "*";
    static constexpr const char* ACCESS_MAP_KEYS = "KEYS";
    static constexpr const char* ACCESS_MAP_VALUES = "VALUES";
    static constexpr const char* ACCESS_NULL = "NULL";

    // Meta-only read modes:
    // - OFFSET_ONLY: read offsets while skipping actual child/string data. For nullable
    //   complex columns, the parent null map is still materialized when needed.
    // - NULL_MAP_ONLY: only read null map (e.g., for IS NULL / IS NOT NULL predicates)
    // When these modes are enabled, actual content data is skipped.
    enum class MetaReadMode : int { DEFAULT, OFFSET_ONLY, NULL_MAP_ONLY };

    bool read_offset_only() const { return _meta_read_mode == MetaReadMode::OFFSET_ONLY; }
    bool read_null_map_only() const { return _meta_read_mode == MetaReadMode::NULL_MAP_ONLY; }

    // The current scanner phase. This is intentionally separate from ReadRequirement
    // (why this iterator is needed) and MetaReadMode (what physical metadata to read).
    enum class ReadPhase : int {
        NORMAL,    // default full materialization without lazy read split
        PREDICATE, // predicate evaluation before row filtering
        LAZY       // post-filter lazy materialization
    };

    virtual void set_read_phase(ReadPhase mode) {
        _read_phase = mode;
        if (mode == ReadPhase::PREDICATE) {
            _has_place_holder_column = false;
        }
    }

    virtual bool need_to_read() const {
        switch (_read_phase) {
        case ReadPhase::NORMAL:
            return _read_requirement != ReadRequirement::SKIP;
        case ReadPhase::PREDICATE:
            return _read_requirement == ReadRequirement::PREDICATE;
        case ReadPhase::LAZY:
            return _read_requirement == ReadRequirement::LAZY_OUTPUT;
        default:
            return false;
        }
    }

    // Whether the current iterator itself should materialize meta columns, such as
    // the null-map column or the offset column, into the destination column.
    //
    // Do not use the virtual need_to_read() here. Complex iterators override
    // need_to_read() in LAZY mode to keep the parent iterator active when only a
    // nested child still has data to materialize. That parent-level control-flow
    // decision is different from materializing the parent's own offsets/null-map:
    // if the parent was already read for predicate evaluation, LAZY mode should
    // only fill the missing children and must not append parent meta again.
    bool need_to_read_meta_columns() const { return ColumnIterator::need_to_read(); }

    virtual void finalize_lazy_phase(MutableColumnPtr& dst) {
        _recovery_from_place_holder_column(dst);
    }

    // Set only this iterator's requirement without modifying requirements of any nested child
    // iterators. Use this when the parent/wrapper state must be updated while child requirements
    // are decided independently.
    virtual void set_read_requirement_self(ReadRequirement requirement) {
        if (static_cast<int>(requirement) > static_cast<int>(_read_requirement)) {
            _read_requirement = requirement;
        }
    }

    // Whether this iterator or any nested iterator has data that must be materialized
    // in lazy mode. Predicate-only branches are read before filtering and must not be
    // re-read in the lazy phase. Meta-only access paths still become lazy targets when
    // they appear only in all_access_paths, because OFFSET/NULL is the requested output.
    virtual bool has_lazy_read_target() const {
        return _read_requirement == ReadRequirement::LAZY_OUTPUT;
    }

protected:
    struct AccessPathSplit {
        TColumnAccessPaths descendant_paths;
        bool reads_current_data = false;
        MetaReadMode current_meta_mode = MetaReadMode::DEFAULT;

        bool has_descendant_paths() const { return !descendant_paths.empty(); }
    };

    // Nested columns share the same current-level access-path planning, while their data-child
    // topology and descendant routing remain container-specific.
    struct NestedAccessPathPlan {
        AccessPathSplit all;
        AccessPathSplit predicate;
        bool skip_data_descendants = false;
    };

    // At their current level, Struct supports null-map metadata. Map and Array additionally
    // support offsets.
    enum class NestedMetaSupport { NULL_MAP, NULL_MAP_AND_OFFSET };

    void _convert_to_place_holder_column(MutableColumnPtr& dst, size_t count);

    void _recovery_from_place_holder_column(MutableColumnPtr& dst);

    // Derive current-level meta-only read mode from an explicit access-path split. Meta-only is
    // valid only when this iterator had no data-read requirement before applying the current paths,
    // no current DATA path exists, and no path must be routed to a descendant iterator.
    Status _check_and_set_meta_read_mode(ReadRequirement requirement_before_access_path,
                                         const AccessPathSplit& all_access_paths);

    // Apply the common current-level access-path state transitions and select a supported
    // parent-owned meta-only mode. When that mode skips data descendants, synchronously invoke the
    // callback once with SKIP before returning the routing plan. The callback is never retained.
    Result<NestedAccessPathPlan> _prepare_nested_access_paths(
            const TColumnAccessPaths& all_access_paths,
            const TColumnAccessPaths& predicate_access_paths, NestedMetaSupport meta_support,
            const std::function<void(ReadRequirement)>& set_all_data_descendants_read_requirement);

    // Normalize the wire encoding, strip this iterator's column name, and explicitly partition
    // paths consumed by this iterator from paths that must be routed to descendants. This helper is
    // intentionally side-effect free; callers apply DATA/predicate read requirements explicitly.
    Result<AccessPathSplit> _split_access_paths(TColumnAccessPaths access_paths) const;
    ColumnIteratorOptions _opts;

    ReadRequirement _read_requirement {ReadRequirement::NORMAL};
    MetaReadMode _meta_read_mode = MetaReadMode::DEFAULT;
    ReadPhase _read_phase {ReadPhase::NORMAL};
    std::string _column_name;

    bool _has_place_holder_column {false};
};

// This iterator is used to read column data from file
// for scalar type
class FileColumnIterator : public ColumnIterator {
public:
    explicit FileColumnIterator(std::shared_ptr<ColumnReader> reader);
    ~FileColumnIterator() override;

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_ordinal(ordinal_t ord) override;

    Status seek_to_page_start();

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override;

    Status next_batch_of_zone_map(size_t* n, MutableColumnPtr& dst) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override;

    Status set_access_paths(const TColumnAccessPaths& all_access_paths,
                            const TColumnAccessPaths& predicate_access_paths) override;

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    // get row ranges by zone map
    // - cond_column is user's query predicate
    // - delete_condition is delete predicate of one version
    Status get_row_ranges_by_zone_map(
            const AndBlockColumnPredicate* col_predicates,
            const std::vector<std::shared_ptr<const ColumnPredicate>>* delete_predicates,
            RowRanges* row_ranges) override;

    Status get_row_ranges_by_bloom_filter(const AndBlockColumnPredicate* col_predicates,
                                          RowRanges* row_ranges) override;

    Status get_row_ranges_by_dict(const AndBlockColumnPredicate* col_predicates,
                                  RowRanges* row_ranges) override;

    ParsedPage* get_current_page() { return &_page; }

    bool is_nullable() { return _reader->is_nullable(); }

    bool is_all_dict_encoding() const override { return _is_all_dict_encoding; }

    Status init_prefetcher(const SegmentPrefetchParams& params) override;
    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override;

protected:
    // Exposed to derived iterators (e.g. StringFileColumnIterator) so they can
    // query column metadata such as the storage field type.
    const std::shared_ptr<ColumnReader>& get_reader() const { return _reader; }

private:
    Status _seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page) const;
    Status _load_next_page(bool* eos);
    Status _read_data_page(const OrdinalPageIndexIterator& iter);
    Status _read_dict_data();
    void _trigger_prefetch_if_eligible(ordinal_t ord);

    std::shared_ptr<ColumnReader> _reader = nullptr;

    BlockCompressionCodec* _compress_codec = nullptr;

    // 1. The _page represents current page.
    // 2. We define an operation is one seek and following read,
    //    If new seek is issued, the _page will be reset.
    ParsedPage _page;

    // keep dict page decoder
    std::unique_ptr<PageDecoder> _dict_decoder;

    // keep dict page handle to avoid released
    PageHandle _dict_page_handle;

    // page iterator used to get next page when current page is finished.
    // This value will be reset when a new seek is issued
    OrdinalPageIndexIterator _page_iter;

    // current value ordinal
    ordinal_t _current_ordinal = 0;

    bool _is_all_dict_encoding = false;

    std::unique_ptr<StringRef[]> _dict_word_info;

    bool _enable_prefetch {false};
    std::unique_ptr<SegmentPrefetcher> _prefetcher;
    std::shared_ptr<io::CachedRemoteFileReader> _cached_remote_file_reader {nullptr};
};

class EmptyFileColumnIterator final : public ColumnIterator {
public:
    Status seek_to_ordinal(ordinal_t ord) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }
};

// StringFileColumnIterator extends FileColumnIterator's NULL metadata support with OFFSET-only
// reading for string/binary column types. When the OFFSET path is detected in set_access_paths, it
// sets only_read_offsets on the ColumnIteratorOptions so that the BinaryPlainPageDecoder skips
// chars memcpy and only fills offsets.
class StringFileColumnIterator final : public FileColumnIterator {
public:
    explicit StringFileColumnIterator(std::shared_ptr<ColumnReader> reader);
    ~StringFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status set_access_paths(const TColumnAccessPaths& all_access_paths,
                            const TColumnAccessPaths& predicate_access_paths) override;
};

// This iterator make offset operation write once for
class OffsetFileColumnIterator final : public ColumnIterator {
public:
    explicit OffsetFileColumnIterator(FileColumnIteratorUPtr offset_reader) {
        _offset_iterator = std::move(offset_reader);
    }

    ~OffsetFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override;

    Status next_batch(size_t* n, MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    ordinal_t get_current_ordinal() const override {
        return _offset_iterator->get_current_ordinal();
    }
    Status seek_to_ordinal(ordinal_t ord) override {
        RETURN_IF_ERROR(_offset_iterator->seek_to_ordinal(ord));
        return Status::OK();
    }

    Status _peek_one_offset(ordinal_t* offset);

    Status _calculate_offsets(ssize_t start, ColumnArray::ColumnOffsets& column_offsets);

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        return _offset_iterator->read_by_rowids(rowids, count, dst);
    }

    void set_read_requirement(ReadRequirement requirement) override {
        set_read_requirement_self(requirement);
        _offset_iterator->set_read_requirement(requirement);
    }

    Status init_prefetcher(const SegmentPrefetchParams& params) override;
    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override;

private:
    std::unique_ptr<FileColumnIterator> _offset_iterator;
    // reuse a tiny column for peek to avoid frequent allocations
    MutableColumnPtr _peek_tmp_col;
};

// This iterator is used to read map value column
class MapFileColumnIterator final : public ColumnIterator {
public:
    explicit MapFileColumnIterator(std::shared_ptr<ColumnReader> reader,
                                   ColumnIteratorUPtr null_iterator,
                                   OffsetFileColumnIteratorUPtr offsets_iterator,
                                   ColumnIteratorUPtr key_iterator,
                                   ColumnIteratorUPtr val_iterator);

    ~MapFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override;

    Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override {
        if (read_null_map_only() && _null_iterator) {
            return _null_iterator->get_current_ordinal();
        }
        return _offsets_iterator->get_current_ordinal();
    }
    Status init_prefetcher(const SegmentPrefetchParams& params) override;
    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override;

    Status set_access_paths(const TColumnAccessPaths& all_access_paths,
                            const TColumnAccessPaths& predicate_access_paths) override;

    void set_lazy_output_requirement() override;

    void remove_pruned_sub_iterators() override;

    void set_read_phase(ReadPhase mode) override;

    bool need_to_read() const override {
        switch (_read_phase) {
        case ReadPhase::NORMAL:
            return _read_requirement != ReadRequirement::SKIP;
        case ReadPhase::PREDICATE:
            return _read_requirement == ReadRequirement::PREDICATE;
        case ReadPhase::LAZY:
            // In lazy mode, read this map only when at least one key/value branch still
            // has non-predicate data to materialize.
            return has_lazy_read_target();
        default:
            return false;
        }
    }

    void finalize_lazy_phase(MutableColumnPtr& dst) override;

    void set_read_requirement(ReadRequirement requirement) override;

    bool has_lazy_read_target() const override;

private:
    std::shared_ptr<ColumnReader> _map_reader = nullptr;
    ColumnIteratorUPtr _null_iterator;
    OffsetFileColumnIteratorUPtr _offsets_iterator; //OffsetFileIterator
    ColumnIteratorUPtr _key_iterator;
    ColumnIteratorUPtr _val_iterator;
};

class StructFileColumnIterator final : public ColumnIterator {
public:
    explicit StructFileColumnIterator(std::shared_ptr<ColumnReader> reader,
                                      ColumnIteratorUPtr null_iterator,
                                      std::vector<ColumnIteratorUPtr>&& sub_column_iterators);

    ~StructFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override;

    Status next_batch(size_t* n, MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override;

    Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override {
        if (read_null_map_only() && _null_iterator) {
            return _null_iterator->get_current_ordinal();
        }
        return _sub_column_iterators[0]->get_current_ordinal();
    }

    Status set_access_paths(const TColumnAccessPaths& all_access_paths,
                            const TColumnAccessPaths& predicate_access_paths) override;

    void set_lazy_output_requirement() override;

    void remove_pruned_sub_iterators() override;

    Status init_prefetcher(const SegmentPrefetchParams& params) override;
    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override;

    void set_read_phase(ReadPhase mode) override;

    bool need_to_read() const override {
        switch (_read_phase) {
        case ReadPhase::NORMAL:
            return _read_requirement != ReadRequirement::SKIP;
        case ReadPhase::PREDICATE:
            return _read_requirement == ReadRequirement::PREDICATE;
        case ReadPhase::LAZY:
            // In lazy mode, read this struct only when at least one nested branch still
            // has non-predicate data to materialize.
            return has_lazy_read_target();
        default:
            return false;
        }
    }

    void finalize_lazy_phase(MutableColumnPtr& dst) override;
    void set_read_requirement(ReadRequirement requirement) override;
    bool has_lazy_read_target() const override;

private:
    std::shared_ptr<ColumnReader> _struct_reader = nullptr;
    ColumnIteratorUPtr _null_iterator;
    std::vector<ColumnIteratorUPtr> _sub_column_iterators;
};

class ArrayFileColumnIterator final : public ColumnIterator {
public:
    explicit ArrayFileColumnIterator(std::shared_ptr<ColumnReader> reader,
                                     OffsetFileColumnIteratorUPtr offset_reader,
                                     ColumnIteratorUPtr item_iterator,
                                     ColumnIteratorUPtr null_iterator);

    ~ArrayFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override;

    Status next_batch(size_t* n, MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override;

    Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override {
        if (read_null_map_only() && _null_iterator) {
            return _null_iterator->get_current_ordinal();
        }
        return _offset_iterator->get_current_ordinal();
    }

    Status set_access_paths(const TColumnAccessPaths& all_access_paths,
                            const TColumnAccessPaths& predicate_access_paths) override;
    void set_lazy_output_requirement() override;

    void remove_pruned_sub_iterators() override;

    Status init_prefetcher(const SegmentPrefetchParams& params) override;
    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override;

    void set_read_phase(ReadPhase mode) override;

    bool need_to_read() const override {
        switch (_read_phase) {
        case ReadPhase::NORMAL:
            return _read_requirement != ReadRequirement::SKIP;
        case ReadPhase::PREDICATE:
            return _read_requirement == ReadRequirement::PREDICATE;
        case ReadPhase::LAZY:
            // In lazy mode, read this array only when its item branch still has
            // non-predicate data to materialize.
            return has_lazy_read_target();
        default:
            return false;
        }
    }

    void finalize_lazy_phase(MutableColumnPtr& dst) override;

    void set_read_requirement(ReadRequirement requirement) override;

    bool has_lazy_read_target() const override;

private:
    std::shared_ptr<ColumnReader> _array_reader = nullptr;
    std::unique_ptr<OffsetFileColumnIterator> _offset_iterator;
    std::unique_ptr<ColumnIterator> _null_iterator;
    std::unique_ptr<ColumnIterator> _item_iterator;

    Status _seek_by_offsets(ordinal_t ord);
};

class RowIdColumnIterator : public ColumnIterator {
public:
    RowIdColumnIterator() = delete;
    RowIdColumnIterator(int64_t tid, RowsetId rid, int32_t segid)
            : _tablet_id(tid), _rowset_id(rid), _segment_id(segid) {}

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = cast_set<uint32_t>(ord_idx);
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        for (size_t i = 0; i < *n; ++i) {
            const auto row_id = cast_set<uint32_t>(_current_rowid + i);
            GlobalRowLoacation location(_tablet_id, _rowset_id, _segment_id, row_id);
            dst->insert_data(reinterpret_cast<const char*>(&location), sizeof(GlobalRowLoacation));
        }
        _current_rowid += *n;
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        for (size_t i = 0; i < count; ++i) {
            rowid_t row_id = rowids[i];
            GlobalRowLoacation location(_tablet_id, _rowset_id, _segment_id, row_id);
            dst->insert_data(reinterpret_cast<const char*>(&location), sizeof(GlobalRowLoacation));
        }
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

private:
    rowid_t _current_rowid = 0;
    int64_t _tablet_id = 0;
    RowsetId _rowset_id;
    int32_t _segment_id = 0;
};

// Add new RowIdColumnIteratorV2
class RowIdColumnIteratorV2 : public ColumnIterator {
public:
    RowIdColumnIteratorV2(uint8_t version, int64_t backend_id, uint32_t file_id)
            : _version(version), _backend_id(backend_id), _file_id(file_id) {}

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = cast_set<uint32_t>(ord_idx);
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

private:
    uint32_t _current_rowid = 0;
    uint8_t _version;
    int64_t _backend_id;
    uint32_t _file_id;
};

// This iterator is used to read default value column
class DefaultValueColumnIterator : public ColumnIterator {
public:
    DefaultValueColumnIterator(bool has_default_value, std::string default_value, bool is_nullable,
                               FieldType type, int precision, int scale, int len)
            : _has_default_value(has_default_value),
              _default_value(std::move(default_value)),
              _is_nullable(is_nullable),
              _type(type),
              _precision(precision),
              _scale(scale),
              _len(len) {}

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = ord_idx;
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override;

    Status next_batch_of_zone_map(size_t* n, MutableColumnPtr& dst) override {
        return next_batch(n, dst);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

private:
    void _insert_many_default(MutableColumnPtr& dst, size_t n);

    bool _has_default_value;
    std::string _default_value;
    bool _is_nullable;
    FieldType _type;
    int _precision;
    int _scale;
    const int _len;
    Field _default_value_field;

    // current rowid
    ordinal_t _current_rowid = 0;
};

// Produces a column whose every row is the same constant Field value.
// Used for read-time-filled constant hidden columns (e.g. __DORIS_COMMIT_TSO_COL__),
// where the on-disk value is only a placeholder and the real value comes from the read
// context (StorageReadOptions).
class ConstantColumnIterator : public ColumnIterator {
public:
    ConstantColumnIterator() = delete;
    explicit ConstantColumnIterator(Field value) : _value(std::move(value)) {}

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = ord_idx;
        return Status::OK();
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    Status next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) override {
        *has_null = _value.is_null();
        Status st = _insert_many(dst, *n);
        if (!st.ok()) {
            return st;
        }
        _current_rowid += *n;
        return st;
    }

    Status next_batch_of_zone_map(size_t* n, MutableColumnPtr& dst) override {
        return next_batch(n, dst);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        return _insert_many(dst, count);
    }

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

private:
    Status _insert_many(MutableColumnPtr& dst, size_t n) {
        if (_value.is_null()) {
            if (UNLIKELY(!dst->is_nullable())) {
                return Status::InternalError(
                        "try to apply constant null value to not nullable target column");
            }
            dst->insert_many_defaults(n);
            return Status::OK();
        }
        dst->insert_duplicate_fields(_value, n);
        return Status::OK();
    }

    Field _value;
    ordinal_t _current_rowid = 0;
};

// A ColumnReader that represents a single constant value for the whole segment instead of reading
// on-disk data. Used for read-time-filled constant columns (e.g. __DORIS_COMMIT_TSO_COL__) on a
// single-version segment, whose on-disk zonemap only reflects the placeholder. It advertises a
// single-value [v, v] zonemap so segment-level pruning matches against the real value, and produces
// a ConstantColumnIterator for data reads.
class ConstantColumnReader : public ColumnReader {
public:
    explicit ConstantColumnReader(Field value) : _value(std::move(value)) {}

    bool has_zone_map() const override { return true; }

    // The base ColumnReader default-constructs without initializing its _meta_type. The data-read
    // path (Segment::new_column_iterator) verifies tablet_column.type() == reader->get_meta_type()
    // when config::enable_column_type_check is on (default true), so derive the real OLAP type from
    // the constant value to avoid a spurious "different type between schema and column reader" error.
    FieldType get_meta_type() override {
        return primitive_type_to_storage_field_type(_value.get_type());
    }

    Status match_condition(const AndBlockColumnPredicate* col_predicates,
                           bool* matched) const override;

    Status new_iterator(ColumnIteratorUPtr* iterator, const TabletColumn* /*col*/,
                        const StorageReadOptions* /*opt*/) override {
        *iterator = std::make_unique<ConstantColumnIterator>(_value);
        return Status::OK();
    }

    Status get_segment_zone_map(segment_v2::ZoneMap* zone_map) const override;

private:
    Field _value;
};

} // namespace segment_v2
} // namespace doris

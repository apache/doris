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

#include <cstddef> // for size_t
#include <cstdint> // for uint32_t
#include <memory>  // for unique_ptr
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h" // for Status
#include "io/fs/file_reader_writer_fwd.h"
#include "io/io_common.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h" // for OrdinalPageIndexIterator
#include "olap/rowset/segment_v2/page_handle.h"        // for PageHandle
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/rowset/segment_v2/parsed_page.h" // for ParsedPage
#include "olap/types.h"
#include "olap/utils.h"
#include "util/once.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h" // ColumnArray
#include "vec/data_types/data_type.h"

namespace doris {

class BlockCompressionCodec;
class WrapperField;
class AndBlockColumnPredicate;
class ColumnPredicate;
class TabletIndex;
class StorageReadOptions;

namespace io {
class FileReader;
} // namespace io
struct Slice;
struct StringRef;

namespace segment_v2 {

class EncodingInfo;
class ColumnIterator;
class BloomFilterIndexReader;
class BitmapIndexIterator;
class BitmapIndexReader;
class InvertedIndexIterator;
class InvertedIndexReader;
class PageDecoder;
class RowRanges;
class ZoneMapIndexReader;

struct ColumnReaderOptions {
    // whether verify checksum when read page
    bool verify_checksum = true;
    // for in memory olap table, use DURABLE CachePriority in page cache
    bool kept_in_memory = false;
};

struct ColumnIteratorOptions {
    io::FileReader* file_reader = nullptr;
    // reader statistics
    OlapReaderStatistics* stats = nullptr;
    bool use_page_cache = false;
    // for page cache allocation
    // page types are divided into DATA_PAGE & INDEX_PAGE
    // INDEX_PAGE including index_page, dict_page and short_key_page
    PageTypePB type;
    io::IOContext io_ctx;
    bool is_predicate_column = false;

    void sanity_check() const {
        CHECK_NOTNULL(file_reader);
        CHECK_NOTNULL(stats);
    }
};

// There will be concurrent users to read the same column. So
// we should do our best to reduce resource usage through share
// same information, such as OrdinalPageIndex and Page data.
// This will cache data shared by all reader
class ColumnReader {
public:
    // Create an initialized ColumnReader in *reader.
    // This should be a lightweight operation without I/O.
    static Status create(const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                         uint64_t num_rows, const io::FileReaderSPtr& file_reader,
                         std::unique_ptr<ColumnReader>* reader);

    enum DictEncodingType { UNKNOWN_DICT_ENCODING, PARTIAL_DICT_ENCODING, ALL_DICT_ENCODING };

    ~ColumnReader();

    // create a new column iterator. Client should delete returned iterator
    Status new_iterator(ColumnIterator** iterator);
    // Client should delete returned iterator
    Status new_bitmap_index_iterator(BitmapIndexIterator** iterator);

    Status new_inverted_index_iterator(const TabletIndex* index_meta,
                                       const StorageReadOptions& read_options,
                                       std::unique_ptr<InvertedIndexIterator>* iterator);

    // Seek to the first entry in the column.
    Status seek_to_first(OrdinalPageIndexIterator* iter);
    Status seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter);

    // read a page from file into a page handle
    Status read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp,
                     PageHandle* handle, Slice* page_body, PageFooterPB* footer,
                     BlockCompressionCodec* codec) const;

    bool is_nullable() const { return _meta_is_nullable; }

    const EncodingInfo* encoding_info() const { return _encoding_info; }

    bool has_zone_map() const { return _zone_map_index != nullptr; }
    bool has_bitmap_index() const { return _bitmap_index != nullptr; }
    bool has_bloom_filter_index(bool ngram) const;
    // Check if this column could match `cond' using segment zone map.
    // Since segment zone map is stored in metadata, this function is fast without I/O.
    // Return true if segment zone map is absent or `cond' could be satisfied, false otherwise.
    bool match_condition(const AndBlockColumnPredicate* col_predicates) const;

    Status next_batch_of_zone_map(size_t* n, vectorized::MutableColumnPtr& dst) const;

    // get row ranges with zone map
    // - cond_column is user's query predicate
    // - delete_condition is a delete predicate of one version
    Status get_row_ranges_by_zone_map(const AndBlockColumnPredicate* col_predicates,
                                      const std::vector<const ColumnPredicate*>* delete_predicates,
                                      RowRanges* row_ranges);

    // get row ranges with bloom filter index
    Status get_row_ranges_by_bloom_filter(const AndBlockColumnPredicate* col_predicates,
                                          RowRanges* row_ranges);

    PagePointer get_dict_page_pointer() const { return _meta_dict_page; }

    bool is_empty() const { return _num_rows == 0; }

    bool prune_predicates_by_zone_map(std::vector<ColumnPredicate*>& predicates,
                                      const int column_id) const;

    CompressionTypePB get_compression() const { return _meta_compression; }

    uint64_t num_rows() const { return _num_rows; }

    void set_dict_encoding_type(DictEncodingType type) {
        _set_dict_encoding_type_once.call([&] {
            _dict_encoding_type = type;
            return Status::OK();
        });
    }

    DictEncodingType get_dict_encoding_type() { return _dict_encoding_type; }

    void disable_index_meta_cache() { _use_index_page_cache = false; }

private:
    ColumnReader(const ColumnReaderOptions& opts, const ColumnMetaPB& meta, uint64_t num_rows,
                 io::FileReaderSPtr file_reader);
    Status init(const ColumnMetaPB* meta);

    // Read column inverted indexes into memory
    // May be called multiple times, subsequent calls will no op.
    Status _ensure_inverted_index_loaded(const TabletIndex* index_meta) {
        // load inverted index only if not loaded or index_id is changed
        RETURN_IF_ERROR(_load_inverted_index_index(index_meta));
        return Status::OK();
    }

    [[nodiscard]] Status _load_zone_map_index(bool use_page_cache, bool kept_in_memory);
    [[nodiscard]] Status _load_ordinal_index(bool use_page_cache, bool kept_in_memory);
    [[nodiscard]] Status _load_bitmap_index(bool use_page_cache, bool kept_in_memory);
    [[nodiscard]] Status _load_inverted_index_index(const TabletIndex* index_meta);
    [[nodiscard]] Status _load_bloom_filter_index(bool use_page_cache, bool kept_in_memory);

    bool _zone_map_match_condition(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                   WrapperField* max_value_container,
                                   const AndBlockColumnPredicate* col_predicates) const;

    void _parse_zone_map(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                         WrapperField* max_value_container) const;

    void _parse_zone_map_skip_null(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                   WrapperField* max_value_container) const;

    Status _get_filtered_pages(const AndBlockColumnPredicate* col_predicates,
                               const std::vector<const ColumnPredicate*>* delete_predicates,
                               std::vector<uint32_t>* page_indexes);

    Status _calculate_row_ranges(const std::vector<uint32_t>& page_indexes, RowRanges* row_ranges);

private:
    int64_t _meta_length;
    FieldType _meta_type;
    FieldType _meta_children_column_type;
    bool _meta_is_nullable;
    bool _use_index_page_cache;

    PagePointer _meta_dict_page;
    CompressionTypePB _meta_compression;

    ColumnReaderOptions _opts;
    uint64_t _num_rows;

    io::FileReaderSPtr _file_reader;

    DictEncodingType _dict_encoding_type;

    TypeInfoPtr _type_info =
            TypeInfoPtr(nullptr, nullptr); // initialized in init(), may changed by subclasses.
    const EncodingInfo* _encoding_info =
            nullptr; // initialized in init(), used for create PageDecoder

    // meta for various column indexes (null if the index is absent)
    std::unique_ptr<ZoneMapPB> _segment_zone_map;

    mutable std::mutex _load_index_lock;
    std::unique_ptr<ZoneMapIndexReader> _zone_map_index;
    std::unique_ptr<OrdinalIndexReader> _ordinal_index;
    std::unique_ptr<BitmapIndexReader> _bitmap_index;
    std::shared_ptr<InvertedIndexReader> _inverted_index;
    std::shared_ptr<BloomFilterIndexReader> _bloom_filter_index;

    std::vector<std::unique_ptr<ColumnReader>> _sub_readers;

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

    // Seek to the first entry in the column.
    virtual Status seek_to_first() = 0;

    // Seek to the given ordinal entry in the column.
    // Entry 0 is the first entry written to the column.
    // If provided seek point is past the end of the file,
    // then returns false.
    virtual Status seek_to_ordinal(ordinal_t ord) = 0;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    virtual Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) {
        return Status::NotSupported("next_batch not implement");
    }

    virtual Status next_batch_of_zone_map(size_t* n, vectorized::MutableColumnPtr& dst) {
        return Status::NotSupported("next_batch_of_zone_map not implement");
    }

    virtual Status read_by_rowids(const rowid_t* rowids, const size_t count,
                                  vectorized::MutableColumnPtr& dst) {
        return Status::NotSupported("read_by_rowids not implement");
    }

    virtual ordinal_t get_current_ordinal() const = 0;

    virtual Status get_row_ranges_by_zone_map(
            const AndBlockColumnPredicate* col_predicates,
            const std::vector<const ColumnPredicate*>* delete_predicates, RowRanges* row_ranges) {
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

protected:
    ColumnIteratorOptions _opts;
};

// This iterator is used to read column data from file
// for scalar type
class FileColumnIterator final : public ColumnIterator {
public:
    explicit FileColumnIterator(ColumnReader* reader);
    ~FileColumnIterator() override;

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    Status seek_to_page_start();

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status next_batch_of_zone_map(size_t* n, vectorized::MutableColumnPtr& dst) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    // get row ranges by zone map
    // - cond_column is user's query predicate
    // - delete_condition is delete predicate of one version
    Status get_row_ranges_by_zone_map(const AndBlockColumnPredicate* col_predicates,
                                      const std::vector<const ColumnPredicate*>* delete_predicates,
                                      RowRanges* row_ranges) override;

    Status get_row_ranges_by_bloom_filter(const AndBlockColumnPredicate* col_predicates,
                                          RowRanges* row_ranges) override;

    Status get_row_ranges_by_dict(const AndBlockColumnPredicate* col_predicates,
                                  RowRanges* row_ranges) override;

    ParsedPage* get_current_page() { return &_page; }

    bool is_nullable() { return _reader->is_nullable(); }

    bool is_all_dict_encoding() const override { return _is_all_dict_encoding; }

private:
    void _seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page) const;
    Status _load_next_page(bool* eos);
    Status _read_data_page(const OrdinalPageIndexIterator& iter);
    Status _read_dict_data();

    ColumnReader* _reader;

    // iterator owned compress codec, should NOT be shared by threads, initialized in init()
    BlockCompressionCodec* _compress_codec;

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
};

class EmptyFileColumnIterator final : public ColumnIterator {
public:
    Status seek_to_first() override { return Status::OK(); }
    Status seek_to_ordinal(ordinal_t ord) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }
};

// This iterator make offset operation write once for
class OffsetFileColumnIterator final : public ColumnIterator {
public:
    explicit OffsetFileColumnIterator(FileColumnIterator* offset_reader) {
        _offset_iterator.reset(offset_reader);
    }

    ~OffsetFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;
    ordinal_t get_current_ordinal() const override {
        return _offset_iterator->get_current_ordinal();
    }
    Status seek_to_ordinal(ordinal_t ord) override {
        RETURN_IF_ERROR(_offset_iterator->seek_to_ordinal(ord));
        return Status::OK();
    }

    Status seek_to_first() override {
        RETURN_IF_ERROR(_offset_iterator->seek_to_first());
        return Status::OK();
    }

    Status _peek_one_offset(ordinal_t* offset);

    Status _calculate_offsets(ssize_t start,
                              vectorized::ColumnArray::ColumnOffsets& column_offsets);

private:
    std::unique_ptr<FileColumnIterator> _offset_iterator;
};

// This iterator is used to read map value column
class MapFileColumnIterator final : public ColumnIterator {
public:
    explicit MapFileColumnIterator(ColumnReader* reader, ColumnIterator* null_iterator,
                                   OffsetFileColumnIterator* offsets_iterator,
                                   ColumnIterator* key_iterator, ColumnIterator* val_iterator);

    ~MapFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;
    Status seek_to_first() override {
        RETURN_IF_ERROR(_key_iterator->seek_to_first());
        RETURN_IF_ERROR(_val_iterator->seek_to_first());
        RETURN_IF_ERROR(_offsets_iterator->seek_to_first());
        if (_map_reader->is_nullable()) {
            RETURN_IF_ERROR(_null_iterator->seek_to_first());
        }
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override {
        return _offsets_iterator->get_current_ordinal();
    }

private:
    ColumnReader* _map_reader;
    std::unique_ptr<ColumnIterator> _null_iterator;
    std::unique_ptr<OffsetFileColumnIterator> _offsets_iterator; //OffsetFileIterator
    std::unique_ptr<ColumnIterator> _key_iterator;
    std::unique_ptr<ColumnIterator> _val_iterator;
};

class StructFileColumnIterator final : public ColumnIterator {
public:
    explicit StructFileColumnIterator(ColumnReader* reader, ColumnIterator* null_iterator,
                                      std::vector<ColumnIterator*>& sub_column_iterators);

    ~StructFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    Status seek_to_first() override {
        for (auto& column_iterator : _sub_column_iterators) {
            RETURN_IF_ERROR(column_iterator->seek_to_first());
        }
        if (_struct_reader->is_nullable()) {
            RETURN_IF_ERROR(_null_iterator->seek_to_first());
        }
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override {
        return _sub_column_iterators[0]->get_current_ordinal();
    }

private:
    ColumnReader* _struct_reader;
    std::unique_ptr<ColumnIterator> _null_iterator;
    std::vector<std::unique_ptr<ColumnIterator>> _sub_column_iterators;
};

class ArrayFileColumnIterator final : public ColumnIterator {
public:
    explicit ArrayFileColumnIterator(ColumnReader* reader, OffsetFileColumnIterator* offset_reader,
                                     ColumnIterator* item_iterator, ColumnIterator* null_iterator);

    ~ArrayFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    Status seek_to_first() override {
        RETURN_IF_ERROR(_offset_iterator->seek_to_first());
        RETURN_IF_ERROR(_item_iterator->seek_to_first()); // lazy???
        if (_array_reader->is_nullable()) {
            RETURN_IF_ERROR(_null_iterator->seek_to_first());
        }
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord) override;

    ordinal_t get_current_ordinal() const override {
        return _offset_iterator->get_current_ordinal();
    }

private:
    ColumnReader* _array_reader;
    std::unique_ptr<OffsetFileColumnIterator> _offset_iterator;
    std::unique_ptr<ColumnIterator> _null_iterator;
    std::unique_ptr<ColumnIterator> _item_iterator;

    Status _seek_by_offsets(ordinal_t ord);
};

class RowIdColumnIterator : public ColumnIterator {
public:
    RowIdColumnIterator() = delete;
    RowIdColumnIterator(int32_t tid, RowsetId rid, int32_t segid)
            : _tablet_id(tid), _rowset_id(rid), _segment_id(segid) {}

    Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = ord_idx;
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override {
        for (size_t i = 0; i < *n; ++i) {
            rowid_t row_id = _current_rowid + i;
            GlobalRowLoacation location(_tablet_id, _rowset_id, _segment_id, row_id);
            dst->insert_data(reinterpret_cast<const char*>(&location), sizeof(GlobalRowLoacation));
        }
        _current_rowid += *n;
        return Status::OK();
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override {
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
    int32_t _tablet_id = 0;
    RowsetId _rowset_id;
    int32_t _segment_id = 0;
};

// This iterator is used to read default value column
class DefaultValueColumnIterator : public ColumnIterator {
public:
    DefaultValueColumnIterator(bool has_default_value, const std::string& default_value,
                               bool is_nullable, TypeInfoPtr type_info, int precision, int scale)
            : _has_default_value(has_default_value),
              _default_value(default_value),
              _is_nullable(is_nullable),
              _type_info(std::move(type_info)),
              _is_default_value_null(false),
              _type_size(0),
              _precision(precision),
              _scale(scale) {}

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = ord_idx;
        return Status::OK();
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status next_batch_of_zone_map(size_t* n, vectorized::MutableColumnPtr& dst) override {
        return next_batch(n, dst);
    }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

    static void insert_default_data(const TypeInfo* type_info, size_t type_size, void* mem_value,
                                    vectorized::MutableColumnPtr& dst, size_t n);

private:
    void _insert_many_default(vectorized::MutableColumnPtr& dst, size_t n);

    bool _has_default_value;
    std::string _default_value;
    bool _is_nullable;
    TypeInfoPtr _type_info;
    bool _is_default_value_null;
    size_t _type_size;
    int _precision;
    int _scale;
    std::vector<char> _mem_value;

    // current rowid
    ordinal_t _current_rowid = 0;
};

} // namespace segment_v2
} // namespace doris

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

#include <cstdint> // for uint32_t
#include <cstddef> // for size_t
#include <memory> // for unique_ptr

#include "common/logging.h"
#include "common/status.h" // for Status
#include "gen_cpp/segment_v2.pb.h" // for ColumnMetaPB
#include "olap/olap_cond.h" // for CondColumn
#include "olap/tablet_schema.h"
#include "olap/rowset/segment_v2/bitmap_index_reader.h" // for BitmapIndexReader
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h" // for OrdinalPageIndexIterator
#include "olap/rowset/segment_v2/row_ranges.h" // for RowRanges
#include "olap/rowset/segment_v2/page_handle.h" // for PageHandle
#include "olap/rowset/segment_v2/parsed_page.h" // for ParsedPage
#include "olap/rowset/segment_v2/zone_map_index.h"
#include "util/once.h"
#include "util/file_cache.h"

namespace doris {

class ColumnBlock;
class TypeInfo;
class BlockCompressionCodec;
class WrapperField;

namespace fs {
class ReadableBlock;
}

namespace segment_v2 {

class EncodingInfo;
class PageHandle;
struct PagePointer;
class ColumnIterator;
class BloomFilterIndexReader;

struct ColumnReaderOptions {
    // whether verify checksum when read page
    bool verify_checksum = true;
    // for in memory olap table, use DURABLE CachePriority in page cache
    bool kept_in_memory = false;
};

struct ColumnIteratorOptions {
    fs::ReadableBlock* rblock = nullptr;
    // reader statistics
    OlapReaderStatistics* stats = nullptr;
    bool use_page_cache = false;

    void sanity_check() const {
        CHECK_NOTNULL(rblock);
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
    static Status create(const ColumnReaderOptions& opts,
                         const ColumnMetaPB& meta,
                         uint64_t num_rows,
                         const std::string& file_name,
                         std::unique_ptr<ColumnReader>* reader);

    ~ColumnReader();

    // create a new column iterator. Client should delete returned iterator
    Status new_iterator(ColumnIterator** iterator);
    // Client should delete returned iterator
    Status new_bitmap_index_iterator(BitmapIndexIterator** iterator);

    // Seek to the first entry in the column.
    Status seek_to_first(OrdinalPageIndexIterator* iter);
    Status seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter);

    // read a page from file into a page handle
    Status read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp,
                     PageHandle* handle, Slice* page_body, PageFooterPB* footer);

    bool is_nullable() const { return _meta.is_nullable(); }

    const EncodingInfo* encoding_info() const { return _encoding_info; }
    const TypeInfo* type_info() const { return _type_info; }

    bool has_zone_map() const { return _zone_map_index_meta != nullptr; }
    bool has_bitmap_index() const { return _bitmap_index_meta != nullptr; }
    bool has_bloom_filter_index() const { return _bf_index_meta != nullptr; }

    // Check if this column could match `cond' using segment zone map.
    // Since segment zone map is stored in metadata, this function is fast without I/O.
    // Return true if segment zone map is absent or `cond' could be satisfied, false otherwise.
    bool match_condition(CondColumn* cond) const;

    // get row ranges with zone map
    // - cond_column is user's query predicate
    // - delete_condition is a delete predicate of one version
    Status get_row_ranges_by_zone_map(CondColumn* cond_column,
                                      CondColumn* delete_condition,
                                      std::unordered_set<uint32_t>* delete_partial_filtered_pages,
                                      RowRanges* row_ranges);

    // get row ranges with bloom filter index
    Status get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges);

    PagePointer get_dict_page_pointer() const { return _meta.dict_page(); }

private:
    ColumnReader(const ColumnReaderOptions& opts,
                 const ColumnMetaPB& meta,
                 uint64_t num_rows,
                 const std::string& file_name);
    Status init();

    // Read and load necessary column indexes into memory if it hasn't been loaded.
    // May be called multiple times, subsequent calls will no op.
    Status _ensure_index_loaded() {
        return _load_index_once.call([this] {
            bool use_page_cache = !config::disable_storage_page_cache;
            RETURN_IF_ERROR(_load_zone_map_index(use_page_cache, _opts.kept_in_memory));
            RETURN_IF_ERROR(_load_ordinal_index(use_page_cache, _opts.kept_in_memory));
            RETURN_IF_ERROR(_load_bitmap_index(use_page_cache, _opts.kept_in_memory));
            RETURN_IF_ERROR(_load_bloom_filter_index(use_page_cache, _opts.kept_in_memory));
            return Status::OK();
        });
    }

    Status _load_zone_map_index(bool use_page_cache, bool kept_in_memory);
    Status _load_ordinal_index(bool use_page_cache, bool kept_in_memory);
    Status _load_bitmap_index(bool use_page_cache, bool kept_in_memory);
    Status _load_bloom_filter_index(bool use_page_cache, bool kept_in_memory);

    bool _zone_map_match_condition(const ZoneMapPB& zone_map,
                                   WrapperField* min_value_container,
                                   WrapperField* max_value_container,
                                   CondColumn* cond) const;

    void _parse_zone_map(const ZoneMapPB& zone_map,
                         WrapperField* min_value_container,
                         WrapperField* max_value_container) const;

    Status _get_filtered_pages(CondColumn* cond_column,
                               CondColumn* delete_conditions,
                               std::unordered_set<uint32_t>* delete_partial_filtered_pages,
                               std::vector<uint32_t>* page_indexes);

    Status _calculate_row_ranges(const std::vector<uint32_t>& page_indexes, RowRanges* row_ranges);

private:
    ColumnReaderOptions _opts;
    ColumnMetaPB _meta;
    uint64_t _num_rows;
    std::string _file_name;

    // initialized in init()
    const TypeInfo* _type_info = nullptr;
    const EncodingInfo* _encoding_info = nullptr;
    const BlockCompressionCodec* _compress_codec = nullptr;
    // meta for various column indexes (null if the index is absent)
    const ZoneMapIndexPB* _zone_map_index_meta = nullptr;
    const OrdinalIndexPB* _ordinal_index_meta = nullptr;
    const BitmapIndexPB* _bitmap_index_meta = nullptr;
    const BloomFilterIndexPB* _bf_index_meta = nullptr;

    DorisCallOnce<Status> _load_index_once;
    std::unique_ptr<ZoneMapIndexReader> _zone_map_index;
    std::unique_ptr<OrdinalIndexReader> _ordinal_index;
    std::unique_ptr<BitmapIndexReader> _bitmap_index;
    std::unique_ptr<BloomFilterIndexReader> _bloom_filter_index;
};

// Base iterator to read one column data
class ColumnIterator {
public:
    ColumnIterator() { }
    virtual ~ColumnIterator() { }

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

    // After one seek, we can call this function many times to read data
    // into ColumnBlockView. when read string type data, memory will allocated
    // from MemPool
    virtual Status next_batch(size_t* n, ColumnBlockView* dst) = 0;

    virtual ordinal_t get_current_ordinal() const = 0;

    virtual Status get_row_ranges_by_zone_map(CondColumn* cond_column,
                                              CondColumn* delete_condition,
                                              RowRanges* row_ranges) { return Status::OK(); }

    virtual Status get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) {
        return Status::OK();
    }

#if 0
    // Call this function every time before next_batch.
    // This function will preload pages from disk into memory if necessary.
    Status prepare_batch(size_t n);

    // Fetch the next vector of values from the page into 'dst'.
    // The output vector must have space for up to n cells.
    //
    // return the size of entries.
    //
    // In the case that the values are themselves references
    // to other memory (eg Slices), the referred-to memory is
    // allocated in the dst column vector's MemPool.
    Status scan(size_t* n, ColumnBlock* dst, MemPool* pool);

    // release next_batch related resource
    Status finish_batch();
#endif
protected:
    ColumnIteratorOptions _opts;
};

// This iterator is used to read column data from file
class FileColumnIterator : public ColumnIterator {
public:
    FileColumnIterator(ColumnReader* reader);
    ~FileColumnIterator() override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    Status next_batch(size_t* n, ColumnBlockView* dst) override;

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    // get row ranges by zone map
    // - cond_column is user's query predicate
    // - delete_condition is delete predicate of one version
    Status get_row_ranges_by_zone_map(CondColumn* cond_column,
                                      CondColumn* delete_condition,
                                      RowRanges* row_ranges) override;

    Status get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) override;

private:
    void _seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page);
    Status _load_next_page(bool* eos);
    Status _read_data_page(const OrdinalPageIndexIterator& iter);

private:
    ColumnReader* _reader;

    // 1. The _page represents current page.
    // 2. We define an operation is one seek and following read,
    //    If new seek is issued, the _page will be reset.
    // 3. When _page is null, it means that this reader can not be read.
    std::unique_ptr<ParsedPage> _page;

    // keep dict page decoder
    std::unique_ptr<PageDecoder> _dict_decoder;

    // keep dict page handle to avoid released
    PageHandle _dict_page_handle;

    // page iterator used to get next page when current page is finished.
    // This value will be reset when a new seek is issued
    OrdinalPageIndexIterator _page_iter;

    // current value ordinal
    ordinal_t _current_ordinal = 0;

    // page indexes those are DEL_PARTIAL_SATISFIED
    std::unordered_set<uint32_t> _delete_partial_statisfied_pages;
};

// This iterator is used to read default value column
class DefaultValueColumnIterator : public ColumnIterator {
public:
    DefaultValueColumnIterator(bool has_default_value, const std::string& default_value,
            bool is_nullable, FieldType type, size_t schema_length) : _has_default_value(has_default_value),
                                                _default_value(default_value),
                                                _is_nullable(is_nullable),
                                                _type(type),
                                                _schema_length(schema_length),
                                                _is_default_value_null(false),
                                                _type_size(0),
                                                _pool(new MemPool(&_tracker)){ }

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = ord_idx;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst) override;

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

private:
    bool _has_default_value;
    std::string _default_value;
    bool _is_nullable;
    FieldType _type;
    size_t _schema_length;
    bool _is_default_value_null;
    size_t _type_size;
    void* _mem_value = nullptr;
    MemTracker _tracker;
    std::unique_ptr<MemPool> _pool;

    // current rowid
    ordinal_t _current_rowid = 0;
};

}
}

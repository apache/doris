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

#include "common/status.h" // for Status
#include "gen_cpp/segment_v2.pb.h" // for ColumnMetaPB
#include "olap/olap_cond.h" // for CondColumn
#include "olap/tablet_schema.h"
#include "olap/rowset/segment_v2/bitmap_index_reader.h" // for BitmapIndexReader
#include "olap/rowset/segment_v2/common.h" // for rowid_t
#include "olap/rowset/segment_v2/ordinal_page_index.h" // for OrdinalPageIndexIterator
#include "olap/rowset/segment_v2/column_zone_map.h" // for ColumnZoneMap
#include "olap/rowset/segment_v2/row_ranges.h" // for RowRanges
#include "olap/rowset/segment_v2/page_handle.h" // for PageHandle
#include "olap/rowset/segment_v2/parsed_page.h" // for ParsedPage
#include "util/once.h"

namespace doris {

class ColumnBlock;
class RandomAccessFile;
class TypeInfo;
class BlockCompressionCodec;

namespace segment_v2 {

class EncodingInfo;
class PageHandle;
class PagePointer;
class ColumnIterator;
class BloomFilterIndexReader;

struct ColumnReaderOptions {
    // whether verify checksum when read page
    bool verify_checksum = true;
};

struct ColumnIteratorOptions {
    // reader statistics
    OlapReaderStatistics* stats = nullptr;
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
                         RandomAccessFile* file,
                         std::unique_ptr<ColumnReader>* reader);

    ~ColumnReader();

    // create a new column iterator. Client should delete returned iterator
    Status new_iterator(ColumnIterator** iterator);
    // Client should delete returned iterator
    Status new_bitmap_index_iterator(BitmapIndexIterator** iterator);

    // Seek to the first entry in the column.
    Status seek_to_first(OrdinalPageIndexIterator* iter);
    Status seek_at_or_before(rowid_t rowid, OrdinalPageIndexIterator* iter);

    // read a page from file into a page handle
    Status read_page(const PagePointer& pp, OlapReaderStatistics* stats, PageHandle* handle);

    bool is_nullable() const { return _meta.is_nullable(); }

    const EncodingInfo* encoding_info() const { return _encoding_info; }

    const TypeInfo* type_info() const { return _type_info; }

    bool has_zone_map() const { return _meta.has_zone_map_page(); }

    bool has_bitmap_index() {
        return _meta.has_bitmap_index();
    }

    bool has_bloom_filter_index() {
        return _meta.has_bloom_filter_index();
    }

    // get row ranges with zone map
    // - cond_column is user's query predicate
    // - delete_conditions is a vector of delete predicate of different version
    Status get_row_ranges_by_zone_map(CondColumn* cond_column,
                                      const std::vector<CondColumn*>& delete_conditions,
                                      OlapReaderStatistics* stats,
                                      std::vector<uint32_t>* delete_partial_filtered_pages,
                                      RowRanges* row_ranges);

    // get row ranges with bloom filter index
    Status get_row_ranges_by_bloom_filter(CondColumn* cond_column,
            OlapReaderStatistics* stats, RowRanges* row_ranges);

    PagePointer get_dict_page_pointer() const { return _meta.dict_page(); }

private:
    ColumnReader(const ColumnReaderOptions& opts,
                 const ColumnMetaPB& meta,
                 uint64_t num_rows,
                 RandomAccessFile* file);
    Status init();

    // Read and load necessary column indexes into memory if it hasn't been loaded.
    // May be called multiple times, subsequent calls will no op.
    Status _ensure_index_loaded() {
        return _load_index_once.call([this] {
            RETURN_IF_ERROR(_load_zone_map_index());
            RETURN_IF_ERROR(_load_ordinal_index());
            RETURN_IF_ERROR(_load_bitmap_index());
            RETURN_IF_ERROR(_load_bloom_filter_index());
            return Status::OK();
        });
    }

    Status _load_zone_map_index();
    Status _load_ordinal_index();
    Status _load_bitmap_index();
    Status _load_bloom_filter_index();

    Status _get_filtered_pages(CondColumn* cond_column,
                               const std::vector<CondColumn*>& delete_conditions,
                               OlapReaderStatistics* stats,
                               std::vector<uint32_t>* delete_partial_filtered_pages,
                               std::vector<uint32_t>* page_indexes);

    Status _calculate_row_ranges(const std::vector<uint32_t>& page_indexes, RowRanges* row_ranges);

private:
    ColumnReaderOptions _opts;
    ColumnMetaPB _meta;
    uint64_t _num_rows;
    RandomAccessFile* _file;

    // initialized in init()
    const TypeInfo* _type_info = nullptr;
    const EncodingInfo* _encoding_info = nullptr;
    const BlockCompressionCodec* _compress_codec = nullptr;

    DorisCallOnce<Status> _load_index_once;
    std::unique_ptr<ColumnZoneMap> _column_zone_map;
    std::unique_ptr<OrdinalPageIndex> _ordinal_index;
    std::unique_ptr<BitmapIndexReader> _bitmap_index_reader;
    std::unique_ptr<BloomFilterIndexReader> _bloom_filter_index_reader;
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
    virtual Status seek_to_ordinal(rowid_t ord_idx) = 0;

    // After one seek, we can call this function many times to read data 
    // into ColumnBlockView. when read string type data, memory will allocated
    // from MemPool
    virtual Status next_batch(size_t* n, ColumnBlockView* dst) = 0;

    virtual rowid_t get_current_ordinal() const = 0;

    virtual Status get_row_ranges_by_conditions(CondColumn* cond_column,
                                                const std::vector<CondColumn*>& delete_conditions,
                                                RowRanges* row_ranges) { return Status::OK(); }

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

    Status seek_to_ordinal(rowid_t ord_idx) override;

    Status next_batch(size_t* n, ColumnBlockView* dst) override;

    rowid_t get_current_ordinal() const override { return _current_rowid; }

    // get row ranges by conditions
    // - cond_column is user's query predicate
    // - delete_conditions is a vector of delete predicate of different version
    Status get_row_ranges_by_conditions(CondColumn* cond_column,
                                      const std::vector<CondColumn*>& delete_conditions,
                                      RowRanges* row_ranges) override;

private:
    void _seek_to_pos_in_page(ParsedPage* page, uint32_t offset_in_page);
    Status _load_next_page(bool* eos);
    Status _read_page(const OrdinalPageIndexIterator& iter, ParsedPage* page);

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

    // current rowid
    rowid_t _current_rowid = 0;

    // page indexes those are DEL_PARTIAL_SATISFIED
    std::vector<uint32_t> _delete_partial_statisfied_pages;
};

// This iterator is used to read default value column
class DefaultValueColumnIterator : public ColumnIterator {
public:
    DefaultValueColumnIterator(bool has_default_value, const std::string& default_value,
            bool is_nullable, FieldType type) : _has_default_value(has_default_value),
                                                _default_value(default_value),
                                                _is_nullable(is_nullable),
                                                _type(type),
                                                _is_default_value_null(false),
                                                _value_size(0) { }

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    Status seek_to_ordinal(rowid_t ord_idx) override {
        _current_rowid = ord_idx;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst) override;

    rowid_t get_current_ordinal() const override { return _current_rowid; }

private:
    bool _has_default_value;
    std::string _default_value;
    bool _is_nullable;
    FieldType _type;
    bool _is_default_value_null;
    size_t _value_size;
    faststring _mem_value;

    // current rowid
    rowid_t _current_rowid = 0;
};

}
}

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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <functional>
#include <memory> // for unique_ptr
#include <ostream>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h" // for Status
#include "core/column/column.h"
#include "core/column/column_vector.h"
#include "storage/index/ann/ann_index_writer.h"
#include "storage/index/bloom_filter/bloom_filter.h"
#include "storage/index/inverted/clucene/inverted_index_writer.h"
#include "storage/segment/common.h"
#include "storage/segment/options.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/segment/variant/variant_statistics.h"
#include "storage/storage_layout.h"       // for StorageBuffers
#include "storage/tablet/tablet_schema.h" // for TabletColumnPtr
#include "util/slice.h"                   // for OwnedSlice

namespace doris {

class BlockCompressionCodec;
class TabletColumn;
class TabletIndex;
struct RowsetWriterContext;

namespace io {
class FileWriter;
}

namespace segment_v2 {

enum class VariantWriterInputFormat : uint8_t {
    UNSET,
    V2,
};

struct ColumnWriterOptions {
    // input and output parameter:
    // - input: column_id/unique_id/type/length/encoding/compression/is_nullable members
    // - output: encoding/indexes/dict_page members
    ColumnMetaPB* meta = nullptr;
    size_t data_page_size = STORAGE_PAGE_SIZE_DEFAULT_VALUE;
    size_t dict_page_size = STORAGE_DICT_PAGE_SIZE_DEFAULT_VALUE;
    // store compressed page only when space saving is above the threshold.
    // space saving = 1 - compressed_size / uncompressed_size
    double compression_min_space_saving = 0.1;
    bool need_zone_map = false;
    bool need_bloom_filter = false;
    bool is_ngram_bf_index = false;
    bool need_inverted_index = false;
    bool need_ann_index = false;
    uint8_t gram_size;
    uint16_t gram_bf_size;
    BloomFilterOptions bf_options;
    std::vector<const TabletIndex*> inverted_indexes;
    IndexFileWriter* index_file_writer = nullptr;
    // The owning segment serves a direct load (stream/broker load,
    // DataWriteType::TYPE_DIRECT) rather than compaction / schema change. Set
    // once by the segment writer and propagated to variant subcolumn writers;
    // forwarded to every created IndexColumnWriter via set_direct_load() so
    // SNII can select its direct-load PRX zstd level without plumbing
    // DataWriteType itself down here.
    bool is_direct_load = false;

    SegmentFooterPB* footer = nullptr;
    io::FileWriter* file_writer = nullptr;
    CompressionTypePB compression_type = UNKNOWN_COMPRESSION;
    RowsetWriterContext* rowset_ctx = nullptr;
    // For collect segment statistics for compaction
    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    const TabletIndex* ann_index = nullptr;

    // Storage format of the owning tablet (V2 or V3). Set once by the segment writer
    // (from TabletMeta::storage_format()) and propagated down to aux child writers
    // (null / array-length / map-length), struct subcolumn writers and variant subcolumn
    // writers. All encoding-default decisions consult this via resolve_default_encoding().
    // Also forwarded to BinaryDictPageBuilder via PageBuilderOptions::binary_plain_encoding.
    TabletStorageFormatPB storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;

    std::string to_string() const {
        std::stringstream ss;
        ss << std::boolalpha << "meta=" << meta->DebugString()
           << ", data_page_size=" << data_page_size << ", dict_page_size=" << dict_page_size
           << ", compression_min_space_saving = " << compression_min_space_saving
           << ", need_zone_map=" << need_zone_map << ", need_bloom_filter" << need_bloom_filter;
        return ss.str();
    }
};

class EncodingInfo;
class NullBitmapBuilder;
class OrdinalIndexWriter;
class PageBuilder;
class BloomFilterIndexWriter;
class ZoneMapIndexWriter;
class VariantColumnWriterImpl;
class VariantShredder;
class VariantPathBuilder;
class ColumnWriter;

// Base interface for scalar, ARRAY, MAP, STRUCT, AGG_STATE and VARIANT columns.
class ColumnWriter {
public:
    static Status create(const ColumnWriterOptions& opts, const TabletColumn* column,
                         io::FileWriter* file_writer, std::unique_ptr<ColumnWriter>* writer);
    static Status create_struct_writer(const ColumnWriterOptions& opts, const TabletColumn* column,
                                       io::FileWriter* file_writer,
                                       std::unique_ptr<ColumnWriter>* writer);
    static Status create_array_writer(const ColumnWriterOptions& opts, const TabletColumn* column,
                                      io::FileWriter* file_writer,
                                      std::unique_ptr<ColumnWriter>* writer);
    static Status create_map_writer(const ColumnWriterOptions& opts, const TabletColumn* column,
                                    io::FileWriter* file_writer,
                                    std::unique_ptr<ColumnWriter>* writer);

    static Status create_variant_writer(const ColumnWriterOptions& opts, const TabletColumn* column,
                                        io::FileWriter* file_writer,
                                        std::unique_ptr<ColumnWriter>* writer);

    static Status create_agg_state_writer(const ColumnWriterOptions& opts,
                                          const TabletColumn* column, io::FileWriter* file_writer,
                                          std::unique_ptr<ColumnWriter>* writer);

    explicit ColumnWriter(TabletColumnPtr column, bool is_nullable, ColumnMetaPB* meta);

    virtual ~ColumnWriter() = default;

    virtual Status init() = 0;

    // Write rows [row_pos, row_pos + num_rows) of `column`.
    virtual Status append(const IColumn& column, size_t row_pos, size_t num_rows) = 0;

    // Write num_rows NULL rows without any data. The variant writers use this to
    // pad a sub-column over rows where its path is absent.
    virtual Status append_nulls(size_t num_rows) = 0;

    virtual Status finish_current_page() = 0;

    virtual uint64_t estimate_buffer_size() = 0;

    // finish append data
    virtual Status finish() = 0;

    // write all data into file
    virtual Status write_data() = 0;

    virtual Status write_ordinal_index() = 0;

    virtual Status write_zone_map() = 0;

    virtual Status write_inverted_index() = 0;

    virtual Status write_ann_index() { return Status::OK(); }

    virtual Status write_bloom_filter_index() = 0;

    virtual ordinal_t get_next_rowid() const = 0;

    virtual uint64_t get_raw_data_bytes() const = 0;
    virtual uint64_t get_total_uncompressed_data_pages_bytes() const = 0;
    virtual uint64_t get_total_compressed_data_pages_bytes() const = 0;
    // Points to one of the three data bytes getters above.
    using DataBytesGetter = uint64_t (ColumnWriter::*)() const;

    bool is_nullable() const { return _is_nullable; }

    const TabletColumn* get_column() const { return _column.get(); }

    // Per-row footprint of a storage cell of this writer's column, used to step across the
    // prepared cells in the append run loops.
    size_t cell_size() const { return field_type_size(_column->type()); }

    ColumnMetaPB* get_column_meta() const { return _column_meta; }

private:
    TabletColumnPtr _column;
    bool _is_nullable;
    ColumnMetaPB* _column_meta;
};

class FlushPageCallback {
public:
    virtual ~FlushPageCallback() = default;
    virtual void put_extra_info_in_page(DataPageFooterPB* footer) {}
};

// Writes scalar values and the offset/null streams of the composite writers. append cuts a
// batch into its NULL runs, written to the null bitmap (append_nulls), and its value runs,
// each converted to its storage cells (append_values) and written to the data pages and
// every index (append_data).
class ScalarColumnWriter : public ColumnWriter {
public:
    ScalarColumnWriter(const ColumnWriterOptions& opts, TabletColumnPtr column,
                       io::FileWriter* file_writer);

    // An AGG_STATE column stores whatever its aggregate function serialises to: storage_type
    // is VARCHAR for strings, BITMAP for bitmaps and AGG_STATE for fixed-length objects. Every
    // other column is stored as its own type.
    ScalarColumnWriter(const ColumnWriterOptions& opts, TabletColumnPtr column,
                       io::FileWriter* file_writer, FieldType storage_type);

    ~ScalarColumnWriter() override;

    Status init() override;

    Status append(const IColumn& column, size_t row_pos, size_t num_rows) final;

    // Writes a run of cells already in this column's storage layout, none of them NULL,
    // page by page. append prepares its cells from a column; the composite writers feed
    // their null maps, whose bytes are the TINYINT cells, directly.
    virtual Status append_data(const uint8_t* cells, size_t num_rows);

    Status append_nulls(size_t num_rows) override;

    Status finish_current_page() override;

    uint64_t estimate_buffer_size() override;

    // finish append data
    Status finish() override;

    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_zone_map() override;
    Status write_inverted_index() override;
    Status write_bloom_filter_index() override;
    ordinal_t get_next_rowid() const override { return _next_rowid; }

    uint64_t get_raw_data_bytes() const override { return _raw_data_bytes; }

    uint64_t get_total_uncompressed_data_pages_bytes() const override {
        return _total_uncompressed_data_pages_size;
    }

    uint64_t get_total_compressed_data_pages_bytes() const override {
        return _total_compressed_data_pages_size;
    }

    void register_flush_page_callback(FlushPageCallback* flush_page_callback) {
        _new_page_callback = flush_page_callback;
    }

protected:
    // used for append not null data. When page is full, will append data not reach num_rows.
    Status append_data_in_current_page(const uint8_t* ptr, size_t* num_written) {
        RETURN_IF_CATCH_EXCEPTION(
                { return _internal_append_data_in_current_page(ptr, num_written); });
    }
    friend class OffsetColumnWriter;

private:
    Status _internal_append_data_in_current_page(const uint8_t* ptr, size_t* num_written);

    // Converts rows [row_pos, row_pos + num_rows) of `values`, a run without NULL rows, to
    // num_rows cells of cell_size() bytes back to back through the StorageLayout of
    // _storage_type, borrowed from the column or held in _buffers until the next run, and
    // writes them.
    Status append_values(const IColumn& values, size_t row_pos, size_t num_rows);

    void _reserve_null_bitmap(const uint8_t* null_map, size_t num_rows);

    // The StorageLayout the values are stored through.
    const FieldType _storage_type;
    // Where append_values puts what it cannot borrow from the column.
    StorageBuffers _buffers;

    std::unique_ptr<PageBuilder> _page_builder;

    std::unique_ptr<NullBitmapBuilder> _null_bitmap_builder;

    ColumnWriterOptions _opts;

    const EncodingInfo* _encoding_info = nullptr;

    ordinal_t _next_rowid = 0;

    // All Pages will be organized into a linked list
    struct Page {
        // the data vector may contain:
        //     1. one OwnedSlice if the page body is compressed
        //     2. one OwnedSlice if the page body is not compressed and doesn't have nullmap
        //     3. two OwnedSlice if the page body is not compressed and has nullmap
        // use vector for easier management for lifetime of OwnedSlice
        std::vector<OwnedSlice> data;
        PageFooterPB footer;
    };

    void _push_back_page(std::unique_ptr<Page> page) {
        for (auto& data_slice : page->data) {
            _data_size += data_slice.slice().size;
        }
        // estimate (page footer + footer size + checksum) took 20 bytes
        _data_size += 20;
        // add page to pages' tail
        _pages.emplace_back(std::move(page));
    }

    Status _write_data_page(Page* page);

private:
    io::FileWriter* _file_writer = nullptr;
    // total size of data page list
    uint64_t _data_size;

    uint64_t _raw_data_bytes {0};
    uint64_t _total_uncompressed_data_pages_size {0};
    uint64_t _total_compressed_data_pages_size {0};

    // cached generated pages,
    std::vector<std::unique_ptr<Page>> _pages;
    ordinal_t _first_rowid = 0;

    BlockCompressionCodec* _compress_codec;

    std::unique_ptr<OrdinalIndexWriter> _ordinal_index_builder;
    std::unique_ptr<ZoneMapIndexWriter> _zone_map_index_builder;
    std::vector<std::unique_ptr<IndexColumnWriter>> _inverted_index_builders;
    std::unique_ptr<BloomFilterIndexWriter> _bloom_filter_index_builder;

    // call before flush data page.
    FlushPageCallback* _new_page_callback = nullptr;
};

// offsetColumnWriter is used column which has offset column, like array, map.
//  column type is only uint64 and should response for whole column value [start, end], end will set
//  in footer.next_array_item_ordinal which in finish_cur_page() callback put_extra_info_in_page()
class OffsetColumnWriter final : public ScalarColumnWriter, FlushPageCallback {
public:
    OffsetColumnWriter(const ColumnWriterOptions& opts, TabletColumnPtr column,
                       io::FileWriter* file_writer);

    ~OffsetColumnWriter() override;

    Status init() override;

    Status append_data(const uint8_t* cells, size_t num_rows) override;

private:
    void put_extra_info_in_page(DataPageFooterPB* footer) override;

    uint64_t _next_offset;
};

// STRUCT values, including nested structs, with one child writer per field.
class StructColumnWriter final : public ColumnWriter {
public:
    explicit StructColumnWriter(const ColumnWriterOptions& opts, TabletColumnPtr column,
                                ScalarColumnWriter* null_writer,
                                std::vector<std::unique_ptr<ColumnWriter>>& sub_column_writers);
    ~StructColumnWriter() override = default;

    Status init() override;

    // Recurse into each sub-column writer; write our own null bits.
    Status append(const IColumn& column, size_t row_pos, size_t num_rows) override;

    uint64_t estimate_buffer_size() override;

    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;
    Status append_nulls(size_t num_rows) override {
        return Status::NotSupported("struct writer can not append_nulls");
    }

    Status finish_current_page() override;

    Status write_zone_map() override {
        if (_opts.need_zone_map) {
            return Status::NotSupported("struct not support zone map");
        }
        return Status::OK();
    }

    Status write_inverted_index() override;
    Status write_bloom_filter_index() override {
        if (_opts.need_bloom_filter) {
            return Status::NotSupported("struct not support bloom filter index");
        }
        return Status::OK();
    }

    ordinal_t get_next_rowid() const override { return _sub_column_writers[0]->get_next_rowid(); }

    uint64_t get_raw_data_bytes() const override {
        return _get_total_data_pages_bytes(&ColumnWriter::get_raw_data_bytes);
    }

    uint64_t get_total_uncompressed_data_pages_bytes() const override {
        return _get_total_data_pages_bytes(&ColumnWriter::get_total_uncompressed_data_pages_bytes);
    }

    uint64_t get_total_compressed_data_pages_bytes() const override {
        return _get_total_data_pages_bytes(&ColumnWriter::get_total_compressed_data_pages_bytes);
    }

private:
    template <typename Func>
    uint64_t _get_total_data_pages_bytes(Func func) const {
        uint64_t size = is_nullable() ? std::invoke(func, _null_writer.get()) : 0;
        for (const auto& writer : _sub_column_writers) {
            size += std::invoke(func, writer.get());
        }
        return size;
    }

private:
    size_t _num_sub_column_writers;
    std::unique_ptr<ScalarColumnWriter> _null_writer;
    std::vector<std::unique_ptr<ColumnWriter>> _sub_column_writers;
    ColumnWriterOptions _opts;
};

// ARRAY values and array-backed AGG_STATE values, with offset, null-map and item writers.
class ArrayColumnWriter final : public ColumnWriter {
public:
    explicit ArrayColumnWriter(const ColumnWriterOptions& opts, TabletColumnPtr column,
                               OffsetColumnWriter* offset_writer, ScalarColumnWriter* null_writer,
                               std::unique_ptr<ColumnWriter> item_writer);
    ~ArrayColumnWriter() override = default;

    Status init() override;

    // Rebase the offsets to disk layout, recurse into _item_writer, feed the
    // array index writers.
    Status append(const IColumn& column, size_t row_pos, size_t num_rows) override;

    uint64_t estimate_buffer_size() override;

    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;
    Status append_nulls(size_t num_rows) override;

    Status finish_current_page() override;

    Status write_zone_map() override {
        if (_opts.need_zone_map) {
            return Status::NotSupported("array not support zone map");
        }
        return Status::OK();
    }

    Status write_inverted_index() override;
    Status write_ann_index() override;
    Status write_bloom_filter_index() override {
        if (_opts.need_bloom_filter) {
            return Status::NotSupported("array not support bloom filter index");
        }
        return Status::OK();
    }
    ordinal_t get_next_rowid() const override { return _offset_writer->get_next_rowid(); }

    uint64_t get_raw_data_bytes() const override {
        return _get_total_data_pages_bytes(&ColumnWriter::get_raw_data_bytes);
    }

    uint64_t get_total_uncompressed_data_pages_bytes() const override {
        return _get_total_data_pages_bytes(&ColumnWriter::get_total_uncompressed_data_pages_bytes);
    }

    uint64_t get_total_compressed_data_pages_bytes() const override {
        return _get_total_data_pages_bytes(&ColumnWriter::get_total_compressed_data_pages_bytes);
    }

private:
    template <typename Func>
    uint64_t _get_total_data_pages_bytes(Func func) const {
        uint64_t size = std::invoke(func, _offset_writer.get());
        if (is_nullable()) {
            size += std::invoke(func, _null_writer.get());
        }
        size += std::invoke(func, _item_writer.get());
        return size;
    }

private:
    bool has_empty_items() const { return _item_writer->get_next_rowid() == 0; }

private:
    std::unique_ptr<OffsetColumnWriter> _offset_writer;
    std::unique_ptr<ScalarColumnWriter> _null_writer;
    std::unique_ptr<ColumnWriter> _item_writer;
    std::unique_ptr<IndexColumnWriter> _inverted_index_writer;
    std::unique_ptr<AnnIndexColumnWriter> _ann_index_writer;
    ColumnWriterOptions _opts;
    // The current batch's offsets as the offset writer and the index writers
    // read them.
    ColumnOffset64::MutablePtr _offsets_buffer = ColumnOffset64::create();
    // Storage cells for array indexes, independent of the element writer's page buffers.
    StorageBuffers _index_buffers;
};

// MAP values and map-backed AGG_STATE values, with offset, null-map, key and value writers.
class MapColumnWriter final : public ColumnWriter {
public:
    explicit MapColumnWriter(const ColumnWriterOptions& opts, TabletColumnPtr column,
                             ScalarColumnWriter* null_writer, OffsetColumnWriter* offsets_writer,
                             std::vector<std::unique_ptr<ColumnWriter>>& _kv_writers);

    ~MapColumnWriter() override = default;

    Status init() override;

    // Rebase the offsets to disk layout, recurse into the key and value writers.
    Status append(const IColumn& column, size_t row_pos, size_t num_rows) override;

    uint64_t estimate_buffer_size() override;

    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_inverted_index() override;
    Status append_nulls(size_t num_rows) override {
        return Status::NotSupported("map writer can not append_nulls");
    }

    Status finish_current_page() override;

    Status write_zone_map() override {
        if (_opts.need_zone_map) {
            return Status::NotSupported("map not support zone map");
        }
        return Status::OK();
    }

    Status write_bloom_filter_index() override {
        if (_opts.need_bloom_filter) {
            return Status::NotSupported("map not support bloom filter index");
        }
        return Status::OK();
    }

    // according key writer to get next rowid
    ordinal_t get_next_rowid() const override { return _offsets_writer->get_next_rowid(); }

    uint64_t get_raw_data_bytes() const override {
        return _get_total_data_pages_bytes(&ColumnWriter::get_raw_data_bytes);
    }

    uint64_t get_total_uncompressed_data_pages_bytes() const override {
        return _get_total_data_pages_bytes(&ColumnWriter::get_total_uncompressed_data_pages_bytes);
    }

    uint64_t get_total_compressed_data_pages_bytes() const override {
        return _get_total_data_pages_bytes(&ColumnWriter::get_total_compressed_data_pages_bytes);
    }

private:
    template <typename Func>
    uint64_t _get_total_data_pages_bytes(Func func) const {
        uint64_t size = std::invoke(func, _offsets_writer.get());
        if (is_nullable()) {
            size += std::invoke(func, _null_writer.get());
        }
        for (const auto& writer : _kv_writers) {
            size += std::invoke(func, writer.get());
        }
        return size;
    }

private:
    std::vector<std::unique_ptr<ColumnWriter>> _kv_writers;
    // we need null writer to make sure a row is null or not
    std::unique_ptr<ScalarColumnWriter> _null_writer;
    std::unique_ptr<OffsetColumnWriter> _offsets_writer;
    std::unique_ptr<IndexColumnWriter> _index_builder;
    ColumnWriterOptions _opts;
    ColumnOffset64::MutablePtr _map_offsets_buffer = ColumnOffset64::create();
};

// Extracted VARIANT subcolumns.
class VariantSubcolumnWriter : public ColumnWriter {
public:
    explicit VariantSubcolumnWriter(const ColumnWriterOptions& opts, TabletColumnPtr column);

    ~VariantSubcolumnWriter() override;

    Status init() override;

    uint64_t estimate_buffer_size() override;

    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;

    Status write_zone_map() override;

    Status write_inverted_index() override;
    Status write_bloom_filter_index() override;
    ordinal_t get_next_rowid() const override { return _next_rowid; }

    uint64_t get_raw_data_bytes() const override { return _writer->get_raw_data_bytes(); }

    uint64_t get_total_uncompressed_data_pages_bytes() const override {
        return _writer->get_total_uncompressed_data_pages_bytes();
    }

    uint64_t get_total_compressed_data_pages_bytes() const override {
        return _writer->get_total_compressed_data_pages_bytes();
    }

    Status append_nulls(size_t num_rows) override {
        return Status::NotSupported("variant writer can not append_nulls");
    }

    // Splits off the null map and hands the nested column down to the
    // variant writers.
    Status append(const IColumn& column, size_t row_pos, size_t num_rows) override;

    Status finish_current_page() override {
        return Status::NotSupported("variant writer has no data, can not finish_current_page");
    }

    size_t get_non_null_size() const { return none_null_size; }

#ifdef BE_TEST
    // Force small chunk budgets so tests can exercise multi-chunk flushes cheaply.
    static void set_chunk_bytes_for_test(size_t bytes);
    static void set_chunk_values_for_test(size_t values);
    size_t num_chunks_for_test() const;
    size_t max_chunk_bytes_for_test() const;
#endif

    Status finalize();

private:
    Status _append(const IColumn& column, size_t row_pos, size_t num_rows, const uint8_t* null_map);
    Status _append_v2(const IColumn& column, size_t row_pos, size_t num_rows,
                      std::span<const uint8_t> outer_nulls);
    Status _ensure_input_format(const IColumn& column);
    Status _initialize_v2_builder();
    Status _promote_chunks(DataTypePtr* inferred_type);
    Status _seal_chunk();
    // Bounded value chunks; each chunk is finalized and flushed independently so the writer never
    // holds a segment-sized ColumnString.
    struct ValueChunk {
        std::unique_ptr<VariantPathBuilder> builder;
        size_t row_begin = 0;
        size_t rows = 0;
    };
    static constexpr size_t kChunkBytes = 256ULL << 20;
    static constexpr size_t kChunkValues = 4ULL << 20;
    bool is_finalized() const;
    bool _is_finalized = false;
    ordinal_t _next_rowid = 0;
    size_t none_null_size = 0;
    VariantWriterInputFormat _input_format = VariantWriterInputFormat::UNSET;
    std::unique_ptr<VariantPathBuilder> _v2_builder;
    std::vector<ValueChunk> _chunks;
    size_t _chunk_row_begin = 0;
    size_t _chunk_rows = 0;
    // Type carried into the next chunk. Inferred promotions also check and update sealed chunks
    // so fallback follows the same sequence as one uninterrupted builder.
    DataTypePtr _running_type;
    size_t _num_rows = 0;
    ColumnWriterOptions _opts;
    std::unique_ptr<ColumnWriter> _writer;
    TabletIndexes _indexes;

    std::unique_ptr<NestedGroupWriteProvider> _nested_group_provider;
    VariantStatistics _statistics;
};

// Root VARIANT values and their physical subcolumns.
class VariantColumnWriter : public ColumnWriter {
public:
    explicit VariantColumnWriter(const ColumnWriterOptions& opts, TabletColumnPtr column);

    ~VariantColumnWriter() override = default;

    Status init() override;

    uint64_t estimate_buffer_size() override;

    Status finish() override;
    Status write_data() override;
    Status write_ordinal_index() override;

    Status write_zone_map() override;

    Status write_inverted_index() override;
    Status write_bloom_filter_index() override;
    ordinal_t get_next_rowid() const override { return _next_rowid; }

    uint64_t get_raw_data_bytes() const override;

    uint64_t get_total_uncompressed_data_pages_bytes() const override;

    uint64_t get_total_compressed_data_pages_bytes() const override;

    Status append_nulls(size_t num_rows) override {
        return Status::NotSupported("variant writer can not append_nulls");
    }

    // Splits off the null map and hands the nested column down to the
    // variant writers.
    Status append(const IColumn& column, size_t row_pos, size_t num_rows) override;

    Status finish_current_page() override {
        return Status::NotSupported("variant writer has no data, can not finish_current_page");
    }

    VariantColumnWriterImpl* impl_for_test() const { return _impl.get(); }

private:
    std::unique_ptr<VariantColumnWriterImpl> _impl;
    ordinal_t _next_rowid = 0;
};

} // namespace segment_v2
} // namespace doris

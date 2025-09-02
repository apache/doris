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

#include <butil/macros.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>

#include <cstdint>
#include <map>
#include <memory> // for unique_ptr
#include <string>
#include <unordered_map>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/status.h" // Status
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/page_cache.h"
#include "olap/rowset/segment_v2/column_reader.h" // ColumnReader
#include "olap/rowset/segment_v2/page_handle.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "util/once.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/json/path_in_data.h"

namespace doris {
namespace vectorized {
class IDataType;
}

class ShortKeyIndexDecoder;
class Schema;
class StorageReadOptions;
class PrimaryKeyIndexReader;
class RowwiseIterator;
struct RowLocation;

namespace segment_v2 {

class BitmapIndexIterator;
class Segment;
class InvertedIndexIterator;
class IndexFileReader;
class IndexIterator;
class ColumnReaderCache;

using SegmentSharedPtr = std::shared_ptr<Segment>;
// A Segment is used to represent a segment in memory format. When segment is
// generated, it won't be modified, so this struct aimed to help read operation.
// It will prepare all ColumnReader to create ColumnIterator as needed.
// And user can create a RowwiseIterator through new_iterator function.
//
// NOTE: This segment is used to a specified TabletSchema, when TabletSchema
// is changed, this segment can not be used any more. For example, after a schema
// change finished, client should disable all cached Segment for old TabletSchema.
class Segment : public std::enable_shared_from_this<Segment>, public MetadataAdder<Segment> {
public:
    static Status open(io::FileSystemSPtr fs, const std::string& path, int64_t tablet_id,
                       uint32_t segment_id, RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
                       const io::FileReaderOptions& reader_options,
                       std::shared_ptr<Segment>* output, InvertedIndexFileInfo idx_file_info = {},
                       OlapReaderStatistics* stats = nullptr);

    static io::UInt128Wrapper file_cache_key(std::string_view rowset_id, uint32_t seg_id);
    io::UInt128Wrapper file_cache_key() const {
        return file_cache_key(_rowset_id.to_string(), _segment_id);
    }

    ~Segment();

    int64_t get_metadata_size() const override;
    void update_metadata_size();

    Status new_iterator(SchemaSPtr schema, const StorageReadOptions& read_options,
                        std::unique_ptr<RowwiseIterator>* iter);

    static Status new_default_iterator(const TabletColumn& tablet_column,
                                       std::unique_ptr<ColumnIterator>* iter);

    uint32_t id() const { return _segment_id; }

    RowsetId rowset_id() const { return _rowset_id; }

    uint32_t num_rows() const { return _num_rows; }

    Status new_column_iterator(const TabletColumn& tablet_column,
                               std::unique_ptr<ColumnIterator>* iter,
                               const StorageReadOptions* opt);

    Status new_column_iterator(int32_t unique_id, const StorageReadOptions* opt,
                               std::unique_ptr<ColumnIterator>* iter);

    Status new_bitmap_index_iterator(const TabletColumn& tablet_column,
                                     const StorageReadOptions& read_options,
                                     std::unique_ptr<BitmapIndexIterator>* iter);

    Status new_index_iterator(const TabletColumn& tablet_column, const TabletIndex* index_meta,
                              const StorageReadOptions& read_options,
                              std::unique_ptr<IndexIterator>* iter);

    const ShortKeyIndexDecoder* get_short_key_index() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _sk_index_decoder.get();
    }

    const PrimaryKeyIndexReader* get_primary_key_index() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _pk_index_reader.get();
    }

    Status lookup_row_key(const Slice& key, const TabletSchema* latest_schema, bool with_seq_col,
                          bool with_rowid, RowLocation* row_location, OlapReaderStatistics* stats,
                          std::string* encoded_seq_value = nullptr);

    Status read_key_by_rowid(uint32_t row_id, std::string* key);

    Status seek_and_read_by_rowid(const TabletSchema& schema, SlotDescriptor* slot, uint32_t row_id,
                                  vectorized::MutableColumnPtr& result, OlapReaderStatistics& stats,
                                  std::unique_ptr<ColumnIterator>& iterator_hint);

    Status load_index(OlapReaderStatistics* stats);

    Status load_pk_index_and_bf(OlapReaderStatistics* stats);

    void update_healthy_status(Status new_status) { _healthy_status.update(new_status); }
    // The segment is loaded into SegmentCache and then will load indices, if there are something wrong
    // during loading indices, should remove it from SegmentCache. If not, it will always report error during
    // query. So we add a healthy status API, the caller should check the healhty status before using the segment.
    Status healthy_status();

    std::string min_key() {
        DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS && _pk_index_meta != nullptr);
        return _pk_index_meta->min_key();
    }
    std::string max_key() {
        DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS && _pk_index_meta != nullptr);
        return _pk_index_meta->max_key();
    }

    io::FileReaderSPtr file_reader() { return _file_reader; }

    // Including the column reader memory.
    // another method `get_metadata_size` not include the column reader, only the segment object itself.
    int64_t meta_mem_usage() const { return _meta_mem_usage; }

    // Get the inner file column's data type
    // ignore_chidren set to false will treat field as variant
    // when it contains children with field paths.
    // nullptr will returned if storage type does not contains such column
    std::shared_ptr<const vectorized::IDataType> get_data_type_of(const TabletColumn& column,
                                                                  bool read_flat_leaves);
    // Check is schema read type equals storage column type
    bool same_with_storage_type(int32_t cid, const Schema& schema, bool read_flat_leaves);

    // If column in segment is the same type in schema, then it is safe to apply predicate
    template <typename Predicate>
    bool can_apply_predicate_safely(int cid, Predicate* pred, const Schema& schema,
                                    ReaderType read_type) {
        const doris::Field* col = schema.column(cid);
        vectorized::DataTypePtr storage_column_type =
                get_data_type_of(col->get_desc(), read_type != ReaderType::READER_QUERY);
        if (storage_column_type == nullptr) {
            // Default column iterator
            return true;
        }
        PrimitiveType type = storage_column_type->get_primitive_type();
        if (type == TYPE_VARIANT || is_complex_type(type)) {
            // Predicate should nerver apply on variant/complex type
            return false;
        }
        bool safe = pred->can_do_apply_safely(storage_column_type->get_primitive_type(),
                                              storage_column_type->is_nullable());
        // Currently only variant column can lead to unsafe
        CHECK(safe || col->type() == FieldType::OLAP_FIELD_TYPE_VARIANT);
        return safe;
    }

    const TabletSchemaSPtr& tablet_schema() { return _tablet_schema; }

    // get the column reader by tablet column, return NOT_FOUND if not found reader in this segment
    Status get_column_reader(const TabletColumn& col, std::shared_ptr<ColumnReader>* column_reader,
                             OlapReaderStatistics* stats);

    // get the column reader by column unique id, return NOT_FOUND if not found reader in this segment
    Status get_column_reader(int32_t col_uid, std::shared_ptr<ColumnReader>* column_reader,
                             OlapReaderStatistics* stats);

private:
    DISALLOW_COPY_AND_ASSIGN(Segment);
    Segment(uint32_t segment_id, RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
            InvertedIndexFileInfo idx_file_info = InvertedIndexFileInfo());
    static Status _open(io::FileSystemSPtr fs, const std::string& path, uint32_t segment_id,
                        RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
                        const io::FileReaderOptions& reader_options,
                        std::shared_ptr<Segment>* output, InvertedIndexFileInfo idx_file_info,
                        OlapReaderStatistics* stats);
    // open segment file and read the minimum amount of necessary information (footer)
    Status _open(OlapReaderStatistics* stats);
    Status _parse_footer(std::shared_ptr<SegmentFooterPB>& footer,
                         OlapReaderStatistics* stats = nullptr);
    Status _create_column_meta(const SegmentFooterPB& footer);
    Status _load_pk_bloom_filter(OlapReaderStatistics* stats);
    // Must ensure _create_column_readers_once has been called before calling this function.
    ColumnReader* _get_column_reader(const TabletColumn& col);

    Status _write_error_file(size_t file_size, size_t offset, size_t bytes_read, char* data,
                             io::IOContext& io_ctx);

    Status _open_index_file_reader();

    Status _create_column_meta_once(OlapReaderStatistics* stats);

    virtual Status _get_segment_footer(std::shared_ptr<SegmentFooterPB>&,
                                       OlapReaderStatistics* stats);

    StoragePageCache::CacheKey get_segment_footer_cache_key() const;

    friend class SegmentIterator;
    friend class ColumnReaderCache;
    friend class MockSegment;

    io::FileSystemSPtr _fs;
    io::FileReaderSPtr _file_reader;
    uint32_t _segment_id;
    uint32_t _num_rows;
    AtomicStatus _healthy_status;

    // 1. Tracking memory use by segment meta data such as footer or index page.
    // 2. Tracking memory use by segment column reader
    // The memory consumed by querying is tracked in segment iterator.
    int64_t _meta_mem_usage;
    int64_t _tracked_meta_mem_usage = 0;

    RowsetId _rowset_id;
    TabletSchemaSPtr _tablet_schema;

    std::unique_ptr<PrimaryKeyIndexMetaPB> _pk_index_meta;
    PagePointerPB _sk_index_page;

    // Limited cache for column readers
    std::unique_ptr<ColumnReaderCache> _column_reader_cache;

    // map column unique id ---> it's footer ordinal
    std::unordered_map<int32_t, size_t> _column_uid_to_footer_ordinal;

    // Init from ColumnMetaPB in SegmentFooterPB
    // map column unique id ---> it's inner data type
    std::map<int32_t, std::shared_ptr<const vectorized::IDataType>> _file_column_types;

    // used to guarantee that short key index will be loaded at most once in a thread-safe way
    DorisCallOnce<Status> _load_index_once;
    // used to guarantee that primary key bloom filter will be loaded at most once in a thread-safe way
    DorisCallOnce<Status> _load_pk_bf_once;

    DorisCallOnce<Status> _create_column_meta_once_call;

    std::weak_ptr<SegmentFooterPB> _footer_pb;

    // used to hold short key index page in memory
    PageHandle _sk_index_handle;
    // short key index decoder
    // all content is in memory
    std::unique_ptr<ShortKeyIndexDecoder> _sk_index_decoder;
    // primary key index reader
    std::unique_ptr<PrimaryKeyIndexReader> _pk_index_reader;
    std::mutex _open_lock;
    // inverted index file reader
    std::shared_ptr<IndexFileReader> _index_file_reader;
    DorisCallOnce<Status> _index_file_reader_open;

    InvertedIndexFileInfo _idx_file_info;

    int _be_exec_version = BeExecVersionManager::get_newest_version();
};

} // namespace segment_v2
} // namespace doris

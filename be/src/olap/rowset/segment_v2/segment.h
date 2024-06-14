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

#include "common/status.h" // Status
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/column_reader.h" // ColumnReader
#include "olap/rowset/segment_v2/hierarchical_data_reader.h"
#include "olap/rowset/segment_v2/page_handle.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "runtime/descriptors.h"
#include "util/once.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/subcolumn_tree.h"
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
class MemTracker;
class PrimaryKeyIndexReader;
class RowwiseIterator;
struct RowLocation;

namespace segment_v2 {

class BitmapIndexIterator;
class Segment;
class InvertedIndexIterator;
class InvertedIndexFileReader;

using SegmentSharedPtr = std::shared_ptr<Segment>;
// A Segment is used to represent a segment in memory format. When segment is
// generated, it won't be modified, so this struct aimed to help read operation.
// It will prepare all ColumnReader to create ColumnIterator as needed.
// And user can create a RowwiseIterator through new_iterator function.
//
// NOTE: This segment is used to a specified TabletSchema, when TabletSchema
// is changed, this segment can not be used any more. For example, after a schema
// change finished, client should disable all cached Segment for old TabletSchema.
class Segment : public std::enable_shared_from_this<Segment> {
public:
    static Status open(io::FileSystemSPtr fs, const std::string& path, uint32_t segment_id,
                       RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
                       const io::FileReaderOptions& reader_options,
                       std::shared_ptr<Segment>* output);

    static io::UInt128Wrapper file_cache_key(std::string_view rowset_id, uint32_t seg_id);
    io::UInt128Wrapper file_cache_key() const {
        return file_cache_key(_rowset_id.to_string(), _segment_id);
    }

    ~Segment();

    Status new_iterator(SchemaSPtr schema, const StorageReadOptions& read_options,
                        std::unique_ptr<RowwiseIterator>* iter);

    uint32_t id() const { return _segment_id; }

    RowsetId rowset_id() const { return _rowset_id; }

    uint32_t num_rows() const { return _num_rows; }

    Status new_column_iterator(const TabletColumn& tablet_column,
                               std::unique_ptr<ColumnIterator>* iter,
                               const StorageReadOptions* opt);

    Status new_column_iterator_with_path(const TabletColumn& tablet_column,
                                         std::unique_ptr<ColumnIterator>* iter,
                                         const StorageReadOptions* opt);

    Status new_column_iterator(int32_t unique_id, std::unique_ptr<ColumnIterator>* iter);

    Status new_bitmap_index_iterator(const TabletColumn& tablet_column,
                                     std::unique_ptr<BitmapIndexIterator>* iter);

    Status new_inverted_index_iterator(const TabletColumn& tablet_column,
                                       const TabletIndex* index_meta,
                                       const StorageReadOptions& read_options,
                                       std::unique_ptr<InvertedIndexIterator>* iter);

    const ShortKeyIndexDecoder* get_short_key_index() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _sk_index_decoder.get();
    }

    const PrimaryKeyIndexReader* get_primary_key_index() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _pk_index_reader.get();
    }

    Status lookup_row_key(const Slice& key, bool with_seq_col, bool with_rowid,
                          RowLocation* row_location);

    Status read_key_by_rowid(uint32_t row_id, std::string* key);

    Status seek_and_read_by_rowid(const TabletSchema& schema, SlotDescriptor* slot, uint32_t row_id,
                                  vectorized::MutableColumnPtr& result, OlapReaderStatistics& stats,
                                  std::unique_ptr<ColumnIterator>& iterator_hint);

    Status load_index();

    Status load_pk_index_and_bf();

    std::string min_key() {
        DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS && _pk_index_meta != nullptr);
        return _pk_index_meta->min_key();
    }
    std::string max_key() {
        DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS && _pk_index_meta != nullptr);
        return _pk_index_meta->max_key();
    }

    io::FileReaderSPtr file_reader() { return _file_reader; }

    int64_t meta_mem_usage() const { return _meta_mem_usage; }

    void remove_from_segment_cache() const;

    // Get the inner file column's data type
    // ignore_chidren set to false will treat field as variant
    // when it contains children with field paths.
    // nullptr will returned if storage type does not contains such column
    std::shared_ptr<const vectorized::IDataType> get_data_type_of(vectorized::PathInDataPtr path,
                                                                  bool is_nullable,
                                                                  bool ignore_children) const;

    // Check is schema read type equals storage column type
    bool same_with_storage_type(int32_t cid, const Schema& schema, bool ignore_children) const;

    // If column in segment is the same type in schema, then it is safe to apply predicate
    template <typename Predicate>
    bool can_apply_predicate_safely(int cid, Predicate* pred, const Schema& schema,
                                    ReaderType read_type) const {
        const Field* col = schema.column(cid);
        vectorized::DataTypePtr storage_column_type = get_data_type_of(
                col->path(), col->is_nullable(), read_type != ReaderType::READER_QUERY);
        if (storage_column_type == nullptr) {
            // Default column iterator
            return true;
        }
        if (vectorized::WhichDataType(vectorized::remove_nullable(storage_column_type))
                    .is_variant_type()) {
            // Predicate should nerver apply on variant type
            return false;
        }
        bool safe =
                pred->can_do_apply_safely(storage_column_type->get_type_as_type_descriptor().type,
                                          storage_column_type->is_nullable());
        // Currently only variant column can lead to unsafe
        CHECK(safe || col->type() == FieldType::OLAP_FIELD_TYPE_VARIANT);
        return safe;
    }

    const TabletSchemaSPtr& tablet_schema() { return _tablet_schema; }

private:
    DISALLOW_COPY_AND_ASSIGN(Segment);
    Segment(uint32_t segment_id, RowsetId rowset_id, TabletSchemaSPtr tablet_schema);
    // open segment file and read the minimum amount of necessary information (footer)
    Status _open();
    Status _parse_footer(SegmentFooterPB* footer);
    Status _create_column_readers(const SegmentFooterPB& footer);
    Status _load_pk_bloom_filter();
    ColumnReader* _get_column_reader(const TabletColumn& col);

    // Get Iterator which will read variant root column and extract with paths and types info
    Status _new_iterator_with_variant_root(const TabletColumn& tablet_column,
                                           std::unique_ptr<ColumnIterator>* iter,
                                           const SubcolumnColumnReaders::Node* root,
                                           vectorized::DataTypePtr target_type_hint);

    Status _load_index_impl();
    Status _open_inverted_index();

    Status _create_column_readers_once();

private:
    friend class SegmentIterator;
    io::FileSystemSPtr _fs;
    io::FileReaderSPtr _file_reader;
    uint32_t _segment_id;
    uint32_t _num_rows;

    // 1. Tracking memory use by segment meta data such as footer or index page.
    // 2. Tracking memory use by segment column reader
    // The memory consumed by querying is tracked in segment iterator.
    // TODO: Segment::_meta_mem_usage Unknown value overflow, causes the value of SegmentMeta mem tracker
    // is similar to `-2912341218700198079`. So, temporarily put it in experimental type tracker.
    int64_t _meta_mem_usage;

    RowsetId _rowset_id;
    TabletSchemaSPtr _tablet_schema;

    std::unique_ptr<PrimaryKeyIndexMetaPB> _pk_index_meta;
    PagePointerPB _sk_index_page;

    // map column unique id ---> column reader
    // ColumnReader for each column in TabletSchema. If ColumnReader is nullptr,
    // This means that this segment has no data for that column, which may be added
    // after this segment is generated.
    std::map<int32_t, std::unique_ptr<ColumnReader>> _column_readers;

    // Init from ColumnMetaPB in SegmentFooterPB
    // map column unique id ---> it's inner data type
    std::map<int32_t, std::shared_ptr<const vectorized::IDataType>> _file_column_types;

    // Each node in the tree represents the sub column reader and type
    // for variants.
    SubcolumnColumnReaders _sub_column_tree;

    // each sprase column's path and types info
    SubcolumnColumnReaders _sparse_column_tree;

    // used to guarantee that short key index will be loaded at most once in a thread-safe way
    DorisCallOnce<Status> _load_index_once;
    // used to guarantee that primary key bloom filter will be loaded at most once in a thread-safe way
    DorisCallOnce<Status> _load_pk_bf_once;

    DorisCallOnce<Status> _create_column_readers_once_call;

    std::unique_ptr<SegmentFooterPB> _footer_pb;

    // used to hold short key index page in memory
    PageHandle _sk_index_handle;
    // short key index decoder
    // all content is in memory
    std::unique_ptr<ShortKeyIndexDecoder> _sk_index_decoder;
    // primary key index reader
    std::unique_ptr<PrimaryKeyIndexReader> _pk_index_reader;
    std::mutex _open_lock;
    // inverted index file reader
    std::shared_ptr<InvertedIndexFileReader> _inverted_index_file_reader;
    DorisCallOnce<Status> _inverted_index_file_reader_open;
};

} // namespace segment_v2
} // namespace doris

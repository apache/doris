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

#include <cstdint>
#include <memory> // for unique_ptr
#include <string>
#include <vector>

#include "common/status.h" // Status
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "io/fs/file_system.h"
#include "olap/iterators.h"
#include "olap/primary_key_index.h"
#include "olap/rowset/segment_v2/column_reader.h" // ColumnReader
#include "olap/rowset/segment_v2/page_handle.h"
#include "olap/short_key_index.h"
#include "olap/tablet_schema.h"
#include "util/faststring.h"
#include "util/once.h"

namespace doris {

class SegmentGroup;
class TabletSchema;
class ShortKeyIndexDecoder;
class Schema;
class StorageReadOptions;

namespace segment_v2 {

class BitmapIndexIterator;
class ColumnReader;
class ColumnIterator;
class Segment;
class SegmentIterator;
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
    static Status open(io::FileSystemSPtr fs, const std::string& path,
                       const std::string& cache_path, uint32_t segment_id, RowsetId rowset_id,
                       TabletSchemaSPtr tablet_schema, std::shared_ptr<Segment>* output);

    ~Segment();

    Status new_iterator(const Schema& schema, const StorageReadOptions& read_options,
                        std::unique_ptr<RowwiseIterator>* iter);

    uint32_t id() const { return _segment_id; }

    RowsetId rowset_id() const { return _rowset_id; }

    uint32_t num_rows() const { return _footer.num_rows(); }

    Status new_column_iterator(const TabletColumn& tablet_column, ColumnIterator** iter);

    Status new_bitmap_index_iterator(const TabletColumn& tablet_column, BitmapIndexIterator** iter);

    const ShortKeyIndexDecoder* get_short_key_index() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _sk_index_decoder.get();
    }

    const PrimaryKeyIndexReader* get_primary_key_index() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _pk_index_reader.get();
    }

    Status lookup_row_key(const Slice& key, RowLocation* row_location);

    // only used by UT
    const SegmentFooterPB& footer() const { return _footer; }

    Status load_index();

    Status load_pk_index_and_bf();

    std::string min_key() {
        DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS && _footer.has_primary_key_index_meta());
        return _footer.primary_key_index_meta().min_key();
    };
    std::string max_key() {
        DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS && _footer.has_primary_key_index_meta());
        return _footer.primary_key_index_meta().max_key();
    };

private:
    DISALLOW_COPY_AND_ASSIGN(Segment);
    Segment(uint32_t segment_id, RowsetId rowset_id, TabletSchemaSPtr tablet_schema);
    // open segment file and read the minimum amount of necessary information (footer)
    Status _open();
    Status _parse_footer();
    Status _create_column_readers();
    Status _load_pk_bloom_filter();

private:
    friend class SegmentIterator;
    io::FileReaderSPtr _file_reader;

    uint32_t _segment_id;
    RowsetId _rowset_id;
    TabletSchemaSPtr _tablet_schema;

    int64_t _meta_mem_usage;
    SegmentFooterPB _footer;

    // Map from column unique id to column ordinal in footer's ColumnMetaPB
    // If we can't find unique id from it, it means this segment is created
    // with an old schema.
    std::unordered_map<uint32_t, uint32_t> _column_id_to_footer_ordinal;

    // map column unique id ---> column reader
    // ColumnReader for each column in TabletSchema. If ColumnReader is nullptr,
    // This means that this segment has no data for that column, which may be added
    // after this segment is generated.
    std::map<int32_t, std::unique_ptr<ColumnReader>> _column_readers;

    // used to guarantee that short key index will be loaded at most once in a thread-safe way
    DorisCallOnce<Status> _load_index_once;
    // used to guarantee that primary key bloom filter will be loaded at most once in a thread-safe way
    DorisCallOnce<Status> _load_pk_bf_once;
    // used to hold short key index page in memory
    PageHandle _sk_index_handle;
    // short key index decoder
    std::unique_ptr<ShortKeyIndexDecoder> _sk_index_decoder;
    // primary key index reader
    std::unique_ptr<PrimaryKeyIndexReader> _pk_index_reader;
    // Segment may be destructed after StorageEngine, in order to exit gracefully.
    std::shared_ptr<MemTracker> _segment_meta_mem_tracker;
};

} // namespace segment_v2
} // namespace doris

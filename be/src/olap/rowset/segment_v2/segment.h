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
#include <string>
#include <memory> // for unique_ptr
#include <vector>

#include "common/status.h" // Status
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "olap/iterators.h"
#include "olap/rowset/segment_v2/common.h" // rowid_t
#include "olap/short_key_index.h"
#include "olap/tablet_schema.h"
#include "util/faststring.h"
#include "util/once.h"

namespace doris {

class RandomAccessFile;
class SegmentGroup;
class TabletSchema;
class ShortKeyIndexDecoder;
class Schema;
class StorageReadOptions;

namespace segment_v2 {

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
    static Status open(std::string filename,
                       uint32_t segment_id,
                       const TabletSchema* tablet_schema,
                       std::shared_ptr<Segment>* output);

    ~Segment();

    Status new_iterator(
            const Schema& schema,
            const StorageReadOptions& read_options,
            std::unique_ptr<RowwiseIterator>* iter);

    uint64_t id() const { return _segment_id; }

    uint32_t num_rows() const { return _footer.num_rows(); }

private:
    friend class SegmentIterator;

    DISALLOW_COPY_AND_ASSIGN(Segment);
    Segment(std::string fname, uint32_t segment_id, const TabletSchema* tablet_schema);
    // open segment file and read the minimum amount of necessary information (footer)
    Status _open();
    Status _parse_footer();
    Status _create_column_readers();

    Status new_column_iterator(uint32_t cid, ColumnIterator** iter);
    size_t num_short_keys() const { return _tablet_schema->num_short_key_columns(); }

    // Load and decode short key index.
    // May be called multiple times, subsequent calls will no op.
    Status _load_index();

    uint32_t num_rows_per_block() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _sk_index_decoder->num_rows_per_block();
    }
    ShortKeyIndexIterator lower_bound(const Slice& key) const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _sk_index_decoder->lower_bound(key);
    }
    ShortKeyIndexIterator upper_bound(const Slice& key) const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        return _sk_index_decoder->upper_bound(key);
    }

    // This will return the last row block in this segment.
    // NOTE: Before call this function , client should assure that
    // this segment is not empty.
    uint32_t last_block() const {
        DCHECK(_load_index_once.has_called() && _load_index_once.stored_result().ok());
        DCHECK(num_rows() > 0);
        return _sk_index_decoder->num_items() - 1;
    }

private:
    std::string _fname;
    uint32_t _segment_id;
    const TabletSchema* _tablet_schema;

    SegmentFooterPB _footer;
    std::unique_ptr<RandomAccessFile> _input_file;

    // Map from column unique id to column ordinal in footer's ColumnMetaPB
    // If we can't find unique id from it, it means this segment is created
    // with an old schema.
    std::unordered_map<uint32_t, uint32_t> _column_id_to_footer_ordinal;

    // ColumnReader for each column in TabletSchema. If ColumnReader is nullptr,
    // This means that this segment has no data for that column, which may be added
    // after this segment is generated.
    std::vector<std::unique_ptr<ColumnReader>> _column_readers;

    // used to guarantee that short key index will be loaded at most once in a thread-safe way
    DorisCallOnce<Status> _load_index_once;
    // used to store short key index
    faststring _sk_index_buf;
    // short key index decoder
    std::unique_ptr<ShortKeyIndexDecoder> _sk_index_decoder;
};

}
}

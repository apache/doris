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
#include "olap/rowset/segment_v2/common.h" // rowid_t
#include "olap/short_key_index.h"
#include "olap/tablet_schema.h"
#include "util/faststring.h"

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
// And user can create a SegmentIterator through new_iterator function.
//
// NOTE: This segment is used to a specified TabletSchema, when TabletSchema
// is changed, this segemnt can not be used any more. For eample, after a schema
// change finished, client should disalbe all cahced Segment for old TabletSchema.
class Segment : public std::enable_shared_from_this<Segment> {
public:
    Segment(std::string fname, uint32_t segment_id,
            const TabletSchema* tablet_schema);
    ~Segment();

    Status open();

    std::unique_ptr<SegmentIterator> new_iterator(const Schema& schema, const StorageReadOptions& read_options);

    uint64_t id() const { return _segment_id; }

    uint32_t num_rows() const { return _footer.num_rows(); }

private:
    friend class SegmentIterator;

    Status new_column_iterator(uint32_t cid, ColumnIterator** iter);
    uint32_t num_rows_per_block() const { return _sk_index_decoder->num_rows_per_block(); }
    size_t num_short_keys() const { return _tablet_schema->num_short_key_columns(); }

    Status _check_magic(uint64_t offset);
    Status _parse_footer();
    Status _parse_index();
    Status _initial_column_readers();

    ShortKeyIndexIterator lower_bound(const Slice& key) const {
        return _sk_index_decoder->lower_bound(key);
    }
    ShortKeyIndexIterator upper_bound(const Slice& key) const {
        return _sk_index_decoder->upper_bound(key);
    }

    // This will return the last row block in this segment.
    // NOTE: Before call this function , client should assure that
    // this segment is not empty.
    uint32_t last_block() const {
        DCHECK(num_rows() > 0);
        return _sk_index_decoder->num_items() - 1;
    }

private:
    std::string _fname;
    uint32_t _segment_id;
    const TabletSchema* _tablet_schema;

    SegmentFooterPB _footer;
    std::unique_ptr<RandomAccessFile> _input_file;
    uint64_t _file_size = 0;

    // ColumnReader for each column in TabletSchema. If ColumnReader is nullptr,
    // This means that this segment has no data for that column, which may be added
    // after this segment is generated.
    std::vector<ColumnReader*> _column_readers;

    // used to store short key index
    faststring _sk_index_buf;

    // short key index decoder
    std::unique_ptr<ShortKeyIndexDecoder> _sk_index_decoder;
};

}
}

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
#include "gen_cpp/segment_v2.pb.h" // SegmentHeaderPB
#include "olap/rowset/segment_v2/common.h" // rowid_t
#include "olap/schema2.h"

namespace doris {

class RandomAccessFile;
class SegmentGroup;
class FieldInfo;
class ShortKeyIndexIterator;
class MemIndex;

namespace segment_v2 {

class ColumnReader;
class ColumnIterator;

class SegmentIterator;

// A Segment is used to represent a segment in memory format. When segment is
// generated, it won't be modified, so this struct aimed to help read operation.
// It will prepare all ColumnReader to create ColumnIterator as needed.
// And user can create a SegmentIterator through new_iterator function.
class Segment : public std::enable_shared_from_this<Segment> {
public:
    Segment(std::string fname, const std::vector<ColumnSchemaV2>& schema,
            size_t num_short_keys, uint32_t segment_id);
    ~Segment();

    Status open();

    Status new_iterator(std::unique_ptr<SegmentIterator>* iter);

    const std::vector<ColumnSchemaV2>& schema() { return _schema; }

    uint64_t id() const { return _segment_id; }

    uint32_t num_rows() const { return _footer.num_rows(); }

private:
    friend class SegmentIterator;

    Status new_column_iterator(uint32_t cid, ColumnIterator** iter);
    ShortKeyIndexIterator* new_short_key_index_iterator();
    uint32_t num_rows_per_block() const { return _num_rows_per_block; }

    Status _parse_magic_and_len(uint64_t offset, uint32_t* length);
    Status _parse_footer();
    Status _parse_header();
    Status _parse_index();
    Status _initial_column_readers();

private:
    std::string _fname;
    std::vector<ColumnSchemaV2> _schema;
    size_t _num_short_keys;
    uint32_t _segment_id;

    SegmentHeaderPB _header;
    SegmentFooterPB _footer;
    std::unique_ptr<RandomAccessFile> _input_file;
    uint64_t _file_size = 0;
    uint32_t _num_rows_per_block = 1024;
    std::vector<ColumnReader*> _column_readers;

    // TODO(zc): this should be refactored
    uint32_t _short_key_length = 0;
    uint32_t _new_short_key_length = 0;
    std::vector<FieldInfo> _field_infos;
    std::unique_ptr<MemIndex> _mem_index;
};

}
}

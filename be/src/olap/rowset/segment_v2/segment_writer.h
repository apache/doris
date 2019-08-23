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
#include <memory> // unique_ptr
#include <string>
#include <vector>

#include "common/logging.h" // LOG
#include "common/status.h" // Status
#include "gen_cpp/segment_v2.pb.h"
#include "olap/schema.h"

namespace doris {

class WritableFile;
class RowBlock;
class RowCursor;
class ShortKeyIndexBuilder;

namespace segment_v2 {

class ColumnWriter;

extern const char* k_segment_magic;
extern const uint32_t k_segment_magic_length;

struct SegmentWriterOptions {
    uint32_t num_rows_per_block = 1024;
};

class SegmentWriter {
public:
    explicit SegmentWriter(std::string file_name,
                           uint32_t segment_id,
                           const TabletSchema* tablet_schema,
                           const SegmentWriterOptions& opts);

    ~SegmentWriter();
    Status init(uint32_t write_mbytes_per_sec);

    template<typename RowType>
    Status append_row(const RowType& row);

    uint64_t estimate_segment_size();

    Status finalize(uint32_t* segment_file_size);

private:
    Status _write_data();
    Status _write_ordinal_index();
    Status _write_zone_map();
    Status _write_short_key_index();
    Status _write_footer();
    Status _write_raw_data(const std::vector<Slice>& slices);

private:
    std::string _fname;
    uint32_t _segment_id;
    const TabletSchema* _tablet_schema;
    size_t _num_short_keys;
    SegmentWriterOptions _opts;

    SegmentFooterPB _footer;
    std::unique_ptr<ShortKeyIndexBuilder> _index_builder;
    std::unique_ptr<WritableFile> _output_file;
    std::vector<ColumnWriter*> _column_writers;
    uint64_t _row_count = 0;
    uint32_t _block_count = 0;
};

}
}

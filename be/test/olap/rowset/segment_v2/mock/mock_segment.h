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

#include <gmock/gmock.h>

#include <memory>
#include <unordered_map>

#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_reader.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/tablet_schema.h"

namespace doris::segment_v2 {

// Forward declaration
class ColumnReaderCache;

class MockSegment : public Segment {
public:
    MockSegment() : Segment(1, RowsetId(), std::make_shared<TabletSchema>(), {}) {}
    ~MockSegment() override = default;

    // Mock methods for file reader
    MOCK_METHOD(io::FileReaderSPtr, file_reader, (), (const));

    // Mock methods for segment info
    MOCK_METHOD(uint32_t, num_rows, (), (const));

    // Mock methods for footer - make it virtual and public
    MOCK_METHOD(Status, _get_segment_footer,
                (std::shared_ptr<SegmentFooterPB>&, OlapReaderStatistics*), ());

    // Helper methods for test setup
    void add_column_uid_mapping(int32_t col_uid, int32_t footer_ordinal) {
        _column_uid_to_footer_ordinal[col_uid] = footer_ordinal;
    }

    void set_footer(std::shared_ptr<SegmentFooterPB> footer) { _footer = footer; }

    // Access to internal data for testing
    const std::unordered_map<int32_t, size_t>& get_column_uid_mapping() const {
        return _column_uid_to_footer_ordinal;
    }

    std::shared_ptr<SegmentFooterPB> get_footer() const { return _footer; }

    std::shared_ptr<SegmentFooterPB> _footer;

    // Make ColumnReaderCache a friend so it can access private members
    friend class ColumnReaderCache;
};

} // namespace doris::segment_v2
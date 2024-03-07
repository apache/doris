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

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/segment_v2/segment_writer.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

class VerticalBetaRowsetWriterHelper {
public:
    VerticalBetaRowsetWriterHelper(
            std::vector<std::unique_ptr<segment_v2::SegmentWriter>>* segment_writers,
            bool& already_built, RowsetMetaSharedPtr& rowset_meta,
            std::atomic<int32_t>* num_segment, RowsetWriterContext& context,
            std::atomic<int64_t>* _num_rows_written,
            std::vector<KeyBoundsPB>* _segments_encoded_key_bounds,
            std::vector<uint32_t>* _segment_num_rows, std::atomic<int64_t>* _total_index_size,
            std::vector<io::FileWriterPtr>* _file_writers, std::atomic<int64_t>* _total_data_size,
            SpinLock* _lock);
    ~VerticalBetaRowsetWriterHelper() = default;

    Status add_columns(const vectorized::Block* block, const std::vector<uint32_t>& col_ids,
                       bool is_key, uint32_t max_rows_per_segment);

    Status flush_columns(bool is_key);

    Status final_flush();

    int64_t num_rows() const { return _total_key_group_rows; }

    void destruct_writer();

private:
    Status _flush_columns(std::unique_ptr<segment_v2::SegmentWriter>* segment_writer,
                          bool is_key = false);
    Status _create_segment_writer(const std::vector<uint32_t>& column_ids, bool is_key,
                                  std::unique_ptr<segment_v2::SegmentWriter>* writer);

private:
    std::vector<std::unique_ptr<segment_v2::SegmentWriter>>* _segment_writers;
    size_t _cur_writer_idx = 0;
    size_t _total_key_group_rows = 0;

    bool& _already_built;
    RowsetMetaSharedPtr& _rowset_meta;
    std::atomic<int32_t>* _num_segment;
    RowsetWriterContext& _context;
    std::atomic<int64_t>* _num_rows_written;
    std::vector<KeyBoundsPB>* _segments_encoded_key_bounds;
    std::vector<uint32_t>* _segment_num_rows;
    std::atomic<int64_t>* _total_index_size;
    std::vector<io::FileWriterPtr>* _file_writers;
    std::atomic<int64_t>* _total_data_size;
    SpinLock* _lock;
};

} // namespace doris
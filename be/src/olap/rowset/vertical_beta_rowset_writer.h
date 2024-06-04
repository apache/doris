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

#include <memory>
#include <type_traits>
#include <vector>

#include "common/status.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/segment_v2/segment_writer.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized

// for vertical compaction
template <class T>
    requires std::is_base_of_v<BaseBetaRowsetWriter, T>
class VerticalBetaRowsetWriter final : public T {
public:
    template <class... Args>
    explicit VerticalBetaRowsetWriter(Args&&... args) : T(std::forward<Args>(args)...) {}

    ~VerticalBetaRowsetWriter() override = default;

    Status add_columns(const vectorized::Block* block, const std::vector<uint32_t>& col_ids,
                       bool is_key, uint32_t max_rows_per_segment) override;

    // flush last segment's column
    Status flush_columns(bool is_key) override;

    // flush when all column finished, flush column footer
    Status final_flush() override;

    int64_t num_rows() const override { return _total_key_group_rows; }

    Status _close_file_writers() override;

private:
    Status _flush_columns(segment_v2::SegmentWriter* segment_writer, bool is_key = false);
    Status _create_segment_writer(const std::vector<uint32_t>& column_ids, bool is_key,
                                  std::unique_ptr<segment_v2::SegmentWriter>* writer);

    std::vector<std::unique_ptr<segment_v2::SegmentWriter>> _segment_writers;
    size_t _cur_writer_idx = 0;
    size_t _total_key_group_rows = 0;
};

} // namespace doris
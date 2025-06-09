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

#include <cctz/time_zone.h>

#include <utility>

#include "vec/exprs/vexpr_fwd.h"
#include "vec/spill/spill_reader.h"

namespace doris {
#include "common/compile_check_begin.h"

struct MaterializedSchemaTableTimeSlice {
    MaterializedSchemaTableTimeSlice(int64_t begin_timestamp, int64_t end_timestamp,
                                     int64_t disk_space_bytes, std::string file_path)
            : begin_timestamp_(begin_timestamp),
              end_timestamp_(end_timestamp),
              disk_space_bytes_(disk_space_bytes),
              file_path_(std::move(file_path)) {}

    std::string debug_string() {
        return fmt::format(
                "begin_timestamp: {}, end_timestamp: {}, disk_space_bytes: {}, file_path: {}",
                begin_timestamp_, end_timestamp_, disk_space_bytes_, file_path_);
    }

    int64_t begin_timestamp_;
    int64_t end_timestamp_;
    int64_t disk_space_bytes_;
    std::string file_path_;
};

// Not thread-safe
class MaterializedSchemaTableReader {
public:
    MaterializedSchemaTableReader(std::vector<MaterializedSchemaTableTimeSlice> time_slices,
                                  std::vector<vectorized::VExprContextSPtr> expr_ctxs,
                                  size_t batch_size, cctz::time_zone timezone_obj)
            : time_slices_(std::move(time_slices)),
              expr_ctxs_(std::move(expr_ctxs)),
              batch_size_(batch_size),
              timezone_obj_(timezone_obj) {}

    ~MaterializedSchemaTableReader() = default;

    Status prepare(vectorized::Block* active_block);

    Status get_batch(vectorized::Block* output, bool* eos);

private:
    Status read_next_time_slice_(vectorized::Block* block, bool* eos);
    Status filter_time_slice_();

    std::vector<MaterializedSchemaTableTimeSlice> time_slices_;
    size_t read_time_slice_offset_ = 0;
    std::vector<vectorized::VExprContextSPtr> expr_ctxs_;
    size_t batch_size_ = 1;
    cctz::time_zone timezone_obj_;

    size_t row_idx_ = 0;
    bool read_time_slice_eos_ = false;
    std::unique_ptr<vectorized::Block> materialized_block_ = nullptr;
};

using MaterializedSchemaTableReaderSPtr = std::shared_ptr<MaterializedSchemaTableReader>;

#include "common/compile_check_end.h"
} // namespace doris

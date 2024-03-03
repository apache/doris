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

#include "olap/rowset/vertical_beta_rowset_writer.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer_context.h"
#include "util/slice.h"
#include "util/spinlock.h"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

VerticalBetaRowsetWriter::VerticalBetaRowsetWriter(StorageEngine& engine)
        : BetaRowsetWriter(engine) {
    _helper = std::make_shared<VerticalBetaRowsetWriterHelper>(
            &_segment_writers, _already_built, _rowset_meta, &_num_segment, _context,
            &_num_rows_written, &_segments_encoded_key_bounds, &_segment_num_rows,
            &_total_index_size, &_file_writers, &_total_data_size, &_lock);
}

VerticalBetaRowsetWriter::~VerticalBetaRowsetWriter() {
    _helper->destruct_writer();
}

Status VerticalBetaRowsetWriter::add_columns(const vectorized::Block* block,
                                             const std::vector<uint32_t>& col_ids, bool is_key,
                                             uint32_t max_rows_per_segment) {
    return _helper->add_columns(block, col_ids, is_key, max_rows_per_segment);
}

Status VerticalBetaRowsetWriter::flush_columns(bool is_key) {
    return _helper->flush_columns(is_key);
}

Status VerticalBetaRowsetWriter::final_flush() {
    return _helper->final_flush();
}

} // namespace doris

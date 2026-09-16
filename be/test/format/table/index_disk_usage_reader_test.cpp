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

#include "format/table/index_disk_usage_reader.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include "io/io_common.h"
#include "runtime/runtime_state.h"

namespace doris {

// The reader tags every index file read with its query, so the file cache and the remote scan
// cache write limiter treat index_disk_usage like any other scan of that query.
TEST(IndexDiskUsageReaderTest, InitReaderBuildsQueryIoContext) {
    RuntimeState state;
    TIndexDiskUsageMetadataParams params;
    params.__set_level("tablet");
    TMetaScanRange scan_range;
    scan_range.__set_index_disk_usage_params(params);

    IndexDiskUsageReader reader({}, &state, nullptr, scan_range);
    const Status st = reader.init_reader();
    ASSERT_TRUE(st.ok()) << st;
    const io::IOContext* io_ctx = reader.options().io_ctx;
    ASSERT_NE(io_ctx, nullptr);
    EXPECT_EQ(ReaderType::READER_QUERY, io_ctx->reader_type);
    EXPECT_EQ(&state.query_id(), io_ctx->query_id);
    EXPECT_TRUE(io_ctx->is_inverted_index);
}

} // namespace doris

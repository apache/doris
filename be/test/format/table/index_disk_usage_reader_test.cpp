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

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>

#include "io/io_common.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

std::unique_ptr<IndexDiskUsageReader> make_reader(RuntimeState* state) {
    TIndexDiskUsageMetadataParams params;
    params.__set_level("tablet");
    TMetaScanRange scan_range;
    scan_range.__set_index_disk_usage_params(params);
    return std::make_unique<IndexDiskUsageReader>(std::vector<SlotDescriptor*> {}, state, nullptr,
                                                  scan_range);
}

} // namespace

// The reader tags every index file read with its query, so the file cache and the remote scan
// cache write limiter treat index_disk_usage like any other scan of that query.
TEST(IndexDiskUsageReaderTest, InitReaderBuildsQueryIoContext) {
    RuntimeState state;
    auto reader = make_reader(&state);
    const Status st = reader->init_reader();
    ASSERT_TRUE(st.ok()) << st;
    const io::IOContext* io_ctx = reader->options().io_ctx;
    ASSERT_NE(io_ctx, nullptr);
    EXPECT_EQ(ReaderType::READER_QUERY, io_ctx->reader_type);
    EXPECT_EQ(&state.query_id(), io_ctx->query_id);
    EXPECT_TRUE(io_ctx->is_inverted_index);
    EXPECT_FALSE(io_ctx->inverted_index_snii_read_no_write_file_cache);
}

// The SNII cache write policy of the session applies to this scan as it does to a tablet read.
TEST(IndexDiskUsageReaderTest, InitReaderCopiesSniiCachePolicy) {
    RuntimeState state;
    TQueryOptions query_options;
    query_options.__set_inverted_index_snii_read_no_write_file_cache(true);
    state.set_query_options(query_options);
    auto reader = make_reader(&state);
    const Status st = reader->init_reader();
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(reader->options().io_ctx->inverted_index_snii_read_no_write_file_cache);
}

// Reads of a TTL table are classified by the tablet TTL, so each tablet gets its own context.
TEST(IndexDiskUsageReaderTest, TabletIoContextCarriesTtl) {
    TUniqueId query_id;
    io::IOContext root;
    root.reader_type = ReaderType::READER_QUERY;
    root.query_id = &query_id;
    const io::IOContext tablet_ctx = IndexDiskUsageReader::tablet_io_context(root, 3600);
    EXPECT_EQ(3600, tablet_ctx.expiration_time);
    EXPECT_EQ(ReaderType::READER_QUERY, tablet_ctx.reader_type);
    EXPECT_EQ(&query_id, tablet_ctx.query_id);
    EXPECT_EQ(0, root.expiration_time);
}

} // namespace doris

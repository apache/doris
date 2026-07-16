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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "cpp/sync_point.h"
#include "io/cache/remote_scan_cache_write_limiter.h"
#include "storage/iterators.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/schema.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;

class IndexStorageVariantIoContextTest : public IndexStorageTestFixture {};

TEST_F(IndexStorageVariantIoContextTest,
       GetDataTypeOfPropagatesQueryIoContextToExternalVariantMeta) {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.max_subcolumns_count = 4;
    VariantPathSpec hot_path;
    hot_path.path = "hot";
    hot_path.type = FieldType::OLAP_FIELD_TYPE_INT;
    variant.predefined_paths = {std::move(hot_path)};

    const auto index_case =
            IndexStorageCaseBuilder("variant_get_data_type_of_query_io_context")
                    .variant_column(std::move(variant))
                    .rowset(0, IndexDataSourceSpec::inline_variant({R"({"hot": 1})"}))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();
    ASSERT_FALSE(rowsets->empty());

    auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(rowsets->front());
    ASSERT_NE(beta_rowset, nullptr);
    std::vector<segment_v2::SegmentSharedPtr> segments;
    ASSERT_TRUE(beta_rowset->load_segments(&segments).ok());
    ASSERT_FALSE(segments.empty());

    auto reader_schema = build_schema_with_variant_path_column(*tablet_schema(), kVariantUid, "hot",
                                                               FieldType::OLAP_FIELD_TYPE_INT);
    const ColumnId path_column_id = static_cast<ColumnId>(reader_schema->num_columns() - 1);
    auto scan_schema = std::make_shared<Schema>(reader_schema->columns(),
                                                std::vector<ColumnId> {path_column_id});

    TUniqueId query_id;
    query_id.hi = 101;
    query_id.lo = 202;
    io::RemoteScanCacheWriteLimiter limiter(query_id, 0);
    OlapReaderStatistics stats;
    StorageReadOptions read_options;
    read_options.stats = &stats;
    read_options.tablet_schema = reader_schema;
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.io_ctx.query_id = &query_id;
    read_options.io_ctx.file_cache_stats = &stats.file_cache_stats;
    read_options.io_ctx.file_cache_miss_policy = io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS;
    read_options.io_ctx.remote_scan_cache_write_limiter = &limiter;

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    bool observed_key_reader_io_ctx = false;
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "VariantExternalMetaReader::init_from_footer:key_reader_io_ctx",
            [&](auto&& args) {
                const auto* source_io_ctx = try_any_cast<const io::IOContext*>(args[0]);
                ASSERT_NE(source_io_ctx, nullptr);
                observed_key_reader_io_ctx = true;
                EXPECT_EQ(source_io_ctx->reader_type, ReaderType::READER_QUERY);
                EXPECT_EQ(source_io_ctx->query_id, &query_id);
                EXPECT_EQ(source_io_ctx->file_cache_stats, &stats.file_cache_stats);
                EXPECT_EQ(source_io_ctx->file_cache_miss_policy,
                          io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS);
                EXPECT_EQ(source_io_ctx->remote_scan_cache_write_limiter, &limiter);
            },
            &guard);

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segments.front()->new_iterator(scan_schema, read_options, &iter);
    sp->disable_processing();

    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(iter, nullptr);
    EXPECT_TRUE(observed_key_reader_io_ctx);
}

} // namespace
} // namespace doris::index_storage_test

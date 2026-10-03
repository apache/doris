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

#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "format/table/index_disk_usage_reader.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "storage/index/index_disk_usage.h"
#include "storage/index/index_writer.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

class IndexDiskUsageRowsetTest : public ::testing::Test {
protected:
    const std::string kTestDir = "./ut_dir/index_disk_usage_rowset_test";

    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(kTestDir, 1024000000);
        auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
        EngineOptions options;
        options.store_paths = paths;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // A duplicate-key table whose value column has an inverted index in the V2 format.
    static TabletSchemaSPtr create_schema() {
        auto schema = std::make_shared<TabletSchema>();
        schema->_keys_type = KeysType::DUP_KEYS;
        schema->_inverted_index_storage_format = InvertedIndexStorageFormatPB::V2;

        TabletColumn key;
        key.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        key.set_unique_id(1);
        key.set_name("k1");
        key.set_is_key(true);
        key.set_index_length(4);
        schema->append_column(key);

        TabletColumn value;
        value.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        value.set_unique_id(2);
        value.set_name("v1");
        value.set_is_key(false);
        schema->append_column(value);

        TabletIndexPB index_pb;
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.set_index_id(10001);
        index_pb.set_index_name("idx_v1");
        index_pb.add_col_unique_id(2);
        TabletIndex index;
        index.init_from_pb(index_pb);
        schema->append_index(std::move(index));
        return schema;
    }

    // Each test uses its own rowset id, so segment footers cached by another test are not reused.
    Status write_rowset(const TabletSchemaSPtr& schema, int64_t rowset_id, int32_t num_rows,
                        RowsetSharedPtr* rowset) {
        RowsetWriterContext context;
        context.rowset_id.init(rowset_id);
        context.tablet_id = 1001;
        context.tablet_schema_hash = 1111;
        context.partition_id = 10;
        context.rowset_type = BETA_ROWSET;
        context.tablet_path = kTestDir;
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.version = Version(2, 2);
        auto writer = DORIS_TRY(RowsetFactory::create_rowset_writer(*_engine, context, false));

        Block block = schema->create_storage_block();
        auto columns = std::move(block).mutate_columns();
        for (int32_t i = 0; i < num_rows; ++i) {
            columns[0]->insert_data(reinterpret_cast<const char*>(&i), sizeof(i));
            columns[1]->insert_data(reinterpret_cast<const char*>(&i), sizeof(i));
        }
        block = schema->create_storage_block();
        block.set_columns(std::move(columns));
        RETURN_IF_ERROR(writer->add_block(&block));
        RETURN_IF_ERROR(writer->flush());
        return writer->build(*rowset);
    }

    StorageEngine* _engine = nullptr;
};

TEST_F(IndexDiskUsageRowsetTest, RowCountFallsBackToSegmentFooters) {
    RowsetSharedPtr rowset;
    const Status write_status = write_rowset(create_schema(), 20001, 8, &rowset);
    ASSERT_TRUE(write_status.ok()) << write_status;
    // Rowsets written before per-segment row counts were persisted have none in their meta.
    rowset->rowset_meta()->set_num_segment_rows({});

    TUniqueId query_id;
    query_id.hi = 7;
    query_id.lo = 9;
    io::IOContext io_ctx;
    io_ctx.reader_type = ReaderType::READER_QUERY;
    io_ctx.query_id = &query_id;
    IndexDiskUsageOptions options;
    options.io_ctx = &io_ctx;

    // The footer reads carry the query context like the index file reads do.
    int footer_reads = 0;
    int footer_reads_with_context = 0;
    auto* sync_point = SyncPoint::get_instance();
    sync_point->enable_processing();
    std::vector<IndexDiskUsageRow> rows;
    Status st;
    {
        SyncPoint::CallbackGuard guard;
        sync_point->set_call_back(
                "Segment::_parse_footer::io_ctx",
                [&](auto&& args) {
                    const auto* ctx = try_any_cast<io::IOContext*>(args[0]);
                    ++footer_reads;
                    if (ctx->reader_type == ReaderType::READER_QUERY &&
                        ctx->query_id == &query_id) {
                        ++footer_reads_with_context;
                    }
                },
                &guard);
        st = collect_rowset_index_disk_usage(rowset, options, /*tablet_id=*/1001, &rows);
    }
    sync_point->disable_processing();
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(rows.empty());
    for (const auto& row : rows) {
        EXPECT_EQ(8, row.row_count) << "index " << row.record.index_id;
    }
    EXPECT_GT(footer_reads, 0);
    EXPECT_EQ(footer_reads, footer_reads_with_context);
}

// A failed footer read must not stay cached in the rowset, where later queries and compactions
// would keep getting it.
TEST_F(IndexDiskUsageRowsetTest, RowCountFailureIsNotCachedInRowset) {
    RowsetSharedPtr rowset;
    const Status write_status = write_rowset(create_schema(), 20002, 8, &rowset);
    ASSERT_TRUE(write_status.ok()) << write_status;
    rowset->rowset_meta()->set_num_segment_rows({});

    auto* sync_point = SyncPoint::get_instance();
    sync_point->enable_processing();
    {
        SyncPoint::CallbackGuard guard;
        sync_point->set_call_back(
                "Segment::parse_footer:magic_number_corruption",
                [](auto&& args) {
                    auto* buf = try_any_cast<uint8_t*>(args[0]);
                    buf[8] = 0xFF;
                },
                &guard);
        std::vector<IndexDiskUsageRow> rows;
        EXPECT_FALSE(collect_rowset_index_disk_usage(rowset, IndexDiskUsageOptions {},
                                                     /*tablet_id=*/1001, &rows)
                             .ok());
    }
    sync_point->disable_processing();

    std::vector<uint32_t> segment_rows;
    OlapReaderStatistics stats;
    const Status st = std::static_pointer_cast<BetaRowset>(rowset)->get_segment_num_rows(
            &segment_rows, /*enable_segment_cache=*/false, &stats);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(std::vector<uint32_t>({8}), segment_rows);
}

// Rows are labeled with the newest schema among the rowsets, so an index added after the tablet
// was cached still resolves to its name.
TEST_F(IndexDiskUsageRowsetTest, LabelSchemaFollowsNewestRowsetSchema) {
    auto old_schema = create_schema();
    auto new_schema = std::make_shared<TabletSchema>();
    new_schema->copy_from(*old_schema);
    TabletIndexPB index_pb;
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.set_index_id(10002);
    index_pb.set_index_name("idx_k1");
    index_pb.add_col_unique_id(1);
    TabletIndex index;
    index.init_from_pb(index_pb);
    new_schema->append_index(std::move(index));
    new_schema->set_schema_version(old_schema->schema_version() + 1);

    RowsetSharedPtr old_rowset;
    RowsetSharedPtr new_rowset;
    ASSERT_TRUE(write_rowset(old_schema, 20003, 4, &old_rowset).ok());
    ASSERT_TRUE(write_rowset(new_schema, 20004, 4, &new_rowset).ok());

    TabletSchemaSPtr label =
            IndexDiskUsageReader::label_schema(old_schema, {new_rowset, old_rowset});
    EXPECT_NE(nullptr, resolve_disk_usage_index(*label, 10002, ""));
    EXPECT_EQ(old_schema.get(), IndexDiskUsageReader::label_schema(old_schema, {}).get());
}

} // namespace doris::segment_v2

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

#include "load/memtable/memtable_flush_executor.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>
#include <sys/file.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include "common/config.h"
#include "exec/sink/autoinc_buffer.h"
#include "io/fs/local_file_system.h"
#include "load/delta_writer/delta_writer.h"
#include "load/memtable/memtable.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "storage/field.h"
#include "storage/options.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "storage/utils.h"
#include "testutil/creators.h"

namespace doris {

namespace {

class MockRowsetWriter final : public RowsetWriter {
public:
    explicit MockRowsetWriter(std::atomic<int>* flush_cnt, bool fail_on_flush = false,
                              const std::string& flush_error_msg = "mock flush failed",
                              int flush_delay_ms = 0)
            : _flush_cnt(flush_cnt),
              _fail_on_flush(fail_on_flush),
              _flush_error_msg(flush_error_msg),
              _flush_delay_ms(flush_delay_ms) {}

    Status init(const RowsetWriterContext& ctx) override {
        _context = ctx;
        return Status::OK();
    }

    Status add_rowset(RowsetSharedPtr) override { return Status::OK(); }

    Status add_rowset_for_linked_schema_change(RowsetSharedPtr) override { return Status::OK(); }

    Status flush() override { return Status::OK(); }

    Status flush_memtable(Block* block, int32_t segment_id, int64_t* flush_size) override {
        EXPECT_GT(block->rows(), 0);
        if (_flush_delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(_flush_delay_ms));
        }
        _last_segment_id = segment_id;
        ++(*_flush_cnt);
        *flush_size = 1;
        if (_fail_on_flush) {
            return Status::InternalError<false>(_flush_error_msg);
        }
        return Status::OK();
    }

    Status build(RowsetSharedPtr& rowset) override {
        rowset = nullptr;
        return Status::OK();
    }

    RowsetSharedPtr manual_build(const RowsetMetaSharedPtr&) override { return nullptr; }

    PUniqueId load_id() override { return _context.load_id; }

    Version version() override { return _context.version; }

    int64_t num_rows() const override { return 0; }
    int64_t num_rows_updated() const override { return 0; }
    int64_t num_rows_deleted() const override { return 0; }
    int64_t num_rows_new_added() const override { return 0; }
    int64_t num_rows_filtered() const override { return 0; }
    RowsetId rowset_id() override { return _context.rowset_id; }
    RowsetTypePB type() const override { return BETA_ROWSET; }
    int32_t allocate_segment_id() override { return _next_segment_id++; }
    int32_t get_allocated_segment_id() override { return _next_segment_id; }
    std::shared_ptr<PartialUpdateInfo> get_partial_update_info() override { return nullptr; }
    bool is_partial_update() override { return false; }

    int32_t last_segment_id() const { return _last_segment_id; }

private:
    std::atomic<int>* _flush_cnt;
    bool _fail_on_flush;
    std::string _flush_error_msg;
    int _flush_delay_ms;
    int32_t _next_segment_id = 0;
    int32_t _last_segment_id = -1;
};

struct GroupFlushTestContext {
    TCreateTabletReq request;
    TDescriptorTable tdesc_tbl;
    TabletSharedPtr tablet;
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    TupleDescriptor* tuple_desc = nullptr;
    std::shared_ptr<MemTable> memtable;
};

class MemTableFlushExecutorGroupFlushTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[1024];
        ASSERT_NE(getcwd(buffer, 1024), nullptr);
        config::storage_root_path = std::string(buffer) + "/flush_test";
        auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(config::storage_root_path);
        ASSERT_TRUE(st.ok()) << st;

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        doris::EngineOptions options;
        options.store_paths = paths;
        auto engine = std::make_unique<StorageEngine>(options);
        Status s = engine->open();
        ASSERT_TRUE(s.ok()) << s.to_string();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_EQ(system("rm -rf ./flush_test"), 0);
        EXPECT_TRUE(
                io::global_local_filesystem()
                        ->delete_directory(std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX)
                        .ok());
    }

    StorageEngine* storage_engine() { return &ExecEnv::GetInstance()->storage_engine().to_local(); }

    std::shared_ptr<OlapTableSchemaParam> create_group_flush_table_schema_param(
            const GroupFlushTestContext& ctx) {
        auto table_schema_param = testutil::create_table_schema_param(
                ctx.tdesc_tbl, ctx.request.tablet_id, ctx.request.tablet_schema.schema_hash,
                ctx.request.tablet_schema.columns, ctx.request.tablet_id + 1,
                ctx.request.tablet_schema.schema_hash + 1, &ctx.request.tablet_schema.columns);
        EXPECT_NE(table_schema_param, nullptr);
        return table_schema_param;
    }

    Status create_group_rowset_writer(const GroupFlushTestContext& ctx, int64_t load_id,
                                      const std::shared_ptr<RowsetWriter>& data_writer,
                                      const std::shared_ptr<RowsetWriter>& binlog_writer,
                                      std::shared_ptr<GroupRowsetWriter>* group_writer) {
        RowsetWriterContext data_ctx;
        data_ctx.tablet_schema = ctx.tablet->tablet_schema();
        data_ctx.load_id.set_hi(load_id);
        data_ctx.load_id.set_lo(load_id);
        data_ctx.write_binlog_opt().mark_primary_writer();
        RETURN_IF_ERROR(data_writer->init(data_ctx));

        RowsetWriterContext binlog_ctx = data_ctx;
        binlog_ctx.write_binlog_opt().mark_binlog_writer();
        RETURN_IF_ERROR(binlog_writer->init(binlog_ctx));

        auto writer = std::make_shared<GroupRowsetWriter>();
        writer->set_data_writer(data_writer);
        writer->set_row_binlog_writer(binlog_writer);
        RETURN_IF_ERROR(writer->init(data_ctx));
        *group_writer = std::move(writer);
        return Status::OK();
    }

    Status create_group_flush_token(const GroupFlushTestContext& ctx,
                                    const std::shared_ptr<GroupRowsetWriter>& group_writer,
                                    int64_t start_lsn, std::shared_ptr<FlushToken>* flush_token,
                                    ThreadPool* pool = nullptr) {
        auto table_schema_param = create_group_flush_table_schema_param(ctx);
        if (table_schema_param == nullptr) {
            return Status::InternalError("failed to create group flush table schema param");
        }

        if (pool == nullptr) {
            RETURN_IF_ERROR(storage_engine()->memtable_flush_executor()->create_flush_token(
                    *flush_token, group_writer, false, nullptr, table_schema_param));
        } else {
            auto token = FlushToken::create_shared(pool, nullptr);
            token->set_rowset_writer(group_writer);
            token->set_table_schema_param(table_schema_param);
            *flush_token = std::move(token);
        }

        auto row_binlog_lsn_buffer = AutoIncIDBuffer::create_shared(
                table_schema_param->db_id(), table_schema_param->table_id(), kBinlogLsnAutoIncId);
        row_binlog_lsn_buffer->append_range_for_test(start_lsn, ctx.memtable->raw_rows());
        (*flush_token)->set_row_binlog_lsn_buffer_for_test(row_binlog_lsn_buffer);
        return Status::OK();
    }

    void prepare_group_flush_test_context(int64_t tablet_id, int32_t schema_hash,
                                          GroupFlushTestContext* ctx) {
        ctx->request = testutil::create_tablet_request(tablet_id, schema_hash, 30002, 3,
                                                       TKeysType::UNIQUE_KEYS,
                                                       {{"k1", TPrimitiveType::TINYINT, true},
                                                        {"k2", TPrimitiveType::SMALLINT, true},
                                                        {"k3", TPrimitiveType::INT, true}});
        ctx->request.__set_enable_unique_key_merge_on_write(true);
        testutil::enable_row_binlog(&ctx->request);
        auto profile = std::make_unique<RuntimeProfile>("CreateTablet");
        ASSERT_TRUE(storage_engine()->create_tablet(ctx->request, profile.get()).ok());

        ctx->tablet = storage_engine()->tablet_manager()->get_tablet(ctx->request.tablet_id);
        ASSERT_NE(ctx->tablet, nullptr);

        ctx->tdesc_tbl = testutil::create_descriptor_table({{TYPE_TINYINT, "k1", false},
                                                            {TYPE_SMALLINT, "k2", false},
                                                            {TYPE_INT, "k3", false}});
        ASSERT_TRUE(DescriptorTbl::create(&ctx->obj_pool, ctx->tdesc_tbl, &ctx->desc_tbl).ok());
        ctx->tuple_desc = ctx->desc_tbl->get_tuple_descriptor(0);
        ASSERT_NE(ctx->tuple_desc, nullptr);

        ctx->memtable = std::make_shared<MemTable>(
                ctx->request.tablet_id, ctx->tablet->tablet_schema(), &ctx->tuple_desc->slots(),
                ctx->tuple_desc, false, nullptr, thread_context()->resource_ctx());
        Block block;
        for (const auto& slot : ctx->tuple_desc->slots()) {
            block.insert(ColumnWithTypeAndName(slot->get_empty_mutable_column(), slot->type(),
                                               slot->col_name()));
        }
        auto cols = block.mutate_columns();
        int8_t k1 = -127;
        int16_t k2 = -32767;
        int32_t k3 = -2147483647;
        cols[0]->insert_data((const char*)&k1, sizeof(k1));
        cols[1]->insert_data((const char*)&k2, sizeof(k2));
        cols[2]->insert_data((const char*)&k3, sizeof(k3));
        ASSERT_TRUE(ctx->memtable->insert(&block, {0}).ok());
    }

    void drop_tablet(const TCreateTabletReq& request) {
        EXPECT_TRUE(storage_engine()
                            ->tablet_manager()
                            ->drop_tablet(request.tablet_id, request.replica_id, false)
                            .ok());
    }
};

} // namespace

void set_up() {
    char buffer[1024];
    ASSERT_NE(getcwd(buffer, 1024), nullptr);
    config::storage_root_path = std::string(buffer) + "/flush_test";
    auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;

    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    auto engine = std::make_unique<StorageEngine>(options);
    Status s = engine->open();
    EXPECT_TRUE(s.ok()) << s.to_string();
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
}

void tear_down() {
    ExecEnv::GetInstance()->set_storage_engine(nullptr);
    system("rm -rf ./flush_test");
    EXPECT_TRUE(io::global_local_filesystem()
                        ->delete_directory(std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX)
                        .ok());
}

TEST(MemTableFlushExecutorTest, TestDynamicThreadPoolUpdate) {
    // Setup
    set_up();

    auto* flush_executor = ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    ASSERT_NE(flush_executor, nullptr);

    // Store original config values
    int32_t original_flush_thread_num = config::flush_thread_num_per_store;
    int32_t original_high_priority_flush_thread_num =
            config::high_priority_flush_thread_num_per_store;
    int32_t original_max_flush_thread_num = config::max_flush_thread_num_per_cpu;
    bool original_adaptive = config::enable_adaptive_flush_threads;

    // Test 1: Get initial thread pool sizes
    int initial_max_threads = flush_executor->flush_pool()->max_threads();
    int initial_min_threads = flush_executor->flush_pool()->min_threads();
    EXPECT_GT(initial_max_threads, 0);
    EXPECT_GT(initial_min_threads, 0);

    // Disable adaptive mode so flush_thread_num_per_store takes effect
    config::enable_adaptive_flush_threads = false;

    // Test 2: Update flush_thread_num_per_store and verify thread pool updates
    config::flush_thread_num_per_store = 10;
    flush_executor->update_memtable_flush_threads();

    int new_min_threads = flush_executor->flush_pool()->min_threads();
    EXPECT_EQ(new_min_threads, 10);

    // Test 3: Update max_flush_thread_num_per_cpu and verify thread pool updates
    config::max_flush_thread_num_per_cpu = 2;
    flush_executor->update_memtable_flush_threads();

    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus > 0) {
        int expected_max = std::min(10 * 1, num_cpus * 2); // 1 disk, 10 threads per store
        int actual_max = flush_executor->flush_pool()->max_threads();
        EXPECT_EQ(actual_max, expected_max);
    }

    // Test 4: Update high_priority_flush_thread_num_per_store
    config::high_priority_flush_thread_num_per_store = 8;
    flush_executor->update_memtable_flush_threads();
    // Note: We can't directly access _high_prio_flush_pool, but update should not crash

    // Test 5: Set very small values
    config::flush_thread_num_per_store = 0; // Should be adjusted to 1 by std::max
    flush_executor->update_memtable_flush_threads();
    EXPECT_GE(flush_executor->flush_pool()->min_threads(), 1);

    // Test 6: Set large values
    config::flush_thread_num_per_store = 100;
    flush_executor->update_memtable_flush_threads();
    EXPECT_GE(flush_executor->flush_pool()->min_threads(), 1);

    // Restore original config values
    config::flush_thread_num_per_store = original_flush_thread_num;
    config::high_priority_flush_thread_num_per_store = original_high_priority_flush_thread_num;
    config::max_flush_thread_num_per_cpu = original_max_flush_thread_num;
    config::enable_adaptive_flush_threads = original_adaptive;
    flush_executor->update_memtable_flush_threads();

    // Cleanup
    tear_down();
}

TEST(MemTableFlushExecutorTest, TestConfigUpdateTrigger) {
    // Setup
    set_up();

    auto* flush_executor = ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    ASSERT_NE(flush_executor, nullptr);

    // Store original config values
    int32_t original_flush_thread_num = config::flush_thread_num_per_store;
    bool original_adaptive = config::enable_adaptive_flush_threads;

    // Disable adaptive mode so flush_thread_num_per_store takes effect
    config::enable_adaptive_flush_threads = false;
    flush_executor->update_memtable_flush_threads();

    // Get initial thread pool size
    int initial_min_threads = flush_executor->flush_pool()->min_threads();

    // Test: Simulate config update via set_config
    config::flush_thread_num_per_store = 15;
    config::update_config("flush_thread_num_per_store", "15");

    // Verify thread pool was updated
    int updated_min_threads = flush_executor->flush_pool()->min_threads();
    EXPECT_EQ(updated_min_threads, 15);
    EXPECT_NE(updated_min_threads, initial_min_threads);

    // Restore original config values
    config::flush_thread_num_per_store = original_flush_thread_num;
    config::enable_adaptive_flush_threads = original_adaptive;
    flush_executor->update_memtable_flush_threads();

    // Cleanup
    tear_down();
}

TEST(MemTableFlushExecutorTest, TestThreadPoolMinMaxRelationship) {
    // Setup
    set_up();

    auto* flush_executor = ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    ASSERT_NE(flush_executor, nullptr);

    // Store original config values
    int32_t original_flush_thread_num = config::flush_thread_num_per_store;
    int32_t original_max_flush_thread_num = config::max_flush_thread_num_per_cpu;
    bool original_adaptive = config::enable_adaptive_flush_threads;

    // Disable adaptive mode so flush_thread_num_per_store takes effect
    config::enable_adaptive_flush_threads = false;

    // Test: Ensure min_threads <= max_threads always
    config::flush_thread_num_per_store = 20;
    config::max_flush_thread_num_per_cpu = 1; // Very restrictive
    flush_executor->update_memtable_flush_threads();

    int min_threads = flush_executor->flush_pool()->min_threads();
    int max_threads = flush_executor->flush_pool()->max_threads();
    EXPECT_LE(min_threads, max_threads);

    // Restore original config values
    config::flush_thread_num_per_store = original_flush_thread_num;
    config::max_flush_thread_num_per_cpu = original_max_flush_thread_num;
    config::enable_adaptive_flush_threads = original_adaptive;
    flush_executor->update_memtable_flush_threads();

    // Cleanup
    tear_down();
}

TEST_F(MemTableFlushExecutorGroupFlushTest, TestGroupFlushToken) {
    SCOPED_INIT_THREAD_CONTEXT();

    {
        GroupFlushTestContext ctx;
        prepare_group_flush_test_context(10001, 270068373, &ctx);

        std::atomic<int> data_flush_cnt = 0;
        std::atomic<int> binlog_flush_cnt = 0;
        auto data_writer = std::make_shared<MockRowsetWriter>(&data_flush_cnt);
        auto binlog_writer = std::make_shared<MockRowsetWriter>(&binlog_flush_cnt);
        std::shared_ptr<GroupRowsetWriter> group_writer;
        ASSERT_TRUE(
                create_group_rowset_writer(ctx, 1, data_writer, binlog_writer, &group_writer).ok());

        std::shared_ptr<FlushToken> flush_token;
        ASSERT_TRUE(create_group_flush_token(ctx, group_writer, 1000, &flush_token).ok());
        ASSERT_TRUE(flush_token->submit(ctx.memtable).ok());
        ASSERT_TRUE(flush_token->wait().ok());
        EXPECT_EQ(1, data_flush_cnt.load());
        EXPECT_EQ(1, binlog_flush_cnt.load());
        EXPECT_EQ(data_writer->last_segment_id(), binlog_writer->last_segment_id());
        auto seg_lsn =
                binlog_writer->context().write_binlog_opt().write_binlog_config().get_seg_lsn(
                        binlog_writer->last_segment_id());
        ASSERT_NE(seg_lsn, nullptr);
        ASSERT_EQ(ctx.memtable->raw_rows(), seg_lsn->size());
        EXPECT_EQ(static_cast<int128_t>(1000), (*seg_lsn)[0]);
        EXPECT_EQ(2, flush_token->get_stats().flush_finish_count.load());
        EXPECT_EQ(0, flush_token->get_stats().flush_submit_count.load());

        drop_tablet(ctx.request);
    }

    {
        GroupFlushTestContext ctx;
        prepare_group_flush_test_context(10002, 270068374, &ctx);

        std::atomic<int> data_flush_cnt = 0;
        std::atomic<int> binlog_flush_cnt = 0;
        auto data_writer = std::make_shared<MockRowsetWriter>(&data_flush_cnt);
        auto binlog_writer =
                std::make_shared<MockRowsetWriter>(&binlog_flush_cnt, true, "binlog flush failed");
        std::shared_ptr<GroupRowsetWriter> group_writer;
        ASSERT_TRUE(
                create_group_rowset_writer(ctx, 2, data_writer, binlog_writer, &group_writer).ok());

        std::shared_ptr<FlushToken> flush_token;
        ASSERT_TRUE(create_group_flush_token(ctx, group_writer, 2000, &flush_token).ok());
        ASSERT_TRUE(flush_token->submit(ctx.memtable).ok());

        Status wait_st = flush_token->wait();
        EXPECT_FALSE(wait_st.ok());
        EXPECT_NE(wait_st.to_string().find("binlog flush failed"), std::string::npos);
        EXPECT_EQ(1, binlog_flush_cnt.load());
        // Data and binlog flush tasks run concurrently. If binlog fails first,
        // data flush may be skipped by the failed flush status.
        EXPECT_LE(flush_token->get_stats().flush_finish_count.load(), 1);
        EXPECT_EQ(0, flush_token->get_stats().flush_submit_count.load());

        drop_tablet(ctx.request);
    }
}

TEST_F(MemTableFlushExecutorGroupFlushTest, TestGroupFlushTokenPartialSuccess) {
    SCOPED_INIT_THREAD_CONTEXT();

    GroupFlushTestContext ctx;
    prepare_group_flush_test_context(10003, 270068375, &ctx);

    std::atomic<int> data_flush_cnt = 0;
    std::atomic<int> binlog_flush_cnt = 0;
    auto data_writer = std::make_shared<MockRowsetWriter>(&data_flush_cnt);
    auto binlog_writer =
            std::make_shared<MockRowsetWriter>(&binlog_flush_cnt, true, "binlog flush failed", 100);
    std::shared_ptr<GroupRowsetWriter> group_writer;
    ASSERT_TRUE(create_group_rowset_writer(ctx, 3, data_writer, binlog_writer, &group_writer).ok());

    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("MemTableGroupFlushTestPool")
                        .set_min_threads(2)
                        .set_max_threads(2)
                        .build(&pool)
                        .ok());

    std::shared_ptr<FlushToken> flush_token;
    ASSERT_TRUE(create_group_flush_token(ctx, group_writer, 3000, &flush_token, pool.get()).ok());
    ASSERT_TRUE(flush_token->submit(ctx.memtable).ok());

    Status wait_st = flush_token->wait();
    EXPECT_FALSE(wait_st.ok());
    EXPECT_NE(wait_st.to_string().find("binlog flush failed"), std::string::npos);
    EXPECT_EQ(1, data_flush_cnt.load());
    EXPECT_EQ(1, binlog_flush_cnt.load());
    EXPECT_EQ(1, flush_token->get_stats().flush_finish_count.load());
    EXPECT_EQ(0, flush_token->get_stats().flush_submit_count.load());

    drop_tablet(ctx.request);
}

} // namespace doris

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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cloud/cloud_delta_writer.h"
#include "cloud/cloud_rowset_builder.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "common/config.h"
#include "core/block/block.h"
#include "core/field.h"
#include "cpp/sync_point.h"
#include "io/fs/local_file_system.h"
#include "io/fs/remote_file_system.h"
#include "load/channel/load_channel.h"
#include "load/channel/load_channel_mgr.h"
#include "load/channel/tablets_channel.h"
#include "load/delta_writer/delta_writer.h"
#include "load/memtable/memtable_flush_executor.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset_builder.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet_info.h"
#include "testutil/creators.h"
#include "util/countdown_latch.h"

namespace doris {
namespace {

// Exercise cloud writer I/O against real temporary files without an object-store service.
class LocalRemoteFileSystem final : public io::RemoteFileSystem {
public:
    explicit LocalRemoteFileSystem(std::string root_path)
            : RemoteFileSystem(std::move(root_path), "bitmap_cancel_test_fs",
                               io::FileSystemType::BROKER) {}

private:
    Status create_file_impl(const io::Path& file, io::FileWriterPtr* writer,
                            const io::FileWriterOptions* opts) override {
        return io::global_local_filesystem()->create_file(file, writer, opts);
    }

    Status create_directory_impl(const io::Path& dir, bool failed_if_exists) override {
        return io::global_local_filesystem()->create_directory(dir, failed_if_exists);
    }

    Status delete_file_impl(const io::Path& file) override {
        return io::global_local_filesystem()->delete_file(file);
    }

    Status batch_delete_impl(const std::vector<io::Path>& files) override {
        return io::global_local_filesystem()->batch_delete(files);
    }

    Status delete_directory_impl(const io::Path& dir) override {
        return io::global_local_filesystem()->delete_directory(dir);
    }

    Status exists_impl(const io::Path& path, bool* res) const override {
        return io::global_local_filesystem()->exists(path, res);
    }

    Status file_size_impl(const io::Path& file, int64_t* file_size) const override {
        return io::global_local_filesystem()->file_size(file, file_size);
    }

    Status list_impl(const io::Path& dir, bool only_file, std::vector<io::FileInfo>* files,
                     bool* exists) override {
        return io::global_local_filesystem()->list(dir, only_file, files, exists);
    }

    Status rename_impl(const io::Path& orig_name, const io::Path& new_name) override {
        return io::global_local_filesystem()->rename(orig_name, new_name);
    }

    Status upload_impl(const io::Path& local_file, const io::Path& remote_file) override {
        return io::global_local_filesystem()->link_file(local_file, remote_file);
    }

    Status batch_upload_impl(const std::vector<io::Path>& local_files,
                             const std::vector<io::Path>& remote_files) override {
        DCHECK_EQ(local_files.size(), remote_files.size());
        for (size_t i = 0; i < local_files.size(); ++i) {
            RETURN_IF_ERROR(upload_impl(local_files[i], remote_files[i]));
        }
        return Status::OK();
    }

    Status download_impl(const io::Path& remote_file, const io::Path& local_file) override {
        return io::global_local_filesystem()->link_file(remote_file, local_file);
    }

    Status open_file_internal(const io::Path& file, io::FileReaderSPtr* reader,
                              const io::FileReaderOptions& opts) override {
        return io::global_local_filesystem()->open_file(file, reader, &opts);
    }
};

} // namespace

class DeltaWriterCancelIntegrationTest : public testing::TestWithParam<bool> {
protected:
    static constexpr int64_t kTabletId = 71001;
    static constexpr int64_t kIndexId = 72001;

    void SetUp() override {
        auto* env = ExecEnv::GetInstance();
        _saved_engine = std::move(env->_storage_engine);
        _saved_limiter = std::move(env->_memtable_memory_limiter);
        _saved_fragment_mgr = env->_fragment_mgr;
        _saved_cloud_id = config::cloud_unique_id;
        _saved_deploy_mode = config::deploy_mode;
        _saved_storage_root = config::storage_root_path;
        _saved_sync_rowsets = config::cloud_mow_sync_rowsets_when_load_txn_begin;
        _saved_file_cache = config::enable_file_cache;
        _saved_between_segments = config::enable_calc_delete_bitmap_between_segments_concurrently;
        config::cloud_unique_id = GetParam() ? "bitmap-cancel-ut" : "";
        config::deploy_mode = GetParam() ? "cloud" : "local";
        config::cloud_mow_sync_rowsets_when_load_txn_begin = false;
        config::enable_file_cache = false;
        config::enable_calc_delete_bitmap_between_segments_concurrently = true;
        _root = (std::filesystem::current_path() / "ut_dir" /
                 (GetParam() ? "bitmap_cancel_cloud" : "bitmap_cancel_local"))
                        .string();
        config::storage_root_path = _root;
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_root).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_root).ok());
        _attach = std::make_unique<AttachTask>(MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::OTHER, "DeltaWriterCancelIntegrationTest"));

        _fragment_mgr = std::make_unique<FragmentMgr>(env);
        env->_fragment_mgr = _fragment_mgr.get();
        env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
        if (GetParam()) {
            auto engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
            engine->init_calc_delete_bitmap_executor_for_UT();
            engine->_memtable_flush_executor = std::make_unique<MemTableFlushExecutor>();
            engine->_memtable_flush_executor->init(1);
            engine->set_latest_fs(std::make_shared<LocalRemoteFileSystem>(_root));
            env->set_storage_engine(std::move(engine));
        } else {
            EngineOptions options;
            options.store_paths.emplace_back(_root, -1);
            auto engine = std::make_unique<StorageEngine>(options);
            ASSERT_TRUE(engine->open().ok());
            env->set_storage_engine(std::move(engine));
        }
        _engine = &env->storage_engine();
        _load_channel = std::make_shared<LoadChannel>(UniqueId {71, 1}, 60, false, "", 0, true, -1);
        create_tablet(kTabletId);
    }

    void TearDown() override {
        // Unblock callbacks on assertion failure, but do not pre-drain the executor:
        // writer destruction itself must enforce the lifetime guarantee.
        _release_bitmap.count_down();
        if (_cleanup.valid()) {
            _cleanup.get();
        }
        _load_channel.reset();
        _tablet.reset();
        auto* sp = SyncPoint::get_instance();
        sp->disable_processing();
        _guards.clear();
        sp->clear_trace();
        auto* env = ExecEnv::GetInstance();
        env->set_memtable_memory_limiter(nullptr);
        env->set_storage_engine(nullptr);
        env->_storage_engine = std::move(_saved_engine);
        env->_memtable_memory_limiter = std::move(_saved_limiter);
        if (_fragment_mgr) {
            _fragment_mgr->stop();
            env->_fragment_mgr = _saved_fragment_mgr;
            _fragment_mgr.reset();
        }
        _attach.reset();
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_root).ok());
        config::cloud_unique_id = _saved_cloud_id;
        config::deploy_mode = _saved_deploy_mode;
        config::storage_root_path = _saved_storage_root;
        config::cloud_mow_sync_rowsets_when_load_txn_begin = _saved_sync_rowsets;
        config::enable_file_cache = _saved_file_cache;
        config::enable_calc_delete_bitmap_between_segments_concurrently = _saved_between_segments;
    }

    void create_tablet(int64_t id) {
        _request = testutil::create_tablet_request(
                id, 73001, 74001, 1, TKeysType::UNIQUE_KEYS,
                {{"k1", TPrimitiveType::INT, true}, {"v1", TPrimitiveType::INT, false}});
        _request.__set_enable_unique_key_merge_on_write(true);
        if (GetParam()) {
            std::unordered_map<uint32_t, uint32_t> ids {{0, 0}, {1, 1}};
            auto meta = TabletMeta::create(_request, TabletUid::gen_uid(), 0, 2, ids);
            auto tablet = std::make_shared<CloudTablet>(_engine->to_cloud(), meta);
            auto rs_meta = std::make_shared<RowsetMeta>();
            rs_meta->set_rowset_type(BETA_ROWSET);
            rs_meta->set_rowset_state(VISIBLE);
            rs_meta->set_tablet_id(id);
            rs_meta->set_tablet_schema_hash(_request.tablet_schema.schema_hash);
            rs_meta->set_tablet_schema(tablet->tablet_schema());
            rs_meta->set_version(Version(0, 1));
            rs_meta->set_segments_overlap(OVERLAP_UNKNOWN);
            RowsetSharedPtr rowset;
            ASSERT_TRUE(RowsetFactory::create_rowset(tablet->tablet_schema(), "", rs_meta, &rowset)
                                .ok());
            {
                std::unique_lock meta_lock(tablet->get_header_lock());
                tablet->add_rowsets({rowset}, false, meta_lock);
                tablet->set_cumulative_layer_point(2);
            }
            _engine->to_cloud().tablet_mgr().put_tablet_for_UT(tablet);
            ASSERT_TRUE(io::global_local_filesystem()
                                ->create_directory(fmt::format("{}/data/{}", _root, id))
                                .ok());
            _tablet = tablet;
        } else {
            ASSERT_TRUE(
                    _engine->to_local().create_tablet(_request, _load_channel->_self_profile).ok());
            _tablet = _engine->to_local().tablet_manager()->get_tablet(id);
        }
        ASSERT_NE(_tablet, nullptr);
    }

    PTabletWriterOpenRequest open_request(int64_t id, bool incremental = false) {
        auto desc = testutil::create_descriptor_table(
                {{TYPE_INT, "k1", false}, {TYPE_INT, "v1", false}});
        auto schema = testutil::create_table_schema_param(
                desc, kIndexId, _request.tablet_schema.schema_hash, _request.tablet_schema.columns);
        PTabletWriterOpenRequest request;
        request.mutable_id()->set_hi(71);
        request.mutable_id()->set_lo(1);
        request.set_index_id(kIndexId);
        request.set_txn_id(75001);
        request.set_txn_expiration(4102444800);
        request.set_num_senders(1);
        request.set_sender_id(0);
        request.set_is_incremental(incremental);
        request.set_is_high_priority(_load_channel->is_high_priority());
        request.set_enable_profile(true);
        schema->to_protobuf(request.mutable_schema());
        auto* tablet = request.add_tablets();
        tablet->set_tablet_id(id);
        tablet->set_partition_id(_request.partition_id);
        return request;
    }

    virtual Status open_load_channel(const PTabletWriterOpenRequest& request) {
        return _load_channel->open(request);
    }

    Status open_writer(int64_t id, bool incremental = false) {
        RETURN_IF_ERROR(open_load_channel(open_request(id, incremental)));
        _writer = _load_channel->_tablets_channels.at(kIndexId)->_tablet_writers.at(id).get();
        _builder = _writer->_rowset_builder.get();
        if (GetParam()) {
            // Only metadata RPCs are skipped; builders and both executors initialize normally.
            static_cast<CloudRowsetBuilder*>(_builder)->set_skip_writing_rowset_metadata(true);
        }
        return Status::OK();
    }

    Block block() {
        auto result = _tablet->tablet_schema()->create_storage_block();
        {
            auto guard = result.mutate_columns_scoped();
            auto& columns = guard.mutable_columns();
            columns[0]->insert(Field::create_field<PrimitiveType::TYPE_INT>(1));
            columns[1]->insert(Field::create_field<PrimitiveType::TYPE_INT>(10));
        }
        return result;
    }

    void check_wiring() {
        _rowset_writer = static_cast<BaseBetaRowsetWriter*>(_builder->rowset_writer().get());
        const auto& cancellation = _load_channel->_delete_bitmap_cancellation;
        ASSERT_EQ(_writer->_req.delete_bitmap_cancellation, cancellation);
        ASSERT_EQ(_builder->_req.delete_bitmap_cancellation, cancellation);
        ASSERT_EQ(_rowset_writer->context().delete_bitmap_cancellation, cancellation);
        ASSERT_NE(_builder->_calc_delete_bitmap_token, nullptr);
        ASSERT_NE(_rowset_writer->_calc_delete_bitmap_token, nullptr);
        EXPECT_EQ(_builder->_calc_delete_bitmap_token->_delete_bitmap_cancellation, cancellation);
        EXPECT_EQ(_rowset_writer->_calc_delete_bitmap_token->_delete_bitmap_cancellation,
                  cancellation);
        EXPECT_EQ(_builder->_calc_delete_bitmap_token->_thread_token->_pool,
                  _engine->calc_delete_bitmap_executor()->_thread_pool.get());
        EXPECT_EQ(_rowset_writer->_calc_delete_bitmap_token->_thread_token->_pool,
                  _engine->calc_delete_bitmap_executor_for_load()->_thread_pool.get());
        EXPECT_NE(_engine->calc_delete_bitmap_executor()->_thread_pool.get(),
                  _engine->calc_delete_bitmap_executor_for_load()->_thread_pool.get());
    }

    template <typename Callback>
    void on(const std::string& point, Callback&& callback) {
        _guards.emplace_back();
        SyncPoint::get_instance()->set_call_back(point, std::forward<Callback>(callback),
                                                 &_guards.back());
        SyncPoint::get_instance()->enable_processing();
    }

    void block_bitmap_and_observe_cleanup() {
        _segment_path = _rowset_writer->context().segment_path(0);
        if (GetParam()) {
            _segment_path = (_engine->to_cloud().latest_fs()->root_path() / _segment_path).string();
        }
        on("BaseBetaRowsetWriter::_generate_delete_bitmap:before_build_tmp", [&](auto&& args) {
            if (try_any_cast<BaseBetaRowsetWriter*>(args[0]) == _rowset_writer) {
                _bitmap_started.count_down();
                _release_bitmap.wait();
                bool exists = false;
                EXPECT_TRUE(io::global_local_filesystem()->exists(_segment_path, &exists).ok());
                EXPECT_TRUE(exists);
                EXPECT_FALSE(_cleanup_started.load());
            }
        });
        on("BaseBetaRowsetWriter::_generate_delete_bitmap:finished", [&](auto&& args) {
            if (try_any_cast<BaseBetaRowsetWriter*>(args[0]) == _rowset_writer) {
                _bitmap_finished = true;
            }
        });
        on("BaseBetaRowsetWriter::~BaseBetaRowsetWriter:before_file_cleanup", [&](auto&& args) {
            if (try_any_cast<BaseBetaRowsetWriter*>(args[0]) == _rowset_writer) {
                EXPECT_TRUE(_bitmap_finished.load());
                _cleanup_started = true;
            }
        });
        on("CloudRowsetBuilder::~CloudRowsetBuilder:before_clear_cache", [&](auto&& args) {
            if (try_any_cast<CloudRowsetBuilder*>(args[0]) == _builder) {
                EXPECT_TRUE(_bitmap_finished.load());
                _cleanup_started = true;
            }
        });
    }

    void start_cleanup(bool cancel) {
        auto resource_ctx = _load_channel->_resource_ctx;
        if (cancel) {
            _cleanup = std::async(std::launch::async, [this, resource_ctx] {
                SCOPED_ATTACH_TASK(resource_ctx);
                EXPECT_TRUE(_load_channel->cancel().ok());
            });
        } else {
            auto& owner =
                    _load_channel->_tablets_channels.at(kIndexId)->_tablet_writers.at(kTabletId);
            _cleanup = std::async(std::launch::async,
                                  [resource_ctx, writer = std::move(owner)]() mutable {
                                      SCOPED_ATTACH_TASK(resource_ctx);
                                      writer.reset();
                                  });
        }
        EXPECT_EQ(_cleanup.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
        EXPECT_FALSE(_cleanup_started.load());
        EXPECT_FALSE(_bitmap_finished.load());
        _release_bitmap.count_down();
        _cleanup.get();
        EXPECT_TRUE(_bitmap_finished.load());
    }

    Status flush_two_memtables() {
        auto data = block();
        for (int i = 0; i < 2; ++i) {
            RETURN_IF_ERROR(_writer->write(&data, TabletAddRowsPayload {.row_idxs = {0}}));
            RETURN_IF_ERROR(_writer->flush_memtable_async());
            RETURN_IF_ERROR(_writer->wait_flush());
        }
        return _rowset_writer->_calc_delete_bitmap_token->wait();
    }

    void exercise_both_phases() {
        ASSERT_TRUE(_writer->init().ok());
        check_wiring();
        ASSERT_TRUE(flush_two_memtables().ok());
        ASSERT_TRUE(_writer->close().ok());
        ASSERT_TRUE(_writer->build_rowset().ok());
        std::atomic<int> builder_submissions {0};
        on("CalcDeleteBitmapToken::submit_func:before_submit", [&](auto&& args) {
            if (try_any_cast<CalcDeleteBitmapToken*>(args[0]) ==
                _builder->_calc_delete_bitmap_token.get()) {
                ++builder_submissions;
            }
        });
        ASSERT_TRUE(_writer->submit_calc_delete_bitmap_task().ok());
        ASSERT_TRUE(_writer->wait_calc_delete_bitmap().ok());
        EXPECT_GT(builder_submissions.load(), 0);
        SyncPoint::get_instance()->clear_call_back(
                "CalcDeleteBitmapToken::submit_func:before_submit");
    }

    Status prepare_partial_init_callback() {
        check_wiring();
        auto data = block();
        if (GetParam()) {
            // Keep a real temporary PREPARED rowset in the builder to exercise its
            // cache-cleanup branch. build() would instead mark the rowset COMMITTED.
            RETURN_IF_ERROR(_rowset_writer->flush_single_block(&data));
            RETURN_IF_ERROR(_rowset_writer->_calc_delete_bitmap_token->wait());
            RETURN_IF_ERROR(_rowset_writer->_build_tmp(_builder->_rowset));
            EXPECT_EQ(_builder->rowset()->rowset_meta()->rowset_state(), PREPARED);
            block_bitmap_and_observe_cleanup();
            // Resubmit the real callback against the already-created segment.
            return _rowset_writer->_generate_delete_bitmap(0);
        }
        block_bitmap_and_observe_cleanup();
        return _rowset_writer->flush_single_block(&data);
    }

    std::string _root;
    std::string _saved_cloud_id;
    std::string _saved_deploy_mode;
    std::string _saved_storage_root;
    bool _saved_sync_rowsets = false;
    bool _saved_file_cache = false;
    bool _saved_between_segments = false;
    std::unique_ptr<BaseStorageEngine> _saved_engine;
    std::unique_ptr<MemTableMemoryLimiter> _saved_limiter;
    FragmentMgr* _saved_fragment_mgr = nullptr;
    std::unique_ptr<FragmentMgr> _fragment_mgr;
    std::unique_ptr<AttachTask> _attach;
    BaseStorageEngine* _engine = nullptr;
    BaseTabletSPtr _tablet;
    TCreateTabletReq _request;
    std::shared_ptr<LoadChannel> _load_channel;
    BaseDeltaWriter* _writer = nullptr;
    BaseRowsetBuilder* _builder = nullptr;
    BaseBetaRowsetWriter* _rowset_writer = nullptr;
    std::string _segment_path;
    std::vector<SyncPoint::CallbackGuard> _guards;
    CountDownLatch _bitmap_started {1};
    CountDownLatch _release_bitmap {1};
    std::atomic<bool> _bitmap_finished {false};
    std::atomic<bool> _cleanup_started {false};
    std::future<void> _cleanup;
};

TEST_P(DeltaWriterCancelIntegrationTest, OrdinaryAndIncrementalOpenWireBothPhases) {
    for (int i = 0; i < 2; ++i) {
        if (i != 0) {
            create_tablet(kTabletId + i);
        }
        ASSERT_TRUE(open_writer(kTabletId + i, i != 0).ok());
        ASSERT_NO_FATAL_FAILURE(exercise_both_phases());
    }
    ASSERT_TRUE(_load_channel->cancel().ok());
    for (const auto& [_, writer] : _load_channel->_tablets_channels.at(kIndexId)->_tablet_writers) {
        auto* builder = writer->_rowset_builder.get();
        auto* rowset_writer = static_cast<BaseBetaRowsetWriter*>(builder->rowset_writer().get());
        EXPECT_TRUE(builder->_calc_delete_bitmap_token->wait().is<ErrorCode::CANCELLED>());
        EXPECT_TRUE(rowset_writer->_calc_delete_bitmap_token->wait().is<ErrorCode::CANCELLED>());
    }
}

TEST_P(DeltaWriterCancelIntegrationTest, CancelDrainsRealWriterCallbackBeforeCleanup) {
    ASSERT_TRUE(open_writer(kTabletId).ok());
    ASSERT_TRUE(_writer->init().ok());
    check_wiring();
    auto data = block();
    block_bitmap_and_observe_cleanup();
    ASSERT_TRUE(_rowset_writer->flush_single_block(&data).ok());
    ASSERT_TRUE(_bitmap_started.wait_for(std::chrono::seconds(10)));
    start_cleanup(true);
    EXPECT_TRUE(_rowset_writer->_calc_delete_bitmap_token->wait().is<ErrorCode::CANCELLED>());
    EXPECT_TRUE(_builder->_calc_delete_bitmap_token->wait().is<ErrorCode::CANCELLED>());
    _load_channel.reset();
    if (!GetParam()) {
        EXPECT_TRUE(_cleanup_started.load());
        bool exists = true;
        ASSERT_TRUE(io::global_local_filesystem()->exists(_segment_path, &exists).ok());
        EXPECT_FALSE(exists);
    }
}

TEST_P(DeltaWriterCancelIntegrationTest, PartialInitDestructionDrainsBeforeFileOrCacheCleanup) {
    ASSERT_TRUE(open_writer(kTabletId).ok());
    const auto failure = Status::InternalError("memtable initialization failed");
    on("BaseDeltaWriter::init:before_memtable_init", [&](auto&& args) {
        if (try_any_cast<BaseDeltaWriter*>(args[0]) != _writer) {
            return;
        }
        auto st = prepare_partial_init_callback();
        EXPECT_TRUE(st.ok()) << st;
        auto* ret = try_any_cast_ret<Status>(args);
        ret->first = st.ok() ? failure : st;
        ret->second = true;
    });
    EXPECT_EQ(_writer->init(), failure);
    ASSERT_FALSE(_writer->_is_init);
    ASSERT_TRUE(_builder->_is_init);
    ASSERT_TRUE(_bitmap_started.wait_for(std::chrono::seconds(10)));
    start_cleanup(false);
    EXPECT_TRUE(_cleanup_started.load());
    if (!GetParam()) {
        bool exists = true;
        ASSERT_TRUE(io::global_local_filesystem()->exists(_segment_path, &exists).ok());
        EXPECT_FALSE(exists);
    }
}

// Local close reports bitmap cancellation through tablet_errors and can still return OK.
// Exercise the manager terminal-state transition with the real close and bitmap paths.
class LocalLoadChannelTerminalStateTest : public DeltaWriterCancelIntegrationTest {
protected:
    enum class CancelMode { WITH_REASON, WITHOUT_REASON, TIMEOUT };

    void SetUp() override {
        DeltaWriterCancelIntegrationTest::SetUp();
        _manager = std::make_unique<LoadChannelMgr>();
        _manager->_load_state_channels =
                std::make_unique<LoadChannelMgr::LoadStateChannelCache>(1024);
        // The fixture does not initialize the memory limiter's background machinery.
        _load_channel->_is_high_priority = true;
    }

    void TearDown() override {
        _release_bitmap.count_down();
        if (_eos.valid()) {
            _eos.get();
        }
        if (_cleanup.valid()) {
            _cleanup.get();
        }
        if (_manager) {
            _manager->stop();
        }
        _manager.reset();
        _tablets_channel.reset();
        DeltaWriterCancelIntegrationTest::TearDown();
    }

    Status open_load_channel(const PTabletWriterOpenRequest& request) override {
        RETURN_IF_ERROR(_manager->open(request));
        _load_channel = _manager->_load_channels.at(UniqueId(request.id()));
        return Status::OK();
    }

    void prepare_eos() {
        ASSERT_TRUE(open_writer(kTabletId).ok());
        ASSERT_TRUE(_writer->init().ok());
        check_wiring();
        _tablets_channel = _load_channel->_tablets_channels.at(kIndexId);
        ASSERT_TRUE(flush_two_memtables().ok());
        _eos_request.mutable_id()->set_hi(71);
        _eos_request.mutable_id()->set_lo(1);
        _eos_request.set_index_id(kIndexId);
        _eos_request.set_sender_id(0);
        _eos_request.set_backend_id(0);
        _eos_request.set_packet_seq(0);
        _eos_request.set_eos(true);
        _eos_request.add_partition_ids(_request.partition_id);
    }

    PTabletWriterCancelRequest cancel_request(const std::string& reason = "") {
        PTabletWriterCancelRequest request;
        request.mutable_id()->CopyFrom(_eos_request.id());
        if (!reason.empty()) {
            request.set_cancel_reason(reason);
        }
        return request;
    }

    void race_eos_with_cancellation(CancelMode mode) {
        ASSERT_NO_FATAL_FAILURE(prepare_eos());
        on("CalcDeleteBitmapToken::submit:before_between_segments", [&](auto&& args) {
            if (try_any_cast<CalcDeleteBitmapToken*>(args[0]) ==
                _builder->_calc_delete_bitmap_token.get()) {
                _bitmap_started.count_down();
                _release_bitmap.wait();
            }
        });
        on("CalcDeleteBitmapToken::wait:before_wait", [&](auto&& args) {
            if (try_any_cast<CalcDeleteBitmapToken*>(args[0]) ==
                _builder->_calc_delete_bitmap_token.get()) {
                _close_waiting.count_down();
            }
        });
        on("DeleteBitmapCancellation::cancel:before_shutdown", [&](auto&& args) {
            if (try_any_cast<DeleteBitmapCancellation*>(args[0]) ==
                _load_channel->_delete_bitmap_cancellation.get()) {
                _cancel_published.count_down();
            }
        });
        _eos = std::async(std::launch::async,
                          [this] { return _manager->add_batch(_eos_request, &_eos_response); });
        ASSERT_TRUE(_bitmap_started.wait_for(std::chrono::seconds(10)));
        ASSERT_TRUE(_close_waiting.wait_for(std::chrono::seconds(10)));
        if (mode == CancelMode::TIMEOUT) {
            _load_channel->_last_updated_time.store(0);
        }
        _cleanup = std::async(std::launch::async, [this, mode] {
            if (mode == CancelMode::TIMEOUT) {
                EXPECT_TRUE(_manager->_start_load_channels_clean().ok());
            } else {
                EXPECT_TRUE(_manager->cancel(cancel_request(mode == CancelMode::WITH_REASON
                                                                    ? "cancel won before EOS"
                                                                    : ""))
                                    .ok());
            }
        });
        ASSERT_TRUE(_cancel_published.wait_for(std::chrono::seconds(10)));
        // Cancellation has removed the channel and published failure, while the real
        // bitmap callback still prevents close from reaching its commit phase.
        PTabletWriterAddBlockResult early_retry;
        EXPECT_TRUE(_manager->add_batch(_eos_request, &early_retry).is<ErrorCode::CANCELLED>());
        EXPECT_EQ(_eos.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
        _release_bitmap.count_down();
        EXPECT_TRUE(_eos.get().is<ErrorCode::CANCELLED>());
        _cleanup.get();
        EXPECT_FALSE(_builder->_is_committed);
        EXPECT_EQ(_eos_response.tablet_vec_size(), 0);
        ASSERT_EQ(_eos_response.tablet_errors_size(), 1);
        EXPECT_EQ(_eos_response.tablet_errors(0).tablet_id(), kTabletId);
        const std::string reason = mode == CancelMode::WITH_REASON ? "cancel won before EOS"
                                   : mode == CancelMode::TIMEOUT   ? "load channel timed out"
                                                                   : "load channel cancelled";
        // Model a lost EOS response: a retry must retain the first cancellation,
        // including after another cancellation tries to replace its reason.
        EXPECT_TRUE(_manager->cancel(cancel_request("later cancellation")).ok());
        PTabletWriterAddBlockResult retry;
        auto status = _manager->add_batch(_eos_request, &retry);
        EXPECT_TRUE(status.is<ErrorCode::CANCELLED>());
        EXPECT_NE(status.to_string().find(reason), std::string::npos);
        EXPECT_TRUE(_manager->_load_channels.empty());
    }

    void reject_reopen_after_cancellation(CancelMode mode) {
        ASSERT_NO_FATAL_FAILURE(prepare_eos());
        const auto request = open_request(kTabletId);
        if (mode == CancelMode::TIMEOUT) {
            _load_channel->_last_updated_time.store(0);
            ASSERT_TRUE(_manager->_start_load_channels_clean().ok());
        } else {
            ASSERT_TRUE(_manager->cancel(cancel_request(mode == CancelMode::WITH_REASON
                                                                ? "cancel before reopen"
                                                                : ""))
                                .ok());
        }
        ASSERT_TRUE(_manager->_load_channels.empty());
        PTabletWriterAddBlockResult before_reopen;
        const auto terminal_status = _manager->add_batch(_eos_request, &before_reopen);
        ASSERT_TRUE(terminal_status.is<ErrorCode::CANCELLED>());
        // Use the real open API with a complete schema/tablet request, including
        // an incremental retry. Neither request may create replacement writers.
        EXPECT_EQ(_manager->open(request), terminal_status);
        EXPECT_TRUE(_manager->_load_channels.empty());
        EXPECT_EQ(_manager->open(open_request(kTabletId, true)), terminal_status);
        EXPECT_TRUE(_manager->_load_channels.empty());
        PTabletWriterAddBlockResult after_reopen;
        EXPECT_EQ(_manager->add_batch(_eos_request, &after_reopen), terminal_status);
        EXPECT_EQ(after_reopen.tablet_vec_size(), 0);
        EXPECT_FALSE(_builder->_is_committed);
    }

    std::unique_ptr<LoadChannelMgr> _manager;
    std::shared_ptr<BaseTabletsChannel> _tablets_channel;
    PTabletWriterAddBlockRequest _eos_request;
    PTabletWriterAddBlockResult _eos_response;
    CountDownLatch _close_waiting {1};
    CountDownLatch _cancel_published {1};
    std::future<Status> _eos;
};

TEST_P(LocalLoadChannelTerminalStateTest, CancelReasonSurvivesLateEosAndRetry) {
    race_eos_with_cancellation(CancelMode::WITH_REASON);
}

TEST_P(LocalLoadChannelTerminalStateTest, EmptyCancelReasonSurvivesLateEosAndRetry) {
    race_eos_with_cancellation(CancelMode::WITHOUT_REASON);
}

TEST_P(LocalLoadChannelTerminalStateTest, TimeoutSurvivesLateEosAndRetry) {
    race_eos_with_cancellation(CancelMode::TIMEOUT);
}

TEST_P(LocalLoadChannelTerminalStateTest, CancelReasonRejectsRealReopen) {
    reject_reopen_after_cancellation(CancelMode::WITH_REASON);
}

TEST_P(LocalLoadChannelTerminalStateTest, EmptyCancelReasonRejectsRealReopen) {
    reject_reopen_after_cancellation(CancelMode::WITHOUT_REASON);
}

TEST_P(LocalLoadChannelTerminalStateTest, TimeoutRejectsRealReopen) {
    reject_reopen_after_cancellation(CancelMode::TIMEOUT);
}

TEST_P(LocalLoadChannelTerminalStateTest, SuccessfulEosSurvivesLateCancellation) {
    ASSERT_NO_FATAL_FAILURE(prepare_eos());
    ASSERT_TRUE(_manager->add_batch(_eos_request, &_eos_response).ok());
    EXPECT_TRUE(_builder->_is_committed);
    ASSERT_EQ(_eos_response.tablet_vec_size(), 1);
    EXPECT_EQ(_eos_response.tablet_errors_size(), 0);
    // An open response can also be retried after the load has completed. Preserve
    // successful EOS retries without allocating a new channel behind that record.
    EXPECT_TRUE(_manager->open(open_request(kTabletId)).ok());
    EXPECT_TRUE(_manager->open(open_request(kTabletId, true)).ok());
    EXPECT_TRUE(_manager->_load_channels.empty());
    EXPECT_TRUE(_manager->cancel(cancel_request("too late")).ok());
    EXPECT_TRUE(_manager->_finish_load_channel(_load_channel->load_id(), _load_channel).ok());
    PTabletWriterAddBlockResult retry;
    EXPECT_TRUE(_manager->add_batch(_eos_request, &retry).ok());
}

TEST_P(LocalLoadChannelTerminalStateTest, LateEosCannotFinishReplacementChannel) {
    ASSERT_NO_FATAL_FAILURE(prepare_eos());
    const auto id = _load_channel->load_id();
    auto replacement = std::make_shared<LoadChannel>(id, 60, true, "", 0, false, -1);
    _manager->_load_channels[id] = replacement;
    EXPECT_TRUE(_manager->_finish_load_channel(id, _load_channel).is<ErrorCode::CANCELLED>());
    EXPECT_EQ(_manager->_load_channels.at(id), replacement);
    auto* handle = _manager->_load_state_channels->lookup(id.to_string());
    EXPECT_EQ(handle, nullptr);
    if (handle != nullptr) {
        _manager->_load_state_channels->release(handle);
    }
}

TEST_P(LocalLoadChannelTerminalStateTest, CancelledChannelCannotSucceedAfterCacheEviction) {
    ASSERT_NO_FATAL_FAILURE(prepare_eos());
    ASSERT_TRUE(_manager->cancel(cancel_request("cancelled")).ok());
    _manager->_load_state_channels->erase(_load_channel->load_id().to_string());
    EXPECT_TRUE(_manager->_finish_load_channel(_load_channel->load_id(), _load_channel)
                        .is<ErrorCode::CANCELLED>());
    PTabletWriterAddBlockResult retry;
    EXPECT_FALSE(_manager->add_batch(_eos_request, &retry).ok());
}

INSTANTIATE_TEST_SUITE_P(Local, LocalLoadChannelTerminalStateTest, testing::Values(false));

INSTANTIATE_TEST_SUITE_P(LocalAndCloud, DeltaWriterCancelIntegrationTest, testing::Bool());

} // namespace doris

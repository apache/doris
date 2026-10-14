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
#include <thread>
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
#include "load/channel/eos_completion.h"
#include "load/channel/load_channel.h"
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
#include "storage/storage_policy.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet_info.h"
#include "testutil/creators.h"
#include "util/countdown_latch.h"

namespace doris {
namespace {

constexpr int64_t kStorageResourceId = 6775971001;

// Exercise cloud writer I/O against real temporary files without an object-store service.
class LocalRemoteFileSystem final : public io::RemoteFileSystem {
public:
    explicit LocalRemoteFileSystem(std::string root_path)
            : RemoteFileSystem(std::move(root_path), std::to_string(kStorageResourceId),
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
            auto fs = std::make_shared<LocalRemoteFileSystem>(_root);
            engine->set_latest_fs(fs);
            // Temporary rowsets copy the resource ID through protobuf metadata and
            // resolve it from the registry when bitmap callbacks reopen their segments.
            put_storage_resource(kStorageResourceId, StorageResource {fs}, 0);
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
        // All cloud writers and callbacks must finish before their resource is removed.
        if (GetParam()) {
            delete_storage_resource(kStorageResourceId);
        }
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

    Status open_writer(int64_t id, bool incremental = false) {
        auto request = open_request(id, incremental);
        Status status;
        // open() attaches the load task itself. Model its RPC worker separately
        // from the fixture's attached task to avoid nesting AttachTask.
        std::thread rpc([&] { status = _load_channel->open(request); });
        rpc.join();
        RETURN_IF_ERROR(status);
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
        const auto& cancellation = _load_channel->_cancel_status;
        ASSERT_EQ(_writer->_req.load_cancel_status, cancellation);
        ASSERT_EQ(_builder->_req.load_cancel_status, cancellation);
        ASSERT_EQ(_rowset_writer->context().load_cancel_status, cancellation);
        ASSERT_NE(_builder->_calc_delete_bitmap_token, nullptr);
        ASSERT_NE(_rowset_writer->_calc_delete_bitmap_token, nullptr);
        EXPECT_EQ(_builder->_calc_delete_bitmap_token->_load_cancel_status, cancellation);
        EXPECT_EQ(_rowset_writer->_calc_delete_bitmap_token->_load_cancel_status, cancellation);
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

    void start_cleanup() {
        auto resource_ctx = _load_channel->_resource_ctx;
        auto& owner = _load_channel->_tablets_channels.at(kIndexId)->_tablet_writers.at(kTabletId);
        _cleanup =
                std::async(std::launch::async, [resource_ctx, writer = std::move(owner)]() mutable {
                    SCOPED_ATTACH_TASK(resource_ctx);
                    writer.reset();
                });
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
        auto st = flush_two_memtables();
        ASSERT_TRUE(st.ok()) << st;
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

TEST_P(DeltaWriterCancelIntegrationTest, PublicationReturnsBeforeFinalOwnerDrainsRealCallback) {
    ASSERT_TRUE(open_writer(kTabletId).ok());
    ASSERT_TRUE(_writer->init().ok());
    check_wiring();
    auto data = block();
    block_bitmap_and_observe_cleanup();
    ASSERT_TRUE(_rowset_writer->flush_single_block(&data).ok());
    ASSERT_TRUE(_bitmap_started.wait_for(std::chrono::seconds(10)));
    ASSERT_TRUE(_load_channel->cancel().ok());
    EXPECT_FALSE(_bitmap_finished.load());
    EXPECT_FALSE(_cleanup_started.load());
    start_cleanup();
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
    start_cleanup();
    EXPECT_TRUE(_cleanup_started.load());
    if (!GetParam()) {
        bool exists = true;
        ASSERT_TRUE(io::global_local_filesystem()->exists(_segment_path, &exists).ok());
        EXPECT_FALSE(exists);
    }
}

TEST_P(DeltaWriterCancelIntegrationTest, FinalOwnerCompletesPendingEosBeforeDestroyingWriters) {
    ASSERT_TRUE(open_writer(kTabletId).ok());
    auto* tablets = _load_channel->_tablets_channels.at(kIndexId).get();
    auto completion = tablets->eos_completion();
    const auto reason = Status::InternalError("cancel before all EOS senders arrive");
    int replies = 0;
    completion->add_waiter([&](const Status& status) {
        EXPECT_EQ(status, reason);
        EXPECT_EQ(tablets->_tablet_writers.at(kTabletId).get(), _writer);
        ++replies;
    });
    {
        // Publishing cancellation must not take either channel lock or run RPC callbacks.
        std::lock_guard load_lock(_load_channel->_lock);
        std::lock_guard tablets_lock(tablets->_lock);
        ASSERT_TRUE(_load_channel->cancel(reason).ok());
        EXPECT_EQ(replies, 0);
    }
    _load_channel.reset();
    EXPECT_EQ(replies, 1);
    // The service may register after add_batch released the last channel owner.
    completion->add_waiter([&](const Status& status) {
        EXPECT_EQ(status, reason);
        ++replies;
    });
    EXPECT_EQ(replies, 2);
}

TEST_P(DeltaWriterCancelIntegrationTest, PublishedCancellationWinsBeforeEosArrival) {
    ASSERT_TRUE(open_writer(kTabletId).ok());
    auto* tablets = _load_channel->_tablets_channels.at(kIndexId).get();
    const auto reason = Status::Cancelled("cancel before final EOS sender");
    ASSERT_TRUE(_load_channel->cancel(reason).ok());
    int replies = 0;
    tablets->eos_completion()->add_waiter([&](const Status& status) {
        EXPECT_EQ(status, reason);
        ++replies;
    });
    PTabletWriterAddBlockResult response;
    std::unique_lock lock(tablets->_lock);
    tablets->_num_remaining_senders = 0;
    EXPECT_EQ(tablets->_close_and_notify(lock, 0, &response), reason);
    EXPECT_EQ(replies, 1);
}

INSTANTIATE_TEST_SUITE_P(LocalCloud, DeltaWriterCancelIntegrationTest, testing::Bool());

} // namespace doris

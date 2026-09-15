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

// Tests spill on object storage (spill_storage_type=s3) against an in-memory
// ObjStorageClient injected through S3ClientFactory. Covers the write/read round trip,
// cleanup (file gc, query directory, previous boot generations), the capacity limit,
// upload back pressure and request statistics.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/spill_counters.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "exec/spill/spill_remote_upload_budget.h"
#include "io/fs/file_system.h"
#include "io/fs/s3_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_profile_counter_names.h"
#include "runtime/workload_management/io_context.h"
#include "runtime/workload_management/resource_context.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/obj_storage_client_test_stub.h"
#include "util/defer_op.h"
#include "util/s3_util.h"
#include "util/threadpool.h"
#include "util/uid_util.h"

namespace doris::vectorized {

namespace {

// ── In-memory object storage ──────────────────────────────────────────────────

struct MockS3Store {
    struct UploadCtx {
        std::string bucket;
        std::string key;
        std::map<int, std::string> parts;
    };

    std::mutex mutex;
    std::condition_variable cv;
    uint64_t next_upload_id {1};
    std::unordered_map<std::string, UploadCtx> uploads;
    std::map<std::string, std::string> objects; // ordered for deterministic listing

    // Fault injection
    std::atomic<bool> fail_uploads {false};
    // When > 0, every put/upload_part beyond this many successful ones fails.
    std::atomic<int64_t> fail_puts_after {0};
    // When > 0, the N-th put/upload_part (1-based) fails immediately, even while uploads are
    // blocked; the others behave normally.
    std::atomic<int64_t> fail_put_index {0};
    std::atomic<bool> fail_create_multipart {false};
    std::atomic<bool> fail_deletes {false};
    std::atomic<bool> block_uploads {false};
    std::atomic<int64_t> put_requests {0};
    std::atomic<int64_t> get_requests {0};
    std::atomic<int64_t> head_requests {0};
    std::atomic<int64_t> list_requests {0};
    std::atomic<int64_t> delete_requests {0};
    std::atomic<int64_t> create_multipart_requests {0};
    std::atomic<int64_t> abort_multipart_requests {0};

    // True when this put/upload_part must fail (fail_uploads, or beyond fail_puts_after).
    bool should_fail_put() {
        if (fail_uploads) {
            return true;
        }
        int64_t limit = fail_puts_after.load();
        return limit > 0 && put_requests.load() > limit;
    }

    std::string make_key(std::string_view bucket, std::string_view key) const {
        return fmt::format("{}/{}", bucket, key);
    }

    void reset() {
        std::lock_guard lock(mutex);
        next_upload_id = 1;
        uploads.clear();
        objects.clear();
        fail_uploads = false;
        fail_puts_after = 0;
        fail_put_index = 0;
        fail_create_multipart = false;
        fail_deletes = false;
        block_uploads = false;
        put_requests = 0;
        get_requests = 0;
        head_requests = 0;
        list_requests = 0;
        delete_requests = 0;
        create_multipart_requests = 0;
        abort_multipart_requests = 0;
    }

    void wait_while_blocked() {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&]() { return !block_uploads.load(); });
    }

    void set_block_uploads(bool block) {
        {
            std::lock_guard lock(mutex);
            block_uploads = block;
        }
        cv.notify_all();
    }

    std::vector<std::string> keys_with_prefix(const std::string& bucket,
                                              const std::string& prefix) {
        std::lock_guard lock(mutex);
        std::vector<std::string> keys;
        auto full_prefix = make_key(bucket, prefix);
        for (const auto& [key, _] : objects) {
            if (key.rfind(full_prefix, 0) == 0) {
                keys.push_back(key.substr(bucket.size() + 1));
            }
        }
        return keys;
    }

    void put_raw(const std::string& bucket, const std::string& key, std::string data) {
        std::lock_guard lock(mutex);
        objects[make_key(bucket, key)] = std::move(data);
    }
};

MockS3Store& mock_store() {
    static MockS3Store store;
    return store;
}

class MockObjStorageClient : public io::ObjStorageClientTestStub {
public:
    explicit MockObjStorageClient(MockS3Store* store) : _store(store) {}

    io::ObjStorageUploadResult create_multipart_upload(const io::ObjStoragePath& opts) override {
        io::ObjStorageUploadResult resp;
        resp.resp = io::ObjStorageResponse::OK();
        _store->create_multipart_requests++;
        if (_store->fail_create_multipart) {
            resp.resp = make_error("injected create_multipart_upload failure");
            return resp;
        }
        std::lock_guard lock(_store->mutex);
        auto upload_id = fmt::format("upload-{}", _store->next_upload_id++);
        resp.upload_id = upload_id;
        _store->uploads[upload_id] = MockS3Store::UploadCtx {opts.bucket, opts.key, {}};
        return resp;
    }

    io::ObjStorageResponse put_object(const io::ObjStoragePath& opts,
                                      std::string_view stream) override {
        int64_t index = ++_store->put_requests;
        if (_store->fail_put_index > 0 && index == _store->fail_put_index) {
            return make_error("injected put_object failure");
        }
        _store->wait_while_blocked();
        if (_store->should_fail_put()) {
            return make_error("injected put_object failure");
        }
        std::lock_guard lock(_store->mutex);
        _store->objects[_store->make_key(opts.bucket, opts.key)] =
                std::string(stream.data(), stream.size());
        return io::ObjStorageResponse::OK();
    }

    io::ObjStorageUploadResult upload_part(const io::ObjStoragePath& opts,
                                           const std::string& upload_id, std::string_view stream,
                                           int part_num) override {
        io::ObjStorageUploadResult resp;
        int64_t index = ++_store->put_requests;
        if (_store->fail_put_index > 0 && index == _store->fail_put_index) {
            resp.resp = make_error("injected upload_part failure");
            return resp;
        }
        _store->wait_while_blocked();
        if (_store->should_fail_put()) {
            resp.resp = make_error("injected upload_part failure");
            return resp;
        }
        std::lock_guard lock(_store->mutex);
        auto ctx_it = _store->uploads.find(upload_id);
        if (ctx_it == _store->uploads.end()) {
            resp.resp = make_error("upload context not found");
            return resp;
        }
        ctx_it->second.parts[part_num] = std::string(stream.data(), stream.size());
        resp.resp = io::ObjStorageResponse::OK();
        resp.etag = fmt::format("\"mock-etag-{}\"", part_num);
        return resp;
    }

    io::ObjStorageResponse complete_multipart_upload(
            const io::ObjStoragePath& opts, const std::string& upload_id,
            const std::vector<io::ObjStorageCompletedPart>& completed_parts) override {
        std::lock_guard lock(_store->mutex);
        auto ctx_it = _store->uploads.find(upload_id);
        if (ctx_it == _store->uploads.end()) {
            return make_error("upload context not found");
        }
        std::string data;
        for (const auto& part : completed_parts) {
            auto part_it = ctx_it->second.parts.find(part.part_num);
            if (part_it == ctx_it->second.parts.end()) {
                return make_error("missing upload part");
            }
            data.append(part_it->second);
        }
        _store->objects[_store->make_key(ctx_it->second.bucket, ctx_it->second.key)] =
                std::move(data);
        _store->uploads.erase(ctx_it);
        return io::ObjStorageResponse::OK();
    }

    io::ObjStorageResponse abort_multipart_upload(const io::ObjStoragePath& /*path*/,
                                                  const std::string& upload_id) override {
        _store->abort_multipart_requests++;
        std::lock_guard lock(_store->mutex);
        _store->uploads.erase(upload_id);
        return io::ObjStorageResponse::OK();
    }

    io::ObjStorageHeadResult head_object(const io::ObjStoragePath& opts) override {
        io::ObjStorageHeadResult resp;
        _store->head_requests++;
        std::lock_guard lock(_store->mutex);
        auto it = _store->objects.find(_store->make_key(opts.bucket, opts.key));
        if (it == _store->objects.end()) {
            resp.resp = make_error("object not found", 404);
            return resp;
        }
        resp.resp = io::ObjStorageResponse::OK();
        resp.file_size = static_cast<long long>(it->second.size());
        return resp;
    }

    io::ObjStorageResponse get_object(const io::ObjStoragePath& opts, void* buffer, size_t offset,
                                      size_t bytes_read, size_t* size_return) override {
        _store->get_requests++;
        std::lock_guard lock(_store->mutex);
        auto it = _store->objects.find(_store->make_key(opts.bucket, opts.key));
        if (it == _store->objects.end()) {
            return make_error("object not found", 404);
        }
        if (offset > it->second.size()) {
            return make_error("offset exceeds object size");
        }
        size_t to_copy = std::min(bytes_read, it->second.size() - offset);
        memcpy(buffer, it->second.data() + offset, to_copy);
        *size_return = to_copy;
        return io::ObjStorageResponse::OK();
    }

    io::ObjStorageListPageResult list_objects_page(const io::ObjStoragePath& opts,
                                                   std::string_view /*token*/) override {
        _store->list_requests++;
        std::lock_guard lock(_store->mutex);
        const auto& object_prefix = opts.prefix.empty() ? opts.key : opts.prefix;
        std::string prefix = _store->make_key(opts.bucket, object_prefix);
        io::ObjStorageListPageResult page {.resp = io::ObjStorageResponse::OK()};
        for (const auto& [key, data] : _store->objects) {
            if (key.rfind(prefix, 0) == 0) {
                page.objects.emplace_back(io::ObjectMeta {
                        .key = key.substr(opts.bucket.size() + 1),
                        .size = static_cast<int64_t>(data.size()),
                });
            }
        }
        return page;
    }

    io::ObjStorageResponse delete_objects(const io::ObjStoragePath& opts,
                                          std::vector<std::string> objs) override {
        _store->delete_requests++;
        if (_store->fail_deletes) {
            return make_error("injected delete failure");
        }
        std::lock_guard lock(_store->mutex);
        for (const auto& obj : objs) {
            _store->objects.erase(_store->make_key(opts.bucket, obj));
        }
        return io::ObjStorageResponse::OK();
    }

    io::ObjStorageResponse delete_object(const io::ObjStoragePath& opts) override {
        _store->delete_requests++;
        if (_store->fail_deletes) {
            return make_error("injected delete failure");
        }
        std::lock_guard lock(_store->mutex);
        _store->objects.erase(_store->make_key(opts.bucket, opts.key));
        return io::ObjStorageResponse::OK();
    }

    std::string generate_presigned_url(const io::ObjStoragePath& opts,
                                       int64_t /*expiration_secs*/) override {
        return fmt::format("mock://{}/{}", opts.bucket, opts.key);
    }

private:
    static io::ObjStorageResponse make_error(std::string msg, int http_code = 500) {
        io::ObjStorageResponse resp;
        resp.status.code = http_code == 404 ? io::ObjStorageStatus::NOT_FOUND
                                            : static_cast<int>(ErrorCode::INTERNAL_ERROR);
        resp.status.msg = std::move(msg);
        resp.http_code = http_code;
        return resp;
    }

    MockS3Store* _store;
};

// Joins on destruction so that an early ASSERT return never leaves a joinable std::thread
// (which would std::terminate the test binary).
class ScopedThread {
public:
    template <typename F>
    explicit ScopedThread(F&& fn) : _thread(std::forward<F>(fn)) {}
    ~ScopedThread() { join(); }
    void join() {
        if (_thread.joinable()) {
            _thread.join();
        }
    }

private:
    std::thread _thread;
};

constexpr const char* kBucket = "spill-mock-bucket";
constexpr const char* kVaultPrefix = "spill_s3_test";
constexpr const char* kCloudUniqueId = "be-cloud-unique-id";
constexpr int64_t kBootId = 1000;

} // namespace

class SpillFileS3Test : public testing::Test {
public:
    static void SetUpTestSuite() {
        _mock_client = std::make_shared<MockObjStorageClient>(&mock_store());
        std::shared_ptr<io::ObjStorageClient> client = _mock_client;
        S3ClientFactory::instance().set_client_creator_for_test(
                [client](const S3ClientConf&) { return client; });

        auto* exec_env = ExecEnv::GetInstance();
        if (exec_env->s3_file_upload_thread_pool() == nullptr) {
            std::unique_ptr<ThreadPool> pool;
            ASSERT_TRUE(ThreadPoolBuilder("s3_upload_file_thread_pool")
                                .set_min_threads(4)
                                .set_max_threads(8)
                                .build(&pool)
                                .ok());
            exec_env->set_s3_file_upload_thread_pool(std::move(pool));
            _owned_upload_pool = true;
        }
        if (exec_env->non_block_close_thread_pool() == nullptr) {
            std::unique_ptr<ThreadPool> pool;
            ASSERT_TRUE(ThreadPoolBuilder("NonBlockCloseThreadPool")
                                .set_min_threads(4)
                                .set_max_threads(8)
                                .build(&pool)
                                .ok());
            exec_env->set_non_block_close_thread_pool(std::move(pool));
            _owned_close_pool = true;
        }

        S3Conf s3_conf;
        s3_conf.bucket = kBucket;
        s3_conf.prefix = kVaultPrefix;
        s3_conf.client_conf.ak = "ak";
        s3_conf.client_conf.sk = "sk";
        s3_conf.client_conf.endpoint = "http://spill-mock-endpoint";
        s3_conf.client_conf.region = "region";
        s3_conf.client_conf.bucket = kBucket;
        auto fs_or = io::S3FileSystem::create(s3_conf, "spill_s3_test_vault");
        ASSERT_TRUE(fs_or.has_value()) << fs_or.error();
        _s3_fs = fs_or.value();
    }

    static void TearDownTestSuite() {
        _s3_fs.reset();
        S3ClientFactory::instance().clear_client_creator_for_test();
        _mock_client.reset();
        auto* exec_env = ExecEnv::GetInstance();
        if (_owned_upload_pool) {
            exec_env->set_s3_file_upload_thread_pool(nullptr);
        }
        if (_owned_close_pool) {
            exec_env->set_non_block_close_thread_pool(nullptr);
        }
    }

protected:
    void SetUp() override {
        mock_store().reset();
        _saved_part_size = config::spill_file_part_size_bytes;
        _saved_buffer_size = config::s3_write_buffer_size;
        _saved_limit = config::spill_s3_storage_limit_bytes;
        _saved_inflight = config::spill_s3_max_inflight_upload_bytes;
        _saved_check_after_upload = config::enable_s3_object_check_after_upload;
        // The manager talks to meta-service through the cloud storage engine only in cloud
        // mode; there is no engine in this test, so pin the mode regardless of earlier tests.
        _saved_deploy_mode = config::deploy_mode;
        _saved_cloud_unique_id = config::cloud_unique_id;
        config::deploy_mode = "";
        config::cloud_unique_id = "";
        // Small buffers so that a few KB of data exercise multipart uploads and part rotation.
        config::s3_write_buffer_size = 8 * 1024;
        config::spill_file_part_size_bytes = 32 * 1024;
        config::spill_s3_storage_limit_bytes = 0;

        _runtime_state = std::make_unique<MockRuntimeState>();
        _profile = std::make_unique<RuntimeProfile>("test");
        _custom_profile = std::make_unique<RuntimeProfile>("CustomCounters");
        _common_profile = std::make_unique<RuntimeProfile>("CommonCounters");
        _common_profile->AddHighWaterMarkCounter("MemoryUsage", TUnit::BYTES, "", 1);
        ADD_TIMER_WITH_LEVEL(_common_profile.get(), "ExecTime", 1);
        SpillWriteCounters write_counters;
        write_counters.init(_custom_profile.get());
        SpillReadCounters read_counters;
        read_counters.init(_custom_profile.get());
        ADD_TIMER_WITH_LEVEL(_custom_profile.get(), profile::SPILL_TOTAL_TIME, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), profile::SPILL_WRITE_FILE_BYTES, TUnit::BYTES,
                               1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), profile::SPILL_WRITE_FILE_TOTAL_COUNT,
                               TUnit::UNIT, 1);
        ADD_COUNTER_WITH_LEVEL(_custom_profile.get(), profile::SPILL_WRITE_FILE_CURRENT_BYTES,
                               TUnit::BYTES, 1);
        _profile->add_child(_custom_profile.get(), true);
        _profile->add_child(_common_profile.get(), true);
    }

    void TearDown() override {
        _destroy_manager();
        _runtime_state.reset();
        mock_store().set_block_uploads(false);
        config::spill_file_part_size_bytes = _saved_part_size;
        config::s3_write_buffer_size = _saved_buffer_size;
        config::spill_s3_storage_limit_bytes = _saved_limit;
        config::spill_s3_max_inflight_upload_bytes = _saved_inflight;
        config::enable_s3_object_check_after_upload = _saved_check_after_upload;
        config::deploy_mode = _saved_deploy_mode;
        config::cloud_unique_id = _saved_cloud_unique_id;
    }

    // Build a manager with one remote store bound to the mock file system.
    void _create_manager(bool bind_fs = true) {
        auto store = std::make_unique<SpillDataDir>(SpillDataDir::Remote {}, "vault-1", kBootId);
        if (bind_fs) {
            store->init_remote_fs(_s3_fs, kCloudUniqueId);
        }
        _data_dir = store.get();
        std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> data_map;
        data_map.emplace("s3", std::move(store));
        _manager = new SpillFileManager(std::move(data_map));
        ExecEnv::GetInstance()->_spill_file_mgr = _manager;
        auto st = _manager->init();
        ASSERT_TRUE(st.ok()) << st;
    }

    void _destroy_manager() {
        if (_manager == nullptr) {
            return;
        }
        _manager->stop();
        SAFE_DELETE(_manager);
        ExecEnv::GetInstance()->_spill_file_mgr = nullptr;
        _data_dir = nullptr;
    }

    static std::string be_root() {
        return fmt::format("{}/spill/{}", kVaultPrefix, kCloudUniqueId);
    }
    static std::string boot_root() { return fmt::format("{}/{}", be_root(), kBootId); }

    static Block _random_string_block(std::mt19937& rng, size_t rows, size_t len) {
        std::vector<std::string> values;
        values.reserve(rows);
        std::uniform_int_distribution<int> dist('a', 'z');
        for (size_t i = 0; i < rows; ++i) {
            std::string s(len, ' ');
            for (auto& c : s) {
                c = static_cast<char>(dist(rng));
            }
            values.emplace_back(std::move(s));
        }
        return ColumnHelper::create_block<DataTypeString>(values);
    }

    static std::vector<std::string> _column_values(const Block& block) {
        std::vector<std::string> out;
        const auto& col = block.get_by_position(0).column;
        for (size_t i = 0; i < col->size(); ++i) {
            out.emplace_back(col->get_data_at(i).to_string());
        }
        return out;
    }

    SpillFileSPtr _write_blocks(const std::string& relative_path, const std::vector<Block>& blocks,
                                Status* close_status) {
        SpillFileSPtr spill_file;
        auto st = _manager->create_spill_file(relative_path, spill_file);
        EXPECT_TRUE(st.ok()) << st;
        SpillFileWriterSPtr writer;
        st = spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
        EXPECT_TRUE(st.ok()) << st;
        for (const auto& block : blocks) {
            st = writer->write_block(_runtime_state.get(), block);
            if (!st.ok()) {
                break;
            }
        }
        if (st.ok()) {
            st = writer->close();
        } else {
            (void)writer->close();
        }
        *close_status = st;
        return spill_file;
    }

    int64_t _counter(const char* name) {
        auto* counter = _custom_profile->get_counter(name);
        EXPECT_NE(counter, nullptr) << name;
        return counter == nullptr ? -1 : counter->value();
    }

    static inline std::shared_ptr<MockObjStorageClient> _mock_client;
    static inline std::shared_ptr<io::S3FileSystem> _s3_fs;
    static inline bool _owned_upload_pool = false;
    static inline bool _owned_close_pool = false;

    std::unique_ptr<MockRuntimeState> _runtime_state;
    std::unique_ptr<RuntimeProfile> _profile;
    std::unique_ptr<RuntimeProfile> _custom_profile;
    std::unique_ptr<RuntimeProfile> _common_profile;
    SpillFileManager* _manager = nullptr;
    SpillDataDir* _data_dir = nullptr;
    int64_t _saved_part_size = 0;
    int64_t _saved_buffer_size = 0;
    int64_t _saved_limit = 0;
    int64_t _saved_inflight = 0;
    bool _saved_check_after_upload = true;
    std::string _saved_deploy_mode;
    std::string _saved_cloud_unique_id;
};

TEST_F(SpillFileS3Test, RemoteStoreLayout) {
    _create_manager();
    ASSERT_TRUE(_data_dir->is_remote());
    ASSERT_TRUE(_data_dir->ready());
    ASSERT_EQ(_data_dir->storage_medium(), TStorageMedium::S3);
    ASSERT_EQ(_data_dir->get_remote_be_root(), fmt::format("spill/{}", kCloudUniqueId));
    ASSERT_EQ(_data_dir->get_spill_data_path(),
              fmt::format("spill/{}/{}", kCloudUniqueId, kBootId));
    ASSERT_EQ(_data_dir->get_spill_data_path("q1"),
              fmt::format("spill/{}/{}/q1", kCloudUniqueId, kBootId));
    ASSERT_EQ(_data_dir->fs().get(), _s3_fs.get());
    ASSERT_FALSE(_data_dir->reach_capacity_limit(1LL << 40)); // unlimited by default
}

TEST_F(SpillFileS3Test, NotReadyUntilVaultResolved) {
    _create_manager(/*bind_fs=*/false);
    ASSERT_FALSE(_data_dir->ready());
    ASSERT_EQ(_data_dir->fs(), nullptr);

    // Not in cloud mode: the store cannot resolve its vault and spill must fail with a clear
    // error instead of touching a null file system.
    SpillFileSPtr spill_file;
    auto st = _manager->create_spill_file("q/not_ready", spill_file);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("only supported in cloud mode") != std::string::npos) << st;
    ASSERT_EQ(spill_file, nullptr);
    ASSERT_TRUE(_manager->remote_startup_cleanup_pending());

    // Cloud mode but the FE heartbeat has not delivered cloud_unique_id yet.
    auto saved_deploy_mode = config::deploy_mode;
    auto saved_cloud_unique_id = config::cloud_unique_id;
    config::deploy_mode = "cloud";
    config::cloud_unique_id = "";
    st = _manager->create_spill_file("q/not_ready", spill_file);
    config::deploy_mode = saved_deploy_mode;
    config::cloud_unique_id = saved_cloud_unique_id;
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("waiting for FE heartbeat") != std::string::npos) << st;
    ASSERT_EQ(spill_file, nullptr);
}

TEST_F(SpillFileS3Test, RoundtripAcrossParts) {
    _create_manager();
    std::mt19937 rng(42);
    std::vector<Block> blocks;
    for (int i = 0; i < 12; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200)); // ~12 KB serialized each
    }

    Status st;
    auto spill_file = _write_blocks("query_1/sort-1-0-1", blocks, &st);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(spill_file->ready_for_reading());

    // Objects live under {vault prefix}/spill/{cloud_unique_id}/{boot_id}/{relative_path}/{part}.
    auto keys = mock_store().keys_with_prefix(kBucket, boot_root() + "/query_1/sort-1-0-1/");
    ASSERT_GT(keys.size(), 1) << "expected several parts";
    int64_t object_bytes = 0;
    {
        std::lock_guard lock(mock_store().mutex);
        for (const auto& key : keys) {
            object_bytes += mock_store().objects.at(mock_store().make_key(kBucket, key)).size();
        }
    }
    ASSERT_EQ(object_bytes, _counter(profile::SPILL_WRITE_FILE_BYTES));
    ASSERT_EQ(object_bytes, _data_dir->get_spill_data_bytes());
    ASSERT_EQ(object_bytes, _counter(profile::SPILL_REMOTE_UPLOAD_BYTES));
    ASSERT_GT(_counter(profile::SPILL_REMOTE_WRITE_REQUESTS), 0);
    ASSERT_GE(_counter(profile::SPILL_REMOTE_WRITE_REQUESTS),
              _counter(profile::SPILL_REMOTE_UPLOAD_PART_REQUESTS));
    ASSERT_EQ(_counter(profile::SPILL_REMOTE_UPLOAD_PART_REQUESTS), mock_store().put_requests);
    ASSERT_EQ(_manager->remote_upload_budget()->inflight_bytes(), 0);

    auto* io_ctx = _runtime_state->get_query_ctx()->resource_ctx()->io_context();
    ASSERT_EQ(io_ctx->spill_write_bytes_to_remote_storage(), object_bytes);
    ASSERT_EQ(io_ctx->spill_write_bytes_to_local_storage(), 0);
    ASSERT_EQ(io_ctx->spill_remote_write_requests(),
              _counter(profile::SPILL_REMOTE_WRITE_REQUESTS));

    // HEADs so far come from check_after_upload on the write path; reading must add none.
    int64_t heads_after_write = mock_store().head_requests;
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    st = reader->open();
    ASSERT_TRUE(st.ok()) << st;
    std::vector<std::string> expected;
    for (const auto& block : blocks) {
        auto values = _column_values(block);
        expected.insert(expected.end(), values.begin(), values.end());
    }
    std::vector<std::string> actual;
    bool eos = false;
    while (!eos) {
        Block block;
        st = reader->read(&block, &eos);
        ASSERT_TRUE(st.ok()) << st;
        if (block.rows() > 0) {
            auto values = _column_values(block);
            actual.insert(actual.end(), values.begin(), values.end());
        }
    }
    ASSERT_EQ(actual, expected);
    st = reader->close();
    ASSERT_TRUE(st.ok());

    // 3 footer reads per part plus one read per block, each a GET.
    int64_t expected_gets =
            3 * static_cast<int64_t>(keys.size()) + static_cast<int64_t>(blocks.size());
    ASSERT_EQ(_counter(profile::SPILL_REMOTE_READ_REQUESTS), expected_gets);
    ASSERT_EQ(mock_store().get_requests, expected_gets);
    ASSERT_EQ(io_ctx->spill_remote_read_requests(), expected_gets);
    ASSERT_EQ(io_ctx->spill_read_bytes_from_remote_storage(), object_bytes);
    ASSERT_EQ(io_ctx->spill_read_bytes_from_local_storage(), 0);
    // Part sizes come from the writer: the reader never asks the store for them.
    ASSERT_EQ(mock_store().head_requests, heads_after_write);
}

TEST_F(SpillFileS3Test, SeekAcrossParts) {
    _create_manager();
    std::mt19937 rng(7);
    std::vector<Block> blocks;
    for (int i = 0; i < 10; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200));
    }
    Status st;
    auto spill_file = _write_blocks("query_2/agg-1-0-1", blocks, &st);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_GT(mock_store().keys_with_prefix(kBucket, boot_root() + "/query_2/").size(), 1);

    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    ASSERT_TRUE(reader->open().ok());
    for (size_t target : {7UL, 2UL, 9UL}) {
        st = reader->seek(target);
        ASSERT_TRUE(st.ok()) << st;
        Block block;
        bool eos = false;
        st = reader->read(&block, &eos);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(eos);
        ASSERT_EQ(_column_values(block), _column_values(blocks[target]));
    }
    st = reader->seek(blocks.size());
    ASSERT_TRUE(st.ok());
    Block block;
    bool eos = false;
    ASSERT_TRUE(reader->read(&block, &eos).ok());
    ASSERT_TRUE(eos);
}

TEST_F(SpillFileS3Test, GcDeletesObjects) {
    _create_manager();
    std::mt19937 rng(1);
    Status st;
    int64_t heads_after_write = 0;
    {
        auto spill_file =
                _write_blocks("query_3/join-1-0-1", {_random_string_block(rng, 64, 200)}, &st);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(mock_store().keys_with_prefix(kBucket, boot_root() + "/query_3/").empty());
        ASSERT_GT(_data_dir->get_spill_data_bytes(), 0);
        heads_after_write = mock_store().head_requests;
    }
    // No exists() probe is needed: the prefix is deleted directly (LIST + DELETE, no HEAD).
    ASSERT_TRUE(mock_store().keys_with_prefix(kBucket, boot_root() + "/query_3/").empty());
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 0);
    ASSERT_EQ(mock_store().head_requests, heads_after_write);
    ASSERT_GE(mock_store().list_requests, 1);
    ASSERT_GE(mock_store().delete_requests, 1);

    // A file that was never written has nothing to delete and issues no request.
    int64_t lists_before = mock_store().list_requests;
    int64_t deletes_before = mock_store().delete_requests;
    SpillFileSPtr empty_file;
    ASSERT_TRUE(_manager->create_spill_file("query_3/never_written", empty_file).ok());
    empty_file.reset();
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 0);
    ASSERT_EQ(mock_store().list_requests, lists_before);
    ASSERT_EQ(mock_store().delete_requests, deletes_before);
}

TEST_F(SpillFileS3Test, QueryDirectoryDeletionRetriesUntilSuccess) {
    _create_manager();
    // Stop the GC thread so that retries are driven by the test.
    _manager->stop();

    std::mt19937 rng(3);
    Status st;
    auto spill_file =
            _write_blocks("query_4/sort-1-0-1", {_random_string_block(rng, 64, 200)}, &st);
    ASSERT_TRUE(st.ok()) << st;
    // Simulate a query whose per-file gc failed: leave the objects in place.
    mock_store().fail_deletes = true;
    spill_file.reset();
    ASSERT_FALSE(mock_store().keys_with_prefix(kBucket, boot_root() + "/query_4/").empty());

    _manager->delete_query_spill_directory("query_4", _data_dir);
    ASSERT_EQ(_manager->pending_delete_dir_count(), 1);
    ASSERT_FALSE(mock_store().keys_with_prefix(kBucket, boot_root() + "/query_4/").empty());

    mock_store().fail_deletes = false;
    _manager->gc(1000);
    ASSERT_EQ(_manager->pending_delete_dir_count(), 0);
    ASSERT_TRUE(mock_store().keys_with_prefix(kBucket, boot_root() + "/query_4/").empty());
}

TEST_F(SpillFileS3Test, StartupCleanupDeletesOnlyOtherBootGenerations) {
    // Residue of two previous boots plus an object of the current generation written by
    // "another running query" of this process.
    mock_store().put_raw(kBucket, be_root() + "/900/query_old/sort-1-0-1/0", "old");
    mock_store().put_raw(kBucket, be_root() + "/900/query_old/sort-1-0-1/1", "old");
    mock_store().put_raw(kBucket, be_root() + "/950/query_old2/agg-1-0-1/0", "old");
    mock_store().put_raw(kBucket, boot_root() + "/query_live/sort-1-0-1/0", "live");
    // Data of another BE must not be touched.
    mock_store().put_raw(kBucket, fmt::format("{}/spill/other-be/900/q/0", kVaultPrefix), "other");

    _create_manager();
    for (int i = 0; i < 100 && _manager->remote_startup_cleanup_pending(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_FALSE(_manager->remote_startup_cleanup_pending());
    ASSERT_TRUE(mock_store().keys_with_prefix(kBucket, be_root() + "/900/").empty());
    ASSERT_TRUE(mock_store().keys_with_prefix(kBucket, be_root() + "/950/").empty());
    ASSERT_EQ(mock_store().keys_with_prefix(kBucket, boot_root() + "/").size(), 1);
    ASSERT_EQ(mock_store()
                      .keys_with_prefix(kBucket, kVaultPrefix + std::string("/spill/other-be/"))
                      .size(),
              1);
}

TEST_F(SpillFileS3Test, StorageLimitIsEnforced) {
    config::spill_s3_storage_limit_bytes = 4 * 1024;
    _create_manager();
    ASSERT_TRUE(_data_dir->reach_capacity_limit(8 * 1024));
    ASSERT_FALSE(_data_dir->reach_capacity_limit(1024));

    std::mt19937 rng(5);
    Status st;
    auto spill_file =
            _write_blocks("query_5/sort-1-0-1", {_random_string_block(rng, 64, 200)}, &st);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is<ErrorCode::DISK_REACH_CAPACITY_LIMIT>()) << st;
    // Nothing was accounted for the rejected block; the store is still usable.
    ASSERT_EQ(_manager->remote_upload_budget()->inflight_bytes(), 0);
    spill_file.reset();
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 0);

    config::spill_s3_storage_limit_bytes = 0;
    ASSERT_TRUE(_data_dir->update_capacity().ok());
    spill_file = _write_blocks("query_5/sort-1-0-2", {_random_string_block(rng, 64, 200)}, &st);
    ASSERT_TRUE(st.ok()) << st;
}

TEST_F(SpillFileS3Test, TryReserveIsAtomic) {
    config::spill_s3_storage_limit_bytes = 1000;
    _create_manager();
    std::atomic<int> granted {0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < 100; ++j) {
                if (_data_dir->try_reserve(10).ok()) {
                    granted++;
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    ASSERT_EQ(granted.load(), 100);
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 1000);
    ASSERT_TRUE(_data_dir->try_reserve(16, /*force=*/true).ok());
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 1016);
    _data_dir->release(1016);
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 0);
}

TEST_F(SpillFileS3Test, UploadBudgetBlocksAndReconciles) {
    config::spill_s3_max_inflight_upload_bytes = 40 * 1024;
    config::spill_file_part_size_bytes = 16 * 1024;
    _create_manager();
    auto* budget = _manager->remote_upload_budget();
    ASSERT_EQ(budget->limit_bytes(), 40 * 1024);

    std::mt19937 rng(11);
    std::vector<Block> blocks;
    for (int i = 0; i < 20; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200)); // ~12 KB each, ~240 KB total
    }

    mock_store().set_block_uploads(true);
    Status write_status;
    std::atomic<bool> finished {false};
    ScopedThread writer_thread([&]() {
        _write_blocks("query_6/sort-1-0-1", blocks, &write_status);
        finished = true;
    });
    // Runs before writer_thread is joined: an early ASSERT return must not leave the writer
    // stuck behind blocked uploads or the budget gate.
    Defer release_writer {[&]() {
        mock_store().set_block_uploads(false);
        _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test teardown"));
    }};

    // With uploads stuck the writer must stall once the budget is used up.
    for (int i = 0; i < 50 && budget->inflight_bytes() < 30 * 1024; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_FALSE(finished.load());
    // A single append may exceed the limit, never more than one.
    ASSERT_LE(budget->inflight_bytes(), 40 * 1024 + 16 * 1024);

    mock_store().set_block_uploads(false);
    writer_thread.join();
    ASSERT_TRUE(write_status.ok()) << write_status;
    ASSERT_EQ(budget->inflight_bytes(), 0);
    ASSERT_EQ(budget->total_acquired_bytes(), budget->total_released_bytes());
    ASSERT_GT(budget->total_acquired_bytes(), 0);
    ASSERT_GT(_counter(profile::SPILL_REMOTE_UPLOAD_WAIT_TIME), 0);
}

// Regression test for the BE-wide hang: bytes sitting in a writer's not-yet-full pending
// buffer must not count against the budget, otherwise a handful of idle open writers could
// hold the whole budget and block every other writer forever.
TEST_F(SpillFileS3Test, PendingBuffersDoNotConsumeBudget) {
    config::spill_s3_max_inflight_upload_bytes = 24 * 1024; // three 8 KB buffers
    config::spill_file_part_size_bytes = 1024 * 1024;
    _create_manager();
    auto* budget = _manager->remote_upload_budget();

    std::mt19937 rng(23);
    // Six idle writers, each holding ~6 KB in a pending buffer (36 KB > limit in total).
    // Written on a deadline thread: with append-time accounting the fourth writer would block
    // forever, and the query is cancelled below so that such a regression fails instead of
    // hanging the binary.
    std::vector<SpillFileSPtr> idle_files;
    std::vector<SpillFileWriterSPtr> idle_writers;
    Status idle_status;
    std::atomic<bool> idle_done {false};
    {
        ScopedThread idle_thread([&]() {
            for (int i = 0; i < 6 && idle_status.ok(); ++i) {
                SpillFileSPtr spill_file;
                idle_status =
                        _manager->create_spill_file(fmt::format("query_10/idle-{}", i), spill_file);
                if (!idle_status.ok()) {
                    break;
                }
                SpillFileWriterSPtr writer;
                idle_status =
                        spill_file->create_writer(_runtime_state.get(), _profile.get(), writer);
                if (!idle_status.ok()) {
                    break;
                }
                idle_status = writer->write_block(_runtime_state.get(),
                                                  _random_string_block(rng, 32, 200));
                idle_files.push_back(std::move(spill_file));
                idle_writers.push_back(std::move(writer));
            }
            idle_done = true;
        });
        for (int i = 0; i < 500 && !idle_done.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        if (!idle_done.load()) {
            _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test deadline"));
        }
        idle_thread.join();
    }
    ASSERT_TRUE(idle_done.load() && idle_status.ok())
            << "idle writers blocked on budget: " << idle_status;
    ASSERT_EQ(budget->inflight_bytes(), 0) << "pending buffers must not be counted";

    // A seventh writer streams several full buffers through the budget; it must finish.
    std::vector<Block> blocks;
    for (int i = 0; i < 10; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200));
    }
    Status write_status;
    std::atomic<bool> finished {false};
    ScopedThread writer_thread([&]() {
        _write_blocks("query_10/active", blocks, &write_status);
        finished = true;
    });
    // Runs before writer_thread is joined: an early ASSERT return must not leave the writer
    // stuck behind blocked uploads or the budget gate.
    Defer release_writer {[&]() {
        mock_store().set_block_uploads(false);
        _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test teardown"));
    }};
    for (int i = 0; i < 500 && !finished.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (!finished.load()) {
        _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test deadline"));
    }
    writer_thread.join();
    ASSERT_TRUE(finished.load() && write_status.ok())
            << "writer blocked on budget held by idle pending buffers: " << write_status;

    for (auto& writer : idle_writers) {
        ASSERT_TRUE(writer->close().ok());
    }
    idle_writers.clear();
    ASSERT_EQ(budget->inflight_bytes(), 0);
    ASSERT_EQ(budget->total_acquired_bytes(), budget->total_released_bytes());
}

TEST_F(SpillFileS3Test, UploadBudgetWaitIsCancellable) {
    config::spill_s3_max_inflight_upload_bytes = 16 * 1024;
    config::spill_file_part_size_bytes = 16 * 1024;
    _create_manager();
    auto* budget = _manager->remote_upload_budget();

    std::mt19937 rng(13);
    std::vector<Block> blocks;
    for (int i = 0; i < 10; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200));
    }

    mock_store().set_block_uploads(true);
    SpillFileSPtr spill_file;
    ASSERT_TRUE(_manager->create_spill_file("query_7/sort-1-0-1", spill_file).ok());
    SpillFileWriterSPtr writer;
    ASSERT_TRUE(spill_file->create_writer(_runtime_state.get(), _profile.get(), writer).ok());

    Status write_status;
    std::atomic<bool> writes_done {false};
    ScopedThread writer_thread([&]() {
        for (const auto& block : blocks) {
            write_status = writer->write_block(_runtime_state.get(), block);
            if (!write_status.ok()) {
                break;
            }
        }
        writes_done = true;
        // close() has to wait for the stuck uploads; the test releases them below.
        (void)writer->close();
    });
    // Runs before writer_thread is joined: an early ASSERT return must not leave the writer
    // stuck behind blocked uploads or the budget gate.
    Defer release_writer {[&]() {
        mock_store().set_block_uploads(false);
        _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test teardown"));
    }};
    for (int i = 0; i < 50 && budget->inflight_bytes() < 16 * 1024; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    // Cancellation is observed through the query's task controller, not the RuntimeState.
    _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test cancel"));
    ASSERT_TRUE(_runtime_state->get_query_ctx()->resource_ctx()->task_controller()->is_cancelled());
    for (int i = 0; i < 100 && !writes_done.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(writes_done.load()) << "writer did not observe the cancellation";
    ASSERT_TRUE(write_status.is<ErrorCode::CANCELLED>()) << write_status;

    mock_store().set_block_uploads(false);
    writer_thread.join();
    // Budget acquired for buffers that were never uploaded is reconciled on close.
    ASSERT_EQ(budget->inflight_bytes(), 0);
    ASSERT_EQ(budget->total_acquired_bytes(), budget->total_released_bytes());
}

TEST_F(SpillFileS3Test, UploadFailureIsReportedAndReconciled) {
    _create_manager();
    mock_store().fail_uploads = true;
    std::mt19937 rng(17);
    std::vector<Block> blocks;
    for (int i = 0; i < 6; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200));
    }
    Status st;
    auto spill_file = _write_blocks("query_8/sort-1-0-1", blocks, &st);
    ASSERT_FALSE(st.ok());
    ASSERT_FALSE(spill_file->ready_for_reading());
    ASSERT_EQ(_manager->remote_upload_budget()->inflight_bytes(), 0);
    ASSERT_EQ(_manager->remote_upload_budget()->total_acquired_bytes(),
              _manager->remote_upload_budget()->total_released_bytes());
    ASSERT_GT(_counter(profile::SPILL_REMOTE_WRITE_REQUESTS), 0);
    ASSERT_EQ(_counter(profile::SPILL_REMOTE_UPLOAD_BYTES), 0);
    // Every multipart upload that was started for the failed part has been aborted.
    ASSERT_GE(mock_store().create_multipart_requests, 1);
    ASSERT_EQ(mock_store().abort_multipart_requests, mock_store().create_multipart_requests);
    {
        std::lock_guard lock(mock_store().mutex);
        ASSERT_TRUE(mock_store().uploads.empty()) << "multipart uploads must be aborted";
    }
    mock_store().fail_uploads = false;
    spill_file.reset();
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 0);
}

// One upload fails while earlier buffers of the same part are still in flight: the writer
// turns failed, the footer write is refused, the part never gets a close(true) and is drained
// through destruction. The budget must balance exactly and the multipart upload be aborted.
TEST_F(SpillFileS3Test, LateUploadFailureBalancesBudget) {
    config::spill_file_part_size_bytes = 1024 * 1024;
    _create_manager();
    auto* budget = _manager->remote_upload_budget();
    // Uploads 1 and 2 hang in the mock; upload 3 fails immediately while they are in flight.
    mock_store().fail_put_index = 3;
    mock_store().set_block_uploads(true);

    std::mt19937 rng(29);
    std::vector<Block> blocks;
    for (int i = 0; i < 8; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200)); // ~12 KB each -> ~12 buffers
    }
    Status write_status;
    ScopedThread writer_thread(
            [&]() { _write_blocks("query_11/sort-1-0-1", blocks, &write_status); });
    // Runs before writer_thread is joined: an early ASSERT return must not leave the writer
    // stuck behind blocked uploads or the budget gate.
    Defer release_writer {[&]() {
        mock_store().set_block_uploads(false);
        _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test teardown"));
    }};
    for (int i = 0; i < 250 && mock_store().put_requests < 3; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GE(mock_store().put_requests, 3);
    // Give the writer time to observe the failure and enter close() with buffers in flight.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    mock_store().set_block_uploads(false);
    writer_thread.join();
    ASSERT_FALSE(write_status.ok());
    ASSERT_EQ(budget->inflight_bytes(), 0);
    ASSERT_EQ(budget->total_acquired_bytes(), budget->total_released_bytes());
    ASSERT_GT(budget->total_acquired_bytes(), 0);
    ASSERT_GE(mock_store().create_multipart_requests, 1);
    ASSERT_EQ(mock_store().abort_multipart_requests, mock_store().create_multipart_requests);
}

// CreateMultipartUpload fails after the first buffer filled: the writer must turn failed and
// drop the full pending buffer; the footer append that close() attempts afterwards must be
// refused instead of overflowing the buffer, and no upload may be issued.
TEST_F(SpillFileS3Test, CreateMultipartFailureIsSafe) {
    config::spill_file_part_size_bytes = 1024 * 1024;
    _create_manager();
    auto* budget = _manager->remote_upload_budget();
    mock_store().fail_create_multipart = true;

    std::mt19937 rng(31);
    std::vector<Block> blocks;
    for (int i = 0; i < 3; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200));
    }
    Status st;
    auto spill_file = _write_blocks("query_12/sort-1-0-1", blocks, &st);
    ASSERT_FALSE(st.ok());
    ASSERT_FALSE(spill_file->ready_for_reading());
    ASSERT_EQ(mock_store().put_requests, 0);
    ASSERT_GE(mock_store().create_multipart_requests, 1);
    ASSERT_EQ(budget->inflight_bytes(), 0);
    ASSERT_EQ(budget->total_acquired_bytes(), budget->total_released_bytes());
    spill_file.reset();
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 0);
}

TEST_F(SpillFileS3Test, ManagerMetrics) {
    _create_manager();
    std::mt19937 rng(19);
    Status st;
    auto spill_file =
            _write_blocks("query_9/sort-1-0-1", {_random_string_block(rng, 64, 200)}, &st);
    ASSERT_TRUE(st.ok()) << st;
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    ASSERT_TRUE(reader->open().ok());
    bool eos = false;
    while (!eos) {
        Block block;
        ASSERT_TRUE(reader->read(&block, &eos).ok());
    }
    ASSERT_TRUE(reader->close().ok());
    ASSERT_EQ(_manager->pending_delete_dir_count(), 0);
    // The startup cleanup runs on the GC thread (spill_gc_interval_ms); give it time.
    for (int i = 0; i < 100 && _manager->remote_startup_cleanup_pending(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_FALSE(_manager->remote_startup_cleanup_pending());
    ASSERT_EQ(_counter(profile::SPILL_REMOTE_READ_REQUESTS), mock_store().get_requests);
    ASSERT_EQ(_counter(profile::SPILL_REMOTE_UPLOAD_PART_REQUESTS), mock_store().put_requests);
}

} // namespace doris::vectorized

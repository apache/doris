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
// cleanup (file gc, query directory, residue of the previous process), the capacity limit,
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
#include <string_view>
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
#include "exec/spill/remote_spill_data_dir.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "exec/spill/spill_remote_upload_budget.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/s3_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_profile_counter_names.h"
#include "runtime/workload_management/io_context.h"
#include "runtime/workload_management/resource_context.h"
#include "service/backend_options.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/obj_storage_client_test_stub.h"
#include "util/defer_op.h"
#include "util/s3_util.h"
#include "util/slice.h"
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
    // LIST of a whole spill root ("{vault}/spill/{ip}_{port}/"), issued by the startup cleanup only.
    std::atomic<int64_t> root_list_requests {0};
    std::atomic<int64_t> get_requests {0};
    // Bytes returned by each GET, in order. Guarded by mutex.
    std::vector<size_t> get_sizes;
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
        root_list_requests = 0;
        get_requests = 0;
        get_sizes.clear();
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
        _store->get_sizes.push_back(to_copy);
        return io::ObjStorageResponse::OK();
    }

    // "{vault}/spill/{ip}_{port}/": nothing after the host component.
    static bool is_spill_root(const std::string& prefix) {
        auto pos = prefix.find("/spill/");
        if (pos == std::string::npos) {
            return false;
        }
        std::string rest = prefix.substr(pos + 7);
        if (!rest.empty() && rest.back() == '/') {
            rest.pop_back();
        }
        return !rest.empty() && rest.find('/') == std::string::npos;
    }

    io::ObjStorageListPageResult list_objects_page(const io::ObjStoragePath& opts,
                                                   std::string_view /*token*/) override {
        if (is_spill_root(opts.prefix)) {
            _store->root_list_requests++; // the startup cleanup, not spill data traffic
        } else {
            _store->list_requests++;
        }
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
constexpr const char* kEndpoint = "10.0.0.1_9050";

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
        _saved_read_coalesce = config::spill_s3_read_coalesce_bytes;
        _saved_heartbeat_interval = config::spill_s3_heartbeat_interval_second;
        // The GC thread would put heartbeat objects in the middle of the request counts below;
        // tests of the heartbeat enable it.
        config::spill_s3_heartbeat_interval_second = 0;
        // The manager talks to meta-service through the cloud storage engine only in cloud
        // mode; there is no engine in this test, so pin the mode regardless of earlier tests.
        _saved_deploy_mode = config::deploy_mode;
        _saved_cloud_unique_id = config::cloud_unique_id;
        config::deploy_mode = "";
        config::cloud_unique_id = "";
        // ensure_ready() reads the address of this BE; tests bind the store explicitly and rely
        // on "unknown" here, which stops ensure_ready() before it needs a storage engine.
        _saved_localhost = BackendOptions::get_localhost();
        BackendOptions::set_localhost("");
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
        config::spill_s3_read_coalesce_bytes = _saved_read_coalesce;
        config::spill_s3_heartbeat_interval_second = _saved_heartbeat_interval;
        config::deploy_mode = _saved_deploy_mode;
        config::cloud_unique_id = _saved_cloud_unique_id;
        BackendOptions::set_localhost(_saved_localhost);
    }

    // Build a manager with one remote store bound to the mock file system.
    void _create_manager(bool bind_fs = true) {
        auto store = std::make_unique<RemoteSpillDataDir>("vault-1");
        if (bind_fs) {
            store->init_remote_fs(_s3_fs, kEndpoint);
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

    // Vault-prefixed keys of the mock store: {vault}/spill/{ip}_{port}/{query_id}/...
    static std::string spill_root() { return fmt::format("{}/spill/{}", kVaultPrefix, kEndpoint); }

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

    // Read the whole spill file and return the values of its first column.
    std::vector<std::string> _read_all(const SpillFileSPtr& spill_file) {
        auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
        auto st = reader->open();
        EXPECT_TRUE(st.ok()) << st;
        std::vector<std::string> values;
        bool eos = false;
        while (st.ok() && !eos) {
            Block block;
            st = reader->read(&block, &eos);
            EXPECT_TRUE(st.ok()) << st;
            if (block.rows() > 0) {
                auto block_values = _column_values(block);
                values.insert(values.end(), block_values.begin(), block_values.end());
            }
        }
        EXPECT_TRUE(reader->close().ok());
        return values;
    }

    static std::vector<std::string> _values_of(const std::vector<Block>& blocks) {
        std::vector<std::string> values;
        for (const auto& block : blocks) {
            auto block_values = _column_values(block);
            values.insert(values.end(), block_values.begin(), block_values.end());
        }
        return values;
    }

    static std::string _object(const std::string& key) {
        std::lock_guard lock(mock_store().mutex);
        return mock_store().objects.at(mock_store().make_key(kBucket, key));
    }

    // Block start offsets of a part object, followed by the offset where its footer starts.
    static std::vector<size_t> _block_offsets(const std::string& object) {
        size_t block_count = 0;
        memcpy(&block_count, object.data() + object.size() - sizeof(size_t), sizeof(size_t));
        size_t footer_start = object.size() - (block_count + 2) * sizeof(size_t);
        std::vector<size_t> offsets(block_count + 1);
        for (size_t i = 0; i < block_count; ++i) {
            memcpy(&offsets[i], object.data() + footer_start + i * sizeof(size_t), sizeof(size_t));
        }
        offsets[block_count] = footer_start;
        return offsets;
    }

    // Sizes of the reads that cover the blocks in order when adjacent blocks are coalesced
    // up to `window` bytes; a block larger than the window is read alone.
    static std::vector<size_t> _coalesced_reads(const std::vector<size_t>& offsets, size_t window) {
        std::vector<size_t> reads;
        const size_t block_count = offsets.size() - 1;
        for (size_t i = 0; i < block_count;) {
            size_t last = i + 1;
            while (last < block_count && offsets[last + 1] - offsets[i] <= window) {
                ++last;
            }
            reads.push_back(offsets[last] - offsets[i]);
            i = last;
        }
        return reads;
    }

    static std::vector<size_t> _get_sizes() {
        std::lock_guard lock(mock_store().mutex);
        return mock_store().get_sizes;
    }

    static void _reset_get_stats() {
        std::lock_guard lock(mock_store().mutex);
        mock_store().get_requests = 0;
        mock_store().get_sizes.clear();
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
    RemoteSpillDataDir* _data_dir = nullptr;
    int64_t _saved_part_size = 0;
    int64_t _saved_buffer_size = 0;
    int64_t _saved_limit = 0;
    int64_t _saved_inflight = 0;
    bool _saved_check_after_upload = true;
    int64_t _saved_read_coalesce = 0;
    int64_t _saved_heartbeat_interval = 0;
    std::string _saved_deploy_mode;
    std::string _saved_cloud_unique_id;
    std::string _saved_localhost;
};

TEST_F(SpillFileS3Test, RemoteStoreLayout) {
    _create_manager();
    ASSERT_TRUE(_data_dir->is_remote());
    ASSERT_TRUE(_data_dir->ready());
    ASSERT_EQ(_data_dir->storage_medium(), TStorageMedium::S3);
    ASSERT_EQ(_data_dir->endpoint(), kEndpoint);
    ASSERT_EQ(_data_dir->get_spill_data_path(), fmt::format("spill/{}", kEndpoint));
    ASSERT_EQ(_data_dir->get_spill_data_path("q1"), fmt::format("spill/{}/q1", kEndpoint));
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

    // Cloud mode but the address of this BE is not known.
    auto saved_deploy_mode = config::deploy_mode;
    auto saved_cloud_unique_id = config::cloud_unique_id;
    config::deploy_mode = "cloud";
    config::cloud_unique_id = "";
    st = _manager->create_spill_file("q/not_ready", spill_file);
    config::deploy_mode = saved_deploy_mode;
    config::cloud_unique_id = saved_cloud_unique_id;
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.to_string().find("address of this BE is unknown") != std::string::npos) << st;
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

    // Objects live under
    // {vault prefix}/spill/{ip}_{port}/{relative_path}/{part}.
    auto keys = mock_store().keys_with_prefix(kBucket, spill_root() + "/query_1/sort-1-0-1/");
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

    // Every part fits in one coalesced read, so it is fetched whole with a single GET.
    int64_t expected_gets = static_cast<int64_t>(keys.size());
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
    ASSERT_GT(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_2/").size(), 1);

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

// One part larger than the footer probe: the footer takes one GET, then adjacent blocks are
// coalesced into reads of at most spill_s3_read_coalesce_bytes, a block larger than that is
// read alone, and a window covering the whole part fetches it in one GET.
TEST_F(SpillFileS3Test, ReadCoalescesAdjacentBlocks) {
    config::spill_file_part_size_bytes = 1024 * 1024;
    _create_manager();
    std::mt19937 rng(11);
    std::vector<Block> blocks;
    for (int i = 0; i < 12; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200));
    }
    Status st;
    auto spill_file = _write_blocks("query_5/join-1-0-1", blocks, &st);
    ASSERT_TRUE(st.ok()) << st;
    auto keys = mock_store().keys_with_prefix(kBucket, spill_root() + "/query_5/");
    ASSERT_EQ(keys.size(), 1);
    const std::string object = _object(keys[0]);
    const auto offsets = _block_offsets(object);
    ASSERT_EQ(offsets.size(), blocks.size() + 1);
    const size_t footer_size = object.size() - offsets.back();
    constexpr size_t kFooterProbe = 64 * 1024;
    ASSERT_GT(object.size(), kFooterProbe);
    size_t min_block = SIZE_MAX;
    for (size_t i = 0; i + 1 < offsets.size(); ++i) {
        min_block = std::min(min_block, offsets[i + 1] - offsets[i]);
    }
    ASSERT_GT(min_block, 1024);

    auto* io_ctx = _runtime_state->get_query_ctx()->resource_ctx()->io_context();
    const auto expected_values = _values_of(blocks);
    for (int64_t window : {int64_t(1024), int64_t(40 * 1024), int64_t(object.size())}) {
        SCOPED_TRACE(fmt::format("window {}", window));
        config::spill_s3_read_coalesce_bytes = window;
        _reset_get_stats();
        int64_t read_bytes_before = io_ctx->spill_read_bytes_from_remote_storage();
        int64_t requests_before = io_ctx->spill_remote_read_requests();

        ASSERT_EQ(_read_all(spill_file), expected_values);

        std::vector<size_t> expected_gets;
        if (static_cast<size_t>(window) >= object.size()) {
            expected_gets.push_back(object.size());
        } else {
            expected_gets.push_back(kFooterProbe);
            auto reads = _coalesced_reads(offsets, window);
            expected_gets.insert(expected_gets.end(), reads.begin(), reads.end());
        }
        ASSERT_EQ(_get_sizes(), expected_gets);
        ASSERT_EQ(io_ctx->spill_remote_read_requests() - requests_before,
                  static_cast<int64_t>(expected_gets.size()));
        size_t total = 0;
        for (size_t size : expected_gets) {
            total += size;
        }
        ASSERT_EQ(io_ctx->spill_read_bytes_from_remote_storage() - read_bytes_before, total);
        if (window == 1024) {
            // Every block is larger than the window, so each is read alone.
            ASSERT_EQ(expected_gets.size(), 1 + blocks.size());
        } else if (static_cast<size_t>(window) < object.size()) {
            ASSERT_LT(expected_gets.size(), 1 + blocks.size());
            // The probe re-reads only the tail blocks in front of the footer.
            ASSERT_EQ(total, object.size() + kFooterProbe - footer_size);
        }
    }
}

// spill_s3_read_coalesce_bytes = 0 falls back to exact reads: the footer of every part (block
// count and max sub block size, then the offsets) and one GET per block.
TEST_F(SpillFileS3Test, ReadCoalesceDisabled) {
    config::spill_s3_read_coalesce_bytes = 0;
    _create_manager();
    std::mt19937 rng(13);
    std::vector<Block> blocks;
    for (int i = 0; i < 10; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200));
    }
    Status st;
    auto spill_file = _write_blocks("query_6/agg-1-0-1", blocks, &st);
    ASSERT_TRUE(st.ok()) << st;
    auto keys = mock_store().keys_with_prefix(kBucket, spill_root() + "/query_6/");
    ASSERT_GT(keys.size(), 1);
    // Parts are read in index order; the listing is lexicographic.
    auto part_index = [](const std::string& key) {
        return std::stoul(key.substr(key.rfind('/') + 1));
    };
    std::sort(keys.begin(), keys.end(), [&](const std::string& a, const std::string& b) {
        return part_index(a) < part_index(b);
    });
    std::vector<size_t> expected_gets;
    int64_t object_bytes = 0;
    for (const auto& key : keys) {
        const std::string object = _object(key);
        object_bytes += object.size();
        const auto offsets = _block_offsets(object);
        expected_gets.push_back(2 * sizeof(size_t));
        expected_gets.push_back((offsets.size() - 1) * sizeof(size_t));
        for (size_t i = 0; i + 1 < offsets.size(); ++i) {
            expected_gets.push_back(offsets[i + 1] - offsets[i]);
        }
    }

    _reset_get_stats();
    ASSERT_EQ(_read_all(spill_file), _values_of(blocks));
    ASSERT_EQ(_get_sizes(), expected_gets);
    auto* io_ctx = _runtime_state->get_query_ctx()->resource_ctx()->io_context();
    ASSERT_EQ(io_ctx->spill_read_bytes_from_remote_storage(), object_bytes);
}

// Seeking inside the part that is already buffered issues no request.
TEST_F(SpillFileS3Test, SeekWithinBufferedPart) {
    config::spill_file_part_size_bytes = 1024 * 1024;
    _create_manager();
    std::mt19937 rng(17);
    std::vector<Block> blocks;
    for (int i = 0; i < 10; ++i) {
        blocks.push_back(_random_string_block(rng, 64, 200));
    }
    Status st;
    auto spill_file = _write_blocks("query_7/sort-1-0-1", blocks, &st);
    ASSERT_TRUE(st.ok()) << st;
    auto keys = mock_store().keys_with_prefix(kBucket, spill_root() + "/query_7/");
    ASSERT_EQ(keys.size(), 1);

    _reset_get_stats();
    auto reader = spill_file->create_reader(_runtime_state.get(), _profile.get());
    ASSERT_TRUE(reader->open().ok());
    ASSERT_EQ(_get_sizes(), std::vector<size_t> {_object(keys[0]).size()});
    for (size_t target : {7UL, 2UL, 9UL, 0UL}) {
        st = reader->seek(target);
        ASSERT_TRUE(st.ok()) << st;
        Block block;
        bool eos = false;
        st = reader->read(&block, &eos);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(eos);
        ASSERT_EQ(_column_values(block), _column_values(blocks[target]));
    }
    ASSERT_EQ(mock_store().get_requests, 1);
    ASSERT_TRUE(reader->close().ok());
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
        ASSERT_FALSE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_3/").empty());
        ASSERT_GT(_data_dir->get_spill_data_bytes(), 0);
        heads_after_write = mock_store().head_requests;
    }
    // No exists() probe is needed: the prefix is deleted directly (LIST + DELETE, no HEAD).
    ASSERT_TRUE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_3/").empty());
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
    ASSERT_FALSE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_4/").empty());

    _manager->delete_query_spill_directory("query_4", _data_dir);
    ASSERT_EQ(_manager->pending_delete_dir_count(), 1);
    ASSERT_FALSE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_4/").empty());

    mock_store().fail_deletes = false;
    _manager->gc(1000);
    ASSERT_EQ(_manager->pending_delete_dir_count(), 0);
    ASSERT_TRUE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_4/").empty());
}

// Query directories under spill/{ip}_{port}/ that no query of this process uses are residue of the
// previous process and go; a directory of a running query of this process, the data of other BEs
// (another host, or another port on this host), and directories created after the first listing
// stay.
// A spill file whose deletion fails keeps its bytes charged, also when the query directory
// deletion fails after it, until a retry deletes the objects: an outage cannot free capacity
// that is still used, and the spill size reported to SHOW DATA does not drop.
TEST_F(SpillFileS3Test, FailedDeletionKeepsCapacityCharged) {
    _create_manager();
    _manager->stop();

    std::mt19937 rng(19);
    Status st;
    auto spill_file =
            _write_blocks("query_8/sort-1-0-1", {_random_string_block(rng, 64, 200)}, &st);
    ASSERT_TRUE(st.ok()) << st;
    const int64_t charged = _data_dir->get_spill_data_bytes();
    ASSERT_GT(charged, 0);

    mock_store().fail_deletes = true;
    spill_file.reset();
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), charged);
    ASSERT_EQ(_manager->remote_spill_data_bytes(), charged);
    ASSERT_EQ(_manager->pending_delete_dir_count(), 1);

    // The limit still counts the retained objects.
    config::spill_s3_storage_limit_bytes = charged;
    ASSERT_TRUE(_data_dir->update_capacity().ok());
    ASSERT_TRUE(_data_dir->reach_capacity_limit(1));

    // The failed query directory deletion takes over the pending spill file directory.
    _manager->delete_query_spill_directory("query_8", _data_dir);
    ASSERT_EQ(_manager->pending_delete_dir_count(), 1);
    _manager->gc(1000);
    ASSERT_EQ(_manager->pending_delete_dir_count(), 1);
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), charged);

    mock_store().fail_deletes = false;
    _manager->gc(1000);
    ASSERT_EQ(_manager->pending_delete_dir_count(), 0);
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 0);
    ASSERT_FALSE(_data_dir->reach_capacity_limit(1));
    ASSERT_TRUE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_8/").empty());
}

// The GC thread rewrites spill/{ip}_{port}/_heartbeat every spill_s3_heartbeat_interval_second;
// the meta-service recycler keeps the directory of a BE whose heartbeat is fresh. The startup
// cleanup of query directories leaves the heartbeat alone.
TEST_F(SpillFileS3Test, HeartbeatObjectIsWritten) {
    config::spill_s3_heartbeat_interval_second = 3600;
    _create_manager();
    _manager->stop();
    _manager->gc(1000);
    const std::string heartbeat_key = spill_root() + "/_heartbeat";
    ASSERT_EQ(mock_store().keys_with_prefix(kBucket, heartbeat_key),
              std::vector<std::string> {heartbeat_key});
    ASSERT_EQ(_object(heartbeat_key).empty(), false);
    ASSERT_FALSE(_manager->remote_startup_cleanup_pending());

    // Not due again within the interval.
    int64_t puts = mock_store().put_requests;
    _manager->gc(1000);
    ASSERT_EQ(mock_store().put_requests, puts);
    ASSERT_EQ(mock_store().keys_with_prefix(kBucket, heartbeat_key).size(), 1);
}

TEST_F(SpillFileS3Test, StartupCleanupDeletesResidueOfPreviousProcess) {
    // The test drives the GC rounds itself.
    const auto saved_gc_interval = config::spill_gc_interval_ms;
    config::spill_gc_interval_ms = 3600 * 1000;
    Defer restore_gc_interval {[&]() { config::spill_gc_interval_ms = saved_gc_interval; }};

    mock_store().put_raw(kBucket, spill_root() + "/query_old/sort-1-0-1/0", "old");
    mock_store().put_raw(kBucket, spill_root() + "/query_old/sort-1-0-1/1", "old");
    mock_store().put_raw(kBucket, spill_root() + "/query_old2/agg-1-0-1/0", "old");
    mock_store().put_raw(kBucket, spill_root() + "/query_live/sort-1-0-1/0", "live");
    mock_store().put_raw(kBucket,
                         fmt::format("{}/spill/10.0.0.2_9050/q/sort-1-0-1/0", kVaultPrefix),
                         "other host");
    // Another BE on the same host: different heartbeat port, different directory.
    mock_store().put_raw(kBucket,
                         fmt::format("{}/spill/10.0.0.1_9051/q/sort-1-0-1/0", kVaultPrefix),
                         "other BE on this host");

    _create_manager();
    // A query of this process registered its directory before writing it.
    _manager->register_remote_query_dir("query_live");
    ASSERT_TRUE(_manager->remote_startup_cleanup_pending());

    // First round: one listing, one directory deleted.
    _manager->gc(1000);
    ASSERT_TRUE(_manager->remote_startup_cleanup_pending());
    ASSERT_EQ(mock_store().root_list_requests, 1);
    // A directory that shows up after the listing is not residue of the previous process.
    mock_store().put_raw(kBucket, spill_root() + "/query_new/sort-1-0-1/0", "new");

    for (int i = 0; i < 10 && _manager->remote_startup_cleanup_pending(); ++i) {
        _manager->gc(1000);
    }
    ASSERT_FALSE(_manager->remote_startup_cleanup_pending());
    ASSERT_EQ(mock_store().root_list_requests, 1);
    ASSERT_TRUE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_old/").empty());
    ASSERT_TRUE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_old2/").empty());
    ASSERT_EQ(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_live/").size(), 1);
    ASSERT_EQ(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_new/").size(), 1);
    ASSERT_EQ(
            mock_store()
                    .keys_with_prefix(kBucket, fmt::format("{}/spill/10.0.0.2_9050/", kVaultPrefix))
                    .size(),
            1);
    ASSERT_EQ(
            mock_store()
                    .keys_with_prefix(kBucket, fmt::format("{}/spill/10.0.0.1_9051/", kVaultPrefix))
                    .size(),
            1);
    ASSERT_EQ(mock_store().put_requests, 0);

    // Further rounds do nothing.
    _manager->gc(1000);
    ASSERT_EQ(mock_store().root_list_requests, 1);
}

// Writing registers the query directory before the first object, so the startup cleanup
// leaves a running query's data alone even when the query spills before the first GC round.
TEST_F(SpillFileS3Test, StartupCleanupKeepsDirectoryOfRunningQuery) {
    const auto saved_gc_interval = config::spill_gc_interval_ms;
    config::spill_gc_interval_ms = 3600 * 1000;
    Defer restore_gc_interval {[&]() { config::spill_gc_interval_ms = saved_gc_interval; }};

    _create_manager();
    std::mt19937 rng(7);
    Status st;
    auto spill_file =
            _write_blocks("query_running/sort-1-0-1", {_random_string_block(rng, 64, 200)}, &st);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_running/").empty());
    for (int i = 0; i < 10 && _manager->remote_startup_cleanup_pending(); ++i) {
        _manager->gc(1000);
    }
    ASSERT_FALSE(_manager->remote_startup_cleanup_pending());
    ASSERT_FALSE(mock_store().keys_with_prefix(kBucket, spill_root() + "/query_running/").empty());
    ASSERT_TRUE(spill_file->ready_for_reading());
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
    // Larger than the budget: the writer must block in the gate of the third buffer, not in
    // the synchronous close of a part that fits the budget exactly.
    config::spill_file_part_size_bytes = 1024 * 1024;
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

TEST_F(SpillFileS3Test, UploadBudgetRejectsCancelledQueryImmediately) {
    // A cancelled query must not be admitted even when the budget is free (fast path)...
    SpillRemoteUploadBudget budget(64 * 1024);
    std::atomic<bool> cancelled {true};
    int64_t wait_ns = 0;
    auto st = budget.acquire(
            8 * 1024, [&]() { return cancelled.load(); }, &wait_ns);
    ASSERT_TRUE(st.is<ErrorCode::CANCELLED>()) << st;
    ASSERT_EQ(budget.inflight_bytes(), 0);
    ASSERT_EQ(budget.total_acquired_bytes(), 0);

    // ...nor when it was waiting and the budget became available at the moment of the cancel.
    cancelled = false;
    ASSERT_TRUE(budget.acquire(64 * 1024, nullptr, nullptr).ok()); // budget full
    std::atomic<bool> waiter_done {false};
    Status waiter_status;
    ScopedThread waiter([&]() {
        waiter_status = budget.acquire(
                8 * 1024, [&]() { return cancelled.load(); }, nullptr);
        waiter_done = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_FALSE(waiter_done.load());
    cancelled = true;
    budget.release(64 * 1024); // wakes the waiter with room available and the query cancelled
    for (int i = 0; i < 200 && !waiter_done.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    waiter.join();
    ASSERT_TRUE(waiter_done.load());
    ASSERT_TRUE(waiter_status.is<ErrorCode::CANCELLED>()) << waiter_status;
    ASSERT_EQ(budget.inflight_bytes(), 0);
    ASSERT_EQ(budget.total_acquired_bytes(), budget.total_released_bytes());
}

TEST_F(SpillFileS3Test, PartialBufferIsChargedAtCapacity) {
    // The last buffer of a part is partially filled but keeps its full allocation; the budget
    // must see the allocation, otherwise many small parts could hold unbounded memory.
    config::spill_s3_max_inflight_upload_bytes = 64 * 1024;
    _create_manager();
    auto* budget = _manager->remote_upload_budget();
    std::mt19937 rng(21);
    std::vector<Block> blocks {_random_string_block(rng, 8, 100)}; // well below 8 KiB

    mock_store().set_block_uploads(true);
    Status write_status;
    ScopedThread writer_thread(
            [&]() { _write_blocks("query_12/agg-1-0-1", blocks, &write_status); });
    Defer release_writer {[&]() {
        mock_store().set_block_uploads(false);
        _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test teardown"));
    }};
    for (int i = 0; i < 250 && budget->inflight_bytes() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    // One partial buffer in flight: charged at s3_write_buffer_size, not at its payload.
    ASSERT_EQ(budget->inflight_bytes(), config::s3_write_buffer_size);
    mock_store().set_block_uploads(false);
    writer_thread.join();
    ASSERT_TRUE(write_status.ok()) << write_status;
    ASSERT_EQ(budget->inflight_bytes(), 0);
    ASSERT_EQ(budget->total_acquired_bytes(), config::s3_write_buffer_size);
    ASSERT_EQ(budget->total_acquired_bytes(), budget->total_released_bytes());
}

// The done callback must report exactly what the gate was charged, for every buffer, on the
// PutObject path (partial buffer), the multipart path (partial last buffer) and the empty
// object created by close() without any append.
TEST_F(SpillFileS3Test, GateAndDoneCallbackReportTheSameCapacity) {
    _create_manager();
    struct Recorder {
        std::mutex mutex;
        std::vector<size_t> gate;
        std::vector<size_t> done;
    };
    auto run = [&](const std::string& name, size_t payload_bytes, size_t expected_buffers) {
        Recorder rec;
        io::FileWriterOptions opts;
        opts.write_file_cache = false;
        opts.upload_submit_gate = [&rec](size_t bytes) -> Status {
            std::lock_guard lock(rec.mutex);
            rec.gate.push_back(bytes);
            return Status::OK();
        };
        opts.upload_done_callback = [&rec](size_t bytes) {
            std::lock_guard lock(rec.mutex);
            rec.done.push_back(bytes);
        };
        io::FileWriterPtr writer;
        ASSERT_TRUE(_s3_fs->create_file(fmt::format("hooks/{}", name), &writer, &opts).ok());
        if (payload_bytes > 0) {
            std::string payload(payload_bytes, 'x');
            ASSERT_TRUE(writer->append(Slice(payload)).ok());
        }
        ASSERT_TRUE(writer->close().ok());
        std::lock_guard lock(rec.mutex);
        const size_t capacity = static_cast<size_t>(config::s3_write_buffer_size);
        ASSERT_EQ(rec.gate.size(), expected_buffers) << name;
        ASSERT_EQ(rec.done.size(), expected_buffers) << name;
        for (size_t b : rec.gate) {
            EXPECT_EQ(b, capacity) << name;
        }
        for (size_t b : rec.done) {
            EXPECT_EQ(b, capacity) << name;
        }
    };
    // Partial buffer -> one PutObject.
    run("partial", 100, 1);
    // 2.5 buffers -> multipart with a partial last buffer.
    run("multipart", static_cast<size_t>(config::s3_write_buffer_size) * 5 / 2, 3);
    ASSERT_EQ(mock_store().create_multipart_requests, 1);
    // No append at all -> empty object, still one gate + one callback.
    run("empty", 0, 1);
    ASSERT_EQ(mock_store().keys_with_prefix(kBucket, kVaultPrefix + std::string("/hooks/")).size(),
              3);
}

// A gate that refuses the buffer of an empty object fails the writer: no object is created and
// the done callback is not invoked for a buffer that never passed the gate.
TEST_F(SpillFileS3Test, EmptyObjectRefusedByGateIsNotCreated) {
    _create_manager();
    std::atomic<int> gate_calls {0};
    std::atomic<int> done_calls {0};
    io::FileWriterOptions opts;
    opts.write_file_cache = false;
    opts.upload_submit_gate = [&](size_t) -> Status {
        ++gate_calls;
        return Status::Cancelled("refused");
    };
    opts.upload_done_callback = [&](size_t) { ++done_calls; };
    io::FileWriterPtr writer;
    ASSERT_TRUE(_s3_fs->create_file("hooks/refused", &writer, &opts).ok());
    const int64_t puts_before = mock_store().put_requests;
    auto st = writer->close();
    ASSERT_TRUE(st.is<ErrorCode::CANCELLED>()) << st;
    ASSERT_EQ(gate_calls, 1);
    ASSERT_EQ(done_calls, 0);
    ASSERT_EQ(mock_store().put_requests, puts_before);
    ASSERT_EQ(mock_store().create_multipart_requests, 0);
    ASSERT_TRUE(
            mock_store().keys_with_prefix(kBucket, kVaultPrefix + std::string("/hooks/")).empty());
}

// A writer whose first append failed before any byte was counted (CreateMultipartUpload
// refused) creates no object when closed, and close() reports the failure.
TEST_F(SpillFileS3Test, FailedAppendThenCloseCreatesNoObject) {
    _create_manager();
    std::atomic<int> gate_calls {0};
    io::FileWriterOptions opts;
    opts.write_file_cache = false;
    opts.upload_submit_gate = [&](size_t) -> Status {
        ++gate_calls;
        return Status::OK();
    };
    io::FileWriterPtr writer;
    ASSERT_TRUE(_s3_fs->create_file("hooks/failed_append", &writer, &opts).ok());
    mock_store().fail_create_multipart = true;
    Defer restore {[&]() { mock_store().fail_create_multipart = false; }};
    std::string payload(static_cast<size_t>(config::s3_write_buffer_size), 'x');
    ASSERT_FALSE(writer->append(Slice(payload)).ok());
    ASSERT_EQ(gate_calls, 1);
    const int64_t puts_before = mock_store().put_requests;
    ASSERT_FALSE(writer->close().ok());
    ASSERT_EQ(mock_store().put_requests, puts_before);
    ASSERT_TRUE(
            mock_store().keys_with_prefix(kBucket, kVaultPrefix + std::string("/hooks/")).empty());
}

// A query cancelled while a multipart upload is open: the last buffer is refused by the gate,
// the upload is aborted and nothing is left behind.
TEST_F(SpillFileS3Test, CancelledQueryCloseAbortsOpenMultipart) {
    config::spill_file_part_size_bytes = 1024 * 1024;
    _create_manager();
    auto* budget = _manager->remote_upload_budget();
    std::mt19937 rng(43);
    SpillFileSPtr spill_file;
    ASSERT_TRUE(_manager->create_spill_file("query_14/sort-1-0-1", spill_file).ok());
    SpillFileWriterSPtr writer;
    ASSERT_TRUE(spill_file->create_writer(_runtime_state.get(), _profile.get(), writer).ok());
    // Several 8 KiB buffers: the multipart upload is created and parts are uploaded before
    // close().
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(writer->write_block(_runtime_state.get(), _random_string_block(rng, 100, 100))
                            .ok());
    }
    ASSERT_EQ(mock_store().create_multipart_requests, 1);
    ASSERT_GT(mock_store().put_requests, 0);
    // Let the submitted uploads finish so that the count below is stable: only the pending
    // partial buffer is left, and that one must not be uploaded after the cancellation.
    for (int i = 0; i < 250 && budget->inflight_bytes() > 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(budget->inflight_bytes(), 0);
    const int64_t puts_before_close = mock_store().put_requests;

    _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test cancel"));
    auto st = writer->close();
    ASSERT_TRUE(st.is<ErrorCode::CANCELLED>()) << st;
    ASSERT_EQ(mock_store().put_requests, puts_before_close);
    ASSERT_EQ(mock_store().abort_multipart_requests, 1);
    ASSERT_EQ(budget->inflight_bytes(), 0);
    ASSERT_EQ(budget->total_acquired_bytes(), budget->total_released_bytes());
    ASSERT_FALSE(spill_file->ready_for_reading());
    writer.reset();
    spill_file.reset();
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 0);
}

// The scenario behind the fast-path cancellation check: a query cancelled before close() must
// not start a new upload for its last buffer; the part is refused, nothing is put, the budget
// stays balanced.
TEST_F(SpillFileS3Test, CancelledQueryCloseIssuesNoUpload) {
    config::spill_file_part_size_bytes = 1024 * 1024;
    _create_manager();
    auto* budget = _manager->remote_upload_budget();
    std::mt19937 rng(37);
    SpillFileSPtr spill_file;
    ASSERT_TRUE(_manager->create_spill_file("query_13/sort-1-0-1", spill_file).ok());
    SpillFileWriterSPtr writer;
    ASSERT_TRUE(spill_file->create_writer(_runtime_state.get(), _profile.get(), writer).ok());
    // Well below one 8 KiB buffer: nothing is submitted before close().
    ASSERT_TRUE(writer->write_block(_runtime_state.get(), _random_string_block(rng, 8, 100)).ok());
    ASSERT_EQ(mock_store().put_requests, 0);

    _runtime_state->get_query_ctx()->cancel(Status::Cancelled("test cancel"));
    auto st = writer->close();
    ASSERT_TRUE(st.is<ErrorCode::CANCELLED>()) << st;
    ASSERT_EQ(mock_store().put_requests, 0);
    ASSERT_EQ(mock_store().create_multipart_requests, 0);
    ASSERT_EQ(budget->inflight_bytes(), 0);
    ASSERT_EQ(budget->total_acquired_bytes(), 0);
    ASSERT_FALSE(spill_file->ready_for_reading());
    writer.reset();
    spill_file.reset();
    ASSERT_EQ(_data_dir->get_spill_data_bytes(), 0);
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

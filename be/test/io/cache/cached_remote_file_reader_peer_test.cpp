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

#include <brpc/details/server_private_accessor.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/iobuf.h>
#include <gen_cpp/internal_service.pb.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "block_file_cache_test_common.h"
#include "cloud/cloud_warm_up_manager.h"
#include "cloud/config.h"
#include "exec/pipeline/task_scheduler.h"
#include "exec/scan/scanner_scheduler.h"
#include "io/cache/peer_file_cache_reader.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_group/workload_group_metrics.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/brpc_client_cache.h"
#include "util/debug/leak_annotations.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris {
Status test_handle_peer_file_cache_block_request(const PFetchPeerDataRequest* request,
                                                 PFetchPeerDataResponse* response,
                                                 brpc::Controller* cntl);
} // namespace doris

namespace doris::io {

namespace {

constexpr size_t kPeerTestBlockSize = 4;
constexpr int64_t kPeerTestTabletId = 10001;

std::shared_ptr<WorkloadGroup> create_peer_test_workload_group(int remote_read_bytes_per_second) {
    static std::atomic<uint64_t> next_workload_group_id {90000};
    const uint64_t workload_group_id = next_workload_group_id.fetch_add(1);
    WorkloadGroupInfo info {.id = workload_group_id,
                            .name = "peer_read_test_wg_" + std::to_string(workload_group_id),
                            .version = 1,
                            .remote_read_bytes_per_second = remote_read_bytes_per_second};
    auto workload_group = std::make_shared<WorkloadGroup>(info);
    workload_group->upsert_scan_io_throttle(&info);
    return workload_group;
}

std::shared_ptr<ResourceContext> create_peer_test_resource_context(
        const std::shared_ptr<MemTrackerLimiter>& mem_tracker,
        const std::shared_ptr<WorkloadGroup>& workload_group) {
    auto resource_context = ResourceContext::create_shared();
    resource_context->memory_context()->set_mem_tracker(mem_tracker);
    resource_context->task_controller()->set_task_id(TUniqueId());
    resource_context->set_workload_group(workload_group);
    return resource_context;
}

FileCacheSettings create_peer_test_settings(size_t block_size, std::string storage = "disk") {
    FileCacheSettings settings;
    settings.query_queue_size = 64;
    settings.query_queue_elements = 16;
    settings.index_queue_size = 64;
    settings.index_queue_elements = 16;
    settings.disposable_queue_size = 64;
    settings.disposable_queue_elements = 16;
    settings.capacity = 64;
    settings.max_file_block_size = block_size;
    settings.max_query_cache_size = 0;
    settings.storage = std::move(storage);
    return settings;
}

void clear_cached_remote_reader_factory() {
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

BlockFileCache* create_peer_test_cache(const fs::path& cache_path, size_t block_size,
                                       std::string storage = "disk") {
    std::error_code ec;
    fs::remove_all(cache_path, ec);
    if (storage != "memory") {
        fs::create_directories(cache_path);
    }
    const std::string cache_base_path = storage == "memory" ? "memory" : cache_path.string();
    auto st = FileCacheFactory::instance()->create_file_cache(
            cache_base_path, create_peer_test_settings(block_size, std::move(storage)));
    CHECK(st.ok()) << st;
    auto* cache = FileCacheFactory::instance()->_path_to_cache[cache_base_path];
    CHECK(cache != nullptr) << cache_base_path;
    wait_until_cache_ready(*cache);
    return cache;
}

fs::path create_peer_test_file(const std::string& name, std::string_view content) {
    fs::path file_path = caches_dir / name;
    std::ofstream out(file_path, std::ios::binary | std::ios::trunc);
    CHECK(out.is_open()) << file_path;
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
    out.close();
    return file_path;
}

void populate_cache_block_with_content(BlockFileCache* cache, const fs::path& file_path,
                                       size_t offset, size_t size, std::string_view content) {
    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    auto hash = BlockFileCache::hash(file_path.filename().native());
    auto holder = cache->get_or_set(hash, offset, size, context);
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    ASSERT_EQ(blocks[0]->state(), FileBlock::State::EMPTY);
    ASSERT_EQ(blocks[0]->get_or_set_downloader(), FileBlock::get_caller_id());
    ASSERT_TRUE(blocks[0]->append(Slice(content.data() + offset, size)).ok());
    ASSERT_TRUE(blocks[0]->finalize().ok());
}

FileBlockSPtr create_manual_peer_test_block(const fs::path& file_path, size_t offset, size_t size,
                                            BlockFileCache* cache = nullptr) {
    FileCacheKey key;
    key.hash = BlockFileCache::hash(file_path.filename().native());
    key.offset = offset;
    key.meta = KeyMeta {.expiration_time = 0, .type = FileCacheType::NORMAL};
    return std::make_shared<FileBlock>(key, size, cache, FileBlock::State::EMPTY);
}

butil::EndPoint start_peer_test_server(brpc::Server* server, google::protobuf::Service* service) {
    CHECK(server != nullptr);
    CHECK(service != nullptr);
    CHECK_EQ(server->AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    brpc::ServerOptions options;
    options.num_threads = 1;
    {
        // BRPC leaves a small shutdown-only allocation from StartInternal() in this UT harness.
        // Disable LSAN only for server bootstrap so the peer-path assertions stay meaningful.
        doris::debug::ScopedLSANDisabler lsan_disabler;
        CHECK_EQ(server->Start(0, &options), 0);
    }

    auto* acceptor = brpc::ServerPrivateAccessor(server).acceptor();
    if (acceptor != nullptr) {
        ANNOTATE_LEAKING_OBJECT_PTR(acceptor);
    }
    return server->listen_address();
}

void stop_peer_test_server(brpc::Server* server) {
    ASSERT_NE(server, nullptr);
    ASSERT_EQ(server->Stop(1000), 0);
    ASSERT_EQ(server->Join(), 0);
    server->ClearServices();
}

struct MockPeerCacheServiceOptions {
    bool use_attachment = false;
    bool drop_last_block = false;
    bool reverse_response_order = false;
    bool duplicate_first_block = false;
    bool negative_block_offset = false;
    bool fail_status = false;
    bool append_trailing_attachment = false;
    size_t truncate_trailing_attachment_bytes = 0;
    int64_t response_offset_delta = 0;
    // Return TStatusCode::NOT_FOUND to simulate a server-side cache miss.
    // Distinct from fail_status (INTERNAL_ERROR): the client must rotate the candidate
    // on NOT_FOUND, not increment the RPC-failure eviction counter.
    bool cache_miss_status = false;
};

class MockPeerCacheService : public PBackendService {
public:
    explicit MockPeerCacheService(std::string content, MockPeerCacheServiceOptions options = {})
            : _content(std::move(content)), _options(options) {}

    void fetch_peer_data(google::protobuf::RpcController* controller,
                         const PFetchPeerDataRequest* request, PFetchPeerDataResponse* response,
                         google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        ++rpc_count;
        request_block_count = request->cache_req_size();
        support_attachment = request->has_support_attachment() && request->support_attachment();
        request_fill = request->has_request_cache_fill() && request->request_cache_fill();
        last_fill_tablet_id = request->has_fill_tablet_id() ? request->fill_tablet_id() : -1;
        {
            std::lock_guard lock(_request_mu);
            last_fill_remote_path =
                    request->has_fill_remote_path() ? request->fill_remote_path() : "";
            last_fill_resource_id =
                    request->has_fill_resource_id() ? request->fill_resource_id() : "";
        }

        if (_options.fail_status) {
            response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
            return;
        }
        if (_options.cache_miss_status) {
            // Simulate server-side cache miss (EMPTY block, no fill):
            // server returns NOT_FOUND so client rotates the candidate instead of evicting.
            response->mutable_status()->add_error_msgs("cache block not downloaded");
            response->mutable_status()->set_status_code(TStatusCode::NOT_FOUND);
            return;
        }
        response->mutable_status()->set_status_code(TStatusCode::OK);
        EXPECT_EQ(request->type(), PFetchPeerDataRequest_Type_PEER_FILE_CACHE_BLOCK);
        EXPECT_FALSE(request->path().empty());
        const bool use_attachment = _options.use_attachment && support_attachment.load();
        response->set_data_in_attachment(use_attachment);

        std::vector<int> request_indexes(request->cache_req_size());
        for (int idx = 0; idx < request->cache_req_size(); ++idx) {
            request_indexes[idx] = idx;
        }
        if (_options.reverse_response_order) {
            std::reverse(request_indexes.begin(), request_indexes.end());
        }

        const size_t file_size =
                request->has_file_size()
                        ? static_cast<size_t>(std::max<int64_t>(0, request->file_size()))
                        : _content.size();
        auto append_response = [&](int64_t response_block_offset, size_t block_offset,
                                   size_t readable_size) {
            auto* data = response->add_datas();
            data->set_block_offset(response_block_offset);
            data->set_block_size(readable_size);
            if (readable_size == 0) {
                return;
            }
            if (use_attachment) {
                cntl->response_attachment().append(_content.data() + block_offset, readable_size);
            } else {
                data->set_data(_content.substr(block_offset, readable_size));
            }
        };
        for (int order_idx = 0; order_idx < request->cache_req_size(); ++order_idx) {
            const int idx = request_indexes[order_idx];
            const auto& block_req = request->cache_req(idx);
            EXPECT_GE(block_req.block_offset(), 0);
            EXPECT_GE(block_req.block_size(), 0);
            size_t block_offset = static_cast<size_t>(block_req.block_offset());
            size_t block_size = static_cast<size_t>(block_req.block_size());
            const size_t readable_size =
                    block_offset >= file_size ? 0 : std::min(block_size, file_size - block_offset);
            if (readable_size > 0) {
                ASSERT_LE(block_offset + readable_size, _content.size());
            }
            if (_options.drop_last_block && idx == request->cache_req_size() - 1) {
                continue;
            }

            const int64_t response_block_offset =
                    _options.negative_block_offset
                            ? -1
                            : block_req.block_offset() + _options.response_offset_delta;
            append_response(response_block_offset, block_offset, readable_size);
        }
        if (_options.duplicate_first_block && request->cache_req_size() > 0) {
            const auto& first_req = request->cache_req(0);
            EXPECT_GE(first_req.block_offset(), 0);
            EXPECT_GE(first_req.block_size(), 0);
            const size_t block_offset = static_cast<size_t>(first_req.block_offset());
            const size_t block_size = static_cast<size_t>(first_req.block_size());
            const size_t readable_size =
                    block_offset >= file_size ? 0 : std::min(block_size, file_size - block_offset);
            if (readable_size > 0) {
                ASSERT_LE(block_offset + readable_size, _content.size());
            }
            append_response(first_req.block_offset(), block_offset, readable_size);
        }
        if (use_attachment && _options.append_trailing_attachment) {
            cntl->response_attachment().append("x", 1);
        }
        if (use_attachment && _options.truncate_trailing_attachment_bytes != 0) {
            const size_t attachment_size = cntl->response_attachment().length();
            ASSERT_GE(attachment_size, _options.truncate_trailing_attachment_bytes);
            butil::IOBuf trimmed_attachment;
            const size_t kept_size = attachment_size - _options.truncate_trailing_attachment_bytes;
            ASSERT_EQ(cntl->response_attachment().cutn(&trimmed_attachment, kept_size), kept_size);
            cntl->response_attachment().swap(trimmed_attachment);
        }
    }

    std::atomic<int> rpc_count {0};
    std::atomic<int> request_block_count {0};
    std::atomic<bool> support_attachment {false};
    std::atomic<bool> request_fill {false};
    std::atomic<int64_t> last_fill_tablet_id {-1};
    std::string get_last_fill_remote_path() const {
        std::lock_guard lock(_request_mu);
        return last_fill_remote_path;
    }
    std::string get_last_fill_resource_id() const {
        std::lock_guard lock(_request_mu);
        return last_fill_resource_id;
    }

private:
    std::string _content;
    MockPeerCacheServiceOptions _options;
    mutable std::mutex _request_mu;
    std::string last_fill_remote_path;
    std::string last_fill_resource_id;
};

class InspectingRemoteFileReader final : public FileReader {
public:
    InspectingRemoteFileReader(std::string content, Path path,
                               std::shared_ptr<WorkloadGroup> expected_workload_group)
            : _content(std::move(content)),
              _path(std::move(path)),
              _expected_workload_group(std::move(expected_workload_group)) {}

    void set_expected_thread_id(std::thread::id thread_id) { _expected_thread_id = thread_id; }

    bool observed_expected_workload_group() const {
        return _observed_expected_workload_group.load(std::memory_order_acquire);
    }

    bool observed_non_orphan_tracker() const {
        return _observed_non_orphan_tracker.load(std::memory_order_acquire);
    }

    bool ran_on_expected_thread() const {
        return _ran_on_expected_thread.load(std::memory_order_acquire);
    }

    int read_call_count() const { return _read_call_count.load(std::memory_order_acquire); }

    int64_t throttle_wait_ms() const { return _throttle_wait_ms.load(std::memory_order_acquire); }

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const Path& path() const override { return _path; }

    size_t size() const override { return _content.size(); }

    bool closed() const override { return _closed; }

    int64_t mtime() const override { return 0; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* /*io_ctx*/) override {
        ++_read_call_count;

        auto* current_thread_context = thread_context();
        std::shared_ptr<WorkloadGroup> workload_group =
                current_thread_context->resource_ctx()->workload_group();
        _observed_expected_workload_group.store(
                workload_group != nullptr && workload_group.get() == _expected_workload_group.get(),
                std::memory_order_release);
        _observed_non_orphan_tracker.store(
                current_thread_context->thread_mem_tracker_mgr->limiter_mem_tracker()->label() !=
                        "Orphan",
                std::memory_order_release);
        _ran_on_expected_thread.store(std::this_thread::get_id() == _expected_thread_id,
                                      std::memory_order_release);

        if (offset > _content.size()) {
            return Status::InternalError("offset exceeds file size(offset: {}, file size: {})",
                                         offset, _content.size());
        }

        const size_t readable_size = std::min(result.size, _content.size() - offset);
        const auto start = std::chrono::steady_clock::now();
        LIMIT_REMOTE_SCAN_IO(bytes_read);
        std::copy_n(_content.data() + offset, readable_size, result.data);
        *bytes_read = readable_size;
        _throttle_wait_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - start)
                                        .count(),
                                std::memory_order_release);
        return Status::OK();
    }

private:
    std::string _content;
    Path _path;
    std::shared_ptr<WorkloadGroup> _expected_workload_group;
    std::thread::id _expected_thread_id;
    std::atomic<bool> _observed_expected_workload_group {false};
    std::atomic<bool> _observed_non_orphan_tracker {false};
    std::atomic<bool> _ran_on_expected_thread {false};
    std::atomic<int> _read_call_count {0};
    std::atomic<int64_t> _throttle_wait_ms {0};
    bool _closed = false;
};

class RealPeerCacheService : public PBackendService {
public:
    void fetch_peer_data(google::protobuf::RpcController* controller,
                         const PFetchPeerDataRequest* request, PFetchPeerDataResponse* response,
                         google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        ++rpc_count;
        request_block_count = request->cache_req_size();
        support_attachment = request->has_support_attachment() && request->support_attachment();
        response->mutable_status()->set_status_code(TStatusCode::OK);
        std::thread worker([&]() {
            auto st = doris::test_handle_peer_file_cache_block_request(request, response, cntl);
            if (!st.ok()) {
                response->mutable_status()->clear_error_msgs();
                response->mutable_status()->add_error_msgs(st.to_string());
                response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
            }
        });
        worker.join();
    }

    std::atomic<int> rpc_count {0};
    std::atomic<int> request_block_count {0};
    std::atomic<bool> support_attachment {false};
};

class SlowFailingPeerCacheService : public PBackendService {
public:
    explicit SlowFailingPeerCacheService(int delay_ms) : _delay_ms(delay_ms) {}

    void fetch_peer_data(google::protobuf::RpcController* /*controller*/,
                         const PFetchPeerDataRequest* request, PFetchPeerDataResponse* response,
                         google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        ++rpc_count;
        request_block_count = request->cache_req_size();
        started.store(true, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(_delay_ms));
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        finished.store(true, std::memory_order_release);
    }

    std::atomic<int> rpc_count {0};
    std::atomic<int> request_block_count {0};
    std::atomic<bool> started {false};
    std::atomic<bool> finished {false};

private:
    int _delay_ms;
};

class CachedRemoteFileReaderPeerTest : public BlockFileCacheTest {
protected:
    void SetUp() override {
        _old_deploy_mode = config::deploy_mode;
        _old_cloud_unique_id = config::cloud_unique_id;
        _old_enable_cache_read_from_peer = config::enable_cache_read_from_peer;
        _old_enable_debug_points = config::enable_debug_points;
        _old_file_cache_each_block_size = config::file_cache_each_block_size;
        _old_internal_client_cache = ExecEnv::GetInstance()->_internal_client_cache;

        config::deploy_mode = "cloud";
        config::cloud_unique_id.clear();
        config::enable_cache_read_from_peer = true;
        config::enable_debug_points = true;
        config::file_cache_each_block_size = kPeerTestBlockSize;
        DebugPoints::instance()->clear();

        if (ExecEnv::GetInstance()->_internal_client_cache == nullptr) {
            ExecEnv::GetInstance()->_internal_client_cache =
                    new BrpcClientCache<PBackendService_Stub>();
            _owned_internal_client_cache = true;
        }
    }

    void TearDown() override {
        DebugPoints::instance()->clear();
        clear_cached_remote_reader_factory();

        if (_owned_internal_client_cache) {
            delete ExecEnv::GetInstance()->_internal_client_cache;
            ExecEnv::GetInstance()->_internal_client_cache = nullptr;
        } else {
            ExecEnv::GetInstance()->_internal_client_cache = _old_internal_client_cache;
        }

        config::deploy_mode = _old_deploy_mode;
        config::cloud_unique_id = _old_cloud_unique_id;
        config::enable_cache_read_from_peer = _old_enable_cache_read_from_peer;
        config::enable_debug_points = _old_enable_debug_points;
        config::file_cache_each_block_size = _old_file_cache_each_block_size;
    }

private:
    std::string _old_deploy_mode;
    std::string _old_cloud_unique_id;
    bool _old_enable_cache_read_from_peer = false;
    bool _old_enable_debug_points = false;
    int64_t _old_file_cache_each_block_size = 0;
    BrpcClientCache<PBackendService_Stub>* _old_internal_client_cache = nullptr;
    bool _owned_internal_client_cache = false;
};

} // namespace

TEST_F(CachedRemoteFileReaderPeerTest, read_at_uses_peer_cache_when_available) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_success.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_success_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 3);
    EXPECT_TRUE(service.support_attachment.load());
    EXPECT_EQ(cache_stats.num_local_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 0);
    EXPECT_EQ(cache_stats.num_peer_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 10);
    EXPECT_EQ(cache_stats.num_remote_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 0);
}

TEST_F(CachedRemoteFileReaderPeerTest, read_at_peer_dryrun_downloads_cache_without_copying_result) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_dryrun.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_dryrun_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string dryrun_buffer(10, '#');
    size_t dryrun_bytes_read = 0;
    IOContext dryrun_ctx;
    dryrun_ctx.is_dryrun = true;

    ASSERT_TRUE(reader.read_at(1, Slice(dryrun_buffer.data(), dryrun_buffer.size()),
                               &dryrun_bytes_read, &dryrun_ctx)
                        .ok());

    EXPECT_EQ(dryrun_buffer, std::string(10, '#'));
    EXPECT_EQ(dryrun_bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 3);
    EXPECT_TRUE(service.support_attachment.load());

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(cache_stats.num_local_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 10);
    EXPECT_EQ(cache_stats.num_peer_io_total, 0);
    EXPECT_EQ(cache_stats.num_remote_io_total, 0);
}

TEST_F(CachedRemoteFileReaderPeerTest, read_at_warmup_skips_peer_attempt) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_warmup_skip_peer.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_warmup_skip_peer_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    io_ctx.is_warmup = true;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 0);
    EXPECT_EQ(cache_stats.num_local_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 0);
    EXPECT_EQ(cache_stats.num_peer_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 0);
    EXPECT_EQ(cache_stats.num_remote_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 10);
}

TEST_F(CachedRemoteFileReaderPeerTest, read_at_bypass_peer_skips_peer_attempt) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_bypass_peer.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_bypass_peer_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = kCrossTabletId;
    opts.mtime = 1;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    io_ctx.bypass_peer_read = true;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 0);
    EXPECT_EQ(cache_stats.num_peer_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 0);
    EXPECT_EQ(cache_stats.num_remote_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 10);
}

TEST_F(CachedRemoteFileReaderPeerTest,
       peer_fill_request_waits_for_concurrent_download_beyond_block_wait_timeout) {
    const bool old_enable_file_cache = config::enable_file_cache;
    const int64_t old_block_wait_timeout_ms = config::block_cache_wait_timeout_ms;
    const int32_t old_peer_fill_timeout_ms = config::peer_server_cache_fill_timeout_ms;
    const bool old_enable_peer_server_cache_fill = config::enable_peer_server_cache_fill;
    Defer restore_config {[&]() {
        config::enable_file_cache = old_enable_file_cache;
        config::block_cache_wait_timeout_ms = old_block_wait_timeout_ms;
        config::peer_server_cache_fill_timeout_ms = old_peer_fill_timeout_ms;
        config::enable_peer_server_cache_fill = old_enable_peer_server_cache_fill;
    }};
    config::enable_file_cache = true;
    config::block_cache_wait_timeout_ms = 100;
    config::peer_server_cache_fill_timeout_ms = 1500;
    config::enable_peer_server_cache_fill = true;

    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_waits_for_downloading.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path =
            caches_dir / "cached_remote_reader_peer_waits_for_downloading_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);

    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    auto hash = BlockFileCache::hash(file_path.filename().native());
    auto holder = cache->get_or_set(hash, 0, kPeerTestBlockSize, context);
    auto cached_blocks = fromHolder(holder);
    ASSERT_EQ(cached_blocks.size(), 1);
    ASSERT_EQ(cached_blocks[0]->state(), FileBlock::State::EMPTY);
    ASSERT_EQ(cached_blocks[0]->get_or_set_downloader(), FileBlock::get_caller_id());

    PFetchPeerDataRequest request;
    request.set_type(PFetchPeerDataRequest_Type_PEER_FILE_CACHE_BLOCK);
    request.set_path(file_path.filename().native());
    request.set_file_size(static_cast<int64_t>(content.size()));
    request.set_support_attachment(false);
    request.set_request_cache_fill(true);
    request.set_fill_tablet_id(kCrossTabletId);
    request.set_fill_remote_path(file_path.string());
    request.set_fill_resource_id("peer_fill_resource");
    auto* cache_req = request.add_cache_req();
    cache_req->set_block_offset(0);
    cache_req->set_block_size(kPeerTestBlockSize);

    PFetchPeerDataResponse response;
    brpc::Controller cntl;
    Status request_status = Status::OK();
    std::thread request_thread([&]() {
        request_status =
                doris::test_handle_peer_file_cache_block_request(&request, &response, &cntl);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_TRUE(cached_blocks[0]->append(Slice(content.data(), kPeerTestBlockSize)).ok());
    ASSERT_TRUE(cached_blocks[0]->finalize().ok());

    request_thread.join();

    ASSERT_TRUE(request_status.ok()) << request_status;
    ASSERT_EQ(response.datas_size(), 1);
    EXPECT_EQ(response.datas(0).block_offset(), 0);
    EXPECT_EQ(response.datas(0).data(), content.substr(0, kPeerTestBlockSize));
}

TEST_F(CachedRemoteFileReaderPeerTest,
       peer_file_cache_reader_fill_request_includes_remote_path_and_resource_id) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_fill_request_fields.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_fill_request_fields_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);
    std::vector<FileBlockSPtr> blocks {
            create_manual_peer_test_block(file_path, 0, kPeerTestBlockSize)};

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;
    const std::string remote_path = "s3://bucket/data/12345/rowset_0.dat";
    const std::string resource_id = "peer_fill_resource";

    ASSERT_TRUE(peer_reader
                        .fetch_blocks(blocks, &result, content.size(), &io_ctx,
                                      /*request_fill=*/true, kCrossTabletId, remote_path,
                                      resource_id)
                        .ok());
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_TRUE(service.request_fill.load());
    EXPECT_EQ(service.last_fill_tablet_id.load(), kCrossTabletId);
    EXPECT_EQ(service.get_last_fill_remote_path(), remote_path);
    EXPECT_EQ(service.get_last_fill_resource_id(), resource_id);
}

TEST_F(CachedRemoteFileReaderPeerTest,
       peer_fill_returns_not_found_when_resource_id_is_unknown_without_tablet_lookup) {
    const bool old_enable_file_cache = config::enable_file_cache;
    const bool old_enable_peer_server_cache_fill = config::enable_peer_server_cache_fill;
    Defer restore_config {[&]() {
        config::enable_file_cache = old_enable_file_cache;
        config::enable_peer_server_cache_fill = old_enable_peer_server_cache_fill;
    }};
    config::enable_file_cache = true;
    config::enable_peer_server_cache_fill = true;

    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_fill_unknown_resource.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path =
            caches_dir / "cached_remote_reader_peer_fill_unknown_resource_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);

    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    auto hash = BlockFileCache::hash(file_path.filename().native());
    auto holder = cache->get_or_set(hash, 0, kPeerTestBlockSize, context);
    auto cached_blocks = fromHolder(holder);
    ASSERT_EQ(cached_blocks.size(), 1);
    ASSERT_EQ(cached_blocks[0]->state(), FileBlock::State::EMPTY);

    PFetchPeerDataRequest request;
    request.set_type(PFetchPeerDataRequest_Type_PEER_FILE_CACHE_BLOCK);
    request.set_path(file_path.filename().native());
    request.set_file_size(static_cast<int64_t>(content.size()));
    request.set_support_attachment(false);
    request.set_request_cache_fill(true);
    request.set_fill_remote_path("s3://bucket/data/12345/rowset_0.dat");
    request.set_fill_resource_id("missing_peer_fill_resource");
    auto* cache_req = request.add_cache_req();
    cache_req->set_block_offset(0);
    cache_req->set_block_size(kPeerTestBlockSize);

    PFetchPeerDataResponse response;
    brpc::Controller cntl;
    auto st = doris::test_handle_peer_file_cache_block_request(&request, &response, &cntl);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());
    EXPECT_EQ(response.status().status_code(), TStatusCode::NOT_FOUND);
    ASSERT_GE(response.status().error_msgs_size(), 1);
    EXPECT_NE(response.status().error_msgs(0).find("storage resource not found"),
              std::string::npos);
}

TEST_F(CachedRemoteFileReaderPeerTest, peer_file_cache_reader_clips_tail_block_by_file_size) {
    const std::string content = "abcdefghijklmn";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_tail_clip.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_tail_clip_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);

    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    auto hash = BlockFileCache::hash(file_path.filename().native());
    auto holder = cache->get_or_set(hash, 12, 4, context);
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    EXPECT_EQ(blocks[0]->range().left, 12);
    EXPECT_EQ(blocks[0]->range().right, 15);

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    ASSERT_TRUE(peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx).ok());

    ASSERT_EQ(result.chunks.size(), 1);
    EXPECT_EQ(result.bytes_read, 2);
    EXPECT_EQ(result.chunks[0].block_index, 0);
    EXPECT_EQ(result.chunks[0].block_offset, 12);
    std::string payload(2, '#');
    ASSERT_EQ(result.chunks[0].payload.copy_to(payload.data(), payload.size()), payload.size());
    EXPECT_EQ(payload, content.substr(12, 2));
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 1);
    EXPECT_TRUE(service.support_attachment.load());
}

TEST_F(CachedRemoteFileReaderPeerTest, peer_file_cache_reader_returns_ok_for_empty_blocks) {
    PeerFileCacheReader peer_reader(Path("unused"), true, "127.0.0.1", 0);
    PeerFetchResult result;
    IOContext io_ctx;

    ASSERT_TRUE(peer_reader.fetch_blocks({}, &result, 0, &io_ctx).ok());
    EXPECT_EQ(result.bytes_read, 0);
    EXPECT_TRUE(result.chunks.empty());
}

TEST_F(CachedRemoteFileReaderPeerTest, peer_file_cache_reader_rejects_non_doris_table_segments) {
    const std::string content = "abcd";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_not_supported.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_not_supported_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);
    std::vector<FileBlockSPtr> blocks {create_manual_peer_test_block(file_path, 0, 4)};

    PeerFileCacheReader peer_reader(Path(file_path.string()), false, "127.0.0.1", 0);
    PeerFetchResult result;
    IOContext io_ctx;

    auto st = peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.msg().find("supports doris table"), std::string::npos);
}

TEST_F(CachedRemoteFileReaderPeerTest,
       peer_file_cache_reader_returns_ok_when_all_blocks_are_clipped_by_file_size) {
    const std::string content = "abcdefghijklmn";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_all_clipped.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_all_clipped_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);
    std::vector<FileBlockSPtr> clipped_blocks {create_manual_peer_test_block(file_path, 16, 4)};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", 0);
    PeerFetchResult result;
    IOContext io_ctx;

    ASSERT_TRUE(peer_reader.fetch_blocks(clipped_blocks, &result, content.size(), &io_ctx).ok());

    EXPECT_EQ(result.bytes_read, 0);
    EXPECT_TRUE(result.chunks.empty());
}

TEST_F(CachedRemoteFileReaderPeerTest, peer_file_cache_reader_ignores_zero_sized_response_blocks) {
    const std::string content = "abcdefghijklmn";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_zero_sized_tail.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_zero_sized_tail_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);
    std::vector<FileBlockSPtr> blocks {create_manual_peer_test_block(file_path, 12, 4),
                                       create_manual_peer_test_block(file_path, 16, 4)};

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    ASSERT_TRUE(peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx).ok());

    ASSERT_EQ(result.chunks.size(), 1);
    EXPECT_EQ(result.bytes_read, 2);
    EXPECT_EQ(result.chunks[0].block_index, 0);
    EXPECT_EQ(result.chunks[0].block_offset, 12);
    std::string payload(2, '#');
    ASSERT_EQ(result.chunks[0].payload.copy_to(payload.data(), payload.size()), payload.size());
    EXPECT_EQ(payload, content.substr(12, 2));
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
}

TEST_F(CachedRemoteFileReaderPeerTest, peer_file_cache_reader_rejects_negative_block_metadata) {
    const std::string content = "abcdefgh";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_negative_metadata.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_negative_metadata_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);
    std::vector<FileBlockSPtr> blocks {create_manual_peer_test_block(file_path, 0, 4)};

    MockPeerCacheServiceOptions options;
    options.negative_block_offset = true;
    MockPeerCacheService service(content, options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    auto st = peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.msg().find("invalid block metadata"), std::string::npos);
}

TEST_F(CachedRemoteFileReaderPeerTest,
       peer_file_cache_reader_rejects_response_out_of_requested_ranges) {
    const std::string content = "abcdefgh";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_out_of_range.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_out_of_range_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);
    std::vector<FileBlockSPtr> blocks {create_manual_peer_test_block(file_path, 0, 4)};

    MockPeerCacheServiceOptions options;
    options.response_offset_delta = 1;
    MockPeerCacheService service(content, options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    auto st = peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.msg().find("out of requested ranges"), std::string::npos);
}

TEST_F(CachedRemoteFileReaderPeerTest, peer_file_cache_reader_rejects_duplicate_ranges) {
    const std::string content = "abcdefgh";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_duplicate_range.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_duplicate_range_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);
    std::vector<FileBlockSPtr> blocks {create_manual_peer_test_block(file_path, 0, 4)};

    MockPeerCacheServiceOptions options;
    options.duplicate_first_block = true;
    MockPeerCacheService service(content, options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    auto st = peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.msg().find("unexpected block range"), std::string::npos);
}

TEST_F(CachedRemoteFileReaderPeerTest, peer_file_cache_reader_propagates_peer_error_status) {
    const std::string content = "abcdefgh";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_error_status.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_error_status_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);
    std::vector<FileBlockSPtr> blocks {create_manual_peer_test_block(file_path, 0, 4)};

    MockPeerCacheServiceOptions options;
    options.fail_status = true;
    MockPeerCacheService service(content, options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    auto st = peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(service.rpc_count.load(), 1);
}

TEST_F(CachedRemoteFileReaderPeerTest,
       peer_file_cache_reader_reads_attachment_payload_from_cloud_internal_service_disk_cache) {
    const bool old_enable_file_cache = config::enable_file_cache;
    Defer restore_file_cache {[&]() { config::enable_file_cache = old_enable_file_cache; }};
    config::enable_file_cache = true;

    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_real_peer_attachment_disk.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_real_peer_attachment_disk_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);
    populate_cache_block_with_content(cache, file_path, 0, kPeerTestBlockSize, content);
    populate_cache_block_with_content(cache, file_path, 8, kPeerTestBlockSize, content);

    RealPeerCacheService service;
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    std::vector<FileBlockSPtr> blocks {
            create_manual_peer_test_block(file_path, 0, kPeerTestBlockSize),
            create_manual_peer_test_block(file_path, 8, kPeerTestBlockSize)};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    ASSERT_TRUE(peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx).ok());

    ASSERT_EQ(result.chunks.size(), 2);
    EXPECT_EQ(result.bytes_read, 8);
    EXPECT_EQ(result.chunks[0].block_index, 0);
    EXPECT_EQ(result.chunks[0].block_offset, 0);
    EXPECT_EQ(result.chunks[1].block_index, 1);
    EXPECT_EQ(result.chunks[1].block_offset, 8);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
    EXPECT_TRUE(service.support_attachment.load());

    std::string first(4, '#');
    ASSERT_EQ(result.chunks[0].payload.copy_to(first.data(), first.size()), first.size());
    EXPECT_EQ(first, content.substr(0, 4));

    std::string second(4, '#');
    ASSERT_EQ(result.chunks[1].payload.copy_to(second.data(), second.size()), second.size());
    EXPECT_EQ(second, content.substr(8, 4));
}

TEST_F(CachedRemoteFileReaderPeerTest,
       peer_file_cache_reader_reads_attachment_payload_from_cloud_internal_service_memory_cache) {
    const bool old_enable_file_cache = config::enable_file_cache;
    Defer restore_file_cache {[&]() { config::enable_file_cache = old_enable_file_cache; }};
    config::enable_file_cache = true;

    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_real_peer_attachment_memory.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path =
            caches_dir / "cached_remote_reader_real_peer_attachment_memory_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize, "memory");
    populate_cache_block_with_content(cache, file_path, 0, kPeerTestBlockSize, content);
    populate_cache_block_with_content(cache, file_path, 8, kPeerTestBlockSize, content);

    RealPeerCacheService service;
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    std::vector<FileBlockSPtr> blocks {
            create_manual_peer_test_block(file_path, 0, kPeerTestBlockSize),
            create_manual_peer_test_block(file_path, 8, kPeerTestBlockSize)};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    ASSERT_TRUE(peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx).ok());

    ASSERT_EQ(result.chunks.size(), 2);
    EXPECT_EQ(result.bytes_read, 8);
    EXPECT_EQ(result.chunks[0].block_index, 0);
    EXPECT_EQ(result.chunks[0].block_offset, 0);
    EXPECT_EQ(result.chunks[1].block_index, 1);
    EXPECT_EQ(result.chunks[1].block_offset, 8);

    std::string first(4, '#');
    ASSERT_EQ(result.chunks[0].payload.copy_to(first.data(), first.size()), first.size());
    EXPECT_EQ(first, content.substr(0, 4));

    std::string second(4, '#');
    ASSERT_EQ(result.chunks[1].payload.copy_to(second.data(), second.size()), second.size());
    EXPECT_EQ(second, content.substr(8, 4));
}

TEST_F(CachedRemoteFileReaderPeerTest,
       peer_file_cache_reader_accepts_split_attachment_ranges_from_cloud_internal_service) {
    const bool old_enable_file_cache = config::enable_file_cache;
    Defer restore_file_cache {[&]() { config::enable_file_cache = old_enable_file_cache; }};
    config::enable_file_cache = true;

    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_real_peer_attachment_split.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path =
            caches_dir / "cached_remote_reader_real_peer_attachment_split_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);
    populate_cache_block_with_content(cache, file_path, 0, kPeerTestBlockSize, content);
    populate_cache_block_with_content(cache, file_path, 4, kPeerTestBlockSize, content);

    RealPeerCacheService service;
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    std::vector<FileBlockSPtr> blocks {create_manual_peer_test_block(file_path, 0, 8)};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    ASSERT_TRUE(peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx).ok());

    ASSERT_EQ(result.chunks.size(), 2);
    EXPECT_EQ(result.bytes_read, 8);
    EXPECT_EQ(result.chunks[0].block_index, 0);
    EXPECT_EQ(result.chunks[0].block_offset, 0);
    EXPECT_EQ(result.chunks[1].block_index, 0);
    EXPECT_EQ(result.chunks[1].block_offset, 4);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 1);
    EXPECT_TRUE(service.support_attachment.load());

    std::string first(4, '#');
    ASSERT_EQ(result.chunks[0].payload.copy_to(first.data(), first.size()), first.size());
    EXPECT_EQ(first, content.substr(0, 4));

    std::string second(4, '#');
    ASSERT_EQ(result.chunks[1].payload.copy_to(second.data(), second.size()), second.size());
    EXPECT_EQ(second, content.substr(4, 4));
}

TEST_F(CachedRemoteFileReaderPeerTest,
       peer_file_cache_reader_clips_tail_request_before_cloud_internal_service_get_or_set) {
    const bool old_enable_file_cache = config::enable_file_cache;
    Defer restore_file_cache {[&]() { config::enable_file_cache = old_enable_file_cache; }};
    config::enable_file_cache = true;

    const std::string content = "abcdefghijklmn";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_real_peer_tail_clip.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_real_peer_tail_clip_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);

    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    auto hash = BlockFileCache::hash(file_path.filename().native());
    auto holder = cache->get_or_set(hash, 12, 4, context);
    auto cached_blocks = fromHolder(holder);
    ASSERT_EQ(cached_blocks.size(), 1);
    ASSERT_EQ(cached_blocks[0]->state(), FileBlock::State::EMPTY);
    ASSERT_EQ(cached_blocks[0]->get_or_set_downloader(), FileBlock::get_caller_id());
    ASSERT_TRUE(cached_blocks[0]->append(Slice(content.data() + 12, 2)).ok());
    ASSERT_TRUE(cached_blocks[0]->finalize().ok());
    EXPECT_EQ(cached_blocks[0]->range().left, 12);
    EXPECT_EQ(cached_blocks[0]->range().right, 13);

    RealPeerCacheService service;
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    std::vector<FileBlockSPtr> blocks {create_manual_peer_test_block(file_path, 12, 4)};

    PeerFileCacheReader peer_reader(Path(file_path.string()), true, "127.0.0.1", addr.port);
    PeerFetchResult result;
    IOContext io_ctx;

    ASSERT_TRUE(peer_reader.fetch_blocks(blocks, &result, content.size(), &io_ctx).ok());

    ASSERT_EQ(result.chunks.size(), 1);
    EXPECT_EQ(result.bytes_read, 2);
    EXPECT_EQ(result.chunks[0].block_index, 0);
    EXPECT_EQ(result.chunks[0].block_offset, 12);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 1);
    EXPECT_TRUE(service.support_attachment.load());

    std::string tail(2, '#');
    ASSERT_EQ(result.chunks[0].payload.copy_to(tail.data(), tail.size()), tail.size());
    EXPECT_EQ(tail, content.substr(12, 2));
}

TEST_F(CachedRemoteFileReaderPeerTest, read_at_uses_single_peer_rpc_for_sparse_blocks_pb_payload) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_sparse_pb.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_sparse_pb_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);
    populate_cache_block_with_content(cache, file_path, 4, kPeerTestBlockSize, content);

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
    EXPECT_TRUE(service.support_attachment.load());
    EXPECT_EQ(cache_stats.num_local_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 4);
    EXPECT_EQ(cache_stats.num_peer_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 6);
    EXPECT_EQ(cache_stats.num_remote_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 0);
}

TEST_F(CachedRemoteFileReaderPeerTest,
       read_at_uses_single_peer_rpc_for_sparse_blocks_pb_payload_memory_cache) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_sparse_pb_memory.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_sparse_pb_memory_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize, "memory");
    populate_cache_block_with_content(cache, file_path, 4, kPeerTestBlockSize, content);

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(12, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(0, 12));
    EXPECT_EQ(bytes_read, 12);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
    EXPECT_TRUE(service.support_attachment.load());
    EXPECT_EQ(cache_stats.num_local_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 4);
    EXPECT_EQ(cache_stats.num_peer_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 8);
    EXPECT_EQ(cache_stats.num_remote_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 0);
}

TEST_F(CachedRemoteFileReaderPeerTest, read_at_accepts_reordered_attachment_response_blocks) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_attachment_reorder.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_attachment_reorder_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);
    populate_cache_block_with_content(cache, file_path, 4, kPeerTestBlockSize, content);

    MockPeerCacheServiceOptions reorder_options;
    reorder_options.use_attachment = true;
    reorder_options.reverse_response_order = true;
    MockPeerCacheService service(content, reorder_options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
    EXPECT_TRUE(service.support_attachment.load());
    EXPECT_EQ(cache_stats.num_local_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 4);
    EXPECT_EQ(cache_stats.num_peer_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 6);
    EXPECT_EQ(cache_stats.num_remote_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 0);
}

TEST_F(CachedRemoteFileReaderPeerTest,
       read_at_uses_single_peer_rpc_for_sparse_blocks_attachment_payload_memory_cache) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_peer_test_file(
            "cached_remote_reader_peer_sparse_attachment_memory.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path =
            caches_dir / "cached_remote_reader_peer_sparse_attachment_memory_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize, "memory");
    populate_cache_block_with_content(cache, file_path, 4, kPeerTestBlockSize, content);

    MockPeerCacheServiceOptions attachment_options;
    attachment_options.use_attachment = true;
    MockPeerCacheService service(content, attachment_options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(12, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(0, 12));
    EXPECT_EQ(bytes_read, 12);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
    EXPECT_TRUE(service.support_attachment.load());
    EXPECT_EQ(cache_stats.num_local_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 4);
    EXPECT_EQ(cache_stats.num_peer_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 8);
    EXPECT_EQ(cache_stats.num_remote_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 0);
}

TEST_F(CachedRemoteFileReaderPeerTest,
       read_at_uses_single_peer_rpc_for_sparse_blocks_attachment_payload) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_sparse_attachment.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_sparse_attachment_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);
    populate_cache_block_with_content(cache, file_path, 4, kPeerTestBlockSize, content);

    MockPeerCacheServiceOptions attachment_options;
    attachment_options.use_attachment = true;
    MockPeerCacheService service(content, attachment_options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(12, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(0, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(0, 12));
    EXPECT_EQ(bytes_read, 12);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
    EXPECT_TRUE(service.support_attachment.load());
    EXPECT_EQ(cache_stats.num_local_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 4);
    EXPECT_EQ(cache_stats.num_peer_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 8);
    EXPECT_EQ(cache_stats.num_remote_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 0);
}

TEST_F(CachedRemoteFileReaderPeerTest, read_at_falls_back_when_attachment_payload_is_truncated) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_attachment_truncated.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_attachment_truncated_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);
    populate_cache_block_with_content(cache, file_path, 4, kPeerTestBlockSize, content);

    MockPeerCacheServiceOptions attachment_options;
    attachment_options.use_attachment = true;
    attachment_options.truncate_trailing_attachment_bytes = 1;
    MockPeerCacheService service(content, attachment_options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
    EXPECT_TRUE(service.support_attachment.load());
    EXPECT_EQ(cache_stats.num_local_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 0);
    EXPECT_EQ(cache_stats.num_peer_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 0);
    EXPECT_EQ(cache_stats.num_remote_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 10);
}

TEST_F(CachedRemoteFileReaderPeerTest, read_at_falls_back_when_attachment_has_trailing_bytes) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_attachment_trailing.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_attachment_trailing_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);
    populate_cache_block_with_content(cache, file_path, 4, kPeerTestBlockSize, content);

    MockPeerCacheServiceOptions attachment_options;
    attachment_options.use_attachment = true;
    attachment_options.append_trailing_attachment = true;
    MockPeerCacheService service(content, attachment_options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
    EXPECT_TRUE(service.support_attachment.load());
    EXPECT_EQ(cache_stats.num_local_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 0);
    EXPECT_EQ(cache_stats.num_peer_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 0);
    EXPECT_EQ(cache_stats.num_remote_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 10);
}

TEST_F(CachedRemoteFileReaderPeerTest, read_at_falls_back_when_peer_response_is_incomplete) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_incomplete.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_incomplete_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    BlockFileCache* cache = create_peer_test_cache(cache_path, kPeerTestBlockSize);
    populate_cache_block_with_content(cache, file_path, 4, kPeerTestBlockSize, content);

    MockPeerCacheServiceOptions incomplete_options;
    incomplete_options.use_attachment = true;
    incomplete_options.drop_last_block = true;
    MockPeerCacheService service(content, incomplete_options);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    DebugPoints::instance()->add_with_params(
            "PeerFileCacheReader::_fetch_from_peer_cache_blocks",
            {{"host", "127.0.0.1"}, {"port", std::to_string(addr.port)}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 1);
    EXPECT_EQ(service.request_block_count.load(), 2);
    EXPECT_TRUE(service.support_attachment.load());
    EXPECT_EQ(cache_stats.num_local_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_local, 0);
    EXPECT_EQ(cache_stats.num_peer_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 0);
    EXPECT_EQ(cache_stats.num_remote_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 10);
}

TEST_F(CachedRemoteFileReaderPeerTest, read_at_falls_back_to_remote_when_peer_read_fails) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path =
            create_peer_test_file("cached_remote_reader_peer_fallback.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cached_remote_reader_peer_fallback_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    DebugPoints::instance()->add_with_params("PeerFileCacheReader::_fetch_from_peer_cache_blocks",
                                             {{"host", "127.0.0.1"}, {"port", "1"}});

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kPeerTestTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(cache_stats.num_peer_io_total, 0);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 0);
    EXPECT_EQ(cache_stats.num_remote_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 10);
}

} // namespace doris::io

// ============================================================
// Part 2 – Cross-CG winner race tests
// ============================================================
// These tests exercise _execute_winner_race() by populating CloudWarmUpManager
// with cross-CG candidates so that the winner race path is triggered.
//
// Requirements:
//   * ExecEnv must hold a CloudStorageEngine (so to_cloud() works).
//   * ExecEnv must hold a CloudClusterInfo whose compute_group_id differs
//     from the candidates' compute_group_id.
//   * The file path used for CachedRemoteFileReader must be parseable by
//     extract_tablet_id() – format: "...data/{tablet_id}/{rowset}_{seg}.dat"
//   * config::enable_peer_s3_race must be true.

#include "cloud/cloud_cluster_info.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_warm_up_manager.h"
#include "io/cache/cached_remote_file_reader.h"
#include "util/threadpool.h"

namespace doris::io {
namespace {

// Tablet id embedded in every cross-CG test path.
constexpr int64_t kCrossTabletId = 12345;

// Create an actual file at a path parseable by extract_tablet_id():
// Format: caches_dir/data/{tablet_id}/{name}
// extract_tablet_id() uses "data/" prefix + 1 slash → version-0 format.
fs::path create_cross_cg_test_file(const std::string& name, std::string_view content) {
    fs::path dir = caches_dir / "data" / std::to_string(kCrossTabletId);
    std::error_code ec;
    fs::create_directories(dir, ec);
    fs::path file_path = dir / name;
    std::ofstream out(file_path, std::ios::binary | std::ios::trunc);
    CHECK(out.is_open()) << file_path;
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
    out.close();
    return file_path;
}

// ----------------------------------------------------------------
// Fixture: extends base peer fixture and adds CloudStorageEngine +
// CloudClusterInfo in ExecEnv so winner-race path is fully reachable.
// ----------------------------------------------------------------
class CrossCGWinnerRaceTest : public CachedRemoteFileReaderPeerTest {
protected:
    void SetUp() override {
        // Base SetUp (sets deploy_mode=cloud, enable_cache_read_from_peer, etc.)
        CachedRemoteFileReaderPeerTest::SetUp();

        // Save old values.
        _old_enable_peer_s3_race = config::enable_peer_s3_race;
        _old_max_concurrent_peer_races = config::max_concurrent_peer_races;
        _old_cluster_info = ExecEnv::GetInstance()->cluster_info();

        // Enable winner race.
        config::enable_peer_s3_race = true;
        config::max_concurrent_peer_races = 1024;

        // Install CloudStorageEngine into ExecEnv so to_cloud() works.
        // Use set_storage_engine so ExecEnv takes ownership (old value nullptr).
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<CloudStorageEngine>(EngineOptions {}));
        auto& cloud_engine = ExecEnv::GetInstance()->storage_engine().to_cloud();
        cloud_engine.set_cloud_warm_up_manager(std::make_unique<CloudWarmUpManager>(cloud_engine));

        // Install CloudClusterInfo with a self compute_group_id that differs
        // from the cross-CG candidate compute_group_id "cg_remote".
        _cluster_info = std::make_unique<CloudClusterInfo>();
        _cluster_info->set_cloud_compute_group_id("cg_self");
        ExecEnv::GetInstance()->set_cluster_info(_cluster_info.get());

        // Install peer_race_s3_thread_pool so the winner race exercises the real pool path
        // (without this, peer_race_s3_thread_pool() returns null and S3 falls back to inline).
        static_cast<void>(ThreadPoolBuilder("PeerRaceS3ThreadPool")
                                  .set_min_threads(0)
                                  .set_max_threads(4)
                                  .build(&_peer_race_s3_pool));
        ExecEnv::GetInstance()->_peer_race_s3_thread_pool = std::move(_peer_race_s3_pool);
    }

    void TearDown() override {
        // Restore ExecEnv cluster info.
        ExecEnv::GetInstance()->set_cluster_info(_old_cluster_info);

        // Destroy the CloudStorageEngine installed in SetUp.
        ExecEnv::GetInstance()->set_storage_engine(nullptr);

        // Tear down peer_race_s3_thread_pool.
        if (ExecEnv::GetInstance()->_peer_race_s3_thread_pool) {
            ExecEnv::GetInstance()->_peer_race_s3_thread_pool->shutdown();
            ExecEnv::GetInstance()->_peer_race_s3_thread_pool.reset(nullptr);
        }

        // Restore race configs.
        config::enable_peer_s3_race = _old_enable_peer_s3_race;
        config::max_concurrent_peer_races = _old_max_concurrent_peer_races;

        CachedRemoteFileReaderPeerTest::TearDown();
    }

    // Register a cross-CG candidate pointing at brpc addr for kCrossTabletId.
    void register_cross_cg_candidate(const std::string& host, int32_t port) {
        ExecEnv::GetInstance()
                ->storage_engine()
                .to_cloud()
                .cloud_warm_up_manager()
                .record_balanced_tablet(kCrossTabletId, host, port, "cg_remote");
    }

    // Register a same-CG candidate (compute_group_id == "cg_self").
    void register_same_cg_candidate(const std::string& host, int32_t port) {
        ExecEnv::GetInstance()
                ->storage_engine()
                .to_cloud()
                .cloud_warm_up_manager()
                .record_balanced_tablet(kCrossTabletId, host, port, "cg_self");
    }

    // Register a second cross-CG candidate from a different remote CG.
    void register_cross_cg_candidate2(const std::string& host, int32_t port) {
        ExecEnv::GetInstance()
                ->storage_engine()
                .to_cloud()
                .cloud_warm_up_manager()
                .record_balanced_tablet(kCrossTabletId, host, port, "cg_remote2");
    }

    // Inspect the current candidate list for kCrossTabletId.
    std::vector<doris::PeerCandidate> get_candidates() {
        return ExecEnv::GetInstance()
                ->storage_engine()
                .to_cloud()
                .cloud_warm_up_manager()
                .get_peer_candidates(kCrossTabletId);
    }

private:
    bool _old_enable_peer_s3_race = false;
    int32_t _old_max_concurrent_peer_races = 64;
    ClusterInfo* _old_cluster_info = nullptr;

    std::unique_ptr<CloudClusterInfo> _cluster_info;
    std::unique_ptr<ThreadPool> _peer_race_s3_pool;
};

} // namespace

// ----------------------------------------------------------------
// Test 1: cross-CG peer wins when it has the data (DOWNLOADED block).
//
// We use a TEST_SYNC_POINT ("CachedRemoteFileReader::_execute_winner_race::s3_before_read")
// inserted in the S3 bthread just before its read_at() call to block S3 until peer has won.
// This guarantees peer always wins regardless of local I/O speed.
// ----------------------------------------------------------------
TEST_F(CrossCGWinnerRaceTest, cross_cg_peer_wins_race) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("peer_wins_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cross_cg_peer_wins_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    // Start a mock peer server that serves all blocks successfully.
    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    // Register candidate pointing at the mock peer server.
    register_cross_cg_candidate("127.0.0.1", addr.port);

    // Install SyncPoint: block S3 bthread until peer has won.
    // The callback waits for service.rpc_count > 0 (peer RPC delivered), then sleeps 10 ms
    // to give the peer bthread time to lock race->mtx and set winner=0. S3 completes after
    // that, finds winner already set, and discards its result.
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard s3_block_guard;
    sp->set_call_back(
            "CachedRemoteFileReader::_execute_winner_race::s3_before_read",
            [&](auto&&) {
                // Wait until mock peer server has received and processed at least one RPC.
                while (service.rpc_count.load(std::memory_order_acquire) == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                // Extra margin: let peer bthread acquire race->mtx and set winner=0.
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            },
            &s3_block_guard);

    Defer disable_sp {[&]() { sp->disable_processing(); }};

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    // Peer should have been contacted at least once.
    EXPECT_GE(service.rpc_count.load(), 1);
    // Data came from peer (cross-CG race: peer won).
    EXPECT_GE(cache_stats.num_cross_cg_peer_io_total, 1);
    EXPECT_GE(cache_stats.bytes_read_from_cross_cg_peer, 10u);
    // Race winner=0 means peer data was returned.
    EXPECT_EQ(cache_stats.num_peer_race_peer_win, 1);
}

TEST_F(CrossCGWinnerRaceTest, cross_cg_peer_race_updates_workload_group_remote_scan_metrics) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("peer_wg_remote_scan_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cross_cg_peer_wg_remote_scan_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    auto workload_group = create_peer_test_workload_group(1024);

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    register_cross_cg_candidate("127.0.0.1", addr.port);

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard s3_block_guard;
    sp->set_call_back(
            "CachedRemoteFileReader::_execute_winner_race::s3_before_read",
            [&](auto&&) {
                // Keep S3 behind the peer path on purpose. This case is not asserting that
                // "throttled peer must lose to S3"; it asserts that peer reads still inherit
                // WG remote-scan throttling inside winner-race execution. Waiting until the peer
                // RPC is already in flight guarantees the peer branch has first paid the ~500 ms
                // IOThrottle delay, then adding a small extra sleep keeps the winner stable.
                while (service.rpc_count.load(std::memory_order_acquire) == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            },
            &s3_block_guard);
    Defer disable_sp {[&]() { sp->disable_processing(); }};

    FileCacheStatistics cache_stats;
    std::string buffer(10, '#');
    size_t bytes_read = 0;
    Status read_status = Status::OK();
    std::thread read_thread {[&]() {
        auto query_mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                                  "CrossCGWinnerRaceWGPeerRead");
        AttachTask attach_task(
                create_peer_test_resource_context(query_mem_tracker, workload_group));

        FileReaderSPtr local_reader;
        read_status = global_local_filesystem()->open_file(file_path.string(), &local_reader);
        if (!read_status.ok()) {
            return;
        }

        FileReaderOptions opts;
        opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
        opts.is_doris_table = true;
        opts.mtime = 1;
        opts.tablet_id = kCrossTabletId;
        CachedRemoteFileReader reader(local_reader, opts);

        IOContext io_ctx;
        io_ctx.file_cache_stats = &cache_stats;
        read_status = reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx);
    }};
    read_thread.join();

    ASSERT_TRUE(read_status.ok()) << read_status;

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(cache_stats.num_peer_race_peer_win, 1);
    EXPECT_GE(cache_stats.num_cross_cg_peer_io_total, 1);
    EXPECT_GE(cache_stats.bytes_read_from_cross_cg_peer, 10u);
}

TEST_F(CrossCGWinnerRaceTest, cross_cg_peer_race_respects_workload_group_remote_scan_throttle) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("peer_wg_throttle_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cross_cg_peer_wg_throttle_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    auto workload_group = create_peer_test_workload_group(20);
    auto remote_scan_io_throttle = workload_group->get_remote_scan_io_throttle();
    ASSERT_NE(remote_scan_io_throttle, nullptr);
    // IOThrottle gates reads in two phases:
    //   1. acquire() runs before the read and waits until now > next_io_time.
    //   2. update_next_io_time(bytes) runs after the read and pushes next_io_time forward by
    //      bytes / remote_read_bytes_per_second.
    //
    // Timeline with remote_read_bytes_per_second = 20 B/s:
    //   t0: next_io_time = 0, so a first read would pass acquire() immediately.
    //   t1: seed update_next_io_time(10), which records 10 / 20 s = 0.5 s of throttle debt.
    //       next_io_time becomes about t1 + 500 ms.
    //   t2: this test's peer read starts right after t1, so acquire() waits until next_io_time,
    //       i.e. about 500 ms, before issuing the RPC.
    remote_scan_io_throttle->update_next_io_time(10);

    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    register_cross_cg_candidate("127.0.0.1", addr.port);

    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard s3_block_guard;
    sp->set_call_back(
            "CachedRemoteFileReader::_execute_winner_race::s3_before_read",
            [&](auto&&) {
                while (service.rpc_count.load(std::memory_order_acquire) == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            },
            &s3_block_guard);
    Defer disable_sp {[&]() { sp->disable_processing(); }};

    FileCacheStatistics cache_stats;
    std::string buffer(10, '#');
    size_t bytes_read = 0;
    Status read_status = Status::OK();
    int64_t elapsed_ms = 0;
    std::thread read_thread {[&]() {
        auto query_mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                                  "CrossCGWinnerRaceWGThrottle");
        AttachTask attach_task(
                create_peer_test_resource_context(query_mem_tracker, workload_group));

        FileReaderSPtr local_reader;
        read_status = global_local_filesystem()->open_file(file_path.string(), &local_reader);
        if (!read_status.ok()) {
            return;
        }

        FileReaderOptions opts;
        opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
        opts.is_doris_table = true;
        opts.mtime = 1;
        opts.tablet_id = kCrossTabletId;
        CachedRemoteFileReader reader(local_reader, opts);

        IOContext io_ctx;
        io_ctx.file_cache_stats = &cache_stats;
        const auto start = std::chrono::steady_clock::now();
        read_status = reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx);
        elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
    }};
    read_thread.join();

    ASSERT_TRUE(read_status.ok()) << read_status;
    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(cache_stats.num_peer_race_peer_win, 1);
    EXPECT_GE(service.rpc_count.load(), 1);
    // Timing rationale: 10 bytes / 20 B/s = 500 ms of throttle debt seeded before the read.
    // The peer bthread must wait ~500 ms in IOThrottle::acquire() before issuing the RPC.
    // The S3 sync point blocks until the peer RPC fires, so S3 also finishes after ~500 ms.
    // 400 ms is a conservative lower bound (80% of expected) to tolerate scheduling jitter.
    EXPECT_GE(elapsed_ms, 400);
}

TEST_F(CrossCGWinnerRaceTest,
       cross_cg_s3_inline_fallback_inherits_workload_group_context_when_pool_unavailable) {
    const std::string content = "abcdefghijklmnop";
    const fs::path cache_path = caches_dir / "cross_cg_s3_inline_fallback_wg_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    auto workload_group = create_peer_test_workload_group(20);
    auto remote_scan_io_throttle = workload_group->get_remote_scan_io_throttle();
    ASSERT_NE(remote_scan_io_throttle, nullptr);
    remote_scan_io_throttle->update_next_io_time(10);

    MockPeerCacheServiceOptions fail_opts;
    fail_opts.fail_status = true;
    MockPeerCacheService fail_service(content, fail_opts);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &fail_service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    register_cross_cg_candidate("127.0.0.1", addr.port);

    auto saved_s3_pool = std::move(ExecEnv::GetInstance()->_peer_race_s3_thread_pool);
    Defer restore_s3_pool {[&]() {
        ExecEnv::GetInstance()->_peer_race_s3_thread_pool = std::move(saved_s3_pool);
    }};
    ASSERT_EQ(ExecEnv::GetInstance()->peer_race_s3_thread_pool(), nullptr);

    auto remote_reader = std::make_shared<InspectingRemoteFileReader>(
            content,
            caches_dir / "data" / std::to_string(kCrossTabletId) /
                    "cross_cg_s3_inline_fallback_wg.dat",
            workload_group);

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(remote_reader, opts);

    FileCacheStatistics cache_stats;
    std::string buffer(10, '#');
    size_t bytes_read = 0;
    Status read_status = Status::OK();
    int64_t elapsed_ms = 0;
    std::thread read_thread {[&]() {
        auto query_mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                                  "CrossCGWinnerRaceS3InlineWG");
        AttachTask attach_task(
                create_peer_test_resource_context(query_mem_tracker, workload_group));
        remote_reader->set_expected_thread_id(std::this_thread::get_id());

        IOContext io_ctx;
        io_ctx.file_cache_stats = &cache_stats;
        const auto start = std::chrono::steady_clock::now();
        read_status = reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx);
        elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
    }};
    read_thread.join();

    ASSERT_TRUE(read_status.ok()) << read_status;

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_GE(fail_service.rpc_count.load(), 1);
    EXPECT_EQ(cache_stats.num_peer_race_s3_win, 1);
    EXPECT_EQ(cache_stats.num_cross_cg_peer_io_total, 0);
    EXPECT_EQ(remote_reader->read_call_count(), 1);
    EXPECT_TRUE(remote_reader->observed_expected_workload_group());
    EXPECT_TRUE(remote_reader->observed_non_orphan_tracker());
    EXPECT_TRUE(remote_reader->ran_on_expected_thread());
    EXPECT_GE(remote_reader->throttle_wait_ms(), 400);
    EXPECT_GE(elapsed_ms, 400);
}

// ----------------------------------------------------------------
// Test 2: S3 wins when peer mock returns an error.
// ----------------------------------------------------------------
TEST_F(CrossCGWinnerRaceTest, cross_cg_s3_wins_when_peer_fails) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("s3_wins_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cross_cg_s3_wins_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    // Start a mock peer server that always returns INTERNAL_ERROR.
    MockPeerCacheServiceOptions fail_opts;
    fail_opts.fail_status = true;
    MockPeerCacheService fail_service(content, fail_opts);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &fail_service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    // Register the failing cross-CG candidate.
    register_cross_cg_candidate("127.0.0.1", addr.port);

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    // Read should still succeed via S3 fallback.
    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    // Peer was contacted but failed.
    EXPECT_GE(fail_service.rpc_count.load(), 1);
    // S3 won the race.
    EXPECT_EQ(cache_stats.num_peer_race_s3_win, 1);
    EXPECT_EQ(cache_stats.num_cross_cg_peer_io_total, 0);
}

TEST_F(CrossCGWinnerRaceTest, cross_cg_s3_win_does_not_wait_for_peer_done) {
    const int delay_ms = 1500;
    const int32_t old_hedge_delay_ms = config::peer_race_hedge_delay_ms;
    Defer restore_hedge_delay {[&]() { config::peer_race_hedge_delay_ms = old_hedge_delay_ms; }};
    config::peer_race_hedge_delay_ms = 0;

    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("s3_no_wait_peer_done_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cross_cg_s3_no_wait_peer_done_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    SlowFailingPeerCacheService slow_service(delay_ms);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &slow_service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    register_cross_cg_candidate("127.0.0.1", addr.port);

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    auto reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    MonotonicStopWatch sw;
    sw.start();
    ASSERT_TRUE(reader->read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    const int64_t elapsed_ms = sw.elapsed_time() / 1000000;

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(cache_stats.num_peer_race_s3_win, 1);
    EXPECT_LT(elapsed_ms, delay_ms / 2);

    for (int i = 0; i < 40 && !slow_service.finished.load(std::memory_order_acquire); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(slow_service.started.load(std::memory_order_acquire));
    EXPECT_TRUE(slow_service.finished.load(std::memory_order_acquire));
}

TEST_F(CrossCGWinnerRaceTest, fill_not_found_does_not_retry_same_compute_group_candidates) {
    const int32_t old_hedge_delay_ms = config::peer_race_hedge_delay_ms;
    const std::string old_fill_cg = config::peer_cache_fill_compute_group_id;
    Defer restore_config {[&]() {
        config::peer_race_hedge_delay_ms = old_hedge_delay_ms;
        config::peer_cache_fill_compute_group_id = old_fill_cg;
    }};
    config::peer_race_hedge_delay_ms = 500;
    config::peer_cache_fill_compute_group_id = "cg_fill";

    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("fill_not_found_no_retry_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "fill_not_found_no_retry_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    MockPeerCacheServiceOptions miss_opts;
    miss_opts.cache_miss_status = true;
    MockPeerCacheService miss_service1(content, miss_opts);
    MockPeerCacheService miss_service2(content, miss_opts);

    brpc::Server server1, server2;
    auto addr1 = start_peer_test_server(&server1, &miss_service1);
    auto addr2 = start_peer_test_server(&server2, &miss_service2);
    Defer stop_servers {[&]() {
        stop_peer_test_server(&server1);
        stop_peer_test_server(&server2);
    }};

    TabletPeerCandidates candidates;
    candidates.candidates = {
            PeerCandidate {
                    .host = "127.0.0.1", .brpc_port = addr1.port, .compute_group_id = "cg_fill"},
            PeerCandidate {
                    .host = "127.0.0.1", .brpc_port = addr2.port, .compute_group_id = "cg_fill"}};
    ExecEnv::GetInstance()
            ->storage_engine()
            .to_cloud()
            .cloud_warm_up_manager()
            .set_tablet_peer_candidates(kCrossTabletId, std::move(candidates));

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    opts.storage_resource_id = "peer_fill_test_resource";
    auto reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader->read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_TRUE(miss_service1.request_fill.load());
    EXPECT_EQ(miss_service1.rpc_count.load(), 1);
    EXPECT_EQ(miss_service2.rpc_count.load(), 0);
    EXPECT_EQ(cache_stats.num_peer_race_s3_win, 1);
}

// ----------------------------------------------------------------
// Test 3: same-CG candidate goes through winner race, peer wins, no cross-CG stats.
// ----------------------------------------------------------------
TEST_F(CrossCGWinnerRaceTest, same_cg_candidate_peer_wins_race_no_cross_cg_stats) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("same_cg_race_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "same_cg_race_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    // Start a successful peer server.
    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    // Register a SAME-CG candidate (compute_group_id == "cg_self").
    register_same_cg_candidate("127.0.0.1", addr.port);

    // Block S3 until peer finishes to ensure peer wins the race.
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard s3_block_guard;
    sp->set_call_back(
            "CachedRemoteFileReader::_execute_winner_race::s3_before_read",
            [&](auto&&) {
                while (service.rpc_count.load(std::memory_order_acquire) == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            },
            &s3_block_guard);
    Defer disable_sp {[&]() { sp->disable_processing(); }};

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    // Same-CG candidate wins race — peer_race_peer_win counted, but NOT cross-CG.
    EXPECT_EQ(cache_stats.num_peer_race_peer_win, 1);
    EXPECT_EQ(cache_stats.num_cross_cg_peer_io_total, 0)
            << "same-CG peer win must NOT increment cross-CG counter";
    EXPECT_GE(cache_stats.num_peer_io_total, 1);
    EXPECT_GE(cache_stats.bytes_read_from_peer, 10u);
}

// ----------------------------------------------------------------
// Test: mixed same-CG + cross-CG candidates → peer bthread retries through cross-CG miss
// to reach same-CG hit; winner reports same-CG (no cross-CG stats).
// ----------------------------------------------------------------
// Storage order: [cg_remote@portB (cache miss), cg_self@portA (has data)].
// Peer bthread tries cg_remote first → NOT_FOUND → retries cg_self → hit → peer wins.
// After success, last_successful_compute_group_id is set to "cg_self", so subsequent
// reads try cg_self first (affinity).
TEST_F(CrossCGWinnerRaceTest, mixed_candidates_peer_retries_to_same_cg_hit) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("mixed_cg_retry_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "mixed_cg_retry_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    // Same-CG server: has the data.
    MockPeerCacheService same_cg_service(content);
    brpc::Server same_cg_server;
    auto same_cg_addr = start_peer_test_server(&same_cg_server, &same_cg_service);
    Defer stop_same {[&]() { stop_peer_test_server(&same_cg_server); }};

    // Cross-CG server: simulates a cache miss (EMPTY block on that BE).
    MockPeerCacheServiceOptions cross_opts;
    cross_opts.cache_miss_status = true;
    MockPeerCacheService cross_cg_service(content, cross_opts);
    brpc::Server cross_cg_server;
    auto cross_cg_addr = start_peer_test_server(&cross_cg_server, &cross_cg_service);
    Defer stop_cross {[&]() { stop_peer_test_server(&cross_cg_server); }};

    // Register same-CG FIRST so it lands at index 1 in storage after cross-CG is pushed to front.
    // storage: [cg_remote@cross_cg_port, cg_self@same_cg_port]
    register_same_cg_candidate("127.0.0.1", same_cg_addr.port);
    register_cross_cg_candidate("127.0.0.1", cross_cg_addr.port);

    auto cands = get_candidates();
    ASSERT_GE(cands.size(), 2u);
    ASSERT_EQ(cands[0].compute_group_id, "cg_remote")
            << "cross-CG candidate must be at index 0 (registered last → inserted at front)";

    // Block S3 until cross-CG server has been contacted (peer needs time to retry).
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard s3_block_guard;
    sp->set_call_back(
            "CachedRemoteFileReader::_execute_winner_race::s3_before_read",
            [&](auto&&) {
                while (cross_cg_service.rpc_count.load(std::memory_order_acquire) == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            },
            &s3_block_guard);
    Defer disable_sp {[&]() { sp->disable_processing(); }};

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());

    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    // Peer won the race via same-CG candidate (after retrying past cross-CG miss).
    EXPECT_EQ(cache_stats.num_peer_race_peer_win, 1);
    EXPECT_EQ(cache_stats.num_cross_cg_peer_io_total, 0)
            << "winner was same-CG — cross-CG counter must stay 0";
    EXPECT_GE(cache_stats.num_peer_io_total, 1);
    EXPECT_GE(cache_stats.bytes_read_from_peer, 10u);
    // Cross-CG server was contacted (cache miss), then same-CG server was contacted (hit).
    EXPECT_GE(cross_cg_service.rpc_count.load(), 1)
            << "cross-CG candidate at index 0 should have been tried first";
    EXPECT_GE(same_cg_service.rpc_count.load(), 1)
            << "same-CG candidate should have been tried after cross-CG miss";
}

// ----------------------------------------------------------------
// Test: cross-CG cache miss (NOT_FOUND) rotates candidate, does NOT evict it.
//
// Before fix: server returned INTERNAL_ERROR for EMPTY blocks; client checked
// ENTRY_NOT_FOUND (-7002) which never matched TStatusCode::INTERNAL_ERROR.
// Both eviction and rotate paths were dead. Fix: server returns NOT_FOUND, client
// checks NOT_FOUND → eviction counter stays 0, candidate is rotated to end.
//
// Scenario: two cross-CG candidates.
//   - cg_remote  (at back after insert order) → mock returns NOT_FOUND (cache miss)
//   - cg_remote2 (at front) → mock returns actual data
//   Peer bthread tries cg_remote2 first (hits), reads succeed.
//   Then on the second read_at we expect cg_remote to appear at the back,
//   meaning it was rotated or stayed put (and was NOT evicted).
// ----------------------------------------------------------------
TEST_F(CrossCGWinnerRaceTest, cross_cg_cache_miss_rotates_not_evicts_candidate) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("cache_miss_rotate_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cache_miss_rotate_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    // cg_remote mock: returns NOT_FOUND (server-side cache miss).
    MockPeerCacheServiceOptions miss_opts;
    miss_opts.cache_miss_status = true;
    MockPeerCacheService miss_service(content, miss_opts);
    brpc::Server miss_server;
    auto miss_addr = start_peer_test_server(&miss_server, &miss_service);
    Defer stop_miss {[&]() { stop_peer_test_server(&miss_server); }};

    // cg_remote2 mock: returns actual data.
    MockPeerCacheService hit_service(content);
    brpc::Server hit_server;
    auto hit_addr = start_peer_test_server(&hit_server, &hit_service);
    Defer stop_hit {[&]() { stop_peer_test_server(&hit_server); }};

    // Register cg_remote2 first, then cg_remote. record_balanced_tablet inserts at front,
    // so the last registration becomes index 0:
    //   after register_cross_cg_candidate2(hit):  [cg_remote2]
    //   after register_cross_cg_candidate(miss):  [cg_remote, cg_remote2]
    // cg_remote is at index 0 → peer bthread tries it first → NOT_FOUND → rotate → cg_remote2.
    // Note: re-registering the same CG does in-place update (no reorder), so we must register
    // in the correct final order from the start.
    register_cross_cg_candidate2("127.0.0.1", hit_addr.port); // cg_remote2 → index 0
    register_cross_cg_candidate("127.0.0.1", miss_addr.port); // cg_remote  → index 0 (front)
    // storage: [cg_remote, cg_remote2]

    auto candidates_before = get_candidates();
    ASSERT_EQ(candidates_before.size(), 2);
    EXPECT_EQ(candidates_before[0].compute_group_id, "cg_remote");
    EXPECT_EQ(candidates_before[1].compute_group_id, "cg_remote2");

    // Keep S3 behind the peer path until the cache-miss candidate has actually been tried.
    // This validates rotate semantics without disabling winner race.
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard s3_block_guard;
    sp->set_call_back(
            "CachedRemoteFileReader::_execute_winner_race::s3_before_read",
            [&](auto&&) {
                while (miss_service.rpc_count.load(std::memory_order_acquire) == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            },
            &s3_block_guard);
    Defer disable_sp {[&]() { sp->disable_processing(); }};

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    // read_at: peer tries cg_remote (front, cache miss NOT_FOUND) → rotates to back,
    // then tries cg_remote2 (now front, data available) → peer wins race.
    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    // miss_service must have been called at least once (cg_remote was tried first).
    EXPECT_GE(miss_service.rpc_count.load(), 1) << "cg_remote mock should have been contacted";

    // Key assertion: cg_remote must still be in the candidate list (NOT evicted).
    auto candidates_after = get_candidates();
    bool miss_cand_found = false;
    for (const auto& c : candidates_after) {
        if (c.compute_group_id == "cg_remote") {
            miss_cand_found = true;
            EXPECT_EQ(c.consecutive_rpc_failures, 0)
                    << "NOT_FOUND (cache miss) must NOT increment consecutive_rpc_failures";
        }
    }
    EXPECT_TRUE(miss_cand_found)
            << "cg_remote candidate must survive after cache miss: rotate, not evict";
}

// ----------------------------------------------------------------
// Test: tablet cooldown — after N consecutive all-miss races, peer is skipped and S3 wins
// directly without peer RPC overhead.
// ----------------------------------------------------------------
TEST_F(CrossCGWinnerRaceTest, cooldown_skips_peer_after_consecutive_all_miss) {
    const std::string content = "abcdefghijklmnopqrstuvwxyz0123456789ABCD";
    const fs::path file_path = create_cross_cg_test_file("cooldown_test_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "cooldown_test_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    // Mock peer server: always returns cache miss (NOT_FOUND).
    MockPeerCacheServiceOptions miss_opts;
    miss_opts.cache_miss_status = true;
    MockPeerCacheService miss_service(content, miss_opts);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &miss_service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    register_cross_cg_candidate("127.0.0.1", addr.port);

    // Set cooldown threshold to 2 for faster test.
    const int old_threshold = config::peer_all_miss_cooldown_threshold;
    const int64_t old_duration = config::peer_all_miss_cooldown_duration_s;
    const int old_hedge_delay_ms = config::peer_race_hedge_delay_ms;
    config::peer_all_miss_cooldown_threshold = 2;
    config::peer_all_miss_cooldown_duration_s = 600; // long enough to not expire during test
    config::peer_race_hedge_delay_ms = 50;
    Defer restore_config {[old_threshold, old_duration, old_hedge_delay_ms]() {
        config::peer_all_miss_cooldown_threshold = old_threshold;
        config::peer_all_miss_cooldown_duration_s = old_duration;
        config::peer_race_hedge_delay_ms = old_hedge_delay_ms;
    }};

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    // Read uncached ranges so each read forces a fresh peer-vs-S3 decision for the same tablet.
    // The hedge delay gives the peer path a head start, making the all-miss accounting
    // deterministic instead of racing with the local-reader S3 path.
    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(cache_stats.num_peer_race_s3_win, 1);
    int rpc_count_after_read1 = miss_service.rpc_count.load();
    EXPECT_GE(rpc_count_after_read1, 1) << "peer was tried in read 1";

    // Read 2: peer miss again → S3 wins. consecutive_all_miss becomes 2 → cooldown triggered.
    cache_stats = {};
    ASSERT_TRUE(reader.read_at(13, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(buffer, content.substr(13, 10));
    EXPECT_EQ(cache_stats.num_peer_race_s3_win, 1);
    int rpc_count_after_read2 = miss_service.rpc_count.load();
    EXPECT_GE(rpc_count_after_read2, rpc_count_after_read1 + 1) << "peer was tried in read 2";

    // Verify cooldown is active.
    auto& manager = ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager();
    EXPECT_TRUE(manager.is_peer_cooldown(kCrossTabletId));

    // Read 3: cooldown active → candidates empty → S3 directly, NO peer RPC.
    cache_stats = {};
    ASSERT_TRUE(reader.read_at(25, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(buffer, content.substr(25, 10));
    // No race entered — S3 was called directly via the empty-candidates path.
    EXPECT_EQ(cache_stats.num_peer_race_peer_win, 0);
    EXPECT_EQ(cache_stats.num_peer_race_s3_win, 0);
    // No additional peer RPCs were made during cooldown.
    EXPECT_EQ(miss_service.rpc_count.load(), rpc_count_after_read2)
            << "peer server must NOT be contacted during cooldown";
}

// ----------------------------------------------------------------
// Test: sequential peer read with empty candidates falls back directly to S3.
// (Covers the _execute_sequential_peer_read empty-candidates path.)
// ----------------------------------------------------------------
TEST_F(CrossCGWinnerRaceTest, sequential_peer_read_falls_back_to_s3_on_empty_candidates) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("seq_empty_cands_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "seq_empty_cands_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    // Disable race so the sequential path is used.
    const bool old_race = config::enable_peer_s3_race;
    config::enable_peer_s3_race = false;
    Defer restore_race {[old_race]() { config::enable_peer_s3_race = old_race; }};

    // Do NOT register any candidates — get_peer_candidates will return empty.
    // But we still need to avoid the "cold miss" bthread fetch, which calls
    // fetch_candidates_from_fe (unimplemented in test). So set cooldown active.
    auto& manager = ExecEnv::GetInstance()->storage_engine().to_cloud().cloud_warm_up_manager();
    const int old_threshold = config::peer_all_miss_cooldown_threshold;
    config::peer_all_miss_cooldown_threshold = 1;
    Defer restore_threshold {
            [old_threshold]() { config::peer_all_miss_cooldown_threshold = old_threshold; }};
    // Record one all-miss to trigger cooldown.
    manager.record_peer_all_miss(kCrossTabletId);

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    // S3 was used (no peer, no race).
    EXPECT_EQ(cache_stats.num_remote_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_remote, 10);
    EXPECT_EQ(cache_stats.num_peer_io_total, 0);
    EXPECT_EQ(cache_stats.num_peer_race_peer_win, 0);
    EXPECT_EQ(cache_stats.num_peer_race_s3_win, 0);
}

// ----------------------------------------------------------------
// Test: sequential peer read success updates affinity (last_successful_compute_group_id).
// (Covers the _execute_sequential_peer_read success path with affinity tracking.)
// ----------------------------------------------------------------
TEST_F(CrossCGWinnerRaceTest, sequential_peer_read_updates_affinity_on_success) {
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("seq_affinity_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "seq_affinity_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    // Disable race so the sequential path is used.
    const bool old_race = config::enable_peer_s3_race;
    config::enable_peer_s3_race = false;
    Defer restore_race {[old_race]() { config::enable_peer_s3_race = old_race; }};

    // Start a mock peer server that serves all blocks successfully.
    MockPeerCacheService service(content);
    brpc::Server server;
    auto addr = start_peer_test_server(&server, &service);
    Defer stop_server {[&]() { stop_peer_test_server(&server); }};

    register_cross_cg_candidate("127.0.0.1", addr.port);

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);
    EXPECT_EQ(service.rpc_count.load(), 1);

    // Peer read was used (sequential path, not race).
    EXPECT_EQ(cache_stats.num_peer_io_total, 1);
    EXPECT_EQ(cache_stats.bytes_read_from_peer, 10);
    EXPECT_EQ(cache_stats.num_remote_io_total, 0);

    // Verify affinity was updated: the successful candidate's CG should be
    // recorded as last_successful_compute_group_id, placing it at index 0
    // after a fresh get_peer_candidates call.
    auto candidates = get_candidates();
    ASSERT_FALSE(candidates.empty());
    EXPECT_EQ(candidates[0].compute_group_id, "cg_remote");
}

// ----------------------------------------------------------------
// Test: in a race, when S3 wins quickly, the peer bthread stops trying
// further candidates (no unnecessary RPCs after S3 victory).
// (Covers the race->winner check in run_peer_race.)
// ----------------------------------------------------------------
TEST_F(CrossCGWinnerRaceTest, race_peer_stops_when_s3_wins) {
    // Verifies that run_peer_race stops trying further candidates once S3 wins.
    // Strategy: use sync points to guarantee ordering:
    //   1. Peer tries candidate 1 → cache miss
    //   2. Sync point blocks peer between candidates
    //   3. S3 completes (wins the race)
    //   4. Release peer → peer sees S3 won → skips candidate 2
    //   5. Assert: candidate 2 received 0 RPCs
    const std::string content = "abcdefghijklmnop";
    const fs::path file_path = create_cross_cg_test_file("peer_stops_0.dat", content);
    Defer cleanup_file {[&]() {
        std::error_code ec;
        fs::remove(file_path, ec);
    }};

    const fs::path cache_path = caches_dir / "peer_stops_cache";
    Defer cleanup_cache {[&]() {
        std::error_code ec;
        fs::remove_all(cache_path, ec);
    }};

    clear_cached_remote_reader_factory();
    create_peer_test_cache(cache_path, kPeerTestBlockSize);

    MockPeerCacheServiceOptions miss_opts;
    miss_opts.cache_miss_status = true;
    MockPeerCacheService miss_service1(content, miss_opts);
    MockPeerCacheService miss_service2(content, miss_opts);

    brpc::Server server1, server2;
    auto addr1 = start_peer_test_server(&server1, &miss_service1);
    auto addr2 = start_peer_test_server(&server2, &miss_service2);
    Defer stop_servers {[&]() {
        stop_peer_test_server(&server1);
        stop_peer_test_server(&server2);
    }};

    // record_balanced_tablet prepends new compute groups, so register candidate 2 first
    // to make candidate 1 the front entry tried by the race.
    register_cross_cg_candidate2("127.0.0.1", addr2.port);
    register_cross_cg_candidate("127.0.0.1", addr1.port);

    auto candidates = get_candidates();
    ASSERT_EQ(candidates.size(), 2);
    EXPECT_EQ(candidates[0].compute_group_id, "cg_remote");
    EXPECT_EQ(candidates[1].compute_group_id, "cg_remote2");

    // Sync point: after candidate 1 fails, block peer bthread until S3 finishes.
    auto* sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();
    SyncPoint::CallbackGuard between_guard;
    sp->set_call_back(
            "run_peer_race::between_candidates",
            [&](auto&&) {
                // Wait until S3 has completed — by then race->winner == 1.
                // The S3 path reads from local_reader (fast), so it finishes quickly.
                // We just need to give it a tiny window to complete.
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            },
            &between_guard);
    Defer disable_sp {[&]() {
        sp->disable_processing();
        sp->clear_all_call_backs();
    }};

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(file_path.string(), &local_reader).ok());

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.mtime = 1;
    opts.tablet_id = kCrossTabletId;
    CachedRemoteFileReader reader(local_reader, opts);

    std::string buffer(10, '#');
    size_t bytes_read = 0;
    IOContext io_ctx;
    FileCacheStatistics cache_stats;
    io_ctx.file_cache_stats = &cache_stats;

    ASSERT_TRUE(reader.read_at(1, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(buffer, content.substr(1, 10));
    EXPECT_EQ(bytes_read, 10);

    // S3 won.
    EXPECT_EQ(cache_stats.num_peer_race_s3_win, 1);

    // Candidate 1 was tried (cache miss), candidate 2 was NOT tried (S3 already won).
    EXPECT_EQ(miss_service1.rpc_count.load(), 1) << "candidate 1 should have been tried";
    EXPECT_EQ(miss_service2.rpc_count.load(), 0) << "candidate 2 should be skipped (S3 won)";
}

} // namespace doris::io

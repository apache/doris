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

#include <aws/core/client/AWSError.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/packed_file_manager.h"
#include "io/fs/packed_file_reader.h"
#include "io/fs/packed_file_system.h"
#include "io/fs/packed_file_writer.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/s3_util.h"
#include "util/slice.h"
#include "util/threadpool.h"

namespace doris::io {

using PackedFileManager = PackedFileManager;
using PackedFileSystem = PackedFileSystem;
using PackedFileWriter = PackedFileWriter;
using PackedFileReader = PackedFileReader;
using PackedSliceLocation = PackedSliceLocation;
using PackedAppendContext = PackedAppendContext;

namespace {

constexpr int64_t kMinSegmentBytes = 10 * 1024;
constexpr int64_t kMaxSegmentBytes = 100 * 1024;
constexpr int kThreadCount = 10;
constexpr int kIterationPerThread = 100;
constexpr int64_t kLargeThreshold = 512 * 1024;

std::unique_ptr<FileCacheFactory> g_owned_cache_factory;
bool g_owned_factory_in_use = false;
bool g_cache_initialized = false;
std::string g_cache_base_path;
bool g_thread_pool_initialized = false;
bool g_s3_upload_pool_initialized = false;
std::shared_ptr<FileSystem> g_remote_file_system;
bool g_owned_fd_cache = false;

FileCacheFactory* ensure_file_cache_factory_on_exec_env() {
    auto* exec_env = ExecEnv::GetInstance();
    auto* factory = exec_env->file_cache_factory();
    if (factory == nullptr) {
        if (!g_owned_cache_factory) {
            g_owned_cache_factory = std::make_unique<FileCacheFactory>();
        }
        factory = g_owned_cache_factory.get();
        exec_env->set_file_cache_factory(factory);
        g_owned_factory_in_use = true;
    }
    if (exec_env->file_cache_open_fd_cache() == nullptr) {
        exec_env->set_file_cache_open_fd_cache(std::make_unique<FDCache>());
        g_owned_fd_cache = true;
    }
    return factory;
}

void release_owned_file_cache_factory() {
    if (g_owned_factory_in_use) {
        ExecEnv::GetInstance()->set_file_cache_factory(nullptr);
        g_owned_cache_factory.reset();
        g_owned_factory_in_use = false;
    }
}

void ensure_non_block_close_pool() {
    auto* exec_env = ExecEnv::GetInstance();
    if (exec_env->non_block_close_thread_pool() == nullptr) {
        std::unique_ptr<ThreadPool> pool;
        auto st = ThreadPoolBuilder("NonBlockCloseThreadPool")
                          .set_min_threads(12)
                          .set_max_threads(48)
                          .build(&pool);
        ASSERT_TRUE(st.ok()) << st;
        exec_env->set_non_block_close_thread_pool(std::move(pool));
        g_thread_pool_initialized = true;
    }
}

void release_non_block_close_pool() {
    if (g_thread_pool_initialized) {
        ExecEnv::GetInstance()->set_non_block_close_thread_pool(nullptr);
        g_thread_pool_initialized = false;
    }
}

void ensure_s3_upload_pool() {
    auto* exec_env = ExecEnv::GetInstance();
    if (exec_env->s3_file_upload_thread_pool() == nullptr) {
        std::unique_ptr<ThreadPool> pool;
        auto st = ThreadPoolBuilder("s3_upload_file_thread_pool")
                          .set_min_threads(5)
                          .set_max_threads(10)
                          .build(&pool);
        ASSERT_TRUE(st.ok()) << st;
        exec_env->set_s3_file_upload_thread_pool(std::move(pool));
        g_s3_upload_pool_initialized = true;
    }
}

void release_s3_upload_pool() {
    if (g_s3_upload_pool_initialized) {
        ExecEnv::GetInstance()->set_s3_file_upload_thread_pool(nullptr);
        g_s3_upload_pool_initialized = false;
    }
}

struct MockS3Store {
    struct UploadCtx {
        std::string bucket;
        std::string key;
        std::map<int, std::string> parts;
    };

    std::mutex mutex;
    uint64_t next_upload_id {1};
    std::unordered_map<std::string, UploadCtx> uploads;
    std::unordered_map<std::string, std::string> objects;

    std::string make_key(std::string_view bucket, std::string_view key) const {
        return fmt::format("{}/{}", bucket, key);
    }
};

MockS3Store& mock_s3_store() {
    static MockS3Store store;
    return store;
}

void reset_mock_s3_store() {
    auto& store = mock_s3_store();
    std::lock_guard lock(store.mutex);
    store.next_upload_id = 1;
    store.uploads.clear();
    store.objects.clear();
}

class MockObjStorageClient : public ObjStorageClient {
public:
    explicit MockObjStorageClient(MockS3Store* store) : _store(store) {}

    ObjectStorageUploadResponse create_multipart_upload(
            const ObjectStoragePathOptions& opts) override {
        ObjectStorageUploadResponse resp;
        resp.resp = ObjectStorageResponse::OK();
        std::lock_guard lock(_store->mutex);
        auto upload_id = fmt::format("upload-{}", _store->next_upload_id++);
        resp.upload_id = upload_id;
        _store->uploads[upload_id] = MockS3Store::UploadCtx {opts.bucket, opts.key, {}};
        return resp;
    }

    ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts,
                                     std::string_view stream) override {
        std::lock_guard lock(_store->mutex);
        _store->objects[_store->make_key(opts.bucket, opts.key)] =
                std::string(stream.data(), stream.size());
        return ObjectStorageResponse::OK();
    }

    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions& opts,
                                            std::string_view stream, int part_num) override {
        ObjectStorageUploadResponse resp;
        if (!opts.upload_id) {
            resp.resp = make_error("missing upload id");
            return resp;
        }
        std::lock_guard lock(_store->mutex);
        auto ctx_it = _store->uploads.find(*opts.upload_id);
        if (ctx_it == _store->uploads.end()) {
            resp.resp = make_error("upload context not found");
            return resp;
        }
        ctx_it->second.parts[part_num] = std::string(stream.data(), stream.size());
        resp.resp = ObjectStorageResponse::OK();
        resp.etag = fmt::format("\"mock-etag-{}\"", part_num);
        return resp;
    }

    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions& opts,
            const std::vector<ObjectCompleteMultiPart>& completed_parts) override {
        if (!opts.upload_id) {
            return make_error("missing upload id");
        }
        std::lock_guard lock(_store->mutex);
        auto ctx_it = _store->uploads.find(*opts.upload_id);
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
        return ObjectStorageResponse::OK();
    }

    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions& opts) override {
        ObjectStorageHeadResponse resp;
        std::lock_guard lock(_store->mutex);
        auto key = _store->make_key(opts.bucket, opts.key);
        auto it = _store->objects.find(key);
        if (it == _store->objects.end()) {
            resp.resp = make_error("object not found",
                                   static_cast<int>(Aws::Http::HttpResponseCode::NOT_FOUND));
            return resp;
        }
        resp.resp = ObjectStorageResponse::OK();
        resp.file_size = it->second.size();
        return resp;
    }

    ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                     size_t offset, size_t bytes_read,
                                     size_t* size_return) override {
        std::lock_guard lock(_store->mutex);
        auto key = _store->make_key(opts.bucket, opts.key);
        auto it = _store->objects.find(key);
        if (it == _store->objects.end()) {
            return make_error("object not found");
        }
        if (offset > it->second.size()) {
            return make_error("offset exceeds object size");
        }
        size_t to_copy = std::min(bytes_read, it->second.size() - offset);
        memcpy(buffer, it->second.data() + offset, to_copy);
        *size_return = to_copy;
        return ObjectStorageResponse::OK();
    }

    ObjectStorageResponse list_objects(const ObjectStoragePathOptions& opts,
                                       std::vector<FileInfo>* files) override {
        std::lock_guard lock(_store->mutex);
        std::string prefix = _store->make_key(opts.bucket, opts.prefix);
        for (const auto& [key, data] : _store->objects) {
            if (key.rfind(prefix, 0) == 0) {
                FileInfo info;
                info.file_name = key.substr(prefix.size());
                info.file_size = data.size();
                info.is_file = true;
                files->push_back(std::move(info));
            }
        }
        return ObjectStorageResponse::OK();
    }

    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                         std::vector<std::string> objs) override {
        std::lock_guard lock(_store->mutex);
        for (const auto& obj : objs) {
            _store->objects.erase(_store->make_key(opts.bucket, obj));
        }
        return ObjectStorageResponse::OK();
    }

    ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) override {
        std::lock_guard lock(_store->mutex);
        _store->objects.erase(_store->make_key(opts.bucket, opts.key));
        return ObjectStorageResponse::OK();
    }

    ObjectStorageResponse delete_objects_recursively(
            const ObjectStoragePathOptions& opts) override {
        std::lock_guard lock(_store->mutex);
        std::string prefix = _store->make_key(opts.bucket, opts.prefix);
        for (auto it = _store->objects.begin(); it != _store->objects.end();) {
            if (it->first.rfind(prefix, 0) == 0) {
                it = _store->objects.erase(it);
            } else {
                ++it;
            }
        }
        return ObjectStorageResponse::OK();
    }

    std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                       int64_t /*expiration_secs*/,
                                       const S3ClientConf& /*conf*/) override {
        return fmt::format("mock://{}/{}", opts.bucket, opts.key);
    }

private:
    static ObjectStorageResponse make_error(std::string msg, int http_code = 500) {
        ObjectStorageResponse resp;
        resp.status.code = static_cast<int>(ErrorCode::INTERNAL_ERROR);
        resp.status.msg = std::move(msg);
        resp.http_code = http_code;
        return resp;
    }

    MockS3Store* _store;
};

std::shared_ptr<MockObjStorageClient> g_mock_obj_client;

void install_mock_environment() {
    g_mock_obj_client = std::make_shared<MockObjStorageClient>(&mock_s3_store());
    auto client = g_mock_obj_client;
    S3ClientFactory::instance().set_client_creator_for_test(
            [client](const S3ClientConf&) { return client; });

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("PackedFileManager::update_meta_service", [](std::vector<std::any>&& args) {
        auto pair = try_any_cast_ret<Status>(args);
        pair->first = Status::OK();
        pair->second = true;
    });
}

void remove_mock_environment() {
    S3ClientFactory::instance().clear_client_creator_for_test();
    g_mock_obj_client.reset();

    auto* sp = SyncPoint::get_instance();
    sp->clear_call_back("PackedFileManager::update_meta_service");
    sp->disable_processing();
}

class MockRemoteReader : public FileReader {
public:
    MockRemoteReader(Path path, std::string bucket, MockS3Store* store)
            : _path(std::move(path)), _bucket(std::move(bucket)), _store(store) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const Path& path() const override { return _path; }

    size_t size() const override {
        std::lock_guard lock(_store->mutex);
        auto it = _store->objects.find(_store->make_key(_bucket, _path.native()));
        return it == _store->objects.end() ? 0 : it->second.size();
    }

    bool closed() const override { return _closed; }

    int64_t mtime() const override { return 0; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* /*io_ctx*/) override {
        std::lock_guard lock(_store->mutex);
        auto key = _store->make_key(_bucket, _path.native());
        auto it = _store->objects.find(key);
        if (it == _store->objects.end()) {
            return Status::InternalError("mock object {} not found", key);
        }
        const auto& data = it->second;
        if (offset > data.size()) {
            return Status::InternalError("offset {} exceeds size {} for {}", offset, data.size(),
                                         key);
        }
        size_t to_copy = std::min(result.size, data.size() - offset);
        memcpy(result.data, data.data() + offset, to_copy);
        *bytes_read = to_copy;
        return Status::OK();
    }

private:
    Path _path;
    std::string _bucket;
    MockS3Store* _store;
    bool _closed = false;
};

class MockRemoteFileSystem : public FileSystem {
public:
    MockRemoteFileSystem(MockS3Store* store, std::string bucket, std::string prefix = {})
            : FileSystem("mock_remote_fs", FileSystemType::S3),
              _store(store),
              _bucket(std::move(bucket)),
              _prefix(std::move(prefix)) {}

protected:
    Status open_file_impl(const Path& file, FileReaderSPtr* reader,
                          const FileReaderOptions* opts) override {
        auto normalized_path = _normalize_remote_path(file);
        auto raw = std::make_shared<MockRemoteReader>(Path(normalized_path), _bucket, _store);
        FileReaderOptions local_opts = opts ? *opts : FileReaderOptions();
        local_opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
        local_opts.is_doris_table = true;
        *reader = std::make_shared<CachedRemoteFileReader>(raw, local_opts);
        return Status::OK();
    }

    Status create_file_impl(const Path& /*file*/, FileWriterPtr* /*writer*/,
                            const FileWriterOptions* /*opts*/) override {
        return Status::NotSupported("mock remote fs does not support create");
    }

    Status create_directory_impl(const Path& /*dir*/, bool /*failed_if_exists*/) override {
        return Status::NotSupported("mock remote fs does not support directories");
    }

    Status delete_file_impl(const Path& /*file*/) override {
        return Status::NotSupported("mock remote fs does not support delete");
    }

    Status delete_directory_impl(const Path& /*dir*/) override {
        return Status::NotSupported("mock remote fs does not support delete dir");
    }

    Status batch_delete_impl(const std::vector<Path>& /*files*/) override {
        return Status::NotSupported("mock remote fs does not support batch delete");
    }

    Status exists_impl(const Path& file, bool* res) const override {
        std::lock_guard lock(_store->mutex);
        *res = _store->objects.count(_store->make_key(_bucket, _normalize_remote_path(file))) > 0;
        return Status::OK();
    }

    Status file_size_impl(const Path& file, int64_t* file_size) const override {
        std::lock_guard lock(_store->mutex);
        auto it = _store->objects.find(_store->make_key(_bucket, _normalize_remote_path(file)));
        if (it == _store->objects.end()) {
            return Status::InternalError("mock object {} not found", file.native());
        }
        *file_size = it->second.size();
        return Status::OK();
    }

    Status rename_impl(const Path& /*orig_name*/, const Path& /*new_name*/) override {
        return Status::NotSupported("mock remote fs does not support rename");
    }

    Status list_impl(const Path& /*dir*/, bool /*only_file*/, std::vector<FileInfo>* /*files*/,
                     bool* /*exists*/) override {
        return Status::NotSupported("mock remote fs does not support list");
    }

    Status absolute_path(const Path& file, Path& abs_path) const override {
        abs_path = file;
        return Status::OK();
    }

private:
    std::string _normalize_remote_path(const Path& file) const {
        constexpr std::string_view kS3Scheme = "s3://";
        std::string_view view(file.native());
        if (view.rfind(kS3Scheme, 0) == 0) {
            view.remove_prefix(kS3Scheme.size());
            auto slash_pos = view.find('/');
            if (slash_pos == std::string_view::npos) {
                return {};
            }
            auto uri_bucket = view.substr(0, slash_pos);
            view.remove_prefix(std::min(view.size(), slash_pos + 1));
            DCHECK(uri_bucket.empty() || uri_bucket == _bucket)
                    << "unexpected bucket " << uri_bucket << " for path " << file.native();
        }
        std::string normalized(view);
        if (!_prefix.empty() && normalized.rfind(_prefix, 0) != 0) {
            normalized = fmt::format("{}/{}", _prefix, normalized);
        }
        return normalized;
    }

    MockS3Store* _store;
    std::string _bucket;
    std::string _prefix;
};

void reset_manager_state(PackedFileManager* manager) {
    manager->stop_background_manager();
    manager->clear_state_for_test();
    manager->file_systems_for_test().clear();
    manager->default_file_system_for_test().reset();
    manager->reset_packed_file_bvars_for_test();
}

void create_file_cache_once(const std::string& base_path) {
    static std::once_flag once;
    std::call_once(once, [&] {
        ensure_file_cache_factory_on_exec_env();
        std::filesystem::create_directories(base_path);
        auto settings = get_file_cache_settings(64 * 1024 * 1024, 8 * 1024 * 1024);
        auto status = FileCacheFactory::instance()->create_file_cache(base_path, settings);
        ASSERT_TRUE(status.ok()) << status;
        g_cache_initialized = true;
        g_cache_base_path = base_path;
    });
}

void cleanup_file_cache_resources() {
    if (g_cache_initialized) {
        auto* factory = ExecEnv::GetInstance()->file_cache_factory();
        if (factory != nullptr) {
            factory->_caches.clear();
            factory->_path_to_cache.clear();
            factory->_capacity = 0;
        }
        g_cache_initialized = false;
        g_cache_base_path.clear();
    }
    g_remote_file_system.reset();
    if (g_owned_fd_cache) {
        ExecEnv::GetInstance()->set_file_cache_open_fd_cache(nullptr);
        g_owned_fd_cache = false;
    }
    release_owned_file_cache_factory();
}

} // namespace

class MergeFileConcurrencyTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        install_mock_environment();
        reset_mock_s3_store();
        ensure_non_block_close_pool();
        ensure_s3_upload_pool();
        S3Conf s3_conf;
        s3_conf.bucket = "mock-bucket";
        s3_conf.prefix = "merge-file-concurrency";
        s3_conf.client_conf.ak = "ak";
        s3_conf.client_conf.sk = "sk";
        s3_conf.client_conf.endpoint = "http://endpoint";
        s3_conf.client_conf.region = "region";
        auto fs_or = S3FileSystem::create(s3_conf, FileSystem::TMP_FS_ID);
        ASSERT_TRUE(fs_or.has_value()) << fs_or.error();
        _s3_fs = fs_or.value();
        g_remote_file_system = std::make_shared<MockRemoteFileSystem>(
                &mock_s3_store(), _s3_fs->bucket(), _s3_fs->prefix());

        std::string cache_path =
                (std::filesystem::current_path() / "ut_dir/packed_file_cache").string();
        create_file_cache_once(cache_path);
    }

    static void TearDownTestSuite() {
        remove_mock_environment();
        cleanup_file_cache_resources();
        release_non_block_close_pool();
        release_s3_upload_pool();
        _s3_fs.reset();
    }

    void SetUp() override {
        reset_mock_s3_store();
        _old_small_threshold = config::small_file_threshold_bytes;
        _old_merge_threshold = config::packed_file_size_threshold_bytes;
        _old_cleanup_seconds = config::uploaded_file_retention_seconds;
        _old_time_threshold = config::packed_file_time_threshold_ms;
        _old_enable_file_cache = config::enable_file_cache;

        config::small_file_threshold_bytes = kLargeThreshold;
        config::packed_file_size_threshold_bytes = kLargeThreshold * 2;
        config::uploaded_file_retention_seconds = 10;
        config::packed_file_time_threshold_ms = 1;
        config::enable_file_cache = true;

        reset_manager_state(PackedFileManager::instance());
        PackedFileManager::instance()->start_background_manager();
    }

    void TearDown() override {
        config::small_file_threshold_bytes = _old_small_threshold;
        config::packed_file_size_threshold_bytes = _old_merge_threshold;
        config::uploaded_file_retention_seconds = _old_cleanup_seconds;
        config::packed_file_time_threshold_ms = _old_time_threshold;
        config::enable_file_cache = _old_enable_file_cache;
        reset_manager_state(PackedFileManager::instance());
    }

    static std::shared_ptr<S3FileSystem> _s3_fs;

private:
    int64_t _old_small_threshold = 0;
    int64_t _old_merge_threshold = 0;
    int64_t _old_cleanup_seconds = 0;
    int64_t _old_time_threshold = 0;
    bool _old_enable_file_cache = false;
};

std::shared_ptr<S3FileSystem> MergeFileConcurrencyTest::_s3_fs;

TEST_F(MergeFileConcurrencyTest, ConcurrentWriteReadCorrectness) {
    auto* manager = PackedFileManager::instance();
    ASSERT_NE(_s3_fs, nullptr);
    ASSERT_NE(g_remote_file_system, nullptr);

    std::vector<std::string> resource_ids;
    resource_ids.reserve(kThreadCount);
    for (int i = 0; i < kThreadCount; ++i) {
        auto id = fmt::format("resource-{}", i);
        resource_ids.emplace_back(id);
        manager->file_systems_for_test()[id] = _s3_fs;
    }

    std::atomic<int> success_count {0};
    std::vector<std::thread> workers;
    workers.reserve(kThreadCount);

    for (int tid = 0; tid < kThreadCount; ++tid) {
        workers.emplace_back([&, tid] {
            SCOPED_INIT_THREAD_CONTEXT();
            std::mt19937_64 rng(0x9e3779b97f4a7c15ULL + tid);
            std::uniform_int_distribution<int> seg_cnt_dist(1, 4);
            std::uniform_int_distribution<int> seg_size_dist(static_cast<int>(kMinSegmentBytes),
                                                             static_cast<int>(kMaxSegmentBytes));
            std::uniform_int_distribution<int> read_size_dist(4 * 1024, 32 * 1024);

            for (int iter = 0; iter < kIterationPerThread; ++iter) {
                // Use unique file names to avoid cache key conflicts between threads
                // since CachedRemoteFileReader uses path().filename() for cache hash
                std::string path =
                        fmt::format("/tablet_{}/rowset_{}/file_t{}_i{}", tid, iter, tid, iter);

                PackedAppendContext append_info;
                append_info.resource_id = resource_ids[tid];
                append_info.tablet_id = 1000 + tid;
                append_info.rowset_id = fmt::format("rowset-{}-{}", tid, iter);
                append_info.txn_id = (tid + 1) * 100000 + iter + 1;

                PackedFileSystem writer_fs(_s3_fs, append_info);
                FileWriterPtr writer;
                ASSERT_TRUE(writer_fs.create_file(Path(path), &writer, nullptr).ok());
                auto* merge_writer = dynamic_cast<PackedFileWriter*>(writer.get());
                ASSERT_NE(merge_writer, nullptr);

                const int seg_count = seg_cnt_dist(rng);
                std::vector<std::string> payloads;
                payloads.reserve(seg_count);
                std::string expected;
                for (int seg = 0; seg < seg_count; ++seg) {
                    int len = seg_size_dist(rng);
                    std::string chunk(len, static_cast<char>('a' + (seg + tid) % 26));
                    payloads.emplace_back(std::move(chunk));
                    expected.append(payloads.back());
                    Slice slice(payloads.back());
                    ASSERT_TRUE(writer->append(slice).ok());
                }

                ASSERT_TRUE(writer->close(true).ok());
                ASSERT_TRUE(writer->close(false).ok());

                PackedSliceLocation index;
                ASSERT_TRUE(merge_writer->get_packed_slice_location(&index).ok());

                std::unordered_map<std::string, PackedSliceLocation> index_map;
                index_map[path] = index;
                PackedFileSystem reader_fs(g_remote_file_system, index_map, append_info);
                FileReaderSPtr reader;
                FileReaderOptions opts;
                opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
                opts.is_doris_table = true;
                ASSERT_TRUE(reader_fs.open_file(Path(path), &reader, &opts).ok());
                // After the fix, CachedRemoteFileReader wraps PackedFileReader (not vice versa)
                // This ensures cache key uses segment path for proper cleanup
                auto* cached_reader = dynamic_cast<CachedRemoteFileReader*>(reader.get());
                ASSERT_NE(cached_reader, nullptr);
                auto* merge_reader =
                        dynamic_cast<PackedFileReader*>(cached_reader->get_remote_reader());
                ASSERT_NE(merge_reader, nullptr);

                IOContext io_ctx;
                size_t verified = 0;
                while (verified < expected.size()) {
                    size_t remain = expected.size() - verified;
                    size_t chunk = std::min<size_t>(remain, read_size_dist(rng));
                    std::string buf(chunk, '\0');
                    Slice read_slice(buf.data(), chunk);
                    size_t bytes_read = 0;
                    ASSERT_TRUE(reader->read_at(verified, read_slice, &bytes_read, &io_ctx).ok());
                    ASSERT_EQ(bytes_read, chunk);
                    ASSERT_EQ(0, std::memcmp(buf.data(), expected.data() + verified, chunk));
                    verified += chunk;
                }

                success_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& th : workers) {
        th.join();
    }

    EXPECT_EQ(success_count.load(), kThreadCount * kIterationPerThread);
}

} // namespace doris::io

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

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "core/block/block.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/obj_storage_client.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "io/fs/s3_obj_storage_client.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "storage/index/inverted/inverted_index_writer.h"
#include "storage/options.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment_index_file_cache_loader.h"
#include "storage/segment/segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "util/defer_op.h"
#include "util/threadpool.h"
#include "util/time.h"

namespace doris {
namespace {

using segment_v2::SegmentIndexFileCacheLoadContext;
using segment_v2::SegmentIndexFileCacheLoadReason;
using segment_v2::SegmentIndexFileCachePreloadTask;

constexpr int64_t kIndexOnlyTabletId = 10005;
constexpr int64_t kIndexOnlyPartitionId = 10006;
constexpr int64_t kIndexOnlyTabletSchemaHash = 10007;
constexpr std::string_view kTestDir = "ut_dir/cloud_file_cache_write_index_only_e2e";
constexpr std::string_view kTmpDir = "ut_dir/cloud_file_cache_write_index_only_e2e/tmp";

bool has_suffix(std::string_view value, std::string_view suffix) {
    return value.size() >= suffix.size() && value.substr(value.size() - suffix.size()) == suffix;
}

struct CreatedS3File {
    std::string path;
    FileType file_type;
    bool is_s3_writer = false;
    bool has_cache_builder = false;
    bool write_file_cache = false;
    bool allow_adaptive_file_cache_write = false;
    uint64_t approximate_bytes_to_write = 0;
    size_t bytes_appended = 0;
    bool saw_put_object = false;
};

struct ObservedIndexPreload {
    SegmentIndexFileCacheLoadReason reason;
    uint32_t segment_id = 0;
    std::string segment_path;
    uint64_t range_offset = 0;
    uint64_t range_size = 0;
    uint64_t segment_file_size = 0;
    int closed_segment_files = 0;
};

struct WriterFlushCounters {
    int vertical_segment_writer_flush = 0;
    int segment_writer_final_flush = 0;
};

struct S3WriteCounters {
    int segment_file_close = 0;
    int open_file = 0;
};

class InMemoryObjStorageClient final : public io::ObjStorageClient {
public:
    io::ObjectStorageUploadResponse create_multipart_upload(
            const io::ObjectStoragePathOptions& /*opts*/) override {
        return {.resp = io::ObjectStorageResponse::OK(), .upload_id = "test-upload-id"};
    }

    io::ObjectStorageResponse put_object(const io::ObjectStoragePathOptions& opts,
                                         std::string_view stream) override {
        std::lock_guard lock(_mutex);
        _objects[opts.key] = std::string(stream);
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageUploadResponse upload_part(const io::ObjectStoragePathOptions& /*opts*/,
                                                std::string_view stream, int part_num) override {
        std::lock_guard lock(_mutex);
        _parts[part_num] = std::string(stream);
        return {.resp = io::ObjectStorageResponse::OK(), .etag = "test-etag"};
    }

    io::ObjectStorageResponse complete_multipart_upload(
            const io::ObjectStoragePathOptions& opts,
            const std::vector<io::ObjectCompleteMultiPart>& completed_parts) override {
        std::lock_guard lock(_mutex);
        std::string object;
        for (const auto& part : completed_parts) {
            object.append(_parts.at(part.part_num));
        }
        _objects[opts.key] = std::move(object);
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageHeadResponse head_object(const io::ObjectStoragePathOptions& opts) override {
        std::lock_guard lock(_mutex);
        return {.resp = io::ObjectStorageResponse::OK(),
                .file_size = static_cast<int64_t>(_objects.at(opts.key).size())};
    }

    io::ObjectStorageResponse get_object(const io::ObjectStoragePathOptions& opts, void* buffer,
                                         size_t offset, size_t bytes_read,
                                         size_t* size_return) override {
        std::lock_guard lock(_mutex);
        const auto& object = _objects.at(opts.key);
        const size_t read_size = std::min(bytes_read, object.size() - offset);
        std::memcpy(buffer, object.data() + offset, read_size);
        *size_return = read_size;
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse list_objects(const io::ObjectStoragePathOptions& /*opts*/,
                                           std::vector<io::FileInfo>* /*files*/) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse delete_objects(const io::ObjectStoragePathOptions& /*opts*/,
                                             std::vector<std::string> /*objects*/) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse delete_object(const io::ObjectStoragePathOptions& /*opts*/) override {
        return io::ObjectStorageResponse::OK();
    }

    io::ObjectStorageResponse delete_objects_recursively(
            const io::ObjectStoragePathOptions& /*opts*/) override {
        return io::ObjectStorageResponse::OK();
    }

    std::string generate_presigned_url(const io::ObjectStoragePathOptions& /*opts*/,
                                       int64_t /*expiration_secs*/,
                                       const S3ClientConf& /*conf*/) override {
        return {};
    }

    std::string object() const {
        std::lock_guard lock(_mutex);
        return _objects.begin()->second;
    }

private:
    mutable std::mutex _mutex;
    std::map<int, std::string> _parts;
    std::map<std::string, std::string> _objects;
};

} // namespace

class CloudFileCacheWriteIndexOnlyConfigTest : public testing::Test {
protected:
    void SetUp() override {
        _origin_index_only = config::enable_file_cache_write_index_file_only;
        _origin_enable_file_cache = config::enable_file_cache;
        _origin_cloud_unique_id = config::cloud_unique_id;
    }

    void TearDown() override {
        auto sp = SyncPoint::get_instance();
        sp->disable_processing();
        sp->clear_all_call_backs();
        sp->clear_trace();

        config::enable_file_cache_write_index_file_only = _origin_index_only;
        config::enable_file_cache = _origin_enable_file_cache;
        config::cloud_unique_id = _origin_cloud_unique_id;
    }

private:
    bool _origin_index_only = false;
    bool _origin_enable_file_cache = false;
    std::string _origin_cloud_unique_id;
};

class CloudFileCacheWriteIndexOnlyTest : public testing::Test {
protected:
    void SetUp() override {
        _origin_index_only = config::enable_file_cache_write_index_file_only;
        _origin_enable_file_cache = config::enable_file_cache;
        _origin_enable_flush_file_cache_async = config::enable_flush_file_cache_async;
        _origin_cloud_unique_id = config::cloud_unique_id;
        _origin_enable_packed_file = config::enable_packed_file;
        _origin_enable_vertical_segment_writer = config::enable_vertical_segment_writer;

        config::enable_file_cache_write_index_file_only = true;
        config::enable_file_cache = true;
        config::enable_flush_file_cache_async = false;
        config::cloud_unique_id = "cloud_file_cache_write_index_only_e2e";
        config::enable_packed_file = false;
        config::enable_vertical_segment_writer = true;

        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(std::string(kTestDir)).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(std::string(kTestDir)).ok());

        _origin_file_cache_factory = ExecEnv::GetInstance()->_file_cache_factory;
        _owned_file_cache_factory = std::make_unique<io::FileCacheFactory>();
        ExecEnv::GetInstance()->_file_cache_factory = _owned_file_cache_factory.get();
        io::FileCacheSettings settings;
        settings.query_queue_size = 64 * 1024 * 1024;
        settings.query_queue_elements = 64;
        settings.index_queue_size = 64 * 1024 * 1024;
        settings.index_queue_elements = 64;
        settings.disposable_queue_size = 1024 * 1024;
        settings.disposable_queue_elements = 16;
        settings.capacity = 128 * 1024 * 1024;
        settings.max_file_block_size = config::file_cache_each_block_size;
        settings.max_query_cache_size = 0;
        settings.storage = "memory";
        ASSERT_TRUE(io::FileCacheFactory::instance()->create_file_cache("memory", settings).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(std::string(kTmpDir), -1);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        if (ExecEnv::GetInstance()->s3_file_upload_thread_pool() == nullptr) {
            std::unique_ptr<ThreadPool> pool;
            ASSERT_TRUE(ThreadPoolBuilder("cloud_file_cache_write_index_only_s3_upload")
                                .set_min_threads(4)
                                .set_max_threads(4)
                                .build(&pool)
                                .ok());
            ExecEnv::GetInstance()->_s3_file_upload_thread_pool = std::move(pool);
            _created_s3_upload_pool = true;
        }

        S3Conf s3_conf;
        s3_conf.client_conf.ak = "fake_ak";
        s3_conf.client_conf.sk = "fake_sk";
        s3_conf.client_conf.endpoint = "fake_s3_endpoint";
        s3_conf.client_conf.region = "fake_s3_region";
        s3_conf.bucket = "fake_s3_bucket";
        s3_conf.prefix = "cloud_file_cache_write_index_only_e2e";
        auto fs = io::S3FileSystem::create(std::move(s3_conf), "cloud-file-cache-index-only-ut-fs");
        ASSERT_TRUE(fs.has_value()) << fs.error();
        _remote_fs = fs.value();

        auto engine = std::make_unique<StorageEngine>(EngineOptions {});
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        auto sp = SyncPoint::get_instance();
        sp->disable_processing();
        sp->clear_all_call_backs();
        sp->clear_trace();

        _remote_fs.reset();
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);

        if (_created_s3_upload_pool) {
            ExecEnv::GetInstance()->_s3_file_upload_thread_pool.reset();
        }
        ExecEnv::GetInstance()->set_tmp_file_dir(nullptr);

        _owned_file_cache_factory.reset();
        ExecEnv::GetInstance()->_file_cache_factory = _origin_file_cache_factory;

        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(std::string(kTestDir)).ok());

        config::enable_file_cache_write_index_file_only = _origin_index_only;
        config::enable_file_cache = _origin_enable_file_cache;
        config::enable_flush_file_cache_async = _origin_enable_flush_file_cache_async;
        config::cloud_unique_id = _origin_cloud_unique_id;
        config::enable_packed_file = _origin_enable_packed_file;
        config::enable_vertical_segment_writer = _origin_enable_vertical_segment_writer;
    }

    TabletSchemaSPtr create_schema(bool with_inverted_index = false,
                                   InvertedIndexStorageFormatPB inverted_index_storage_format =
                                           InvertedIndexStorageFormatPB::V2) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(KeysType::DUP_KEYS);
        tablet_schema_pb.set_num_short_key_columns(1);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(3);

        auto* key = tablet_schema_pb.add_column();
        key->set_unique_id(1);
        key->set_name("k1");
        key->set_type("INT");
        key->set_is_key(true);
        key->set_length(4);
        key->set_index_length(4);
        key->set_is_nullable(false);
        key->set_is_bf_column(false);

        auto* value = tablet_schema_pb.add_column();
        value->set_unique_id(2);
        value->set_name("v1");
        value->set_type("INT");
        value->set_is_key(false);
        value->set_length(4);
        value->set_index_length(4);
        value->set_is_nullable(false);
        value->set_is_bf_column(false);

        if (with_inverted_index) {
            tablet_schema_pb.set_inverted_index_storage_format(inverted_index_storage_format);
            auto* index = tablet_schema_pb.add_index();
            index->set_index_id(10000);
            index->set_index_name("v1_idx");
            index->set_index_type(IndexType::INVERTED);
            index->add_col_unique_id(2);
        }

        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    RowsetWriterContext create_context(const TabletSchemaSPtr& tablet_schema,
                                       DataWriteType write_type = DataWriteType::TYPE_DEFAULT,
                                       ReaderType compaction_type = ReaderType::UNKNOWN) {
        RowsetId rowset_id;
        rowset_id.init(_next_rowset_id++);

        RowsetWriterContext context;
        context.rowset_id = rowset_id;
        context.tablet_id = kIndexOnlyTabletId;
        context.partition_id = kIndexOnlyPartitionId;
        context.tablet_schema_hash = kIndexOnlyTabletSchemaHash;
        context.rowset_type = BETA_ROWSET;
        context.tablet_schema = tablet_schema;
        context.rowset_state = VISIBLE;
        context.version = Version(_next_rowset_id, _next_rowset_id);
        context.segments_overlap = OVERLAPPING;
        context.max_rows_per_segment = UINT32_MAX;
        context.data_dir = nullptr;
        context.write_type = write_type;
        context.compaction_type = compaction_type;
        context.storage_resource = StorageResource(_remote_fs);
        context.tablet_path = "unused_local_tablet_path";
        context.write_file_cache = true;
        context.approximate_bytes_to_write = 4096;
        context.newest_write_timestamp = UnixSeconds();
        context.allow_packed_file = false;
        context.encrypt_algorithm = EncryptionAlgorithmPB::PLAINTEXT;
        return context;
    }

    Block create_full_block(const TabletSchemaSPtr& tablet_schema, int32_t start_key = 1) {
        auto block = tablet_schema->create_block();
        auto columns = std::move(block).mutate_columns();
        for (int32_t i = 0; i < 8; ++i) {
            int32_t key = start_key + i;
            int32_t value = key * 10;
            columns[0]->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
            columns[1]->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
        }
        block.set_columns(std::move(columns));
        return block;
    }

    Block create_column_block(const TabletSchemaSPtr& tablet_schema,
                              const std::vector<uint32_t>& column_ids, int32_t row_count = 8,
                              int32_t start_key = 1) {
        auto block = tablet_schema->create_block(column_ids);
        auto columns = std::move(block).mutate_columns();
        for (int32_t i = 0; i < row_count; ++i) {
            int32_t key = start_key + i;
            int32_t value = column_ids[0] == 0 ? key : key * 10;
            columns[0]->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
        }
        block.set_columns(std::move(columns));
        return block;
    }

    void install_observers(
            std::vector<ObservedIndexPreload>* observed, std::vector<CreatedS3File>* created_files,
            int* preload_task_count, WriterFlushCounters* writer_flush_counters,
            S3WriteCounters* s3_write_counters, SyncPoint::CallbackGuard* load_guard,
            SyncPoint::CallbackGuard* task_guard, SyncPoint::CallbackGuard* vertical_writer_guard,
            SyncPoint::CallbackGuard* segment_writer_guard,
            SyncPoint::CallbackGuard* s3_client_guard, SyncPoint::CallbackGuard* s3_put_guard,
            SyncPoint::CallbackGuard* create_file_guard, SyncPoint::CallbackGuard* close_file_guard,
            SyncPoint::CallbackGuard* s3_open_file_guard) {
        auto sp = SyncPoint::get_instance();
        sp->clear_all_call_backs();
        sp->enable_processing();
        sp->set_call_back(
                "s3_client_factory::create",
                [](auto&& args) {
                    auto* ret = try_any_cast_ret<std::shared_ptr<io::S3ObjStorageClient>>(args);
                    ret->second = true;
                },
                s3_client_guard);
        sp->set_call_back(
                "S3FileWriter::_put_object",
                [created_files](auto&& args) {
                    auto* writer = try_any_cast<io::S3FileWriter*>(args[0]);
                    for (auto& file : *created_files) {
                        if (has_suffix(writer->path().native(), file.path)) {
                            file.bytes_appended = writer->bytes_appended();
                            file.saw_put_object = true;
                            break;
                        }
                    }
                    auto* should_return = try_any_cast<bool*>(args.back());
                    *should_return = true;
                },
                s3_put_guard);
        sp->set_call_back(
                "BaseBetaRowsetWriter::_create_file_writer",
                [created_files](auto&& args) {
                    auto* path = try_any_cast<const std::string*>(args[0]);
                    auto* file_type = try_any_cast<FileType*>(args[1]);
                    auto* writer = try_any_cast<io::FileWriter*>(args[2]);
                    auto* opts = try_any_cast<io::FileWriterOptions*>(args[3]);
                    created_files->push_back(CreatedS3File {
                            .path = *path,
                            .file_type = *file_type,
                            .is_s3_writer = dynamic_cast<io::S3FileWriter*>(writer) != nullptr,
                            .has_cache_builder = writer->cache_builder() != nullptr,
                            .write_file_cache = opts->write_file_cache,
                            .allow_adaptive_file_cache_write =
                                    opts->allow_adaptive_file_cache_write,
                            .approximate_bytes_to_write = opts->approximate_bytes_to_write});
                },
                create_file_guard);
        sp->set_call_back(
                "SegmentFileCollection::close_file_writer",
                [s3_write_counters](auto&& args) {
                    auto* writer = try_any_cast<io::FileWriter*>(args[0]);
                    if (has_suffix(writer->path().native(), ".dat")) {
                        ++s3_write_counters->segment_file_close;
                    }
                },
                close_file_guard);
        sp->set_call_back(
                "S3FileSystem::open_file_internal",
                [s3_write_counters](auto&& /*args*/) { ++s3_write_counters->open_file; },
                s3_open_file_guard);
        sp->set_call_back(
                "SegmentIndexFileCacheLoader::preload_segment_indexes_to_file_cache",
                [preload_task_count](auto&& args) {
                    auto* tasks =
                            try_any_cast<const std::vector<SegmentIndexFileCachePreloadTask>*>(
                                    args[1]);
                    *preload_task_count += static_cast<int>(tasks->size());
                },
                task_guard);
        sp->set_call_back(
                "SegmentIndexFileCacheLoader::load_segment_index_to_file_cache",
                [observed, s3_write_counters](auto&& args) {
                    auto* ctx = try_any_cast<const SegmentIndexFileCacheLoadContext*>(args[0]);
                    auto* io_ctx = try_any_cast<io::IOContext*>(args[1]);
                    EXPECT_TRUE(io_ctx->is_index_data);
                    EXPECT_TRUE(io_ctx->is_dryrun);
                    EXPECT_FALSE(io_ctx->is_warmup);
                    observed->push_back(ObservedIndexPreload {
                            .reason = ctx->reason,
                            .segment_id = ctx->segment_id,
                            .segment_path = ctx->segment_path,
                            .range_offset = ctx->range.offset,
                            .range_size = ctx->range.size,
                            .segment_file_size = ctx->segment_file_size,
                            .closed_segment_files = s3_write_counters->segment_file_close});

                    auto* ret = try_any_cast_ret<Status>(args);
                    ret->first = Status::OK();
                    ret->second = true;
                },
                load_guard);
        sp->set_call_back(
                "SegmentFlusher::flush_vertical_segment_writer",
                [writer_flush_counters](auto&& args) {
                    static_cast<void>(try_any_cast<uint32_t*>(args[0]));
                    ++writer_flush_counters->vertical_segment_writer_flush;
                },
                vertical_writer_guard);
        sp->set_call_back(
                "VerticalBetaRowsetWriter::final_flush_segment_writer",
                [writer_flush_counters](auto&& args) {
                    static_cast<void>(try_any_cast<uint32_t*>(args[0]));
                    ++writer_flush_counters->segment_writer_final_flush;
                },
                segment_writer_guard);
    }

    void expect_segment_write_bypasses_file_cache(const std::vector<CreatedS3File>& created_files) {
        bool saw_segment_file = false;
        for (const auto& file : created_files) {
            if (file.file_type != FileType::SEGMENT_FILE) {
                continue;
            }
            saw_segment_file = true;
            EXPECT_TRUE(file.is_s3_writer) << file.path;
            EXPECT_FALSE(file.write_file_cache) << file.path;
            EXPECT_FALSE(file.allow_adaptive_file_cache_write) << file.path;
            EXPECT_EQ(file.approximate_bytes_to_write, 0) << file.path;
            EXPECT_FALSE(file.has_cache_builder) << file.path;

            auto cache_key = std::filesystem::path(file.path).filename().native();
            auto cache_blocks = io::FileCacheFactory::instance()->get_cache_data_by_path(cache_key);
            EXPECT_TRUE(cache_blocks.empty()) << file.path;
        }
        EXPECT_TRUE(saw_segment_file);
    }

    void expect_inverted_index_writes_file_cache(const std::vector<CreatedS3File>& created_files) {
        bool saw_index_file = false;
        for (const auto& file : created_files) {
            if (file.file_type != FileType::INVERTED_INDEX_FILE) {
                continue;
            }
            saw_index_file = true;
            EXPECT_TRUE(file.is_s3_writer) << file.path;
            EXPECT_TRUE(file.write_file_cache) << file.path;
            EXPECT_FALSE(file.allow_adaptive_file_cache_write) << file.path;
            EXPECT_EQ(file.approximate_bytes_to_write, 0) << file.path;
            EXPECT_TRUE(file.has_cache_builder) << file.path;
            EXPECT_TRUE(file.saw_put_object) << file.path;

            auto cache_key = std::filesystem::path(file.path).filename().native();
            auto cache_blocks = io::FileCacheFactory::instance()->get_cache_data_by_path(cache_key);
            if (file.bytes_appended == 0) {
                EXPECT_TRUE(cache_blocks.empty()) << file.path;
            } else {
                EXPECT_FALSE(cache_blocks.empty())
                        << file.path << ", bytes_appended=" << file.bytes_appended;
            }
        }
        EXPECT_TRUE(saw_index_file);
    }

    void expect_loader_open_file_is_mocked_out(const S3WriteCounters& s3_write_counters) {
        EXPECT_EQ(s3_write_counters.open_file, 0);
    }

    StorageEngine* _engine = nullptr;
    std::shared_ptr<io::S3FileSystem> _remote_fs;

    io::FileCacheFactory* _origin_file_cache_factory = nullptr;
    std::unique_ptr<io::FileCacheFactory> _owned_file_cache_factory;
    bool _created_s3_upload_pool = false;

    bool _origin_index_only = false;
    bool _origin_enable_file_cache = false;
    bool _origin_enable_flush_file_cache_async = false;
    std::string _origin_cloud_unique_id;
    bool _origin_enable_packed_file = false;
    bool _origin_enable_vertical_segment_writer = false;
    int64_t _next_rowset_id = 20000;
};

TEST_F(CloudFileCacheWriteIndexOnlyConfigTest, FileWriterOptionsKeepLegacyWhenIndexOnlyDisabled) {
    config::enable_file_cache_write_index_file_only = false;

    RowsetWriterContext context;
    context.write_file_cache = false;
    context.approximate_bytes_to_write = 12345;

    auto segment_opts = context.get_file_writer_options(FileType::SEGMENT_FILE);
    EXPECT_FALSE(segment_opts.write_file_cache);
    EXPECT_TRUE(segment_opts.allow_adaptive_file_cache_write);
    EXPECT_EQ(segment_opts.approximate_bytes_to_write, 12345);

    auto index_opts = context.get_file_writer_options(FileType::INVERTED_INDEX_FILE);
    EXPECT_FALSE(index_opts.write_file_cache);
    EXPECT_TRUE(index_opts.allow_adaptive_file_cache_write);
    EXPECT_EQ(index_opts.approximate_bytes_to_write, 12345);
}

TEST_F(CloudFileCacheWriteIndexOnlyConfigTest, IndexOnlyOptionsSplitSegmentAndInvertedIndexFiles) {
    config::enable_file_cache_write_index_file_only = true;

    RowsetWriterContext context;
    context.write_file_cache = false;
    context.approximate_bytes_to_write = 12345;

    auto segment_opts = context.get_file_writer_options(FileType::SEGMENT_FILE);
    EXPECT_FALSE(segment_opts.write_file_cache);
    EXPECT_FALSE(segment_opts.allow_adaptive_file_cache_write);
    EXPECT_EQ(segment_opts.approximate_bytes_to_write, 0);

    auto index_opts = context.get_file_writer_options(FileType::INVERTED_INDEX_FILE);
    EXPECT_TRUE(index_opts.write_file_cache);
    EXPECT_FALSE(index_opts.allow_adaptive_file_cache_write);
    EXPECT_EQ(index_opts.approximate_bytes_to_write, 0);
}

TEST_F(CloudFileCacheWriteIndexOnlyConfigTest,
       IndexOnlyIgnoresRequestWriteFileCacheForSegmentData) {
    config::enable_file_cache_write_index_file_only = true;

    RowsetWriterContext context;
    context.write_file_cache = true;
    context.approximate_bytes_to_write = 12345;

    auto segment_opts = context.get_file_writer_options(FileType::SEGMENT_FILE);
    EXPECT_FALSE(segment_opts.write_file_cache);
    EXPECT_FALSE(segment_opts.allow_adaptive_file_cache_write);

    auto index_opts = context.get_file_writer_options(FileType::INVERTED_INDEX_FILE);
    EXPECT_TRUE(index_opts.write_file_cache);
    EXPECT_FALSE(index_opts.allow_adaptive_file_cache_write);
}

TEST_F(CloudFileCacheWriteIndexOnlyConfigTest, SegmentIndexFileCacheLoaderSkipsWhenConfigDisabled) {
    config::enable_file_cache = false;
    config::enable_file_cache_write_index_file_only = true;

    segment_v2::SegmentIndexFileCacheLoadContext context;
    context.range = {.offset = 1, .size = 1};
    context.segment_file_size = 2;

    EXPECT_TRUE(segment_v2::SegmentIndexFileCacheLoader::load_segment_index_to_file_cache(context)
                        .ok());
}

TEST_F(CloudFileCacheWriteIndexOnlyConfigTest,
       SegmentIndexFileCacheLoaderSkipsEmptyRangeBeforeOpenFile) {
    config::enable_file_cache = true;
    config::enable_file_cache_write_index_file_only = true;
    config::cloud_unique_id = "cloud_file_cache_empty_range_ut";

    S3Conf s3_conf;
    s3_conf.client_conf.ak = "fake_ak";
    s3_conf.client_conf.sk = "fake_sk";
    s3_conf.client_conf.endpoint = "fake_s3_endpoint";
    s3_conf.client_conf.region = "fake_s3_region";
    s3_conf.bucket = "fake_s3_bucket";
    s3_conf.prefix = "cloud_file_cache_empty_range_ut";
    auto fs = io::S3FileSystem::create(std::move(s3_conf), "cloud-file-cache-empty-range-ut-fs");
    ASSERT_TRUE(fs.has_value()) << fs.error();

    int open_file_count = 0;
    SyncPoint::CallbackGuard s3_open_file_guard;
    auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();
    sp->set_call_back(
            "S3FileSystem::open_file_internal",
            [&open_file_count](auto&& /*args*/) {
                ++open_file_count;
                ADD_FAILURE() << "empty range should return before opening segment";
            },
            &s3_open_file_guard);

    segment_v2::SegmentIndexFileCacheLoadContext context;
    context.fs = fs.value();
    context.segment_path = "empty_range_should_not_open.dat";
    context.tablet_id = kIndexOnlyTabletId;
    context.segment_file_size = 2;

    EXPECT_TRUE(segment_v2::SegmentIndexFileCacheLoader::load_segment_index_to_file_cache(context)
                        .ok());
    EXPECT_EQ(open_file_count, 0);
}

TEST_F(CloudFileCacheWriteIndexOnlyTest,
       LoadUsesVerticalSegmentWriterAndPreloadsAfterAllSegmentFilesClosed) {
    auto tablet_schema = create_schema(true);
    RowsetWriterContext context = create_context(tablet_schema);

    std::vector<ObservedIndexPreload> observed;
    std::vector<CreatedS3File> created_files;
    int preload_task_count = 0;
    WriterFlushCounters writer_flush_counters;
    S3WriteCounters s3_write_counters;
    SyncPoint::CallbackGuard load_guard;
    SyncPoint::CallbackGuard task_guard;
    SyncPoint::CallbackGuard vertical_writer_guard;
    SyncPoint::CallbackGuard segment_writer_guard;
    SyncPoint::CallbackGuard s3_client_guard;
    SyncPoint::CallbackGuard s3_put_guard;
    SyncPoint::CallbackGuard create_file_guard;
    SyncPoint::CallbackGuard close_file_guard;
    SyncPoint::CallbackGuard s3_open_file_guard;
    install_observers(&observed, &created_files, &preload_task_count, &writer_flush_counters,
                      &s3_write_counters, &load_guard, &task_guard, &vertical_writer_guard,
                      &segment_writer_guard, &s3_client_guard, &s3_put_guard, &create_file_guard,
                      &close_file_guard, &s3_open_file_guard);

    auto writer_result = RowsetFactory::create_rowset_writer(*_engine, context, false);
    ASSERT_TRUE(writer_result.has_value()) << writer_result.error();
    auto rowset_writer = std::move(writer_result).value();

    auto block = create_full_block(tablet_schema, 1);
    auto st = rowset_writer->flush_single_block(&block);
    ASSERT_TRUE(st.ok()) << st;
    auto second_block = create_full_block(tablet_schema, 100);
    st = rowset_writer->flush_single_block(&second_block);
    ASSERT_TRUE(st.ok()) << st;

    RowsetSharedPtr rowset;
    st = rowset_writer->build(rowset);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(rowset, nullptr);
    EXPECT_EQ(rowset->rowset_meta()->num_segments(), 2);

    EXPECT_EQ(writer_flush_counters.vertical_segment_writer_flush, 2);
    EXPECT_EQ(writer_flush_counters.segment_writer_final_flush, 0);
    EXPECT_EQ(preload_task_count, 2);
    ASSERT_EQ(observed.size(), 4);
    std::vector<int> ranges_per_segment(2, 0);
    for (const auto& item : observed) {
        ASSERT_LT(item.segment_id, ranges_per_segment.size());
        ++ranges_per_segment[item.segment_id];
        EXPECT_EQ(item.reason, SegmentIndexFileCacheLoadReason::LOAD);
        EXPECT_EQ(item.segment_path, context.segment_path(item.segment_id));
        EXPECT_GT(item.range_offset, 0);
        EXPECT_GT(item.range_size, 0);
        EXPECT_LE(item.range_offset + item.range_size, item.segment_file_size);
        EXPECT_EQ(item.closed_segment_files, 2);
    }
    EXPECT_EQ(ranges_per_segment[0], 2);
    EXPECT_EQ(ranges_per_segment[1], 2);

    expect_segment_write_bypasses_file_cache(created_files);
    expect_inverted_index_writes_file_cache(created_files);
    expect_loader_open_file_is_mocked_out(s3_write_counters);
}

TEST_F(CloudFileCacheWriteIndexOnlyTest,
       VerticalCompactionUsesSegmentWriterAndPreloadsAfterAllSegmentFilesClosed) {
    auto tablet_schema = create_schema(true);
    RowsetWriterContext context = create_context(tablet_schema, DataWriteType::TYPE_COMPACTION,
                                                 ReaderType::READER_CUMULATIVE_COMPACTION);

    std::vector<ObservedIndexPreload> observed;
    std::vector<CreatedS3File> created_files;
    int preload_task_count = 0;
    WriterFlushCounters writer_flush_counters;
    S3WriteCounters s3_write_counters;
    SyncPoint::CallbackGuard load_guard;
    SyncPoint::CallbackGuard task_guard;
    SyncPoint::CallbackGuard vertical_writer_guard;
    SyncPoint::CallbackGuard segment_writer_guard;
    SyncPoint::CallbackGuard s3_client_guard;
    SyncPoint::CallbackGuard s3_put_guard;
    SyncPoint::CallbackGuard create_file_guard;
    SyncPoint::CallbackGuard close_file_guard;
    SyncPoint::CallbackGuard s3_open_file_guard;
    install_observers(&observed, &created_files, &preload_task_count, &writer_flush_counters,
                      &s3_write_counters, &load_guard, &task_guard, &vertical_writer_guard,
                      &segment_writer_guard, &s3_client_guard, &s3_put_guard, &create_file_guard,
                      &close_file_guard, &s3_open_file_guard);

    auto writer_result = RowsetFactory::create_rowset_writer(*_engine, context, true);
    ASSERT_TRUE(writer_result.has_value()) << writer_result.error();
    auto rowset_writer = std::move(writer_result).value();

    std::vector<uint32_t> key_column_ids = {0};
    auto key_block = create_column_block(tablet_schema, key_column_ids, 8, 1);
    auto st = rowset_writer->add_columns(&key_block, key_column_ids, true, 4, false);
    ASSERT_TRUE(st.ok()) << st;
    auto second_key_block = create_column_block(tablet_schema, key_column_ids, 8, 100);
    st = rowset_writer->add_columns(&second_key_block, key_column_ids, true, 4, false);
    ASSERT_TRUE(st.ok()) << st;
    st = rowset_writer->flush_columns(true);
    ASSERT_TRUE(st.ok()) << st;

    std::vector<uint32_t> value_column_ids = {1};
    auto value_block = create_column_block(tablet_schema, value_column_ids, 16, 1);
    st = rowset_writer->add_columns(&value_block, value_column_ids, false, UINT32_MAX, false);
    ASSERT_TRUE(st.ok()) << st;
    st = rowset_writer->flush_columns(false);
    ASSERT_TRUE(st.ok()) << st;
    st = rowset_writer->final_flush();
    ASSERT_TRUE(st.ok()) << st;

    RowsetSharedPtr rowset;
    st = rowset_writer->build(rowset);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(rowset, nullptr);
    EXPECT_EQ(rowset->rowset_meta()->num_segments(), 2);

    EXPECT_EQ(writer_flush_counters.vertical_segment_writer_flush, 0);
    EXPECT_EQ(writer_flush_counters.segment_writer_final_flush, 2);
    EXPECT_EQ(preload_task_count, 2);
    ASSERT_EQ(observed.size(), 6);
    std::vector<int> ranges_per_segment(2, 0);
    for (const auto& item : observed) {
        ASSERT_LT(item.segment_id, ranges_per_segment.size());
        ++ranges_per_segment[item.segment_id];
        EXPECT_EQ(item.reason, SegmentIndexFileCacheLoadReason::CUMULATIVE_COMPACTION);
        EXPECT_EQ(item.segment_path, context.segment_path(item.segment_id));
        EXPECT_GT(item.range_offset, 0);
        EXPECT_GT(item.range_size, 0);
        EXPECT_LE(item.range_offset + item.range_size, item.segment_file_size);
        EXPECT_EQ(item.closed_segment_files, 2);
    }
    EXPECT_EQ(ranges_per_segment[0], 3);
    EXPECT_EQ(ranges_per_segment[1], 3);

    expect_segment_write_bypasses_file_cache(created_files);
    expect_inverted_index_writes_file_cache(created_files);
    expect_loader_open_file_is_mocked_out(s3_write_counters);
}

TEST_F(CloudFileCacheWriteIndexOnlyTest,
       VerticalCompactionV1InvertedIndexUsesIndexOnlyFileWriterOptions) {
    auto tablet_schema = create_schema(true, InvertedIndexStorageFormatPB::V1);
    RowsetWriterContext context = create_context(tablet_schema, DataWriteType::TYPE_COMPACTION,
                                                 ReaderType::READER_CUMULATIVE_COMPACTION);

    std::vector<ObservedIndexPreload> observed;
    std::vector<CreatedS3File> created_files;
    int preload_task_count = 0;
    WriterFlushCounters writer_flush_counters;
    S3WriteCounters s3_write_counters;
    SyncPoint::CallbackGuard load_guard;
    SyncPoint::CallbackGuard task_guard;
    SyncPoint::CallbackGuard vertical_writer_guard;
    SyncPoint::CallbackGuard segment_writer_guard;
    SyncPoint::CallbackGuard s3_client_guard;
    SyncPoint::CallbackGuard s3_put_guard;
    SyncPoint::CallbackGuard create_file_guard;
    SyncPoint::CallbackGuard close_file_guard;
    SyncPoint::CallbackGuard s3_open_file_guard;
    install_observers(&observed, &created_files, &preload_task_count, &writer_flush_counters,
                      &s3_write_counters, &load_guard, &task_guard, &vertical_writer_guard,
                      &segment_writer_guard, &s3_client_guard, &s3_put_guard, &create_file_guard,
                      &close_file_guard, &s3_open_file_guard);
    int index_writer_create_count = 0;
    SyncPoint::CallbackGuard index_writer_create_guard;
    SyncPoint::get_instance()->set_call_back(
            "BaseBetaRowsetWriter::create_inverted_index_file_writer",
            [&index_writer_create_count](auto&& args) {
                static_cast<void>(try_any_cast<uint32_t*>(args[0]));
                ++index_writer_create_count;
            },
            &index_writer_create_guard);

    auto writer_result = RowsetFactory::create_rowset_writer(*_engine, context, true);
    ASSERT_TRUE(writer_result.has_value()) << writer_result.error();
    auto rowset_writer = std::move(writer_result).value();

    std::vector<uint32_t> key_column_ids = {0};
    auto key_block = create_column_block(tablet_schema, key_column_ids, 8, 1);
    auto st = rowset_writer->add_columns(&key_block, key_column_ids, true, 4, false);
    ASSERT_TRUE(st.ok()) << st;

    EXPECT_EQ(writer_flush_counters.vertical_segment_writer_flush, 0);
    EXPECT_EQ(writer_flush_counters.segment_writer_final_flush, 0);
    EXPECT_EQ(preload_task_count, 0);
    EXPECT_EQ(index_writer_create_count, 1);
    expect_segment_write_bypasses_file_cache(created_files);
}

class SegmentWriterFileCacheConcurrencyTest : public CloudFileCacheWriteIndexOnlyTest {
protected:
    void SetUp() override {
        _origin_file_cache_each_block_size = config::file_cache_each_block_size;
        _origin_s3_write_buffer_size = config::s3_write_buffer_size;
        // One S3 part spans four cache blocks. An index range that starts off a cache-block
        // boundary can therefore create a cache block that crosses the S3 part boundary.
        config::file_cache_each_block_size = kCacheBlockSize;
        config::s3_write_buffer_size = kS3WriteBufferSize;
        CloudFileCacheWriteIndexOnlyTest::SetUp();
    }

    void TearDown() override {
        Defer restore_config {[&]() {
            config::file_cache_each_block_size = _origin_file_cache_each_block_size;
            config::s3_write_buffer_size = _origin_s3_write_buffer_size;
        }};
        CloudFileCacheWriteIndexOnlyTest::TearDown();
    }

    static constexpr int64_t kCacheBlockSize = 64;
    static constexpr int64_t kS3WriteBufferSize = 256;

private:
    int64_t _origin_file_cache_each_block_size = 0;
    int64_t _origin_s3_write_buffer_size = 0;
};

TEST_F(SegmentWriterFileCacheConcurrencyTest, ConcurrentLaterPartMustNotShiftCachedSegmentBytes) {
    // Let the S3 upload buffers populate the file cache. SegmentWriter still allocates the final
    // index range and changes those blocks to INDEX during finalize().
    config::enable_file_cache_write_index_file_only = false;

    // Multipart completion assembles parts by part number, independently of the cache-write order
    // forced below. The resulting object is the source of truth for the expected segment bytes.
    auto mock_client = std::make_shared<InMemoryObjStorageClient>();
    _remote_fs->client_holder()->_client = mock_client;

    auto* sync_point = SyncPoint::get_instance();
    sync_point->clear_all_call_backs();
    sync_point->enable_processing();

    struct UploadOrder {
        std::mutex mutex;
        std::condition_variable cv;
        std::set<size_t> entered_offsets;
        std::set<size_t> released_offsets;
        std::set<size_t> completed_offsets;
        bool release_all = false;
    } upload_order;

    // Pause the first two S3 parts immediately before each part allocates its cache holder. They
    // will be released in a deterministic order from the SegmentWriter finalize callback.
    SyncPoint::CallbackGuard upload_cache_guard;
    sync_point->set_call_back(
            "UploadFileBuffer::upload_to_local_file_cache",
            [&](auto&& args) {
                auto* buffer = try_any_cast<io::UploadFileBuffer*>(args[0]);
                const size_t offset = buffer->get_file_offset();
                if (offset != 0 && offset != kS3WriteBufferSize) {
                    return;
                }
                std::unique_lock lock(upload_order.mutex);
                upload_order.entered_offsets.insert(offset);
                upload_order.cv.notify_all();
                upload_order.cv.wait(lock, [&]() {
                    return upload_order.release_all ||
                           upload_order.released_offsets.contains(offset);
                });
            },
            &upload_cache_guard);
    SyncPoint::CallbackGuard upload_cache_done_guard;
    sync_point->set_call_back(
            "UploadFileBuffer::upload_to_local_file_cache_done",
            [&](auto&& args) {
                auto* buffer = try_any_cast<io::UploadFileBuffer*>(args[0]);
                const size_t offset = buffer->get_file_offset();
                if (offset != 0 && offset != kS3WriteBufferSize) {
                    return;
                }
                std::lock_guard lock(upload_order.mutex);
                upload_order.completed_offsets.insert(offset);
                upload_order.cv.notify_all();
            },
            &upload_cache_done_guard);
    Defer release_uploads_on_exit {[&]() {
        {
            std::lock_guard lock(upload_order.mutex);
            upload_order.release_all = true;
        }
        upload_order.cv.notify_all();
    }};

    const std::string segment_path = "segment_writer_concurrent_file_cache_0.dat";
    io::FileWriterOptions file_options;
    file_options.write_file_cache = true;
    io::FileWriterPtr file_writer;
    auto status = _remote_fs->create_file(segment_path, &file_writer, &file_options);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(file_writer->cache_builder(), nullptr);
    const auto cache_hash = file_writer->cache_builder()->_cache_hash;
    auto* cache = file_writer->cache_builder()->_cache;
    bool observed_index_cache_holder = false;
    std::optional<io::FileBlock::Range> crossing_range;
    SyncPoint::CallbackGuard index_cache_holder_guard;
    sync_point->set_call_back(
            "SegmentWriter::finalize::index_cache_holder",
            [&](auto&& args) {
                auto* holder = try_any_cast<io::FileBlocksHolder*>(args[0]);
                ASSERT_NE(holder, nullptr);
                // This callback runs after the unaligned index range has been allocated and its
                // cache type changed. Record the block shared by the two adjacent S3 parts.
                auto crossing_block =
                        std::find_if(holder->file_blocks.begin(), holder->file_blocks.end(),
                                     [](const auto& block) {
                                         return block->range().left < kS3WriteBufferSize &&
                                                block->range().right >= kS3WriteBufferSize;
                                     });
                EXPECT_NE(crossing_block, holder->file_blocks.end());
                if (crossing_block != holder->file_blocks.end()) {
                    crossing_range.emplace((*crossing_block)->range());
                    EXPECT_EQ((*crossing_block)->state(), io::FileBlock::State::EMPTY);
                    EXPECT_EQ((*crossing_block)->cache_type(), io::FileCacheType::INDEX);
                }
                observed_index_cache_holder = true;

                std::unique_lock lock(upload_order.mutex);
                const auto both_parts_entered =
                        upload_order.cv.wait_for(lock, std::chrono::seconds(10), [&]() {
                            return upload_order.entered_offsets.contains(0) &&
                                   upload_order.entered_offsets.contains(kS3WriteBufferSize);
                        });
                EXPECT_TRUE(both_parts_entered);
                if (!both_parts_entered) {
                    upload_order.release_all = true;
                    lock.unlock();
                    upload_order.cv.notify_all();
                    return;
                }

                // Make the later part claim and finalize the crossing block first. Its cache-copy
                // loop starts at position zero in the later part buffer even though the shared
                // block starts before this part. The first part then observes a DOWNLOADED block
                // and cannot replace the shifted bytes.
                upload_order.released_offsets.insert(kS3WriteBufferSize);
                upload_order.cv.notify_all();
                const auto later_part_completed =
                        upload_order.cv.wait_for(lock, std::chrono::seconds(10), [&]() {
                            return upload_order.completed_offsets.contains(kS3WriteBufferSize);
                        });
                EXPECT_TRUE(later_part_completed);

                upload_order.released_offsets.insert(0);
                upload_order.cv.notify_all();
                const auto first_part_completed = upload_order.cv.wait_for(
                        lock, std::chrono::seconds(10),
                        [&]() { return upload_order.completed_offsets.contains(0); });
                EXPECT_TRUE(first_part_completed);
            },
            &index_cache_holder_guard);

    auto tablet_schema = create_schema();
    TabletMetaPB tablet_meta_pb;
    tablet_meta_pb.set_tablet_id(kIndexOnlyTabletId);
    tablet_meta_pb.set_schema_hash(kIndexOnlyTabletSchemaHash);
    tablet_meta_pb.set_tablet_state(PB_RUNNING);
    tablet_schema->to_schema_pb(tablet_meta_pb.mutable_schema());
    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(tablet_meta_pb);
    auto tablet = std::make_shared<Tablet>(*_engine, tablet_meta, nullptr);
    auto rowset_context = create_context(tablet_schema);
    segment_v2::SegmentWriterOptions writer_options;
    writer_options.compression_type = NO_COMPRESSION;
    writer_options.rowset_ctx = &rowset_context;
    writer_options.write_type = rowset_context.write_type;
    segment_v2::SegmentWriter writer(file_writer.get(), 0, tablet_schema, tablet, nullptr,
                                     writer_options, nullptr);
    status = writer.init();
    ASSERT_TRUE(status.ok()) << status;
    auto block = create_full_block(tablet_schema);
    status = writer.append_block(&block, 0, block.rows());
    ASSERT_TRUE(status.ok()) << status;

    uint64_t segment_file_size = 0;
    uint64_t index_size = 0;
    segment_v2::SegmentIndexFileCacheInfo index_cache_info;
    status = writer.finalize(&segment_file_size, &index_size, &index_cache_info);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_GT(segment_file_size, 2 * kS3WriteBufferSize);
    ASSERT_GT(index_size, 0);
    ASSERT_GT(index_cache_info.cache_start_offset(), 0);
    ASSERT_LT(index_cache_info.cache_start_offset(), kS3WriteBufferSize);
    ASSERT_NE(index_cache_info.cache_start_offset() % kCacheBlockSize, 0);
    EXPECT_TRUE(observed_index_cache_holder);

    const auto close_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    do {
        status = file_writer->try_finish_close();
        if (status.is<ErrorCode::NEED_SEND_AGAIN>()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    } while (status.is<ErrorCode::NEED_SEND_AGAIN>() &&
             std::chrono::steady_clock::now() < close_deadline);
    ASSERT_TRUE(status.ok()) << status;

    const std::string remote_object = mock_client->object();
    ASSERT_EQ(remote_object.size(), segment_file_size);
    ASSERT_TRUE(crossing_range.has_value());

    auto cache_blocks = cache->get_blocks_by_key(cache_hash);
    ASSERT_FALSE(cache_blocks.empty());
    auto crossing_block = cache_blocks.find(crossing_range->left);
    ASSERT_NE(crossing_block, cache_blocks.end());
    ASSERT_EQ(crossing_block->second->range().left, crossing_range->left);
    ASSERT_EQ(crossing_block->second->range().right, crossing_range->right);
    ASSERT_EQ(crossing_block->second->state(), io::FileBlock::State::DOWNLOADED);
    ASSERT_EQ(crossing_block->second->cache_type(), io::FileCacheType::INDEX);

    // First compare the cache entry with the correctly assembled remote object. This correctness
    // assertion exposes the corruption without going through the reader layer.
    std::string cached_bytes(crossing_range->size(), '\0');
    status = crossing_block->second->read(Slice(cached_bytes.data(), cached_bytes.size()), 0);
    ASSERT_TRUE(status.ok()) << status;
    const std::string expected_bytes =
            remote_object.substr(crossing_range->left, crossing_range->size());
    EXPECT_EQ(cached_bytes, expected_bytes)
            << "cache block " << crossing_range->to_string()
            << " was populated by the later S3 buffer at offset " << kS3WriteBufferSize;

    // Then read through the normal cached remote-file reader to demonstrate the same corruption
    // is visible to a cache consumer, rather than only through FileBlock test internals.
    io::FileReaderOptions reader_options;
    reader_options.cache_type = io::FileCachePolicy::FILE_BLOCK_CACHE;
    reader_options.is_doris_table = true;
    reader_options.file_size = static_cast<int64_t>(segment_file_size);
    reader_options.tablet_id = kIndexOnlyTabletId;
    io::FileReaderSPtr cached_reader;
    status = _remote_fs->open_file(segment_path, &cached_reader, &reader_options);
    ASSERT_TRUE(status.ok()) << status;
    std::string reader_bytes(crossing_range->size(), '\0');
    size_t bytes_read = 0;
    status = cached_reader->read_at(crossing_range->left,
                                    Slice(reader_bytes.data(), reader_bytes.size()), &bytes_read);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(bytes_read, expected_bytes.size());
    EXPECT_EQ(reader_bytes, expected_bytes)
            << "cached S3 reader returned shifted bytes for " << crossing_range->to_string();
}

} // namespace doris

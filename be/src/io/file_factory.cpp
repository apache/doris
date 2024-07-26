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

#include "io/file_factory.h"

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>

#include <mutex>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/broker_file_system.h"
#include "io/fs/broker_file_writer.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs_file_reader.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/hdfs_file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/multi_table_pipe.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "io/fs/stream_load_pipe.h"
#include "io/hdfs_builder.h"
#include "io/hdfs_util.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"
#include "util/uid_util.h"

namespace doris {

constexpr std::string_view RANDOM_CACHE_BASE_PATH = "random";

io::FileReaderOptions FileFactory::get_reader_options(RuntimeState* state,
                                                      const io::FileDescription& fd) {
    io::FileReaderOptions opts {
            .cache_base_path {},
            .file_size = fd.file_size,
            .mtime = fd.mtime,
    };
    if (config::enable_file_cache && state != nullptr &&
        state->query_options().__isset.enable_file_cache &&
        state->query_options().enable_file_cache) {
        opts.cache_type = io::FileCachePolicy::FILE_BLOCK_CACHE;
    }
    if (state != nullptr && state->query_options().__isset.file_cache_base_path &&
        state->query_options().file_cache_base_path != RANDOM_CACHE_BASE_PATH) {
        opts.cache_base_path = state->query_options().file_cache_base_path;
    }
    return opts;
}

Result<io::FileSystemSPtr> FileFactory::create_fs(const io::FSPropertiesRef& fs_properties,
                                                  const io::FileDescription& file_description) {
    switch (fs_properties.type) {
    case TFileType::FILE_LOCAL:
        return io::global_local_filesystem();
    case TFileType::FILE_BROKER:
        return io::BrokerFileSystem::create((*fs_properties.broker_addresses)[0],
                                            *fs_properties.properties, io::FileSystem::TMP_FS_ID);
    case TFileType::FILE_S3: {
        S3URI s3_uri(file_description.path);
        RETURN_IF_ERROR_RESULT(s3_uri.parse());
        S3Conf s3_conf;
        RETURN_IF_ERROR_RESULT(S3ClientFactory::convert_properties_to_s3_conf(
                *fs_properties.properties, s3_uri, &s3_conf));
        return io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID);
    }
    case TFileType::FILE_HDFS:
        return fs_properties.hdfs_params
                       ? io::HdfsFileSystem::create(*fs_properties.hdfs_params,
                                                    file_description.fs_name,
                                                    io::FileSystem::TMP_FS_ID, nullptr)
                       : io::HdfsFileSystem::create(*fs_properties.properties,
                                                    file_description.fs_name,
                                                    io::FileSystem::TMP_FS_ID, nullptr);
    default:
        return ResultError(Status::InternalError("unsupported fs type: {}",
                                                 std::to_string(fs_properties.type)));
    }
}

Result<io::FileWriterPtr> FileFactory::create_file_writer(
        TFileType::type type, ExecEnv* env, const std::vector<TNetworkAddress>& broker_addresses,
        const std::map<std::string, std::string>& properties, const std::string& path,
        const io::FileWriterOptions& options) {
    io::FileWriterPtr file_writer;
    switch (type) {
    case TFileType::FILE_LOCAL: {
        RETURN_IF_ERROR_RESULT(io::global_local_filesystem()->create_file(path, &file_writer));
        return file_writer;
    }
    case TFileType::FILE_BROKER: {
        return io::BrokerFileWriter::create(env, broker_addresses[0], properties, path);
    }
    case TFileType::FILE_S3: {
        S3URI s3_uri(path);
        RETURN_IF_ERROR_RESULT(s3_uri.parse());
        S3Conf s3_conf;
        RETURN_IF_ERROR_RESULT(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf));
        auto client = std::make_shared<io::ObjClientHolder>(std::move(s3_conf.client_conf));
        RETURN_IF_ERROR_RESULT(client->init());
        return std::make_unique<io::S3FileWriter>(std::move(client), std::move(s3_conf.bucket),
                                                  s3_uri.get_key(), &options);
    }
    case TFileType::FILE_HDFS: {
        THdfsParams hdfs_params = parse_properties(properties);
        std::shared_ptr<io::HdfsHandler> handler;
        RETURN_IF_ERROR_RESULT(io::HdfsHandlerCache::instance()->get_connection(
                hdfs_params, hdfs_params.fs_name, &handler));
        return io::HdfsFileWriter::create(path, handler, hdfs_params.fs_name, &options);
    }
    default:
        return ResultError(
                Status::InternalError("unsupported file writer type: {}", std::to_string(type)));
    }
}

Result<io::FileReaderSPtr> FileFactory::create_file_reader(
        const io::FileSystemProperties& system_properties,
        const io::FileDescription& file_description, const io::FileReaderOptions& reader_options,
        RuntimeProfile* profile) {
    TFileType::type type = system_properties.system_type;
    switch (type) {
    case TFileType::FILE_LOCAL: {
        io::FileReaderSPtr file_reader;
        RETURN_IF_ERROR_RESULT(io::global_local_filesystem()->open_file(
                file_description.path, &file_reader, &reader_options));
        return file_reader;
    }
    case TFileType::FILE_S3: {
        S3URI s3_uri(file_description.path);
        RETURN_IF_ERROR_RESULT(s3_uri.parse());
        S3Conf s3_conf;
        RETURN_IF_ERROR_RESULT(S3ClientFactory::convert_properties_to_s3_conf(
                system_properties.properties, s3_uri, &s3_conf));
        auto client_holder = std::make_shared<io::ObjClientHolder>(s3_conf.client_conf);
        RETURN_IF_ERROR_RESULT(client_holder->init());
        return io::S3FileReader::create(std::move(client_holder), s3_conf.bucket, s3_uri.get_key(),
                                        file_description.file_size, profile)
                .and_then([&](auto&& reader) {
                    return io::create_cached_file_reader(std::move(reader), reader_options);
                });
    }
    case TFileType::FILE_HDFS: {
        std::shared_ptr<io::HdfsHandler> handler;
        // FIXME(plat1ko): Explain the difference between `system_properties.hdfs_params.fs_name`
        // and `file_description.fs_name`, it's so confused.
        const auto* fs_name = &file_description.fs_name;
        if (fs_name->empty()) {
            fs_name = &system_properties.hdfs_params.fs_name;
        }
        RETURN_IF_ERROR_RESULT(io::HdfsHandlerCache::instance()->get_connection(
                system_properties.hdfs_params, *fs_name, &handler));
        return io::HdfsFileReader::create(file_description.path, handler->hdfs_fs, *fs_name,
                                          reader_options, profile)
                .and_then([&](auto&& reader) {
                    return io::create_cached_file_reader(std::move(reader), reader_options);
                });
    }
    case TFileType::FILE_BROKER: {
        // TODO(plat1ko): Create `FileReader` without FS
        return io::BrokerFileSystem::create(system_properties.broker_addresses[0],
                                            system_properties.properties, io::FileSystem::TMP_FS_ID)
                .and_then([&](auto&& fs) -> Result<io::FileReaderSPtr> {
                    io::FileReaderSPtr file_reader;
                    RETURN_IF_ERROR_RESULT(
                            fs->open_file(file_description.path, &file_reader, &reader_options));
                    return file_reader;
                });
    }
    default:
        return ResultError(
                Status::InternalError("unsupported file reader type: {}", std::to_string(type)));
    }
}

// file scan node/stream load pipe
Status FileFactory::create_pipe_reader(const TUniqueId& load_id, io::FileReaderSPtr* file_reader,
                                       RuntimeState* runtime_state, bool need_schema) {
    auto stream_load_ctx = ExecEnv::GetInstance()->new_load_stream_mgr()->get(load_id);
    if (!stream_load_ctx) {
        return Status::InternalError("unknown stream load id: {}", UniqueId(load_id).to_string());
    }
    if (need_schema) {
        // Here, a portion of the data is processed to parse column information
        auto pipe = std::make_shared<io::StreamLoadPipe>(
                io::kMaxPipeBufferedBytes /* max_buffered_bytes */, 64 * 1024 /* min_chunk_size */,
                stream_load_ctx->schema_buffer->pos /* total_length */);
        stream_load_ctx->schema_buffer->flip();
        RETURN_IF_ERROR(pipe->append(stream_load_ctx->schema_buffer));
        RETURN_IF_ERROR(pipe->finish());
        *file_reader = std::move(pipe);
    } else {
        *file_reader = stream_load_ctx->pipe;
    }

    return Status::OK();
}

} // namespace doris

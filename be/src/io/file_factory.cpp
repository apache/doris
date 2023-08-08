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
#include "io/fs/file_reader_options.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/multi_table_pipe.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/stream_load_pipe.h"
#include "io/hdfs_builder.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"
#include "util/uid_util.h"

namespace doris {
namespace io {
class FileWriter;
} // namespace io

static io::FileBlockCachePathPolicy BLOCK_CACHE_POLICY;
static std::string RANDOM_CACHE_BASE_PATH = "random";

io::FileReaderOptions FileFactory::get_reader_options(RuntimeState* state) {
    io::FileCachePolicy cache_policy = io::FileCachePolicy::NO_CACHE;
    if (config::enable_file_cache && state != nullptr &&
        state->query_options().__isset.enable_file_cache &&
        state->query_options().enable_file_cache) {
        cache_policy = io::FileCachePolicy::FILE_BLOCK_CACHE;
    }
    io::FileReaderOptions reader_options(cache_policy, BLOCK_CACHE_POLICY);
    if (state != nullptr && state->query_options().__isset.file_cache_base_path &&
        state->query_options().file_cache_base_path != RANDOM_CACHE_BASE_PATH) {
        reader_options.specify_cache_path(state->query_options().file_cache_base_path);
    }
    return reader_options;
}

io::FileReaderOptions FileFactory::NO_CACHE_READER_OPTIONS =
        FileFactory::get_reader_options(nullptr);

Status FileFactory::create_file_writer(TFileType::type type, ExecEnv* env,
                                       const std::vector<TNetworkAddress>& broker_addresses,
                                       const std::map<std::string, std::string>& properties,
                                       const std::string& path, int64_t start_offset,
                                       std::unique_ptr<io::FileWriter>& file_writer) {
    switch (type) {
    case TFileType::FILE_LOCAL: {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &file_writer));
        break;
    }
    case TFileType::FILE_BROKER: {
        std::shared_ptr<io::BrokerFileSystem> fs;
        RETURN_IF_ERROR(io::BrokerFileSystem::create(broker_addresses[0], properties, &fs));
        RETURN_IF_ERROR(fs->create_file(path, &file_writer));
        break;
    }
    case TFileType::FILE_S3: {
        S3URI s3_uri(path);
        RETURN_IF_ERROR(s3_uri.parse());
        S3Conf s3_conf;
        RETURN_IF_ERROR(
                S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf));
        std::shared_ptr<io::S3FileSystem> fs;
        RETURN_IF_ERROR(io::S3FileSystem::create(s3_conf, "", &fs));
        RETURN_IF_ERROR(fs->create_file(path, &file_writer));
        break;
    }
    case TFileType::FILE_HDFS: {
        THdfsParams hdfs_params = parse_properties(properties);
        std::shared_ptr<io::HdfsFileSystem> fs;
        RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", nullptr, &fs));
        RETURN_IF_ERROR(fs->create_file(path, &file_writer));
        break;
    }
    default:
        return Status::InternalError("unsupported file writer type: {}", std::to_string(type));
    }

    return Status::OK();
}

Status FileFactory::create_file_reader(const io::FileSystemProperties& system_properties,
                                       const io::FileDescription& file_description,
                                       const io::FileReaderOptions& reader_options,
                                       std::shared_ptr<io::FileSystem>* file_system,
                                       io::FileReaderSPtr* file_reader, RuntimeProfile* profile) {
    TFileType::type type = system_properties.system_type;
    switch (type) {
    case TFileType::FILE_LOCAL: {
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_description, reader_options,
                                                                 file_reader));
        break;
    }
    case TFileType::FILE_S3: {
        RETURN_IF_ERROR(create_s3_reader(system_properties.properties, file_description,
                                         reader_options, file_system, file_reader));
        break;
    }
    case TFileType::FILE_HDFS: {
        RETURN_IF_ERROR(create_hdfs_reader(system_properties.hdfs_params, file_description,
                                           reader_options, file_system, file_reader, profile));
        break;
    }
    case TFileType::FILE_BROKER: {
        RETURN_IF_ERROR(create_broker_reader(system_properties.broker_addresses[0],
                                             system_properties.properties, file_description,
                                             reader_options, file_system, file_reader));
        break;
    }
    default:
        return Status::NotSupported("unsupported file reader type: {}", std::to_string(type));
    }
    return Status::OK();
}

// file scan node/stream load pipe
Status FileFactory::create_pipe_reader(const TUniqueId& load_id, io::FileReaderSPtr* file_reader,
                                       RuntimeState* runtime_state) {
    auto stream_load_ctx = ExecEnv::GetInstance()->new_load_stream_mgr()->get(load_id);
    if (!stream_load_ctx) {
        return Status::InternalError("unknown stream load id: {}", UniqueId(load_id).to_string());
    }
    *file_reader = stream_load_ctx->pipe;

    if (file_reader->get() == nullptr) {
        return Status::OK();
    }

    auto multi_table_pipe = std::dynamic_pointer_cast<io::MultiTablePipe>(*file_reader);
    if (multi_table_pipe == nullptr || runtime_state == nullptr) {
        return Status::OK();
    }

    TUniqueId pipe_id;
    if (runtime_state->enable_pipeline_exec()) {
        pipe_id = io::StreamLoadPipe::calculate_pipe_id(runtime_state->query_id(),
                                                        runtime_state->fragment_id());
    } else {
        pipe_id = runtime_state->fragment_instance_id();
    }
    *file_reader = multi_table_pipe->getPipe(pipe_id);
    LOG(INFO) << "create pipe reader for fragment instance: " << pipe_id
              << " pipe: " << (*file_reader).get();

    return Status::OK();
}

Status FileFactory::create_hdfs_reader(const THdfsParams& hdfs_params,
                                       const io::FileDescription& fd,
                                       const io::FileReaderOptions& reader_options,
                                       std::shared_ptr<io::FileSystem>* hdfs_file_system,
                                       io::FileReaderSPtr* reader, RuntimeProfile* profile) {
    std::shared_ptr<io::HdfsFileSystem> fs;
    RETURN_IF_ERROR(io::HdfsFileSystem::create(hdfs_params, "", profile, &fs));
    RETURN_IF_ERROR(fs->open_file(fd, reader_options, reader));
    *hdfs_file_system = std::move(fs);
    return Status::OK();
}

Status FileFactory::create_s3_reader(const std::map<std::string, std::string>& prop,
                                     const io::FileDescription& fd,
                                     const io::FileReaderOptions& reader_options,
                                     std::shared_ptr<io::FileSystem>* s3_file_system,
                                     io::FileReaderSPtr* reader) {
    S3URI s3_uri(fd.path);
    RETURN_IF_ERROR(s3_uri.parse());
    S3Conf s3_conf;
    RETURN_IF_ERROR(S3ClientFactory::convert_properties_to_s3_conf(prop, s3_uri, &s3_conf));
    std::shared_ptr<io::S3FileSystem> fs;
    RETURN_IF_ERROR(io::S3FileSystem::create(std::move(s3_conf), "", &fs));
    RETURN_IF_ERROR(fs->open_file(fd, reader_options, reader));
    *s3_file_system = std::move(fs);
    return Status::OK();
}

Status FileFactory::create_broker_reader(const TNetworkAddress& broker_addr,
                                         const std::map<std::string, std::string>& prop,
                                         const io::FileDescription& fd,
                                         const io::FileReaderOptions& reader_options,
                                         std::shared_ptr<io::FileSystem>* broker_file_system,
                                         io::FileReaderSPtr* reader) {
    std::shared_ptr<io::BrokerFileSystem> fs;
    RETURN_IF_ERROR(io::BrokerFileSystem::create(broker_addr, prop, &fs));
    RETURN_IF_ERROR(fs->open_file(fd, reader_options, reader));
    *broker_file_system = std::move(fs);
    return Status::OK();
}
} // namespace doris

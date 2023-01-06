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

#include "common/status.h"
#include "io/broker_reader.h"
#include "io/broker_writer.h"
#include "io/buffered_reader.h"
#include "io/fs/broker_file_system.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/hdfs_file_reader.h"
#include "io/hdfs_writer.h"
#include "io/local_file_reader.h"
#include "io/local_file_writer.h"
#include "io/s3_reader.h"
#include "io/s3_writer.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/new_load_stream_mgr.h"

namespace doris {

Status FileFactory::create_file_writer(TFileType::type type, ExecEnv* env,
                                       const std::vector<TNetworkAddress>& broker_addresses,
                                       const std::map<std::string, std::string>& properties,
                                       const std::string& path, int64_t start_offset,
                                       std::unique_ptr<FileWriter>& file_writer) {
    switch (type) {
    case TFileType::FILE_LOCAL: {
        file_writer.reset(new LocalFileWriter(path, start_offset));
        break;
    }
    case TFileType::FILE_BROKER: {
        file_writer.reset(new BrokerWriter(env, broker_addresses, properties, path, start_offset));
        break;
    }
    case TFileType::FILE_S3: {
        file_writer.reset(new S3Writer(properties, path, start_offset));
        break;
    }
    case TFileType::FILE_HDFS: {
        RETURN_IF_ERROR(create_hdfs_writer(
                const_cast<std::map<std::string, std::string>&>(properties), path, file_writer));
        break;
    }
    default:
        return Status::InternalError("unsupported file writer type: {}", std::to_string(type));
    }

    return Status::OK();
}

// ============================
// broker scan node/unique ptr
Status FileFactory::create_file_reader(TFileType::type type, ExecEnv* env, RuntimeProfile* profile,
                                       const std::vector<TNetworkAddress>& broker_addresses,
                                       const std::map<std::string, std::string>& properties,
                                       const TBrokerRangeDesc& range, int64_t start_offset,
                                       std::unique_ptr<FileReader>& file_reader) {
    FileReader* file_reader_ptr;
    switch (type) {
    case TFileType::FILE_LOCAL: {
        file_reader_ptr = new LocalFileReader(range.path, start_offset);
        break;
    }
    case TFileType::FILE_BROKER: {
        file_reader_ptr = new BufferedReader(
                profile,
                new BrokerReader(env, broker_addresses, properties, range.path, start_offset,
                                 range.__isset.file_size ? range.file_size : 0));
        break;
    }
    case TFileType::FILE_S3: {
        file_reader_ptr =
                new BufferedReader(profile, new S3Reader(properties, range.path, start_offset));
        break;
    }
    case TFileType::FILE_HDFS: {
        FileReader* hdfs_reader = nullptr;
        hdfs_reader = new HdfsFileReader(range.hdfs_params, range.path, start_offset);
        file_reader_ptr = new BufferedReader(profile, hdfs_reader);
        break;
    }
    default:
        return Status::InternalError("unsupported file reader type: " + std::to_string(type));
    }
    file_reader.reset(file_reader_ptr);

    return Status::OK();
}

// ============================
// file scan node/unique ptr
Status FileFactory::create_file_reader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                                       const std::string& path, int64_t start_offset,
                                       int64_t file_size, int64_t buffer_size,
                                       std::unique_ptr<FileReader>& file_reader) {
    FileReader* file_reader_ptr;
    TFileType::type type = params.file_type;
    switch (type) {
    case TFileType::FILE_LOCAL: {
        file_reader_ptr = new LocalFileReader(path, start_offset);
        break;
    }
    case TFileType::FILE_S3: {
        file_reader_ptr = new S3Reader(params.properties, path, start_offset);
        break;
    }
    case TFileType::FILE_HDFS: {
        file_reader_ptr = new HdfsFileReader(params.hdfs_params, path, start_offset);
        break;
    }
    case TFileType::FILE_BROKER: {
        file_reader_ptr = new BrokerReader(ExecEnv::GetInstance(), params.broker_addresses,
                                           params.properties, path, start_offset, file_size);
        break;
    }
    default:
        return Status::InternalError("unsupported file reader type: {}", std::to_string(type));
    }

    if (buffer_size > 0) {
        file_reader.reset(new BufferedReader(profile, file_reader_ptr, buffer_size));
    } else {
        file_reader.reset(file_reader_ptr);
    }
    return Status::OK();
}

Status FileFactory::create_file_reader(RuntimeProfile* /*profile*/,
                                       const FileSystemProperties& system_properties,
                                       const FileDescription& file_description,
                                       std::unique_ptr<io::FileSystem>* file_system,
                                       io::FileReaderSPtr* file_reader) {
    TFileType::type type = system_properties.system_type;
    io::FileSystem* file_system_ptr = nullptr;
    switch (type) {
    case TFileType::FILE_LOCAL: {
        RETURN_IF_ERROR(
                io::global_local_filesystem()->open_file(file_description.path, file_reader));
        break;
    }
    case TFileType::FILE_S3: {
        RETURN_IF_ERROR(create_s3_reader(system_properties.properties, file_description.path,
                                         &file_system_ptr, file_reader));
        break;
    }
    case TFileType::FILE_HDFS: {
        RETURN_IF_ERROR(create_hdfs_reader(system_properties.hdfs_params, file_description.path,
                                           &file_system_ptr, file_reader));
        break;
    }
    case TFileType::FILE_BROKER: {
        RETURN_IF_ERROR(create_broker_reader(system_properties.broker_addresses[0],
                                             system_properties.properties, file_description,
                                             &file_system_ptr, file_reader));
        break;
    }
    default:
        return Status::NotSupported("unsupported file reader type: {}", std::to_string(type));
    }
    file_system->reset(file_system_ptr);
    return Status::OK();
}

// file scan node/stream load pipe
Status FileFactory::create_pipe_reader(const TUniqueId& load_id, io::FileReaderSPtr* file_reader) {
    *file_reader = ExecEnv::GetInstance()->new_load_stream_mgr()->get(load_id);
    if (!(*file_reader)) {
        return Status::InternalError("unknown stream load id: {}", UniqueId(load_id).to_string());
    }
    return Status::OK();
}

Status FileFactory::create_pipe_reader(const TUniqueId& load_id,
                                       std::shared_ptr<FileReader>& file_reader) {
    file_reader = ExecEnv::GetInstance()->load_stream_mgr()->get(load_id);
    if (!file_reader) {
        return Status::InternalError("unknown stream load id: {}", UniqueId(load_id).to_string());
    }
    return Status::OK();
}

Status FileFactory::create_hdfs_reader(const THdfsParams& hdfs_params, const std::string& path,
                                       io::FileSystem** hdfs_file_system,
                                       io::FileReaderSPtr* reader) {
    *hdfs_file_system = new io::HdfsFileSystem(hdfs_params, "");
    RETURN_IF_ERROR((dynamic_cast<io::HdfsFileSystem*>(*hdfs_file_system))->connect());
    RETURN_IF_ERROR((*hdfs_file_system)->open_file(path, reader));
    return Status::OK();
}

Status FileFactory::create_hdfs_writer(const std::map<std::string, std::string>& properties,
                                       const std::string& path,
                                       std::unique_ptr<FileWriter>& writer) {
    writer.reset(new HDFSWriter(properties, path));
    return Status::OK();
}

Status FileFactory::create_s3_reader(const std::map<std::string, std::string>& prop,
                                     const std::string& path, io::FileSystem** s3_file_system,
                                     io::FileReaderSPtr* reader) {
    S3URI s3_uri(path);
    if (!s3_uri.parse()) {
        return Status::InvalidArgument("s3 uri is invalid: {}", path);
    }
    S3Conf s3_conf;
    RETURN_IF_ERROR(ClientFactory::convert_properties_to_s3_conf(prop, s3_uri, &s3_conf));
    *s3_file_system = new io::S3FileSystem(s3_conf, "");
    RETURN_IF_ERROR((dynamic_cast<io::S3FileSystem*>(*s3_file_system))->connect());
    RETURN_IF_ERROR((*s3_file_system)->open_file(s3_uri.get_key(), reader));
    return Status::OK();
}

Status FileFactory::create_broker_reader(const TNetworkAddress& broker_addr,
                                         const std::map<std::string, std::string>& prop,
                                         const FileDescription& file_description,
                                         io::FileSystem** broker_file_system,
                                         io::FileReaderSPtr* reader) {
    *broker_file_system = new io::BrokerFileSystem(broker_addr, prop, file_description.file_size);
    RETURN_IF_ERROR((dynamic_cast<io::BrokerFileSystem*>(*broker_file_system))->connect());
    RETURN_IF_ERROR((*broker_file_system)->open_file(file_description.path, reader));
    return Status::OK();
}
} // namespace doris

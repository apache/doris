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

#include "vec/exec/format/file_reader/new_file_factory.h"

#include "io/broker_reader.h"
#include "io/broker_writer.h"
#include "io/buffered_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/hdfs_reader_writer.h"
#include "io/local_file_reader.h"
#include "io/local_file_writer.h"
#include "io/s3_reader.h"
#include "io/s3_writer.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "util/s3_util.h"

namespace doris {

Status NewFileFactory::create_file_writer(TFileType::type type, ExecEnv* env,
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
        RETURN_IF_ERROR(HdfsReaderWriter::create_writer(
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
Status NewFileFactory::create_file_reader(TFileType::type type, ExecEnv* env,
                                          RuntimeProfile* profile,
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
        RETURN_IF_ERROR(HdfsReaderWriter::create_reader(range.hdfs_params, range.path, start_offset,
                                                        &hdfs_reader));
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
Status NewFileFactory::create_file_reader(RuntimeProfile* /*profile*/,
                                          const FileSystemProperties& system_properties,
                                          const FileDescription& file_description,
                                          std::unique_ptr<io::FileSystem>* file_system,
                                          io::FileReaderSPtr* file_reader) {
    TFileType::type type = system_properties.system_type;
    switch (type) {
    case TFileType::FILE_S3: {
        S3URI s3_uri(file_description.path);
        if (!s3_uri.parse()) {
            return Status::InvalidArgument("s3 uri is invalid: {}", file_description.path);
        }
        S3Conf s3_conf;
        RETURN_IF_ERROR(ClientFactory::convert_properties_to_s3_conf(system_properties.properties,
                                                                     s3_uri, &s3_conf));
        io::FileSystem* s3_file_system_ptr = new io::S3FileSystem(s3_conf, "");
        (dynamic_cast<io::S3FileSystem*>(s3_file_system_ptr))->connect();
        s3_file_system_ptr->open_file(s3_uri.get_key(), file_reader);
        file_system->reset(s3_file_system_ptr);
        break;
    }
    case TFileType::FILE_HDFS: {
        io::FileSystem* hdfs_file_system_ptr;
        RETURN_IF_ERROR(HdfsReaderWriter::create_new_reader(system_properties.hdfs_params,
                                                            file_description.path,
                                                            &hdfs_file_system_ptr, file_reader));
        file_system->reset(hdfs_file_system_ptr);
        break;
    }
    default:
        return Status::NotSupported("unsupported file reader type: {}", std::to_string(type));
    }
    return Status::OK();
}

// file scan node/stream load pipe
Status NewFileFactory::create_pipe_reader(const TUniqueId& load_id,
                                          std::shared_ptr<FileReader>& file_reader) {
    file_reader = ExecEnv::GetInstance()->load_stream_mgr()->get(load_id);
    if (!file_reader) {
        return Status::InternalError("unknown stream load id: {}", UniqueId(load_id).to_string());
    }
    return Status::OK();
}
} // namespace doris

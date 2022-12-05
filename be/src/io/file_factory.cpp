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

#include "io/broker_reader.h"
#include "io/broker_writer.h"
#include "io/buffered_reader.h"
#include "io/hdfs_reader_writer.h"
#include "io/local_file_reader.h"
#include "io/local_file_writer.h"
#include "io/s3_reader.h"
#include "io/s3_writer.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/load_stream_mgr.h"

doris::Status doris::FileFactory::create_file_writer(
        TFileType::type type, doris::ExecEnv* env,
        const std::vector<TNetworkAddress>& broker_addresses,
        const std::map<std::string, std::string>& properties, const std::string& path,
        int64_t start_offset, std::unique_ptr<FileWriter>& file_writer) {
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
doris::Status doris::FileFactory::create_file_reader(
        doris::TFileType::type type, doris::ExecEnv* env, RuntimeProfile* profile,
        const std::vector<TNetworkAddress>& broker_addresses,
        const std::map<std::string, std::string>& properties, const doris::TBrokerRangeDesc& range,
        int64_t start_offset, std::unique_ptr<FileReader>& file_reader) {
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
doris::Status doris::FileFactory::create_file_reader(RuntimeProfile* profile,
                                                     const TFileScanRangeParams& params,
                                                     const std::string& path, int64_t start_offset,
                                                     int64_t file_size, int64_t buffer_size,
                                                     std::unique_ptr<FileReader>& file_reader) {
    FileReader* file_reader_ptr;
    doris::TFileType::type type = params.file_type;
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
        RETURN_IF_ERROR(HdfsReaderWriter::create_reader(params.hdfs_params, path, start_offset,
                                                        &file_reader_ptr));
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

// file scan node/stream load pipe
doris::Status doris::FileFactory::create_pipe_reader(const TUniqueId& load_id,
                                                     std::shared_ptr<FileReader>& file_reader) {
    file_reader = ExecEnv::GetInstance()->load_stream_mgr()->get(load_id);
    if (!file_reader) {
        return Status::InternalError("unknown stream load id: {}", UniqueId(load_id).to_string());
    }
    return Status::OK();
}

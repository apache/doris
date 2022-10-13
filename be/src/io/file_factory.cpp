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
        return Status::InternalError("UnSupport File Writer Type: " + std::to_string(type));
    }

    return Status::OK();
}

doris::Status doris::FileFactory::_new_file_reader(
        doris::TFileType::type type, doris::ExecEnv* env, RuntimeProfile* profile,
        const std::vector<TNetworkAddress>& broker_addresses,
        const std::map<std::string, std::string>& properties, const TBrokerRangeDesc& range,
        int64_t start_offset, FileReader*& file_reader) {
    switch (type) {
    case TFileType::FILE_LOCAL: {
        file_reader = new LocalFileReader(range.path, start_offset);
        break;
    }
    case TFileType::FILE_BROKER: {
        file_reader = new BufferedReader(
                profile,
                new BrokerReader(env, broker_addresses, properties, range.path, start_offset,
                                 range.__isset.file_size ? range.file_size : 0));
        break;
    }
    case TFileType::FILE_S3: {
        file_reader =
                new BufferedReader(profile, new S3Reader(properties, range.path, start_offset));
        break;
    }
    case TFileType::FILE_HDFS: {
        FileReader* hdfs_reader = nullptr;
        RETURN_IF_ERROR(HdfsReaderWriter::create_reader(range.hdfs_params, range.path, start_offset,
                                                        &hdfs_reader));
        file_reader = new BufferedReader(profile, hdfs_reader);
        break;
    }
    default:
        return Status::InternalError("UnSupport File Reader Type: " + std::to_string(type));
    }

    return Status::OK();
}

doris::Status doris::FileFactory::create_file_reader(
        doris::TFileType::type type, doris::ExecEnv* env, RuntimeProfile* profile,
        const std::vector<TNetworkAddress>& broker_addresses,
        const std::map<std::string, std::string>& properties, const doris::TBrokerRangeDesc& range,
        int64_t start_offset, std::unique_ptr<FileReader>& file_reader) {
    if (type == TFileType::FILE_STREAM) {
        return Status::InternalError("UnSupport UniquePtr For FileStream type");
    }

    FileReader* file_reader_ptr;
    RETURN_IF_ERROR(_new_file_reader(type, env, profile, broker_addresses, properties, range,
                                     start_offset, file_reader_ptr));
    file_reader.reset(file_reader_ptr);

    return Status::OK();
}

doris::Status doris::FileFactory::create_file_reader(
        doris::TFileType::type type, doris::ExecEnv* env, RuntimeProfile* profile,
        const std::vector<TNetworkAddress>& broker_addresses,
        const std::map<std::string, std::string>& properties, const doris::TBrokerRangeDesc& range,
        int64_t start_offset, std::shared_ptr<FileReader>& file_reader) {
    if (type == TFileType::FILE_STREAM) {
        file_reader = env->load_stream_mgr()->get(range.load_id);
        if (!file_reader) {
            VLOG_NOTICE << "unknown stream load id: " << UniqueId(range.load_id);
            return Status::InternalError("unknown stream load id");
        }
    } else {
        FileReader* file_reader_ptr;
        RETURN_IF_ERROR(_new_file_reader(type, env, profile, broker_addresses, properties, range,
                                         start_offset, file_reader_ptr));
        file_reader.reset(file_reader_ptr);
    }
    return Status::OK();
}

doris::Status doris::FileFactory::_new_file_reader(doris::ExecEnv* env, RuntimeProfile* profile,
                                                   const TFileScanRangeParams& params,
                                                   const doris::TFileRangeDesc& range,
                                                   FileReader*& file_reader_ptr) {
    doris::TFileType::type type = params.file_type;

    if (type == TFileType::FILE_STREAM) {
        return Status::InternalError("UnSupport UniquePtr For FileStream type");
    }

    int64_t start_offset = range.start_offset;
    switch (params.format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
        if (start_offset != 0) {
            start_offset -= 1;
        }
        break;
    default:
        break;
    }

    switch (type) {
    case TFileType::FILE_LOCAL: {
        file_reader_ptr = new LocalFileReader(range.path, start_offset);
        break;
    }
    case TFileType::FILE_S3: {
        file_reader_ptr = new BufferedReader(
                profile, new S3Reader(params.properties, range.path, start_offset));
        break;
    }
    case TFileType::FILE_HDFS: {
        FileReader* hdfs_reader = nullptr;
        RETURN_IF_ERROR(HdfsReaderWriter::create_reader(params.hdfs_params, range.path,
                                                        start_offset, &hdfs_reader));
        file_reader_ptr = new BufferedReader(profile, hdfs_reader);
        break;
    }
    default:
        return Status::InternalError("Unsupported File Reader Type: " + std::to_string(type));
    }

    return Status::OK();
}

doris::Status doris::FileFactory::create_file_reader(doris::ExecEnv* env, RuntimeProfile* profile,
                                                     const TFileScanRangeParams& params,
                                                     const doris::TFileRangeDesc& range,
                                                     std::shared_ptr<FileReader>& file_reader) {
    FileReader* file_reader_ptr;
    RETURN_IF_ERROR(_new_file_reader(env, profile, params, range, file_reader_ptr));
    file_reader.reset(file_reader_ptr);

    return Status::OK();
}

doris::Status doris::FileFactory::create_file_reader(doris::ExecEnv* env, RuntimeProfile* profile,
                                                     const TFileScanRangeParams& params,
                                                     const doris::TFileRangeDesc& range,
                                                     std::unique_ptr<FileReader>& file_reader) {
    FileReader* file_reader_ptr;
    RETURN_IF_ERROR(_new_file_reader(env, profile, params, range, file_reader_ptr));
    file_reader.reset(file_reader_ptr);

    return Status::OK();
}

doris::Status doris::FileFactory::create_file_reader(RuntimeProfile* profile,
                                                     const TFileScanRangeParams& params,
                                                     const TFileRangeDesc& range,
                                                     std::unique_ptr<FileReader>& file_reader,
                                                     int64_t buffer_size) {
    doris::TFileType::type type = params.file_type;
    FileReader* file_reader_ptr;
    switch (type) {
    case TFileType::FILE_LOCAL: {
        file_reader_ptr = new LocalFileReader(range.path, range.start_offset);
        break;
    }
    case TFileType::FILE_S3: {
        file_reader_ptr = new S3Reader(params.properties, range.path, range.start_offset);
        break;
    }
    case TFileType::FILE_HDFS: {
        RETURN_IF_ERROR(HdfsReaderWriter::create_reader(params.hdfs_params, range.path,
                                                        range.start_offset, &file_reader_ptr));
        break;
    }
    default:
        return Status::InternalError("Unsupported File Reader Type: " + std::to_string(type));
    }
    if (buffer_size > 0) {
        file_reader.reset(new BufferedReader(profile, file_reader_ptr, buffer_size));
    } else {
        file_reader.reset(file_reader_ptr);
    }
    return Status::OK();
}

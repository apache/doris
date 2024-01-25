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
#pragma once

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <string>

#include "common/factory_creator.h"
#include "common/status.h"
#include "io/fs/file_reader.h"

namespace doris {
namespace io {
class FileSystem;
class FileWriter;

struct FileSystemProperties {
    TFileType::type system_type;
    std::map<std::string, std::string> properties;
    THdfsParams hdfs_params;
    std::vector<TNetworkAddress> broker_addresses;
};

struct FileDescription {
    std::string path;
    // length of the file in bytes.
    // -1 means unset.
    // If the file length is not set, the file length will be fetched from the file system.
    int64_t file_size = -1;
    // modification time of this file.
    // 0 means unset.
    int64_t mtime = 0;
    // for hdfs, eg: hdfs://nameservices1/
    // because for a hive table, differenet partitions may have different
    // locations(or fs), so different files may have different fs.
    std::string fs_name;
};

} // namespace io
class ExecEnv;
class RuntimeProfile;
class RuntimeState;

class FileFactory {
    ENABLE_FACTORY_CREATOR(FileFactory);

public:
    static io::FileReaderOptions get_reader_options(RuntimeState* state,
                                                    const io::FileDescription& fd);

    /// Create FileWriter
    static Status create_file_writer(TFileType::type type, ExecEnv* env,
                                     const std::vector<TNetworkAddress>& broker_addresses,
                                     const std::map<std::string, std::string>& properties,
                                     const std::string& path, int64_t start_offset,
                                     std::unique_ptr<io::FileWriter>& file_writer);

    /// Create FileReader
    static Status create_file_reader(const io::FileSystemProperties& system_properties,
                                     const io::FileDescription& file_description,
                                     const io::FileReaderOptions& reader_options,
                                     std::shared_ptr<io::FileSystem>* file_system,
                                     io::FileReaderSPtr* file_reader,
                                     RuntimeProfile* profile = nullptr);

    // Create FileReader for stream load pipe
    static Status create_pipe_reader(const TUniqueId& load_id, io::FileReaderSPtr* file_reader,
                                     RuntimeState* runtime_state, bool need_schema);

    static Status create_hdfs_reader(const THdfsParams& hdfs_params, const io::FileDescription& fd,
                                     const io::FileReaderOptions& reader_options,
                                     std::shared_ptr<io::FileSystem>* hdfs_file_system,
                                     io::FileReaderSPtr* reader, RuntimeProfile* profile);

    static Status create_s3_reader(const std::map<std::string, std::string>& prop,
                                   const io::FileDescription& fd,
                                   const io::FileReaderOptions& reader_options,
                                   std::shared_ptr<io::FileSystem>* s3_file_system,
                                   io::FileReaderSPtr* reader);

    static Status create_broker_reader(const TNetworkAddress& broker_addr,
                                       const std::map<std::string, std::string>& prop,
                                       const io::FileDescription& fd,
                                       const io::FileReaderOptions& reader_options,
                                       std::shared_ptr<io::FileSystem>* hdfs_file_system,
                                       io::FileReaderSPtr* reader);

    static TFileType::type convert_storage_type(TStorageBackendType::type type) {
        switch (type) {
        case TStorageBackendType::LOCAL:
            return TFileType::FILE_LOCAL;
        case TStorageBackendType::S3:
            return TFileType::FILE_S3;
        case TStorageBackendType::BROKER:
            return TFileType::FILE_BROKER;
        case TStorageBackendType::HDFS:
            return TFileType::FILE_HDFS;
        default:
            LOG(FATAL) << "not match type to convert, from type:" << type;
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }
};

} // namespace doris

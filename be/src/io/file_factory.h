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

#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "io/file_reader.h"
#include "io/file_writer.h"

namespace doris {
class ExecEnv;
class TNetworkAddress;
class RuntimeProfile;

class FileFactory {
public:
    // Create FileWriter
    static Status create_file_writer(TFileType::type type, ExecEnv* env,
                                     const std::vector<TNetworkAddress>& broker_addresses,
                                     const std::map<std::string, std::string>& properties,
                                     const std::string& path, int64_t start_offset,
                                     std::unique_ptr<FileWriter>& file_writer);

    /**
     * Create FileReader for broker scan node related scanners and readers
     */
    static Status create_file_reader(TFileType::type type, ExecEnv* env, RuntimeProfile* profile,
                                     const std::vector<TNetworkAddress>& broker_addresses,
                                     const std::map<std::string, std::string>& properties,
                                     const TBrokerRangeDesc& range, int64_t start_offset,
                                     std::unique_ptr<FileReader>& file_reader);
    /**
     * Create FileReader for file scan node rlated scanners and readers
     * If buffer_size > 0, use BufferedReader to wrap the underlying FileReader;
     * Otherwise, return the underlying FileReader directly.
     */
    static Status create_file_reader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                                     const std::string& path, int64_t start_offset,
                                     int64_t file_size, int64_t buffer_size,
                                     std::unique_ptr<FileReader>& file_reader);

    // Create FileReader for stream load pipe
    static Status create_pipe_reader(const TUniqueId& load_id,
                                     std::shared_ptr<FileReader>& file_reader);

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
        __builtin_unreachable();
    }
};

} // namespace doris

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
    static Status create_file_writer(TFileType::type type, ExecEnv* env,
                                     const std::vector<TNetworkAddress>& broker_addresses,
                                     const std::map<std::string, std::string>& properties,
                                     const std::string& path, int64_t start_offset,
                                     std::unique_ptr<FileWriter>& file_writer);

    // Because StreamLoadPipe use std::shared_ptr, here we have to support both unique_ptr
    // and shared_ptr create_file_reader
    static Status create_file_reader(TFileType::type type, ExecEnv* env, RuntimeProfile* profile,
                                     const std::vector<TNetworkAddress>& broker_addresses,
                                     const std::map<std::string, std::string>& properties,
                                     const TBrokerRangeDesc& range, int64_t start_offset,
                                     std::unique_ptr<FileReader>& file_reader);

    static Status create_file_reader(TFileType::type type, ExecEnv* env, RuntimeProfile* profile,
                                     const std::vector<TNetworkAddress>& broker_addresses,
                                     const std::map<std::string, std::string>& properties,
                                     const TBrokerRangeDesc& range, int64_t start_offset,
                                     std::shared_ptr<FileReader>& file_reader);

    static Status create_file_reader(ExecEnv* env, RuntimeProfile* profile,
                                     const TFileScanRangeParams& params,
                                     const TFileRangeDesc& range,
                                     std::unique_ptr<FileReader>& file_reader);

    static Status create_file_reader(ExecEnv* env, RuntimeProfile* profile,
                                     const TFileScanRangeParams& params,
                                     const TFileRangeDesc& range,
                                     std::shared_ptr<FileReader>& file_reader);

    static Status create_file_reader(const TFileScanRangeParams& params,
                                     const TFileRangeDesc& range,
                                     std::unique_ptr<FileReader>& file_reader);

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

private:
    // Note: if the function return Status::OK() means new the file_reader. the caller
    // should delete the memory of file_reader or use the smart_ptr to hold the own of file_reader
    static Status _new_file_reader(TFileType::type type, ExecEnv* env, RuntimeProfile* profile,
                                   const std::vector<TNetworkAddress>& broker_addresses,
                                   const std::map<std::string, std::string>& properties,
                                   const TBrokerRangeDesc& range, int64_t start_offset,
                                   FileReader*& file_reader);

    static Status _new_file_reader(ExecEnv* env, RuntimeProfile* profile,
                                   const TFileScanRangeParams& params, const TFileRangeDesc& range,
                                   FileReader*& file_reader);
};

} // namespace doris
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

#include <map>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace doris {
class THiveTableSink;
class TIcebergTableSink;
class TIcebergDeleteSink;
class TResultFileSinkOptions;
class TTVFTableSink;

namespace io {
struct FileSystemProperties {
    TFileType::type system_type;
    std::map<std::string, std::string> properties;
    THdfsParams hdfs_params;
    std::vector<TNetworkAddress> broker_addresses;
    // Typed native Azure access for FILE_S3 from the FE scan or sink. When set,
    // FileFactory builds the Azure client from it and ignores the S3-style Azure keys in
    // `properties`.
    std::optional<TAzureClientConfig> azure_client_config = std::nullopt;

    // Reuse the writer's storage binding when it rereads existing delete files.
    TFileScanRangeParams to_scan_range_params() const;
};

// The typed native Azure access a scan carries, if FE sent one
// (TFileScanRangeParams.azure_client_config); every reader that builds FileSystemProperties from
// scan parameters copies it through this.
inline std::optional<TAzureClientConfig> azure_client_config_of(
        const TFileScanRangeParams& params) {
    return params.__isset.azure_client_config
                   ? std::optional<TAzureClientConfig>(params.azure_client_config)
                   : std::nullopt;
}

// A non-owning view. The source request or FileSystemProperties must outlive its use.
struct FSPropertiesRef {
    TFileType::type type;
    const std::map<std::string, std::string>* properties {nullptr};
    const THdfsParams* hdfs_params {nullptr};
    std::span<const TNetworkAddress> broker_addresses;
    // Typed native Azure access for FILE_S3; see FileSystemProperties::azure_client_config.
    const TAzureClientConfig* azure_client_config {nullptr};

    FSPropertiesRef(TFileType::type type_) : type(type_) {}

    FSPropertiesRef(const FileSystemProperties& fs_properties)
            : type(fs_properties.system_type),
              properties(&fs_properties.properties),
              hdfs_params(&fs_properties.hdfs_params),
              broker_addresses(fs_properties.broker_addresses),
              azure_client_config(fs_properties.azure_client_config.has_value()
                                          ? &*fs_properties.azure_client_config
                                          : nullptr) {}

    static FSPropertiesRef from(const TIcebergTableSink& sink);
    static FSPropertiesRef from(const THiveTableSink& sink, TFileType::type partition_type);
    static FSPropertiesRef from(const TIcebergDeleteSink& sink);
    static FSPropertiesRef from(const TTVFTableSink& sink);
    static FSPropertiesRef from(const TResultFileSinkOptions& options, TFileType::type type);
    static FSPropertiesRef from(const TParquetMetadataParams& params);

    FileSystemProperties to_owned() const;
};

// A merge plan expands into data and delete Thrift sinks. Forward only the fields sent by FE.
template <typename Source, typename Target>
void copy_table_sink_file_system_properties(const Source& source, Target& target) {
    if (source.__isset.file_type) {
        target.__set_file_type(source.file_type);
    }
    if (source.__isset.hadoop_config) {
        target.__set_hadoop_config(source.hadoop_config);
    }
    if (source.__isset.broker_addresses) {
        target.__set_broker_addresses(source.broker_addresses);
    }
    if (source.__isset.azure_client_config) {
        target.__set_azure_client_config(source.azure_client_config);
    }
}

} // namespace io
} // namespace doris

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
#include "io/file_system_properties.h"

#include <gen_cpp/DataSinks_types.h>

#include "io/hdfs_builder.h"

namespace doris::io {
namespace {

template <typename Request>
FSPropertiesRef borrow_properties(TFileType::type type, const Request& request,
                                  const std::map<std::string, std::string>& properties) {
    FSPropertiesRef ref(type);
    ref.properties = &properties;
    if constexpr (requires { request.__isset.broker_addresses; }) {
        if (request.__isset.broker_addresses) {
            ref.broker_addresses = request.broker_addresses;
        }
    }
    if (request.__isset.azure_client_config) {
        ref.azure_client_config = &request.azure_client_config;
    }
    return ref;
}

const std::map<std::string, std::string>& optional_properties(
        bool is_set, const std::map<std::string, std::string>& properties) {
    static const std::map<std::string, std::string> empty;
    return is_set ? properties : empty;
}

} // namespace

FSPropertiesRef FSPropertiesRef::from(const TIcebergTableSink& sink) {
    return borrow_properties(sink.file_type, sink, sink.hadoop_config);
}

FSPropertiesRef FSPropertiesRef::from(const THiveTableSink& sink, TFileType::type partition_type) {
    return borrow_properties(partition_type, sink, sink.hadoop_config);
}

FSPropertiesRef FSPropertiesRef::from(const TIcebergDeleteSink& sink) {
    auto type = sink.__isset.file_type ? sink.file_type : TFileType::FILE_HDFS;
    return borrow_properties(type, sink,
                             optional_properties(sink.__isset.hadoop_config, sink.hadoop_config));
}

FSPropertiesRef FSPropertiesRef::from(const TTVFTableSink& sink) {
    return borrow_properties(sink.file_type, sink,
                             optional_properties(sink.__isset.properties, sink.properties));
}

FSPropertiesRef FSPropertiesRef::from(const TResultFileSinkOptions& options, TFileType::type type) {
    return borrow_properties(
            type, options,
            optional_properties(options.__isset.broker_properties, options.broker_properties));
}

FSPropertiesRef FSPropertiesRef::from(const TParquetMetadataParams& params) {
    return borrow_properties(params.file_type, params,
                             optional_properties(params.__isset.properties, params.properties));
}

FileSystemProperties FSPropertiesRef::to_owned() const {
    FileSystemProperties result;
    result.system_type = type;
    if (properties != nullptr) {
        result.properties = *properties;
    }
    result.broker_addresses.assign(broker_addresses.begin(), broker_addresses.end());
    if (hdfs_params != nullptr) {
        result.hdfs_params = *hdfs_params;
    } else if (type == TFileType::FILE_HDFS) {
        result.hdfs_params = parse_properties(result.properties);
    }
    if (azure_client_config != nullptr) {
        result.azure_client_config = *azure_client_config;
    }
    return result;
}

TFileScanRangeParams FileSystemProperties::to_scan_range_params() const {
    TFileScanRangeParams params;
    params.__set_file_type(system_type);
    params.__set_properties(properties);
    if (system_type == TFileType::FILE_HDFS) {
        params.__set_hdfs_params(hdfs_params);
    }
    if (!broker_addresses.empty()) {
        params.__set_broker_addresses(broker_addresses);
    }
    if (azure_client_config) {
        params.__set_azure_client_config(*azure_client_config);
    }
    return params;
}

} // namespace doris::io

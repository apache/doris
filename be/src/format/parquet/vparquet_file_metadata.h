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
#include <gen_cpp/parquet_types.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "format/parquet/schema_desc.h"

namespace parquet {
class FileMetaData;
}

namespace doris {
class FileMetaData {
public:
    FileMetaData(tparquet::FileMetaData& metadata, size_t mem_size,
                 std::vector<uint8_t> serialized_metadata = {});
    ~FileMetaData();
    Status init_schema(const bool enable_mapping_varbinary, const bool enable_mapping_timestamp_tz);
    const FieldDescriptor& schema() const { return _schema; }
    FieldDescriptor& schema() { return _schema; }
    const tparquet::FileMetaData& to_thrift() const;
    Status get_arrow_metadata(std::shared_ptr<::parquet::FileMetaData>* metadata) const;
    std::string debug_string() const;
    size_t get_mem_size() const { return _mem_size + _serialized_metadata.size(); }

private:
    tparquet::FileMetaData _metadata;
    FieldDescriptor _schema;
    size_t _mem_size;
    mutable std::mutex _arrow_metadata_mutex;
    mutable std::shared_ptr<::parquet::FileMetaData> _arrow_metadata;
    mutable size_t _arrow_metadata_mem_size = 0;
    std::vector<uint8_t> _serialized_metadata;
};

} // namespace doris

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

#include "format/parquet/vparquet_file_metadata.h"

#include <gen_cpp/parquet_types.h>

#include <sstream>
#include <vector>

#include "format/parquet/schema_desc.h"
#include "format/parquet_metadata_memory.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

FileMetaData::FileMetaData(tparquet::FileMetaData& metadata) : _metadata(metadata) {
    _mem_size = sizeof(*this) + parquet_metadata_dynamic_size(_metadata);
    _tracked_mem_size = _mem_size;
    ExecEnv::GetInstance()->parquet_meta_tracker()->consume(_tracked_mem_size);
}

FileMetaData::~FileMetaData() {
    ExecEnv::GetInstance()->parquet_meta_tracker()->release(_tracked_mem_size);
}

void FileMetaData::release_memory_tracking() const {
    // The cache's tracked value now owns this charge. Active uncached readers keep the
    // parquet tracker charge; cached values must not be counted by both limiters.
    ExecEnv::GetInstance()->parquet_meta_tracker()->release(_tracked_mem_size);
    _tracked_mem_size = 0;
}

Status FileMetaData::init_schema(const bool enable_mapping_varbinary,
                                 bool enable_mapping_timestamp_tz) {
    if (_metadata.schema[0].num_children <= 0) {
        return Status::Corruption("Invalid parquet schema");
    }
    _schema.set_enable_mapping_varbinary(enable_mapping_varbinary);
    _schema.set_enable_mapping_timestamp_tz(enable_mapping_timestamp_tz);
    auto status = _schema.parse_from_thrift(_metadata.schema);
    const size_t schema_size = _schema.estimated_memory_size();
    _mem_size += schema_size;
    _tracked_mem_size += schema_size;
    ExecEnv::GetInstance()->parquet_meta_tracker()->consume(schema_size);
    return status;
}

const tparquet::FileMetaData& FileMetaData::to_thrift() const {
    return _metadata;
}

std::string FileMetaData::debug_string() const {
    std::stringstream out;
    out << "Parquet Metadata(";
    out << "; version=" << _metadata.version;
    out << ")";
    return out.str();
}

} // namespace doris

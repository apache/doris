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
#include <parquet/metadata.h>

#include <algorithm>
#include <sstream>
#include <vector>

#include "common/cast_set.h"
#include "format/parquet/schema_desc.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/thrift_util.h"

namespace doris {

FileMetaData::FileMetaData(tparquet::FileMetaData& metadata, size_t mem_size,
                           std::vector<uint8_t> serialized_metadata)
        : _metadata(metadata),
          _mem_size(mem_size),
          _serialized_metadata(std::move(serialized_metadata)) {
    ExecEnv::GetInstance()->parquet_meta_tracker()->consume(mem_size + _serialized_metadata.size());
}

FileMetaData::~FileMetaData() {
    ExecEnv::GetInstance()->parquet_meta_tracker()->release(
            _mem_size + _serialized_metadata.size() + _arrow_metadata_mem_size);
}

Status FileMetaData::init_schema(const bool enable_mapping_varbinary,
                                 bool enable_mapping_timestamp_tz) {
    if (_metadata.schema[0].num_children <= 0) {
        return Status::Corruption("Invalid parquet schema");
    }
    _schema.set_enable_mapping_varbinary(enable_mapping_varbinary);
    _schema.set_enable_mapping_timestamp_tz(enable_mapping_timestamp_tz);
    RETURN_IF_ERROR(_schema.parse_from_thrift(_metadata.schema));
    // Cached metadata is immutable after publication; assign native field IDs before insertion.
    _schema.assign_ids();
    return Status::OK();
}

const tparquet::FileMetaData& FileMetaData::to_thrift() const {
    return _metadata;
}

Status FileMetaData::get_arrow_metadata(std::shared_ptr<::parquet::FileMetaData>* metadata) const {
    DORIS_CHECK(metadata != nullptr);
    std::lock_guard lock(_arrow_metadata_mutex);
    if (_arrow_metadata == nullptr) {
        try {
            std::vector<uint8_t> fallback_serialized_metadata;
            const std::vector<uint8_t>* serialized_metadata = &_serialized_metadata;
            if (serialized_metadata->empty()) {
                ThriftSerializer serializer(/*compact=*/true,
                                            static_cast<int>(std::max<size_t>(_mem_size, 4096)));
                RETURN_IF_ERROR(
                        serializer.serialize(const_cast<tparquet::FileMetaData*>(&_metadata),
                                             &fallback_serialized_metadata));
                serialized_metadata = &fallback_serialized_metadata;
            }
            uint32_t serialized_size = cast_set<uint32_t>(serialized_metadata->size());
            auto parsed =
                    ::parquet::FileMetaData::Make(serialized_metadata->data(), &serialized_size,
                                                  ::parquet::default_reader_properties());
            DORIS_CHECK(static_cast<size_t>(serialized_size) == serialized_metadata->size());
            // Native and Arrow planners describe the same immutable footer. Cache the adapter at
            // the footer-cache lifecycle so repeated v2 opens do not serialize and parse it again.
            _arrow_metadata_mem_size = parsed->size();
            ExecEnv::GetInstance()->parquet_meta_tracker()->consume(_arrow_metadata_mem_size);
            _arrow_metadata = std::move(parsed);
        } catch (const ::parquet::ParquetException& e) {
            return Status::Corruption("Failed to adapt cached Parquet metadata: {}", e.what());
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to adapt cached Parquet metadata: {}", e.what());
        }
    }
    *metadata = _arrow_metadata;
    return Status::OK();
}

std::string FileMetaData::debug_string() const {
    std::stringstream out;
    out << "Parquet Metadata(";
    out << "; version=" << _metadata.version;
    out << ")";
    return out.str();
}

} // namespace doris

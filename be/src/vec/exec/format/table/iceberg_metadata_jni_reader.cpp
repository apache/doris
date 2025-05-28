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

#include "iceberg_metadata_jni_reader.h"

#include "util/string_util.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

static const std::string HADOOP_OPTION_PREFIX = "hadoop.";

IcebergMetadataJniReader::IcebergMetadataJniReader(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        RuntimeProfile* profile, const TIcebergMetadataParams* range_params)
        : JniReader(file_slot_descs, state, profile) {
    std::vector<std::string> required_fields;
    for (const auto& desc : _file_slot_descs) {
        required_fields.emplace_back(desc->col_name());
    }
    std::map<std::string, std::string> params;
    params["serialized_table"] = range_params->serialized_table;
    params["serialized_split"] = range_params->serialized_split;
    params["required_fields"] = join(required_fields, ",");
    // TODO: set time_zone
    for (const auto& kv : range_params->hadoop_props) {
        params[HADOOP_OPTION_PREFIX + kv.first] = kv.second;
    }

    switch (range_params->iceberg_query_type) {
    case TIcebergQueryType::HISTORY:
        _jni_connector = std::make_unique<JniConnector>(
                "org/apache/doris/iceberg/IcebergHistoryJniScanner", params, required_fields);
        break;
    case TIcebergQueryType::FILES:
        _jni_connector = std::make_unique<JniConnector>(
                "org/apache/doris/iceberg/IcebergFilesJniScanner", params, required_fields);
        break;
    case TIcebergQueryType::SNAPSHOTS:
        _jni_connector = std::make_unique<JniConnector>(
                "org/apache/doris/iceberg/IcebergSnapshotsJniScanner", params, required_fields);
        break;
        // TODO: more iceberg metadata tables
    default:
        // TODO: save error msg
        break;
    }
}

Status IcebergMetadataJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    return _jni_connector->get_next_block(block, read_rows, eof);
}

Status IcebergMetadataJniReader::get_columns(
        std::unordered_map<std::string, DataTypePtr>* name_to_type,
        std::unordered_set<std::string>* missing_cols) {
    // TODO: move this to common JniReader
    for (const auto& desc : _file_slot_descs) {
        name_to_type->emplace(desc->col_name(), desc->type());
    }
    return Status::OK();
}

Status IcebergMetadataJniReader::init_reader(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    if (_jni_connector == nullptr) {
        return Status::InternalError("JniConnector is not initialized");
    }
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized

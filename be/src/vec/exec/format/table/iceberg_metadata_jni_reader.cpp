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
        RuntimeProfile* profile, const TIcebergMetadataParams& range_params)
        : JniReader(file_slot_descs, state, profile), _range_params(range_params) {}

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
    std::vector<std::string> required_fields;
    for (const auto& desc : _file_slot_descs) {
        required_fields.emplace_back(desc->col_name());
    }
    std::map<std::string, std::string> params;
    params["serialized_table"] = _range_params.serialized_table;
    params["serialized_split"] = _range_params.serialized_split;
    params["required_fields"] = join(required_fields, ",");
    // TODO: set time_zone
    for (const auto& kv : _range_params.hadoop_props) {
        params[HADOOP_OPTION_PREFIX + kv.first] = kv.second;
    }
    std::string connector_class;
    switch (_range_params.iceberg_query_type) {
    case TIcebergQueryType::HISTORY:
        connector_class = "org/apache/doris/iceberg/IcebergHistoryJniScanner";
        break;
    case TIcebergQueryType::METADATA_LOG_ENTRIES:
        connector_class = "org/apache/doris/iceberg/IcebergMetadataLogEntriesJniScanner";
        break;
    case TIcebergQueryType::SNAPSHOTS:
        connector_class = "org/apache/doris/iceberg/IcebergSnapshotsJniScanner";
        break;
    case TIcebergQueryType::ENTRIES:
        connector_class = "org/apache/doris/iceberg/IcebergEntriesJniScanner";
        break;
    case TIcebergQueryType::FILES:
        connector_class = "org/apache/doris/iceberg/IcebergFilesJniScanner";
        break;
    case TIcebergQueryType::MANIFESTS:
        connector_class = "org/apache/doris/iceberg/IcebergManifestsJniScanner";
        break;
    case TIcebergQueryType::PARTITIONS:
        connector_class = "org/apache/doris/iceberg/IcebergPartitionsJniScanner";
        break;
    case TIcebergQueryType::POSITION_DELETES:
        connector_class = "org/apache/doris/iceberg/IcebergPositionDeletesJniScanner";
        break;
    case TIcebergQueryType::REFS:
        connector_class = "org/apache/doris/iceberg/IcebergRefsJniScanner";
        break;
    default:
        return Status::InternalError("Unsupported iceberg query type: {}",
                                     _range_params.iceberg_query_type);
    }
    _jni_connector =
            std::make_unique<JniConnector>(connector_class, std::move(params), required_fields);
    if (_jni_connector == nullptr) {
        return Status::InternalError("JniConnector failed to initialize");
    }
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized

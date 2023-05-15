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

#include "avro_reader.h"

#include <map>
#include <ostream>

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "vec/core/types.h"
#include "orc/Reader.hh"

namespace doris::vectorized {

AvroReader::AvroReader(RuntimeState *state, RuntimeProfile *profile,
                       const TFileScanRangeParams &params,
                       const std::vector<SlotDescriptor *> &file_slot_descs)
        : _file_slot_descs(file_slot_descs), _state(state), _profile(profile) {
    std::ostringstream required_fields;
    std::ostringstream columns_types;
    std::vector<std::string> column_names;
    int index = 0;
    for (auto &desc: _file_slot_descs) {
        std::string field = desc->col_name();
        column_names.emplace_back(field);
        std::string type = JniConnector::get_hive_type(desc->type());
        if (index == 0) {
            required_fields << field;
            columns_types << type;
        } else {
            required_fields << "," << field;
            columns_types << "#" << type;
        }
        index++;
    }

    TFileType::type type = params.file_type;
    std::map<String, String> required_param = {{"required_fields", required_fields.str()},
                                               {"columns_types",   columns_types.str()},
                                               {"file_type",       std::to_string(type)}};;
    switch (type) {
        case TFileType::FILE_HDFS:
            required_param.insert(pair<String, String>("uri", params.hdfs_params.hdfs_conf.data()->value));
            break;
        case TFileType::FILE_S3:
            required_param.insert(params.properties.begin(), params.properties.end());
            break;
        case TFileType::FILE_LOCAL:
        case TFileType::FILE_BROKER:
        case TFileType::FILE_STREAM:
        default:
            Status::InternalError("unsupported file reader type: {}", std::to_string(type));
    }

    _jni_connector = std::make_unique<JniConnector>("org/apache/doris/avro/AvroScanner",
                                                    required_param, column_names);
}

AvroReader::~AvroReader() = default;

Status AvroReader::get_next_block(Block *block, size_t *read_rows, bool *eof) {
    RETURN_IF_ERROR(_jni_connector->get_nex_block(block, read_rows, eof));
    if (*eof) {
        RETURN_IF_ERROR(_jni_connector->close());
    }
    return Status::OK();
}

Status AvroReader::get_columns(std::unordered_map<std::string, TypeDescriptor> *name_to_type,
                               std::unordered_set<std::string> *missing_cols) {
    for (auto &desc: _file_slot_descs) {
        name_to_type->emplace(desc->col_name(), desc->type());
    }
    return Status::OK();
}

Status AvroReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType> *colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}

} // namespace doris::vectorized

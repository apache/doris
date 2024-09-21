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

#include "hive_jni_reader.h"

#include <map>
#include <ostream>

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "common/logging.h"

namespace doris::vectorized {

HiveJNIReader::HiveJNIReader(RuntimeState* state, RuntimeProfile* profile,
                             const TFileScanRangeParams& params,
                             const std::vector<SlotDescriptor*>& file_slot_descs,
                             const TFileRangeDesc& range)
        : JniReader(file_slot_descs, state, profile), _params(params), _range(range) {
        }

HiveJNIReader::~HiveJNIReader() = default;

TFileType::type HiveJNIReader::get_file_type() {
    TFileType::type type;
    if (_range.__isset.file_type) {
        type = _range.file_type;
    } else {
        type = _params.file_type;
    }
    return type;
}

Status HiveJNIReader::init_fetch_table_reader(
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range)
{
    _colname_to_value_range = colname_to_value_range;
    std::ostringstream required_fields;
    std::ostringstream columns_types;
    std::vector<std::string> column_names;
    int index = 0;
    for (auto& desc : _file_slot_descs) {
        std::string field = desc->col_name();
        column_names.emplace_back(field);
        std::string type = JniConnector::get_jni_type_v2(desc->type());
        if(index == 0) {
            required_fields << field;
            columns_types << type;
        } else {
            required_fields << "," << field;
            columns_types << "#" << type;
        }
        index++;
    }

    TFileType::type type = get_file_type();
    std::map<String, String> required_params = {
            {"uri", _range.path},
            {"file_type", std::to_string(type)},
            {"file_format", std::to_string(_params.format_type)},
            {"required_fields", required_fields.str()},
            {"columns_types", columns_types.str()},
            {"split_start_offset", std::to_string(_range.start_offset)},
            {"split_size", std::to_string(_range.size)}
    };
    if (type == TFileType::FILE_S3) {
        required_params.insert(_params.properties.begin(), _params.properties.end());
    }
    _jni_connector = std::make_unique<JniConnector>("org/apache/doris/hive/HiveJNIScanner",
                                                    required_params, column_names);
    RETURN_IF_ERROR(_jni_connector->init(_colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}

Status HiveJNIReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_jni_connector->get_next_block(block, read_rows, eof));
    if (*eof) {
        RETURN_IF_ERROR(_jni_connector->close());
    }
    return Status::OK();
}

Status HiveJNIReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (auto& desc : _file_slot_descs) {
        name_to_type->emplace(desc->col_name(), desc->type());
    }
    return Status::OK();
}

}
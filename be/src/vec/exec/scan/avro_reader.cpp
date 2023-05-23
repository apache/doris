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

namespace doris::vectorized {

AvroReader::AvroReader(RuntimeState *state, RuntimeProfile *profile,
                       const TFileScanRangeParams &params,
                       const std::vector<SlotDescriptor *> &file_slot_descs)
        : _file_slot_descs(file_slot_descs), _state(state),_profile(profile), _params(params) {
}

AvroReader::AvroReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                       const std::vector<SlotDescriptor *> &file_slot_descs)
                      : _file_slot_descs(file_slot_descs),
                        _params(params),
                        _range(range){
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

Status AvroReader::init_fetch_table_reader(
        std::unordered_map<std::string, ColumnValueRangeType> *colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
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

    TFileType::type type = _params.file_type;
    std::map<String, String> required_param = {{"required_fields", required_fields.str()},
                                               {"columns_types", columns_types.str()},
                                               {"file_type", std::to_string(type)}};
    switch (type) {
        case TFileType::FILE_HDFS:
            required_param.insert(std::make_pair("uri", _params.hdfs_params.hdfs_conf.data()->value));
            break;
        case TFileType::FILE_S3:
            required_param.insert(_params.properties.begin(), _params.properties.end());
            break;
        default:
            Status::InternalError("unsupported file reader type: {}", std::to_string(type));
    }
    required_param.insert(_params.properties.begin(), _params.properties.end());
    _jni_connector = std::make_unique<JniConnector>("org/apache/doris/avro/AvroScanner",
                                                    required_param, column_names);
    RETURN_IF_ERROR(_jni_connector->init(_colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}

Status AvroReader::init_fetch_table_schema_reader() {
    std::map<String, String> required_param = {{"uri", _range.path},
                                               {"file_type", std::to_string(_params.file_type)}};

    required_param.insert(_params.properties.begin(), _params.properties.end());
    _jni_connector = std::make_unique<JniConnector>("org/apache/doris/avro/AvroScanner",
                                                    required_param);
    return _jni_connector->open();
}

Status AvroReader::get_parsed_schema(std::vector<std::string> *col_names, std::vector<TypeDescriptor> *col_types) {
    std::string table_schema_str;
    RETURN_IF_ERROR(_jni_connector->get_table_schema(table_schema_str));

    int hash_pod = table_schema_str.find('#');
    int len = table_schema_str.length();
    std::string schema_name_str = table_schema_str.substr(0, hash_pod);
    std::string schema_type_str = table_schema_str.substr(hash_pod + 1, len);
    std::vector<std::string>  schema_name = split(schema_name_str, ",");
    std::vector<std::string>  schema_type = split(schema_type_str, ",");

    for(int i = 0; i < schema_name.size(); ++i) {
        col_names->emplace_back(schema_name[i]);
        int schema_type_number = std::stoi(schema_type[i]);
        ::doris::TPrimitiveType::type type = static_cast< ::doris::TPrimitiveType::type>(schema_type_number);
        PrimitiveType ptype = thrift_to_type(type);
        col_types->emplace_back(ptype);
    }
    return _jni_connector->close();
}

} // namespace doris::vectorized

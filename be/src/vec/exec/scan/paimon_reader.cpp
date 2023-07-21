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

#include "paimon_reader.h"

#include <map>
#include <ostream>

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "vec/core/types.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

PaimonJniReader::PaimonJniReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                 RuntimeState* state, RuntimeProfile* profile,
                                 const TFileRangeDesc& range)
        : _file_slot_descs(file_slot_descs), _state(state), _profile(profile) {
    std::vector<std::string> column_names;
    for (auto& desc : _file_slot_descs) {
        std::string field = desc->col_name();
        column_names.emplace_back(field);
    }
    std::map<String, String> params;
    params["required_fields"] = range.table_format_params.paimon_params.paimon_column_names;
    params["columns_types"] = range.table_format_params.paimon_params.paimon_column_types;
    params["columns_id"] = range.table_format_params.paimon_params.paimon_column_ids;
    params["hive.metastore.uris"] = range.table_format_params.paimon_params.hive_metastore_uris;
    params["warehouse"] = range.table_format_params.paimon_params.warehouse;
    params["db_name"] = range.table_format_params.paimon_params.db_name;
    params["table_name"] = range.table_format_params.paimon_params.table_name;
    params["length_byte"] = range.table_format_params.paimon_params.length_byte;
    params["split_byte"] =
            std::to_string((int64_t)range.table_format_params.paimon_params.paimon_split.data());
    _jni_connector = std::make_unique<JniConnector>("org/apache/doris/paimon/PaimonJniScanner",
                                                    params, column_names);
}

Status PaimonJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_jni_connector->get_nex_block(block, read_rows, eof));
    if (*eof) {
        RETURN_IF_ERROR(_jni_connector->close());
    }
    return Status::OK();
}

Status PaimonJniReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                    std::unordered_set<std::string>* missing_cols) {
    for (auto& desc : _file_slot_descs) {
        name_to_type->emplace(desc->col_name(), desc->type());
    }
    return Status::OK();
}

Status PaimonJniReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}
} // namespace doris::vectorized

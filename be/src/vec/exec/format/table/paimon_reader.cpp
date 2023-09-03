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

const std::string PaimonJniReader::PAIMON_OPTION_PREFIX = "paimon_option_prefix.";

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
    params["db_name"] = range.table_format_params.paimon_params.db_name;
    params["table_name"] = range.table_format_params.paimon_params.table_name;
    params["paimon_split"] = range.table_format_params.paimon_params.paimon_split;
    params["paimon_column_names"] = range.table_format_params.paimon_params.paimon_column_names;
    params["paimon_predicate"] = range.table_format_params.paimon_params.paimon_predicate;

    // Used to create paimon option
    for (auto& kv : range.table_format_params.paimon_params.paimon_options) {
        params[PAIMON_OPTION_PREFIX + kv.first] = kv.second;
    }
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

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

#include "lakesoul_jni_reader.h"

#include <map>
#include <ostream>

#include "common/logging.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
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
LakeSoulJniReader::LakeSoulJniReader(const TLakeSoulFileDesc& lakesoul_params,
                                     const std::vector<SlotDescriptor*>& file_slot_descs,
                                     RuntimeState* state, RuntimeProfile* profile)
        : _lakesoul_params(lakesoul_params),
          _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile) {
    std::vector<std::string> required_fields;
    for (auto& desc : _file_slot_descs) {
        required_fields.emplace_back(desc->col_name());
    }

    std::map<String, String> params = {
            {"query_id", print_id(_state->query_id())},
            {"file_paths", join(_lakesoul_params.file_paths, ";")},
            {"primary_keys", join(_lakesoul_params.primary_keys, ";")},
            {"partition_descs", join(_lakesoul_params.partition_descs, ";")},
            {"required_fields", join(required_fields, ";")},
            {"options", _lakesoul_params.options},
            {"table_schema", _lakesoul_params.table_schema},
    };
    _jni_connector = std::make_unique<JniConnector>("org/apache/doris/lakesoul/LakeSoulJniScanner",
                                                    params, required_fields);
}

Status LakeSoulJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    return _jni_connector->get_next_block(block, read_rows, eof);
}

Status LakeSoulJniReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                      std::unordered_set<std::string>* missing_cols) {
    for (auto& desc : _file_slot_descs) {
        name_to_type->emplace(desc->col_name(), desc->type());
    }
    return Status::OK();
}

Status LakeSoulJniReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}
} // namespace doris::vectorized

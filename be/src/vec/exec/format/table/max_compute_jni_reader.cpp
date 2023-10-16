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

#include "max_compute_jni_reader.h"

#include <glog/logging.h>

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

MaxComputeJniReader::MaxComputeJniReader(const MaxComputeTableDescriptor* mc_desc,
                                         const std::vector<SlotDescriptor*>& file_slot_descs,
                                         const TFileRangeDesc& range, RuntimeState* state,
                                         RuntimeProfile* profile)
        : _file_slot_descs(file_slot_descs), _range(range), _state(state), _profile(profile) {
    _table_desc = mc_desc;
    std::ostringstream required_fields;
    std::ostringstream columns_types;
    std::vector<std::string> column_names;
    int index = 0;
    for (auto& desc : _file_slot_descs) {
        std::string field = desc->col_name();
        std::string type = JniConnector::get_jni_type(desc->type());
        column_names.emplace_back(field);
        if (index == 0) {
            required_fields << field;
            columns_types << type;
        } else {
            required_fields << "," << field;
            columns_types << "#" << type;
        }
        index++;
    }
    std::map<String, String> params = {{"region", _table_desc->region()},
                                       {"access_key", _table_desc->access_key()},
                                       {"secret_key", _table_desc->secret_key()},
                                       {"project", _table_desc->project()},
                                       {"table", _table_desc->table()},
                                       {"public_access", _table_desc->public_access()},
                                       {"start_offset", std::to_string(_range.start_offset)},
                                       {"split_size", std::to_string(_range.size)},
                                       {"required_fields", required_fields.str()},
                                       {"columns_types", columns_types.str()}};
    _jni_connector = std::make_unique<JniConnector>(
            "org/apache/doris/maxcompute/MaxComputeJniScanner", params, column_names);
}

Status MaxComputeJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_jni_connector->get_nex_block(block, read_rows, eof));
    if (*eof) {
        RETURN_IF_ERROR(_jni_connector->close());
    }
    return Status::OK();
}

Status MaxComputeJniReader::get_columns(
        std::unordered_map<std::string, TypeDescriptor>* name_to_type,
        std::unordered_set<std::string>* missing_cols) {
    for (auto& desc : _file_slot_descs) {
        name_to_type->emplace(desc->col_name(), desc->type());
    }
    return Status::OK();
}

Status MaxComputeJniReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}
} // namespace doris::vectorized

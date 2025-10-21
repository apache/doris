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

#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/core/types.h"
#include "vec/exec/format/jni_reader.h"

namespace doris {
#include "common/compile_check_begin.h"
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
        : JniReader(file_slot_descs, state, profile), _lakesoul_params(lakesoul_params) {
    std::vector<std::string> required_fields;
    for (const auto& desc : _file_slot_descs) {
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

Status LakeSoulJniReader::init_reader(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized

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

#include "arrow_result_jni_reader.h"

#include <map>

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/jni-util.h"
#include "vec/core/types.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

ArrowResultJniReader::ArrowResultJniReader(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        RuntimeProfile* profile, const TFileRangeDesc& range)
        : JniReader(file_slot_descs, state, profile) {
    std::vector<std::string> column_names;
    std::vector<std::string> column_types;
    for (const auto& desc : _file_slot_descs) {
        std::string field = desc->col_name();
        column_names.emplace_back(field);
        column_types.emplace_back(JniConnector::get_jni_type(desc->type()));
    }
    std::map<String, String> params = {
            {"ip", range.table_format_params.arrow_result_params.ip},
            {"arrow_port", range.table_format_params.arrow_result_params.arrow_port},
            {"ticket", range.table_format_params.arrow_result_params.ticket},
            {"location_uri",
             range.table_format_params.arrow_result_params.location_uri},
            {"user",
             range.table_format_params.arrow_result_params.user},
            {"password",
             range.table_format_params.arrow_result_params.password},
            {"required_fields", join(column_names, ",")},
            {"columns_types", join(column_types, "#")}};

    _jni_connector = std::make_unique<JniConnector>(
            "org/apache/doris/arrowresult/ArrowResultScanner", params, column_names);
}

Status ArrowResultJniReader::init_reader(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized

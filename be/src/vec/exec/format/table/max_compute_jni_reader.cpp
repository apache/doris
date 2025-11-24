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
#include "common/compile_check_begin.h"
MaxComputeJniReader::MaxComputeJniReader(const MaxComputeTableDescriptor* mc_desc,
                                         const TMaxComputeFileDesc& max_compute_params,
                                         const std::vector<SlotDescriptor*>& file_slot_descs,
                                         const TFileRangeDesc& range, RuntimeState* state,
                                         RuntimeProfile* profile)
        : JniReader(file_slot_descs, state, profile),
          _max_compute_params(max_compute_params),
          _range(range) {
    _table_desc = mc_desc;
    std::ostringstream required_fields;
    std::ostringstream columns_types;
    std::vector<std::string> column_names;
    int index = 0;
    for (const auto& desc : _file_slot_descs) {
        std::string field = desc->col_name();
        std::string type = JniConnector::get_jni_type_with_different_string(desc->type());
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
    std::map<String, String> params = {
            {"access_key", _table_desc->access_key()},
            {"secret_key", _table_desc->secret_key()},
            {"endpoint", _table_desc->endpoint()},
            {"quota", _table_desc->quota()},
            {"project", _table_desc->project()},
            {"table", _table_desc->table()},

            {"session_id", _max_compute_params.session_id},
            {"scan_serializer", _max_compute_params.table_batch_read_session},

            {"start_offset", std::to_string(_range.start_offset)},
            {"split_size", std::to_string(_range.size)},
            {"required_fields", required_fields.str()},
            {"columns_types", columns_types.str()},

            {"connect_timeout", std::to_string(_max_compute_params.connect_timeout)},
            {"read_timeout", std::to_string(_max_compute_params.read_timeout)},
            {"retry_count", std::to_string(_max_compute_params.retry_times)}};
    _jni_connector = std::make_unique<JniConnector>(
            "org/apache/doris/maxcompute/MaxComputeJniScanner", params, column_names);
}

Status MaxComputeJniReader::init_reader(
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized

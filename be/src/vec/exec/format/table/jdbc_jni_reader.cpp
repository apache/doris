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

#include "jdbc_jni_reader.h"

#include <sstream>

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "vec/core/types.h"
#include "vec/exec/jni_connector.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

JdbcJniReader::JdbcJniReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile,
                             const std::map<std::string, std::string>& jdbc_params)
        : JniReader(file_slot_descs, state, profile), _jdbc_params(jdbc_params) {
    std::vector<std::string> column_names;
    std::ostringstream required_fields;
    std::ostringstream columns_types;

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

    // Merge JDBC-specific params with schema params
    std::map<String, String> params = _jdbc_params;
    params["required_fields"] = required_fields.str();
    params["columns_types"] = columns_types.str();

    _jni_connector = std::make_unique<JniConnector>("org/apache/doris/jdbc/JdbcJniScanner", params,
                                                    column_names);
}

Status JdbcJniReader::init_reader() {
    RETURN_IF_ERROR(_jni_connector->init());
    return _jni_connector->open(_state, _profile);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized

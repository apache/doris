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

#include "exec/schema_scanner/schema_user_scanner.h"

#include <gen_cpp/FrontendService_types.h>

#include <vector>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaUserScanner::_s_user_columns = {
        {"Host", TYPE_CHAR, sizeof(StringRef), false},
        {"User", TYPE_CHAR, sizeof(StringRef), false},
        {"Node_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Admin_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Grant_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Select_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Load_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Alter_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Create_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Drop_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Usage_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Show_view_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Cluster_usage_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"Stage_usage_priv", TYPE_CHAR, sizeof(StringRef), false},
        {"ssl_type", TYPE_CHAR, sizeof(StringRef), false},
        {"ssl_cipher", TYPE_VARCHAR, sizeof(StringRef), false},
        {"x509_issuer", TYPE_VARCHAR, sizeof(StringRef), false},
        {"x509_subject", TYPE_VARCHAR, sizeof(StringRef), false},
        {"max_questions", TYPE_BIGINT, sizeof(int64_t), false},
        {"max_updates", TYPE_BIGINT, sizeof(int64_t), false},
        {"max_connections", TYPE_BIGINT, sizeof(int64_t), false},
        {"max_user_connections", TYPE_BIGINT, sizeof(int64_t), false},
        {"plugin", TYPE_CHAR, sizeof(StringRef), false},
        {"authentication_string", TYPE_VARCHAR, sizeof(StringRef), false},
        {"password_policy.expiration_seconds", TYPE_VARCHAR, sizeof(StringRef), false},
        {"password_policy.password_creation_time", TYPE_VARCHAR, sizeof(StringRef), false},
        {"password_policy.history_num", TYPE_VARCHAR, sizeof(StringRef), false},
        {"password_policy.history_passwords", TYPE_VARCHAR, sizeof(StringRef), false},
        {"password_policy.num_failed_login", TYPE_VARCHAR, sizeof(StringRef), false},
        {"password_policy.password_lock_seconds", TYPE_VARCHAR, sizeof(StringRef), false},
        {"password_policy.failed_login_counter", TYPE_VARCHAR, sizeof(StringRef), false},
        {"password_policy.lock_time", TYPE_VARCHAR, sizeof(StringRef), false}};

SchemaUserScanner::SchemaUserScanner()
        : SchemaScanner(_s_user_columns, TSchemaTableType::SCH_USER) {}

SchemaUserScanner::~SchemaUserScanner() = default;

Status SchemaUserScanner::start(RuntimeState* state) {
    TShowUserRequest request;
    RETURN_IF_ERROR(SchemaHelper::show_user(*(_param->common_param->ip), _param->common_param->port,
                                            request, &_user_result));

    return Status::OK();
}

Status SchemaUserScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    if (_user_result.userinfo_list.empty()) {
        return Status::OK();
    }

    return _fill_block_impl(block);
}

Status SchemaUserScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    const auto& userinfo_list = _user_result.userinfo_list;
    size_t row_num = userinfo_list.size();
    if (row_num == 0) {
        return Status::OK();
    }

    for (size_t col_idx = 0; col_idx < _s_user_columns.size(); ++col_idx) {
        std::vector<StringRef> str_refs(row_num);
        std::vector<int64_t> int_vals(row_num);
        std::vector<void*> datas(row_num);
        std::vector<std::string> column_values(
                row_num); // Store the strings to ensure their lifetime

        for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
            const auto& row = userinfo_list[row_idx];
            std::string column_value = row.size() > col_idx ? row[col_idx] : "";

            // Ensure column value assignment and type casting
            column_values[row_idx] = column_value;
            if (_s_user_columns[col_idx].type == TYPE_BIGINT) {
                int64_t val = !column_value.empty() ? std::stoll(column_value) : 0;
                int_vals[row_idx] = val;
                datas[row_idx] = &int_vals[row_idx];
            } else {
                str_refs[row_idx] =
                        StringRef(column_values[row_idx].data(), column_values[row_idx].size());
                datas[row_idx] = &str_refs[row_idx];
            }
        }

        RETURN_IF_ERROR(fill_dest_column_for_range(block, col_idx, datas));
    }

    return Status::OK();
}

} // namespace doris

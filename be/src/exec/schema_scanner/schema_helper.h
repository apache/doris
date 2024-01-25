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

#pragma once

#include <stdint.h>

#include <string>

#include "common/status.h"

namespace doris {
class TDescribeTableParams;
class TDescribeTableResult;
class TDescribeTablesParams;
class TDescribeTablesResult;
class TGetDbsParams;
class TGetDbsResult;
class TGetTablesParams;
class TGetTablesResult;
class TListPrivilegesResult;
class TListTableStatusResult;
class TListTableMetadataNameIdsResult;
class TShowVariableRequest;
class TShowVariableResult;

// this class is a helper for getting schema info from FE
class SchemaHelper {
public:
    static Status get_db_names(const std::string& ip, const int32_t port,
                               const TGetDbsParams& db_params, TGetDbsResult* db_result);

    static Status get_table_names(const std::string& ip, const int32_t port,
                                  const TGetTablesParams& table_params,
                                  TGetTablesResult* table_result);

    static Status list_table_status(const std::string& ip, const int32_t port,
                                    const TGetTablesParams& table_params,
                                    TListTableStatusResult* table_result);
    static Status list_table_metadata_name_ids(const std::string& ip, const int32_t port,
                                               const doris::TGetTablesParams& request,
                                               TListTableMetadataNameIdsResult* result);

    static Status describe_tables(const std::string& ip, const int32_t port,
                                  const TDescribeTablesParams& desc_params,
                                  TDescribeTablesResult* desc_result);

    static Status show_variables(const std::string& ip, const int32_t port,
                                 const TShowVariableRequest& var_params,
                                 TShowVariableResult* var_result);

    static Status list_table_privilege_status(const std::string& ip, const int32_t port,
                                              const TGetTablesParams& table_params,
                                              TListPrivilegesResult* privileges_result);

    static Status list_schema_privilege_status(const std::string& ip, const int32_t port,
                                               const TGetTablesParams& table_params,
                                               TListPrivilegesResult* privileges_result);

    static Status list_user_privilege_status(const std::string& ip, const int32_t port,
                                             const TGetTablesParams& table_params,
                                             TListPrivilegesResult* privileges_result);

    static std::string extract_db_name(const std::string& full_name);
};

} // namespace doris

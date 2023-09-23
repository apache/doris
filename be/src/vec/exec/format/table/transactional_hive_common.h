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

#include <string>
#include <vector>

#include "runtime/define_primitive_type.h"

namespace doris::vectorized {
struct TransactionalHive {
    static const std::string OPERATION;
    static const std::string ORIGINAL_TRANSACTION;
    static const std::string BUCKET;
    static const std::string ROW_ID;
    static const std::string CURRENT_TRANSACTION;
    static const std::string ROW;
    static const std::string OPERATION_LOWER_CASE;
    static const std::string ORIGINAL_TRANSACTION_LOWER_CASE;
    static const std::string BUCKET_LOWER_CASE;
    static const std::string ROW_ID_LOWER_CASE;
    static const std::string CURRENT_TRANSACTION_LOWER_CASE;
    static const std::string ROW_LOWER_CASE;
    static const int ROW_OFFSET;
    struct Param {
        const std::string column_name;
        const std::string column_lower_case;
        const PrimitiveType type;
    };
    static const std::vector<Param> DELETE_ROW_PARAMS;
    static const std::vector<Param> READ_PARAMS;
    static const std::vector<std::string> DELETE_ROW_COLUMN_NAMES;
    static const std::vector<std::string> DELETE_ROW_COLUMN_NAMES_LOWER_CASE;
    static const std::vector<std::string> READ_ROW_COLUMN_NAMES;
    static const std::vector<std::string> READ_ROW_COLUMN_NAMES_LOWER_CASE;
    static const std::vector<std::string> ACID_COLUMN_NAMES;
    static const std::vector<std::string> ACID_COLUMN_NAMES_LOWER_CASE;
};
} // namespace doris::vectorized

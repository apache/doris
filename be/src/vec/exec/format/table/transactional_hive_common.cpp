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

#include "transactional_hive_common.h"

namespace doris::vectorized {

const std::string TransactionalHive::OPERATION = "operation";
const std::string TransactionalHive::ORIGINAL_TRANSACTION = "originalTransaction";
const std::string TransactionalHive::BUCKET = "bucket";
const std::string TransactionalHive::ROW_ID = "rowId";
const std::string TransactionalHive::CURRENT_TRANSACTION = "currentTransaction";
const std::string TransactionalHive::ROW = "row";

const std::string TransactionalHive::OPERATION_LOWER_CASE = "operation";
const std::string TransactionalHive::ORIGINAL_TRANSACTION_LOWER_CASE = "originaltransaction";
const std::string TransactionalHive::BUCKET_LOWER_CASE = "bucket";
const std::string TransactionalHive::ROW_ID_LOWER_CASE = "rowid";
const std::string TransactionalHive::CURRENT_TRANSACTION_LOWER_CASE = "currenttransaction";
const std::string TransactionalHive::ROW_LOWER_CASE = "row";

const int TransactionalHive::ROW_OFFSET = 5;

const std::vector<TransactionalHive::Param> TransactionalHive::DELETE_ROW_PARAMS = {
        {TransactionalHive::ORIGINAL_TRANSACTION,
         TransactionalHive::ORIGINAL_TRANSACTION_LOWER_CASE, PrimitiveType::TYPE_BIGINT},
        {TransactionalHive::BUCKET, TransactionalHive::BUCKET_LOWER_CASE, PrimitiveType::TYPE_INT},
        {TransactionalHive::ROW_ID, TransactionalHive::ROW_ID_LOWER_CASE,
         PrimitiveType::TYPE_BIGINT}};
const std::vector<TransactionalHive::Param> TransactionalHive::READ_PARAMS = {
        {TransactionalHive::ORIGINAL_TRANSACTION,
         TransactionalHive::ORIGINAL_TRANSACTION_LOWER_CASE, PrimitiveType::TYPE_BIGINT},
        {TransactionalHive::BUCKET, TransactionalHive::BUCKET_LOWER_CASE, PrimitiveType::TYPE_INT},
        {TransactionalHive::ROW_ID, TransactionalHive::ROW_ID_LOWER_CASE,
         PrimitiveType::TYPE_BIGINT}};
const std::vector<std::string> TransactionalHive::DELETE_ROW_COLUMN_NAMES = {
        DELETE_ROW_PARAMS[0].column_name, DELETE_ROW_PARAMS[1].column_name,
        DELETE_ROW_PARAMS[2].column_name};

const std::vector<std::string> TransactionalHive::DELETE_ROW_COLUMN_NAMES_LOWER_CASE = {
        DELETE_ROW_PARAMS[0].column_lower_case, DELETE_ROW_PARAMS[1].column_lower_case,
        DELETE_ROW_PARAMS[2].column_lower_case};

const std::vector<std::string> TransactionalHive::READ_ROW_COLUMN_NAMES = {
        READ_PARAMS[0].column_name, READ_PARAMS[1].column_name, READ_PARAMS[2].column_name};

const std::vector<std::string> TransactionalHive::READ_ROW_COLUMN_NAMES_LOWER_CASE = {
        READ_PARAMS[0].column_lower_case, READ_PARAMS[1].column_lower_case,
        READ_PARAMS[2].column_lower_case};

const std::vector<std::string> TransactionalHive::ACID_COLUMN_NAMES = {
        OPERATION, ORIGINAL_TRANSACTION, BUCKET, ROW_ID, CURRENT_TRANSACTION, ROW};

const std::vector<std::string> TransactionalHive::ACID_COLUMN_NAMES_LOWER_CASE = {
        OPERATION_LOWER_CASE, ORIGINAL_TRANSACTION_LOWER_CASE, BUCKET_LOWER_CASE,
        ROW_ID_LOWER_CASE,    CURRENT_TRANSACTION_LOWER_CASE,  ROW_LOWER_CASE};

} // namespace doris::vectorized

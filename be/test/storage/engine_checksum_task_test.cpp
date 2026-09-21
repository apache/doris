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

#include "storage/task/engine_checksum_task.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "common/consts.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace {

ColumnPB make_checksum_test_column(int32_t unique_id, const std::string& name,
                                   const std::string& type, bool is_key) {
    ColumnPB column;
    column.set_unique_id(unique_id);
    column.set_name(name);
    column.set_type(type);
    column.set_is_key(is_key);
    column.set_is_nullable(!is_key);
    column.set_aggregation("NONE");
    return column;
}

TabletSchema make_checksum_test_schema(bool enable_row_ttl) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    *schema_pb.add_column() = make_checksum_test_column(0, "k", "INT", true);
    if (enable_row_ttl) {
        *schema_pb.add_column() = make_checksum_test_column(1, TTL_COL, "BIGINT", false);
        schema_pb.set_ttl_col_idx(1);
    }

    TabletSchema schema;
    schema.init_from_pb(schema_pb);
    return schema;
}

TEST(EngineChecksumTaskTest, RejectRowTtlSchema) {
    const TabletSchema ordinary_schema = make_checksum_test_schema(false);
    const TabletSchema row_ttl_schema = make_checksum_test_schema(true);
    EXPECT_TRUE(EngineChecksumTask::is_supported(ordinary_schema));
    EXPECT_TRUE(EngineChecksumTask::check_supported(ordinary_schema, 1).ok());
    EXPECT_FALSE(EngineChecksumTask::is_supported(row_ttl_schema));
    const Status status = EngineChecksumTask::check_supported(row_ttl_schema, 2);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
}

} // namespace
} // namespace doris

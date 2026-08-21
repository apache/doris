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

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <string>

#include "common/status.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#include "exec/scan/meta_scanner.h"
#undef private
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris {

TEST(MetaScannerTest, BuildLanceIndexEntriesMetadataRequest) {
    TLanceIndexMetadataParams lance_params;
    lance_params.__set_catalog("lance_ctl");
    lance_params.__set_database("db1");
    lance_params.__set_table("tbl1");

    TMetaScanRange meta_scan_range;
    meta_scan_range.__set_metadata_type(TMetadataType::LANCE_INDEX_ENTRIES);
    meta_scan_range.__set_lance_index_params(lance_params);

    TUserIdentity user_identity;
    user_identity.__set_username("lance_user");
    user_identity.__set_host("%");

    TFetchSchemaTableDataRequest request;
    Status status = MetaScanner::_build_lance_index_entries_metadata_request(
            meta_scan_range, user_identity, &request);
    EXPECT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ(request.cluster_name, "");
    EXPECT_EQ(request.schema_table_name, TSchemaTableName::METADATA_TABLE);
    ASSERT_TRUE(request.__isset.metada_table_params);
    const TMetadataTableRequestParams& table_params = request.metada_table_params;
    EXPECT_EQ(table_params.metadata_type, TMetadataType::LANCE_INDEX_ENTRIES);
    ASSERT_TRUE(table_params.__isset.lance_index_metadata_params);
    EXPECT_EQ(table_params.lance_index_metadata_params.catalog, "lance_ctl");
    EXPECT_EQ(table_params.lance_index_metadata_params.database, "db1");
    EXPECT_EQ(table_params.lance_index_metadata_params.table, "tbl1");
    ASSERT_TRUE(table_params.__isset.current_user_ident);
    EXPECT_EQ(table_params.current_user_ident.username, "lance_user");
    EXPECT_EQ(table_params.current_user_ident.host, "%");
}

TEST(MetaScannerTest, BuildLanceIndexEntriesMetadataRequestMissingParams) {
    TMetaScanRange meta_scan_range;
    meta_scan_range.__set_metadata_type(TMetadataType::LANCE_INDEX_ENTRIES);

    TFetchSchemaTableDataRequest request;
    Status status = MetaScanner::_build_lance_index_entries_metadata_request(
            meta_scan_range, TUserIdentity(), &request);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("TLanceIndexMetadataParams"), std::string::npos)
            << status.to_string();
}

} // namespace doris

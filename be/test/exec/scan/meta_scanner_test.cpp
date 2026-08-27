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

#include "exec/scan/meta_scanner.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/mock_scan_operator.h"
#include "runtime/cluster_info.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"

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

// Exercises the TMetadataType::LANCE_INDEX_ENTRIES dispatch inside _fetch_metadata,
// which the static-assembler tests above cannot reach. The request is assembled
// successfully, then the FE-master RPC fails fast because the UT has no master
// address configured; the dispatch lines still execute before that failure.
// Private-member access relies on the build-wide -fno-access-control flag used
// for doris_be_test, the same mechanism as scanner_late_arrival_rf_test.cpp.
TEST(MetaScannerTest, FetchMetadataLanceIndexEntriesDispatch) {
    ObjectPool pool;
    auto data_type = std::make_shared<DataTypeInt32>();
    auto row_descriptor = MockRowDescriptor({data_type}, &pool);

    MockRuntimeState state;
    auto op = std::make_shared<MockScanOperatorX>();
    op->_row_descriptor = row_descriptor;
    op->_output_row_descriptor =
            std::make_unique<MockRowDescriptor>(std::vector<DataTypePtr> {data_type}, &pool);
    op->_output_tuple_desc = op->_output_row_descriptor->tuple_descriptors()[0];
    auto local_state = std::make_shared<MockScanLocalState>(&state, op.get());

    RuntimeProfile profile("meta_scanner");
    // _scan_range is a reference member bound into this params object, so the
    // params must outlive the scanner.
    TScanRangeParams scan_range_params;
    TUserIdentity user_identity;
    user_identity.__set_username("lance_user");
    user_identity.__set_host("%");
    MetaScanner scanner(&state, local_state.get(), /*tuple_id=*/0, scan_range_params,
                        /*limit=*/-1, &profile, user_identity);

    // Zero slots: the filter-columns loop after the dispatch is a no-op.
    TupleDescriptor tuple_desc;
    scanner._tuple_desc = &tuple_desc;

    // A default ClusterInfo carries an empty master address, so
    // ThriftRpcHelper::rpc returns SERVICE_UNAVAILABLE immediately instead of
    // attempting any network IO. Restore the previous value on the way out.
    ClusterInfo cluster_info;
    ExecEnv* exec_env = ExecEnv::GetInstance();
    ClusterInfo* previous_cluster_info = exec_env->cluster_info();
    exec_env->set_cluster_info(&cluster_info);

    TLanceIndexMetadataParams lance_params;
    lance_params.__set_catalog("lance_ctl");
    lance_params.__set_database("db1");
    lance_params.__set_table("tbl1");
    TMetaScanRange meta_scan_range;
    meta_scan_range.__set_metadata_type(TMetadataType::LANCE_INDEX_ENTRIES);
    meta_scan_range.__set_lance_index_params(lance_params);

    Status status = scanner._fetch_metadata(meta_scan_range);
    EXPECT_FALSE(status.ok()) << status.to_string();

    exec_env->set_cluster_info(previous_cluster_info);
}

} // namespace doris

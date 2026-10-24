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

#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include "util/debug_util.h"

namespace doris {

TEST(FragmentParamsLogTest, KeepsIdentifiersAndInstanceCount) {
    TPipelineFragmentParams params;
    TUniqueId query_id;
    query_id.__set_hi(0x1234);
    query_id.__set_lo(0x5678);
    params.__set_query_id(query_id);
    params.__set_fragment_id(9);
    params.local_params.resize(2);

    EXPECT_EQ("query_id=1234-5678, fragment_id=9, local_instances=2",
              print_fragment_params_summary(params));
}

TEST(FragmentParamsLogTest, OmitsCredentialsWithoutChangingExecutionParams) {
    TPipelineFragmentParams params;
    TFileScanRangeParams scan_params;
    scan_params.__set_properties({{"AWS_ACCESS_KEY", "synthetic-access-key"},
                                  {"AWS_SECRET_KEY", "synthetic-secret-key"},
                                  {"AWS_TOKEN", "synthetic-session-token"},
                                  {"unknown_property", "synthetic-future-credential"}});
    params.__set_file_scan_params({{1, scan_params}});

    TJdbcTable jdbc_table;
    jdbc_table.__set_jdbc_password("synthetic-jdbc-password");
    TTableDescriptor table;
    table.__set_jdbcTable(jdbc_table);
    table.__set_tableName("synthetic-sensitive-table-name");
    TDescriptorTable descriptors;
    descriptors.__set_tableDescriptors({table});
    params.__set_desc_tbl(descriptors);
    const auto original_params = params;

    // Neither known credential keys nor future free-form properties belong in the summary.
    EXPECT_EQ("query_id=0-0, fragment_id=0, local_instances=0",
              print_fragment_params_summary(params));
    EXPECT_EQ(original_params, params);
}

TEST(FragmentParamsLogTest, HandlesAbsentOptionalParams) {
    const TPipelineFragmentParams params;
    EXPECT_EQ("query_id=0-0, fragment_id=0, local_instances=0",
              print_fragment_params_summary(params));
}

} // namespace doris

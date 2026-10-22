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

#include <gen_cpp/DataSinks_types.h>
#include <gtest/gtest.h>

#include "exec/operator/result_sink_operator.h"

namespace doris {

TEST(ResultFileOptionsTest, KeepsTypedAzureAccess) {
    TResultFileSinkOptions t_opt;
    t_opt.__set_file_path("abfss://container@account.dfs.core.windows.net/out/");
    t_opt.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    TAzureSas sas;
    sas.__set_token("sv=2024-01-01&sig=x");
    TAzureCredential credential;
    credential.__set_sas(sas);
    TAzureClientConfig azure;
    azure.__set_endpoint("https://account.blob.core.windows.net");
    azure.__set_credential(credential);
    t_opt.__set_azure_client_config(azure);

    ResultFileOptions options(t_opt);

    ASSERT_TRUE(options.azure_client_config.has_value());
    EXPECT_EQ(azure, *options.azure_client_config);
}

TEST(ResultFileOptionsTest, LeavesAzureAccessUnsetForOtherStorage) {
    TResultFileSinkOptions t_opt;
    t_opt.__set_file_path("s3://bucket/out/");
    t_opt.__set_file_format(TFileFormatType::FORMAT_CSV_PLAIN);

    ResultFileOptions options(t_opt);

    EXPECT_FALSE(options.azure_client_config.has_value());
}

} // namespace doris

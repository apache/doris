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

#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/Status_types.h>
#include <gtest/gtest.h>

#include <string>

#include "runtime/exec_env.h"
#include "service/backend_service.h"

namespace doris {
namespace {

class UnstartedBackendService final : public BaseBackendService {
public:
    UnstartedBackendService() : BaseBackendService(ExecEnv::GetInstance()) {}

    Status start_thrift_dependencies() override {
        dependencies_started = true;
        return Status::OK();
    }

    bool dependencies_started = false;
};

} // namespace

TEST(LanceIndexMutationTest, RejectsEveryOperationBeforeServiceStartup) {
    UnstartedBackendService service;
    BackendServiceIf& rpc = service;
    for (auto operation : {TLanceIndexMutationType::CREATE, TLanceIndexMutationType::REPLACE,
                           TLanceIndexMutationType::DROP}) {
        TLanceIndexMutationRequest request;
        request.__set_invocation_id("lance-unstarted-service-test");
        request.__set_be_process_epoch(1);
        request.__set_deadline_ms(1);
        request.__set_mutation_type(operation);
        request.__set_index_name("idx");
        request.__set_column_name("embedding");
        request.__set_index_type("IVF_PQ");
        // An unstarted service must not open a dataset or echo its storage options.
        request.__set_dataset_uri("s3://unavailable-bucket/private-dataset");
        request.__set_admitted_dataset_version(7);
        request.__set_storage_options({{"aws_secret_access_key", "private-storage-secret"}});

        TLanceIndexMutationResult result;
        rpc.lance_index_mutate(result, request);

        EXPECT_EQ(TStatusCode::CANCELLED, result.status.status_code);
        ASSERT_EQ(1, result.status.error_msgs.size());
        EXPECT_NE(std::string::npos,
                  result.status.error_msgs[0].find("lance index worker service is not accepting "
                                                   "invocations"));
        EXPECT_EQ(std::string::npos, result.status.error_msgs[0].find("private-storage-secret"));
        EXPECT_TRUE(result.__isset.invocation_id);
        EXPECT_EQ(request.invocation_id, result.invocation_id);
        EXPECT_TRUE(result.__isset.be_process_epoch);
        EXPECT_EQ(request.be_process_epoch, result.be_process_epoch);
        // A refusal must not fabricate a native outcome or expose provider diagnostics.
        EXPECT_FALSE(result.__isset.lance_error_code);
        EXPECT_FALSE(result.__isset.message);
        EXPECT_FALSE(result.__isset.worker_phase);
        EXPECT_FALSE(result.__isset.observed_latest_version);
        EXPECT_FALSE(service.dependencies_started);
    }
}

} // namespace doris

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

#include <gen_cpp/DorisExternalService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "util/debug_points.h"

namespace doris {

class FragmentMgrLogTest : public testing::Test, public google::LogSink {
public:
    void SetUp() override {
        _old_debug_points = config::enable_debug_points;
        config::enable_debug_points = true;
        // Stop after request logging, before creating a query context or dispatching any work.
        DebugPoints::instance()->add("FragmentMgr._get_query_ctx.failed");
        _old_vlog_level = google::SetVLOGLevel("fragment_mgr", 10);
        google::AddLogSink(this);
        _fragment_mgr = std::make_unique<FragmentMgr>(&_exec_env);
    }

    void TearDown() override {
        _fragment_mgr->stop();
        _fragment_mgr.reset();
        google::RemoveLogSink(this);
        google::SetVLOGLevel("fragment_mgr", _old_vlog_level);
        DebugPoints::instance()->remove("FragmentMgr._get_query_ctx.failed");
        config::enable_debug_points = _old_debug_points;
    }

    void send(google::LogSeverity /*severity*/, const char* /*full_filename*/,
              const char* /*base_filename*/, int /*line*/, const google::LogMessageTime& /*time*/,
              const char* message, std::size_t message_len) override {
        std::lock_guard lock(_mutex);
        _messages.append(message, message_len);
        _messages.push_back('\n');
    }

protected:
    static TDescriptorTable credential_descriptors() {
        TJdbcTable jdbc_table;
        jdbc_table.__set_jdbc_password("synthetic-jdbc-password");
        TTableDescriptor table;
        table.__set_id(1);
        table.__set_tableType(TTableType::JDBC_TABLE);
        table.__set_jdbcTable(jdbc_table);
        TTupleDescriptor tuple;
        tuple.__set_id(0);
        tuple.__set_byteSize(0);
        tuple.__set_numNullBytes(0);
        TDescriptorTable descriptors;
        descriptors.__set_tupleDescriptors({tuple});
        descriptors.__set_tableDescriptors({table});
        return descriptors;
    }

    std::string messages() {
        std::lock_guard lock(_mutex);
        return _messages;
    }

    ExecEnv _exec_env;
    std::unique_ptr<FragmentMgr> _fragment_mgr;

private:
    std::mutex _mutex;
    std::string _messages;
    bool _old_debug_points = false;
    int _old_vlog_level = 0;
};

TEST_F(FragmentMgrLogTest, PipelineRequestOmitsCredentials) {
    TPipelineFragmentParams params;
    TUniqueId query_id;
    query_id.__set_hi(0x1234);
    query_id.__set_lo(0x5678);
    params.__set_query_id(query_id);
    params.__set_fragment_id(9);
    params.__set_desc_tbl(credential_descriptors());
    TFileScanRangeParams scan_params;
    scan_params.__set_properties({{"AWS_SECRET_KEY", "synthetic-storage-secret"},
                                  {"unknown_property", "synthetic-future-credential"}});
    params.__set_file_scan_params({{1, scan_params}});
    TTxnParams txn_params;
    txn_params.__set_token("synthetic-transaction-token");
    params.__set_txn_conf(txn_params);

    const auto status = _fragment_mgr->exec_plan_fragment(params, QuerySource::INTERNAL_FRONTEND,
                                                          TPipelineFragmentParamsList {});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("FragmentMgr._get_query_ctx.failed"));
    const auto log = messages();
    EXPECT_NE(std::string::npos, log.find("exec_plan_fragment query_id=1234-5678, fragment_id=9"));
    EXPECT_EQ(std::string::npos, log.find("synthetic-"));
}

TEST_F(FragmentMgrLogTest, ExternalRequestOmitsDescriptorCredentials) {
    TQueryPlanInfo query_plan;
    query_plan.__set_desc_tbl(credential_descriptors());
    TUniqueId query_id;
    query_id.__set_hi(0x1234);
    query_id.__set_lo(0x5678);
    TUniqueId instance_id;
    instance_id.__set_hi(0xabcd);
    instance_id.__set_lo(0xef01);
    std::vector<TScanColumnDesc> selected_columns;

    const auto status = _fragment_mgr->exec_external_plan_fragment(
            TScanOpenParams {}, query_plan, query_id, instance_id, &selected_columns);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("FragmentMgr._get_query_ctx.failed"));
    const auto log = messages();
    EXPECT_NE(std::string::npos, log.find("query_id=1234-5678, fragment_instance_id=abcd-ef01"));
    EXPECT_NE(std::string::npos, log.find("external exec_plan_fragment query_id=1234-5678"));
    EXPECT_EQ(std::string::npos, log.find("synthetic-"));
}

} // namespace doris

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

#include "olap/memtable_flush_executor.h"

#include <gtest/gtest.h>
#include <sys/file.h>

#include <string>

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/delta_writer.h"
#include "olap/field.h"
#include "olap/memtable.h"
#include "olap/options.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/tuple.h"
#include "util/file_utils.h"
#include "util/logging.h"

namespace doris {

StorageEngine* k_engine = nullptr;
MemTableFlushExecutor* k_flush_executor = nullptr;

void set_up() {
    char buffer[1024];
    getcwd(buffer, 1024);
    config::storage_root_path = std::string(buffer) + "/flush_test";
    FileUtils::remove_all(config::storage_root_path);
    FileUtils::create_dir(config::storage_root_path);
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    Status s = doris::StorageEngine::open(options, &k_engine);
    ASSERT_TRUE(s.ok()) << s.to_string();

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_storage_engine(k_engine);

    k_flush_executor = k_engine->memtable_flush_executor();
}

void tear_down() {
    delete k_engine;
    k_engine = nullptr;
    system("rm -rf ./flush_test");
    FileUtils::remove_all(std::string(getenv("DORIS_HOME")) + UNUSED_PREFIX);
}

Schema create_schema() {
    std::vector<TabletColumn> col_schemas;
    col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_SMALLINT, true);
    col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_INT, true);
    col_schemas.emplace_back(OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_BIGINT, true);
    Schema schema(col_schemas, 2);
    return schema;
}

class TestMemTableFlushExecutor : public ::testing::Test {
public:
    TestMemTableFlushExecutor() {}
    ~TestMemTableFlushExecutor() {}

    void SetUp() { std::cout << "setup" << std::endl; }

    void TearDown() { std::cout << "tear down" << std::endl; }
};

TEST_F(TestMemTableFlushExecutor, create_flush_handler) {
    std::vector<DataDir*> data_dir = k_engine->get_stores();
    size_t path_hash = data_dir[0]->path_hash();

    std::shared_ptr<FlushHandler> flush_handler;
    k_flush_executor->create_flush_handler(path_hash, &flush_handler);
    ASSERT_NE(nullptr, flush_handler.get());

    FlushResult res;
    res.flush_status = OLAP_SUCCESS;
    res.flush_time_ns = 100;
    flush_handler->on_flush_finished(res);
    ASSERT_FALSE(flush_handler->is_cancelled());
    ASSERT_EQ(100, flush_handler->get_stats().flush_time_ns);
    ASSERT_EQ(1, flush_handler->get_stats().flush_count);

    FlushResult res2;
    res2.flush_status = OLAP_ERR_OTHER_ERROR;
    flush_handler->on_flush_finished(res2);
    ASSERT_TRUE(flush_handler->is_cancelled());
    ASSERT_EQ(100, flush_handler->get_stats().flush_time_ns);
    ASSERT_EQ(1, flush_handler->get_stats().flush_count);

    ASSERT_EQ(OLAP_ERR_OTHER_ERROR, flush_handler->wait());
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    doris::set_up();
    ret = RUN_ALL_TESTS();
    doris::tear_down();
    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}

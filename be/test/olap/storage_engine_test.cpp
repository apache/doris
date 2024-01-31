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

#include "olap/storage_engine.h"

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/tablet_manager.h"
#include "util/threadpool.h"

namespace doris {
using namespace config;

class StorageEngineTest : public testing::Test {
public:
    virtual void SetUp() {
        _engine_data_path = "./be/test/olap/test_data/converter_test_data/tmp";
        EXPECT_TRUE(
                io::global_local_filesystem()->delete_and_create_directory(_engine_data_path).ok());
        EXPECT_TRUE(
                io::global_local_filesystem()->create_directory(_engine_data_path + "/meta").ok());
        _data_dir.reset(new DataDir(_engine_data_path, 100000000));
        static_cast<void>(_data_dir->init());

        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();

        _storage_engine.reset(new StorageEngine(options));
        ExecEnv::GetInstance()->set_storage_engine(_storage_engine.get());
    }

    virtual void TearDown() {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    std::unique_ptr<StorageEngine> _storage_engine;
    std::string _engine_data_path;
    std::unique_ptr<DataDir> _data_dir;
};

TEST_F(StorageEngineTest, TestBrokenDisk) {
    DEFINE_mString(broken_storage_path, "");
    std::string path = config::custom_config_dir + "/be_custom.conf";

    std::error_code ec;
    {
        _storage_engine->add_broken_path("broken_path1");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(broken_storage_path, "broken_path1;");
    }

    {
        _storage_engine->add_broken_path("broken_path2");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path2"), 1);
        EXPECT_EQ(broken_storage_path, "broken_path1;broken_path2;");
    }

    {
        _storage_engine->add_broken_path("broken_path2");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path2"), 1);
        EXPECT_EQ(broken_storage_path, "broken_path1;broken_path2;");
    }

    {
        _storage_engine->remove_broken_path("broken_path2");
        EXPECT_EQ(std::filesystem::exists(path, ec), true);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path1"), 1);
        EXPECT_EQ(_storage_engine->get_broken_paths().count("broken_path2"), 0);
        EXPECT_EQ(broken_storage_path, "broken_path1;");
    }
}

TEST_F(StorageEngineTest, TestAsyncPublish) {
    auto st = ThreadPoolBuilder("TabletPublishTxnThreadPool")
                      .set_min_threads(config::tablet_publish_txn_max_thread)
                      .set_max_threads(config::tablet_publish_txn_max_thread)
                      .build(&_storage_engine->tablet_publish_txn_thread_pool());
    EXPECT_EQ(st, Status::OK());

    int64_t partition_id = 1;
    int64_t tablet_id = 111;

    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(tablet_id);
    create_tablet_req.__set_version(10);

    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir.get());
    RuntimeProfile profile("CreateTablet");
    st = _storage_engine->tablet_manager()->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_EQ(st, Status::OK());
    TabletSharedPtr tablet = _storage_engine->tablet_manager()->get_tablet(tablet_id);
    EXPECT_EQ(tablet->max_version().second, 10);

    for (int64_t i = 5; i < 12; ++i) {
        _storage_engine->add_async_publish_task(partition_id, tablet_id, i, i, false);
    }
    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(), 7);
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), 5);
    for (int64_t i = 1; i < 8; ++i) {
        _storage_engine->_process_async_publish();
        EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(), 7 - i);
    }
    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->_async_publish_tasks.size(), 0);

    for (int64_t i = 100; i < config::max_tablet_version_num + 120; ++i) {
        _storage_engine->add_async_publish_task(partition_id, tablet_id, i, i, false);
    }
    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(),
              config::max_tablet_version_num + 20);

    for (int64_t i = 90; i < 120; ++i) {
        _storage_engine->add_async_publish_task(partition_id, tablet_id, i, i, false);
    }
    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(),
              config::max_tablet_version_num + 30);
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), 90);

    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(),
              config::max_tablet_version_num);
    EXPECT_EQ(_storage_engine->get_pending_publish_min_version(tablet_id), 120);

    st = _storage_engine->tablet_manager()->drop_tablet(tablet_id, 0, false);
    EXPECT_EQ(st, Status::OK());

    EXPECT_EQ(_storage_engine->_async_publish_tasks[tablet_id].size(),
              config::max_tablet_version_num);
    _storage_engine->_process_async_publish();
    EXPECT_EQ(_storage_engine->_async_publish_tasks.size(), 0);
}

} // namespace doris

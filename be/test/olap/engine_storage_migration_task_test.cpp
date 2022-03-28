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

#include "olap/task/engine_storage_migration_task.h"

#include <gtest/gtest.h>
#include <sys/file.h>

#include <string>

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/delta_writer.h"
#include "olap/field.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/tuple.h"
#include "util/file_utils.h"
#include "util/logging.h"

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;

StorageEngine* k_engine = nullptr;
std::string path1;
std::string path2;

void set_up() {
    char buffer[MAX_PATH_LEN];
    ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    path1 = std::string(buffer) + "/data_test_1";
    path2 = std::string(buffer) + "/data_test_2";
    config::storage_root_path = path1 + ";" + path2;
    FileUtils::remove_all(path1);
    FileUtils::create_dir(path1);

    FileUtils::remove_all(path2);
    FileUtils::create_dir(path2);
    std::vector<StorePath> paths;
    paths.emplace_back(path1, -1);
    paths.emplace_back(path2, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    Status s = doris::StorageEngine::open(options, &k_engine);
    ASSERT_TRUE(s.ok()) << s.to_string();

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_storage_engine(k_engine);
    k_engine->start_bg_threads();
}

void tear_down() {
    if (k_engine != nullptr) {
        k_engine->stop();
        delete k_engine;
        k_engine = nullptr;
    }
    ASSERT_EQ(system("rm -rf ./data_test_1"), 0);
    ASSERT_EQ(system("rm -rf ./data_test_2"), 0);
    FileUtils::remove_all(std::string(getenv("DORIS_HOME")) + UNUSED_PREFIX);
}

void create_tablet_request_with_sequence_col(int64_t tablet_id, int32_t schema_hash,
                                             TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::UNIQUE_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->tablet_schema.__set_sequence_col_idx(2);

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn sequence_col;
    sequence_col.column_name = SEQUENCE_COL;
    sequence_col.__set_is_key(false);
    sequence_col.column_type.type = TPrimitiveType::INT;
    sequence_col.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(sequence_col);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    v1.column_type.type = TPrimitiveType::DATETIME;
    v1.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v1);
}

TDescriptorTable create_descriptor_tablet_with_sequence_col() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("k1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("k2").column_pos(1).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .column_name(SEQUENCE_COL)
                                   .column_pos(2)
                                   .build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_DATETIME).column_name("v1").column_pos(3).build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

class TestEngineStorageMigrationTask : public ::testing::Test {
public:
    TestEngineStorageMigrationTask() {}
    ~TestEngineStorageMigrationTask() {}

    void SetUp() {
        // Create local data dir for StorageEngine.
        std::cout << "setup" << std::endl;
    }

    void TearDown() {
        // Remove all dir.
        std::cout << "tear down" << std::endl;
        //doris::tear_down();
        //ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }
};

TEST_F(TestEngineStorageMigrationTask, write_and_migration) {
    TCreateTabletReq request;
    create_tablet_request_with_sequence_col(10005, 270068377, &request);
    OLAPStatus res = k_engine->create_tablet(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {10005, 270068377, WriteType::LOAD, 20003,
                              30003, load_id,   tuple_desc,      &(tuple_desc->slots())};
    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer);
    ASSERT_NE(delta_writer, nullptr);

    MemTracker tracker;
    MemPool pool(&tracker);
    // Tuple 1
    {
        Tuple* tuple = reinterpret_cast<Tuple*>(pool.allocate(tuple_desc->byte_size()));
        memset(tuple, 0, tuple_desc->byte_size());
        *(int8_t*)(tuple->get_slot(slots[0]->tuple_offset())) = 123;
        *(int16_t*)(tuple->get_slot(slots[1]->tuple_offset())) = 456;
        *(int32_t*)(tuple->get_slot(slots[2]->tuple_offset())) = 1;
        ((DateTimeValue*)(tuple->get_slot(slots[3]->tuple_offset())))
                ->from_date_str("2020-07-16 19:39:43", 19);

        res = delta_writer->write(tuple);
        ASSERT_EQ(OLAP_SUCCESS, res);
    }

    res = delta_writer->close();
    ASSERT_EQ(OLAP_SUCCESS, res);
    res = delta_writer->close_wait(nullptr, false);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // publish version success
    TabletSharedPtr tablet =
            k_engine->tablet_manager()->get_tablet(write_req.tablet_id, write_req.schema_hash);
    std::cout << "before publish, tablet row nums:" << tablet->num_rows() << std::endl;
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->rowset_with_max_version()->end_version() + 1;
    version.second = tablet->rowset_with_max_version()->end_version() + 1;
    std::cout << "start to add rowset version:" << version.first << "-" << version.second
              << std::endl;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
            write_req.txn_id, write_req.partition_id, &tablet_related_rs);
    for (auto& tablet_rs : tablet_related_rs) {
        std::cout << "start to publish txn" << std::endl;
        RowsetSharedPtr rowset = tablet_rs.second;
        res = k_engine->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                   write_req.tablet_id, write_req.schema_hash,
                                                   tablet_rs.first.tablet_uid, version);
        ASSERT_EQ(OLAP_SUCCESS, res);
        std::cout << "start to add inc rowset:" << rowset->rowset_id()
                  << ", num rows:" << rowset->num_rows() << ", version:" << rowset->version().first
                  << "-" << rowset->version().second << std::endl;
        res = tablet->add_inc_rowset(rowset);
        ASSERT_EQ(OLAP_SUCCESS, res);
    }
    ASSERT_EQ(1, tablet->num_rows());
    // we should sleep 1 second for the migrated tablet has different time with the current tablet
    sleep(1);

    // test case 1
    // prepare
    DataDir* dest_store = nullptr;
    if (tablet->data_dir()->path() == path1) {
        dest_store = StorageEngine::instance()->get_store(path2);
    } else if (tablet->data_dir()->path() == path2) {
        dest_store = StorageEngine::instance()->get_store(path1);
    }
    ASSERT_NE(dest_store, nullptr);
    std::cout << "dest store:" << dest_store->path() << std::endl;
    // migrating
    EngineStorageMigrationTask engine_task(tablet, dest_store);
    res = engine_task.execute();
    ASSERT_EQ(OLAP_SUCCESS, res);
    // reget the tablet from manager after migration
    auto tablet_id = 10005;
    auto schema_hash = 270068377;
    TabletSharedPtr tablet2 = k_engine->tablet_manager()->get_tablet(tablet_id, schema_hash);
    // check path
    ASSERT_EQ(tablet2->data_dir()->path(), dest_store->path());
    // check rows
    ASSERT_EQ(1, tablet2->num_rows());
    // tablet2 should not equal to tablet
    ASSERT_NE(tablet2, tablet);

    // test case 2
    // migrate tablet2 back to the tablet's path
    // sleep 1 second for update time
    sleep(1);
    dest_store = StorageEngine::instance()->get_store(tablet->data_dir()->path());
    ASSERT_NE(dest_store, nullptr);
    ASSERT_NE(dest_store->path(), tablet2->data_dir()->path());
    std::cout << "dest store:" << dest_store->path() << std::endl;
    EngineStorageMigrationTask engine_task2(tablet2, dest_store);
    res = engine_task2.execute();
    ASSERT_EQ(OLAP_SUCCESS, res);
    TabletSharedPtr tablet3 = k_engine->tablet_manager()->get_tablet(tablet_id, schema_hash);
    // check path
    ASSERT_EQ(tablet3->data_dir()->path(), tablet->data_dir()->path());
    // check rows
    ASSERT_EQ(1, tablet3->num_rows());
    // orgi_tablet should not equal to new_tablet and tablet
    ASSERT_NE(tablet3, tablet2);
    ASSERT_NE(tablet3, tablet);
    // test case 2 end

    res = k_engine->tablet_manager()->drop_tablet(tablet_id, schema_hash);
    ASSERT_EQ(OLAP_SUCCESS, res);
    delete delta_writer;
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    doris::set_up();
    ret = RUN_ALL_TESTS();
    doris::tear_down();
    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}

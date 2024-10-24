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

#include <gen_cpp/AgentService_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdlib.h>
#include <unistd.h>

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/object_pool.h"
#include "exec/tablet_info.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/delta_writer.h"
#include "olap/iterators.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset_builder.h"
#include "olap/schema.h"
#include "olap/short_key_index.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/task/engine_publish_version_task.h"
#include "olap/txn_manager.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class OlapMeta;

// This is DeltaWriter unit test which used by streaming load.
// And also it should take schema change into account after streaming load.

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = std::string(buffer) + "/data_test";
    auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    auto engine = std::make_unique<StorageEngine>(options);
    engine_ref = engine.get();
    Status s = engine->open();
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_TRUE(s.ok()) << s;

    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(new MemTableMemoryLimiter());
    exec_env->set_storage_engine(std::move(engine));
}

static void tear_down() {
    ExecEnv* exec_env = doris::ExecEnv::GetInstance();
    exec_env->set_memtable_memory_limiter(nullptr);
    engine_ref = nullptr;
    exec_env->set_storage_engine(nullptr);
    EXPECT_EQ(system("rm -rf ./data_test"), 0);
    static_cast<void>(io::global_local_filesystem()->delete_directory(
            std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX));
}

static void create_tablet_request_with_sequence_col(int64_t tablet_id, int32_t schema_hash,
                                                    TCreateTabletReq* request,
                                                    bool enable_mow = false) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->partition_id = 30004;
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::UNIQUE_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->tablet_schema.__set_sequence_col_idx(4);
    request->__set_storage_format(TStorageFormat::V2);
    request->__set_enable_unique_key_merge_on_write(enable_mow);
    request->tablet_schema.cluster_key_idxes.push_back(1);
    request->tablet_schema.cluster_key_idxes.push_back(0);

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.__set_col_unique_id(0);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k1.__set_col_unique_id(1);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn v1;
    v1.column_name = "v1";
    v1.__set_is_key(false);
    k1.__set_col_unique_id(2);
    v1.column_type.type = TPrimitiveType::DATETIME;
    v1.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v1);

    TColumn v2;
    v2.column_name = "v2";
    v2.__set_is_key(false);
    k1.__set_col_unique_id(3);
    v2.column_type.type = TPrimitiveType::DATEV2;
    v2.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(v2);

    TColumn sequence_col;
    sequence_col.column_name = SEQUENCE_COL;
    sequence_col.__set_is_key(false);
    k1.__set_col_unique_id(4);
    sequence_col.column_type.type = TPrimitiveType::INT;
    sequence_col.__set_aggregation_type(TAggregationType::REPLACE);
    request->tablet_schema.columns.push_back(sequence_col);
}

static TDescriptorTable create_descriptor_tablet_with_sequence_col() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_TINYINT).column_name("k1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_SMALLINT).column_name("k2").column_pos(1).build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATETIME)
                                   .column_name("v1")
                                   .column_pos(2)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_DATEV2)
                                   .column_name("v2")
                                   .column_pos(3)
                                   .nullable(false)
                                   .build());
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_INT)
                                   .column_name(SEQUENCE_COL)
                                   .column_pos(4)
                                   .nullable(false)
                                   .build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

static void generate_data(vectorized::Block* block, int8_t k1, int16_t k2, int32_t seq) {
    auto columns = block->mutate_columns();
    int8_t c1 = k1;
    columns[0]->insert_data((const char*)&c1, sizeof(c1));

    int16_t c2 = k2;
    columns[1]->insert_data((const char*)&c2, sizeof(c2));

    VecDateTimeValue c3;
    c3.from_date_str("2020-07-16 19:39:43", 19);
    int64_t c3_int = c3.to_int64();
    columns[2]->insert_data((const char*)&c3_int, sizeof(c3));

    DateV2Value<DateV2ValueType> c4;
    c4.unchecked_set_time(2022, 6, 6, 0, 0, 0, 0);
    uint32_t c4_int = c4.to_date_int_val();
    columns[3]->insert_data((const char*)&c4_int, sizeof(c4));

    int32_t c5 = seq;
    columns[4]->insert_data((const char*)&c5, sizeof(c2));
}

class TestDeltaWriterClusterKey : public ::testing::Test {
public:
    TestDeltaWriterClusterKey() {}
    ~TestDeltaWriterClusterKey() {}
    static void SetUpTestSuite() {
        config::min_file_descriptor_number = 100;
        set_up();
    }

    static void TearDownTestSuite() { tear_down(); }
};

TEST_F(TestDeltaWriterClusterKey, vec_sequence_col) {
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("CreateTablet");
    TCreateTabletReq request;
    // sleep(20);
    create_tablet_request_with_sequence_col(10005, 270068377, &request, true);
    Status res = engine_ref->create_tablet(request, profile.get());
    ASSERT_TRUE(res.ok());

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    static_cast<void>(DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl));
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    auto param = std::make_shared<OlapTableSchemaParam>();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req;
    write_req.tablet_id = 10005;
    write_req.schema_hash = 270068377;
    write_req.txn_id = 20003;
    write_req.partition_id = 30003;
    write_req.load_id = load_id;
    write_req.tuple_desc = tuple_desc;
    write_req.slots = &(tuple_desc->slots());
    write_req.is_high_priority = false;
    write_req.table_schema_param = param;
    profile = std::make_unique<RuntimeProfile>("LoadChannels");
    auto delta_writer =
            std::make_unique<DeltaWriter>(*engine_ref, write_req, profile.get(), TUniqueId {});

    vectorized::Block block;
    for (const auto& slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }

    auto rows = 4;
    generate_data(&block, 123, 456, 100);
    res = delta_writer->write(&block, {0});
    ASSERT_TRUE(res.ok());
    generate_data(&block, 123, 457, 100);
    res = delta_writer->write(&block, {1});
    ASSERT_TRUE(res.ok());
    generate_data(&block, 123, 455, 90);
    res = delta_writer->write(&block, {2});
    ASSERT_TRUE(res.ok());
    generate_data(&block, 123, 457, 90); // row 1 has larger sequence number
    res = delta_writer->write(&block, {3});
    ASSERT_TRUE(res.ok());
    generate_data(&block, 122, 456, 90);
    res = delta_writer->write(&block, {4});
    ASSERT_TRUE(res.ok());

    res = delta_writer->close();
    ASSERT_TRUE(res.ok());
    res = delta_writer->wait_flush();
    ASSERT_TRUE(res.ok());
    res = delta_writer->build_rowset();
    ASSERT_TRUE(res.ok());
    res = delta_writer->submit_calc_delete_bitmap_task();
    ASSERT_TRUE(res.ok());
    res = delta_writer->wait_calc_delete_bitmap();
    ASSERT_TRUE(res.ok());
    res = delta_writer->commit_txn(PSlaveTabletNodes());
    ASSERT_TRUE(res.ok());

    // publish version success
    TabletSharedPtr tablet = engine_ref->tablet_manager()->get_tablet(write_req.tablet_id);
    std::cout << "before publish, tablet row nums:" << tablet->num_rows() << std::endl;
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->get_rowset_with_max_version()->end_version() + 1;
    version.second = tablet->get_rowset_with_max_version()->end_version() + 1;
    std::cout << "start to add rowset version:" << version.first << "-" << version.second
              << std::endl;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    engine_ref->txn_manager()->get_txn_related_tablets(write_req.txn_id, write_req.partition_id,
                                                       &tablet_related_rs);
    ASSERT_EQ(1, tablet_related_rs.size());

    std::cout << "start to publish txn" << std::endl;
    RowsetSharedPtr rowset = tablet_related_rs.begin()->second;
    TabletPublishStatistics pstats;
    res = engine_ref->txn_manager()->publish_txn(
            meta, write_req.partition_id, write_req.txn_id, write_req.tablet_id,
            tablet_related_rs.begin()->first.tablet_uid, version, &pstats);
    ASSERT_TRUE(res.ok());
    std::cout << "start to add inc rowset:" << rowset->rowset_id()
              << ", num rows:" << rowset->num_rows() << ", version:" << rowset->version().first
              << "-" << rowset->version().second << std::endl;
    res = tablet->add_inc_rowset(rowset);
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(rows, tablet->num_rows());
    std::vector<segment_v2::SegmentSharedPtr> segments;
    res = ((BetaRowset*)rowset.get())->load_segments(&segments);
    ASSERT_TRUE(res.ok());
    ASSERT_EQ(1, rowset->num_segments());
    ASSERT_EQ(1, segments.size());

    // read data, verify the data correct
    OlapReaderStatistics stats;
    StorageReadOptions opts;
    opts.stats = &stats;
    opts.tablet_schema = rowset->tablet_schema();

    std::unique_ptr<RowwiseIterator> iter;
    std::shared_ptr<Schema> schema = std::make_shared<Schema>(rowset->tablet_schema());
    auto s = segments[0]->new_iterator(schema, opts, &iter);
    ASSERT_TRUE(s.ok());
    auto read_block = rowset->tablet_schema()->create_block();
    res = iter->next_batch(&read_block);
    ASSERT_TRUE(res.ok()) << res;
    ASSERT_EQ(rows, read_block.rows());

    // get the value from k2 column
    ASSERT_EQ(455, read_block.get_by_position(1).column->get_int(0));
    ASSERT_EQ(456, read_block.get_by_position(1).column->get_int(1));
    ASSERT_EQ(456, read_block.get_by_position(1).column->get_int(2));
    ASSERT_EQ(457, read_block.get_by_position(1).column->get_int(3));
    // get the value from k1 column
    ASSERT_EQ(122, read_block.get_by_position(0).column->get_int(1));
    ASSERT_EQ(123, read_block.get_by_position(0).column->get_int(2));
    // get the value from sequence column
    ASSERT_EQ(100, read_block.get_by_position(4).column->get_int(3));

    // check primary key index
    const PrimaryKeyIndexReader* pk_index_reader = segments[0]->get_primary_key_index();
    ASSERT_TRUE(pk_index_reader != nullptr);
    // check short key index
    const ShortKeyIndexDecoder* sk_index_decoder = segments[0]->get_short_key_index();
    // TODO see Segment::_load_index_impl, short key index is not loaded currently
    ASSERT_TRUE(sk_index_decoder == nullptr);

    res = engine_ref->tablet_manager()->drop_tablet(request.tablet_id, request.replica_id, false);
    ASSERT_TRUE(res.ok());
}
} // namespace doris

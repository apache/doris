#include <gtest/gtest.h>

#include <memory>

#include "common/status.h"
#include "io/fs/file_system_map.h"
#include "io/fs/s3_file_system.h"
#include "olap/delta_writer.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "runtime/descriptor_helper.h"
#include "runtime/tuple.h"
#include "util/file_utils.h"
#include "util/s3_util.h"

namespace doris {

static StorageEngine* k_engine = nullptr;

static const std::string kTestDir = "./ut_dir/tablet_clone_test";
static std::string kSnapshotDir = "./ut_dir/tablet_clone_test/snapshot";
static const std::string kResourceId = "TabletCloneTest";
static const int64_t kTabletId = 10005;
static const int32_t KSchemaHash = 270068377;

static const std::string AK = "ak";
static const std::string SK = "sk";
static const std::string ENDPOINT = "endpoint";
static const std::string REGION = "region";
static const std::string BUCKET = "bucket";
static const std::string PREFIX = "prefix";

// remove DISABLED_ when need run this test
#define TabletCloneTest DISABLED_TabletCloneTest
#define private public
class TabletCloneTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        std::map<std::string, std::string> properties = {
                {S3_AK, AK}, {S3_SK, SK}, {S3_ENDPOINT, ENDPOINT}, {S3_REGION, REGION}};
        auto s3_fs = std::make_shared<io::S3FileSystem>(properties, BUCKET, PREFIX, kResourceId);
        s3_fs->connect();
        io::FileSystemMap::instance()->insert(kResourceId, s3_fs);

        config::storage_root_path = kTestDir;
        config::min_file_descriptor_number = 1000;
        FileUtils::remove_all(kTestDir);
        FileUtils::create_dir(kTestDir);

        std::vector<StorePath> paths {{kTestDir, -1}};

        EngineOptions options;
        options.store_paths = paths;
        doris::StorageEngine::open(options, &k_engine);
        k_engine->start_bg_threads();
    }

    static void TearDownTestSuite() {
        if (k_engine != nullptr) {
            k_engine->stop();
            delete k_engine;
            k_engine = nullptr;
        }
    }
};

static void create_tablet_request_with_sequence_col(int64_t tablet_id, int32_t schema_hash,
                                                    TCreateTabletReq* request) {
    request->tablet_id = tablet_id;
    request->__set_version(1);
    request->tablet_schema.schema_hash = schema_hash;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::UNIQUE_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;
    request->tablet_schema.__set_sequence_col_idx(2);
    request->__set_storage_format(TStorageFormat::V2);

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

static TDescriptorTable create_descriptor_tablet_with_sequence_col() {
    TDescriptorTableBuilder desc_tbl_builder;
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
    tuple_builder.build(&desc_tbl_builder);

    return desc_tbl_builder.desc_tbl();
}

TEST_F(TabletCloneTest, convert_rowset_ids_has_file_in_s3) {
    TCreateTabletReq request;
    create_tablet_request_with_sequence_col(kTabletId, KSchemaHash, &request);
    Status st = k_engine->create_tablet(request);
    ASSERT_EQ(Status::OK(), st);

    TDescriptorTable tdesc_tbl = create_descriptor_tablet_with_sequence_col();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    auto& slots = tuple_desc->slots();

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    WriteRequest write_req = {kTabletId, KSchemaHash, WriteType::LOAD, 20003,
                              30003,     load_id,     tuple_desc,      &(tuple_desc->slots())};
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

        st = delta_writer->write(tuple);
        ASSERT_EQ(Status::OK(), st);
    }

    st = delta_writer->close();
    ASSERT_EQ(Status::OK(), st);
    st = delta_writer->close_wait();
    ASSERT_EQ(Status::OK(), st);

    // publish version success
    TabletSharedPtr tablet =
            k_engine->tablet_manager()->get_tablet(write_req.tablet_id, write_req.schema_hash);
    OlapMeta* meta = tablet->data_dir()->get_meta();
    Version version;
    version.first = tablet->rowset_with_max_version()->end_version() + 1;
    version.second = tablet->rowset_with_max_version()->end_version() + 1;
    std::map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
    StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
            write_req.txn_id, write_req.partition_id, &tablet_related_rs);
    for (auto& tablet_rs : tablet_related_rs) {
        RowsetSharedPtr rowset = tablet_rs.second;
        rowset->rowset_meta()->set_resource_id(kResourceId);
        st = k_engine->txn_manager()->publish_txn(meta, write_req.partition_id, write_req.txn_id,
                                                  write_req.tablet_id, write_req.schema_hash,
                                                  tablet_rs.first.tablet_uid, version);
        ASSERT_EQ(Status::OK(), st);
        st = tablet->add_inc_rowset(rowset);
        ASSERT_EQ(Status::OK(), st);
    }
    EXPECT_EQ(1, tablet->num_rows());

    TSnapshotRequest snapshot_req;
    snapshot_req.tablet_id = kTabletId;
    snapshot_req.schema_hash = KSchemaHash;
    bool allow_incremental_clone = false;
    st = SnapshotManager::instance()->_create_snapshot_files(tablet, snapshot_req, &kSnapshotDir,
                                                             &allow_incremental_clone);
    ASSERT_EQ(Status::OK(), st);
    st = SnapshotManager::instance()->convert_rowset_ids(kTestDir, kTabletId, request.replica_id,
                                                         KSchemaHash);
    ASSERT_NE(Status::OK(), st);
    delete delta_writer;
}

} // namespace doris

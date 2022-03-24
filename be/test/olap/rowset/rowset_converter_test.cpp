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

#include "olap/rowset/rowset_converter.h"

#include <fstream>
#include <sstream>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "json2pb/json_to_pb.h"
#include "olap/data_dir.h"
#include "olap/olap_cond.h"
#include "olap/olap_meta.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "runtime/exec_env.h"
#include "util/file_utils.h"
#include "util/logging.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;
StorageEngine* k_engine = nullptr;

void create_rowset_writer_context(TabletSchema* tablet_schema, RowsetTypePB dst_type,
                                  RowsetWriterContext* rowset_writer_context) {
    RowsetId rowset_id;
    rowset_id.init(10000);
    rowset_writer_context->rowset_id = rowset_id;
    rowset_writer_context->tablet_id = 12345;
    rowset_writer_context->tablet_schema_hash = 1111;
    rowset_writer_context->partition_id = 10;
    rowset_writer_context->rowset_type = dst_type;
    rowset_writer_context->path_desc.filepath = config::storage_root_path + "/data/0/12345/1111";
    rowset_writer_context->rowset_state = VISIBLE;
    rowset_writer_context->tablet_schema = tablet_schema;
    rowset_writer_context->version.first = 0;
    rowset_writer_context->version.second = 1;
}

void create_rowset_reader_context(TabletSchema* tablet_schema,
                                  const std::vector<uint32_t>* return_columns,
                                  const DeleteHandler* delete_handler,
                                  std::vector<ColumnPredicate*>* predicates,
                                  std::set<uint32_t>* load_bf_columns, Conditions* conditions,
                                  RowsetReaderContext* rowset_reader_context) {
    rowset_reader_context->reader_type = READER_ALTER_TABLE;
    rowset_reader_context->tablet_schema = tablet_schema;
    rowset_reader_context->need_ordered_result = true;
    rowset_reader_context->return_columns = return_columns;
    rowset_reader_context->seek_columns = return_columns;
    rowset_reader_context->delete_handler = delete_handler;
    rowset_reader_context->lower_bound_keys = nullptr;
    rowset_reader_context->is_lower_keys_included = nullptr;
    rowset_reader_context->upper_bound_keys = nullptr;
    rowset_reader_context->is_upper_keys_included = nullptr;
    rowset_reader_context->predicates = predicates;
    rowset_reader_context->load_bf_columns = load_bf_columns;
    rowset_reader_context->conditions = conditions;
}

void create_tablet_schema(KeysType keys_type, TabletSchema* tablet_schema) {
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_keys_type(keys_type);
    tablet_schema_pb.set_num_short_key_columns(2);
    tablet_schema_pb.set_num_rows_per_row_block(1024);
    tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
    tablet_schema_pb.set_next_column_unique_id(4);

    ColumnPB* column_1 = tablet_schema_pb.add_column();
    column_1->set_unique_id(1);
    column_1->set_name("k1");
    column_1->set_type("INT");
    column_1->set_is_key(true);
    column_1->set_length(4);
    column_1->set_index_length(4);
    column_1->set_is_nullable(false);
    column_1->set_is_bf_column(false);

    ColumnPB* column_2 = tablet_schema_pb.add_column();
    column_2->set_unique_id(2);
    column_2->set_name("k2");
    column_2->set_type("VARCHAR");
    column_2->set_length(20);
    column_2->set_index_length(20);
    column_2->set_is_key(true);
    column_2->set_is_nullable(false);
    column_2->set_is_bf_column(false);

    ColumnPB* column_3 = tablet_schema_pb.add_column();
    column_3->set_unique_id(3);
    column_3->set_name("v1");
    column_3->set_type("INT");
    column_3->set_length(4);
    column_3->set_is_key(false);
    column_3->set_is_nullable(false);
    column_3->set_is_bf_column(false);
    column_3->set_aggregation("SUM");

    tablet_schema->init_from_pb(tablet_schema_pb);
}

void create_tablet_meta(TabletSchema* tablet_schema, TabletMeta* tablet_meta) {
    TabletMetaPB tablet_meta_pb;
    tablet_meta_pb.set_table_id(10000);
    tablet_meta_pb.set_tablet_id(12345);
    tablet_meta_pb.set_schema_hash(1111);
    tablet_meta_pb.set_partition_id(10);
    tablet_meta_pb.set_shard_id(0);
    tablet_meta_pb.set_creation_time(1575020449);
    tablet_meta_pb.set_tablet_state(PB_RUNNING);
    PUniqueId* tablet_uid = tablet_meta_pb.mutable_tablet_uid();
    tablet_uid->set_hi(10);
    tablet_uid->set_lo(10);

    TabletSchemaPB* tablet_schema_pb = tablet_meta_pb.mutable_schema();
    tablet_schema->to_schema_pb(tablet_schema_pb);

    tablet_meta->init_from_pb(tablet_meta_pb);
}

class RowsetConverterTest : public testing::Test {
public:
    virtual void SetUp() {
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        config::path_gc_check = false;
        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = std::string(buffer) + "/data_test";
        FileUtils::remove_all(config::storage_root_path);
        ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        doris::EngineOptions options;
        options.store_paths = paths;
        if (k_engine == nullptr) {
            Status s = doris::StorageEngine::open(options, &k_engine);
            ASSERT_TRUE(s.ok()) << s.to_string();
        }

        ExecEnv* exec_env = doris::ExecEnv::GetInstance();
        exec_env->set_storage_engine(k_engine);

        std::string data_path = config::storage_root_path + "/data";
        ASSERT_TRUE(FileUtils::create_dir(data_path).ok());
        std::string shard_path = data_path + "/0";
        ASSERT_TRUE(FileUtils::create_dir(shard_path).ok());
        std::string tablet_path = shard_path + "/12345";
        ASSERT_TRUE(FileUtils::create_dir(tablet_path).ok());
        _schema_hash_path = tablet_path + "/1111";
        ASSERT_TRUE(FileUtils::create_dir(_schema_hash_path).ok());
        _mem_tracker.reset(new MemTracker(-1));
        _mem_pool.reset(new MemPool(_mem_tracker.get()));
    }

    virtual void TearDown() { FileUtils::remove_all(config::storage_root_path); }

    void process(RowsetTypePB src_type, RowsetTypePB dst_type);

private:
    std::string _schema_hash_path;
    std::shared_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
};

void RowsetConverterTest::process(RowsetTypePB src_type, RowsetTypePB dst_type) {
    // write
    TabletSchema tablet_schema;
    create_tablet_schema(AGG_KEYS, &tablet_schema);
    RowsetWriterContext rowset_writer_context;
    create_rowset_writer_context(&tablet_schema, src_type, &rowset_writer_context);
    std::unique_ptr<RowsetWriter> _rowset_writer;
    ASSERT_EQ(OLAP_SUCCESS,
              RowsetFactory::create_rowset_writer(rowset_writer_context, &_rowset_writer));
    RowCursor row;
    OLAPStatus res = row.init(tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, res);

    std::vector<std::string> test_data;
    for (int i = 0; i < 1024; ++i) {
        test_data.push_back("well" + std::to_string(i));

        int32_t field_0 = i;
        row.set_field_content(0, reinterpret_cast<char*>(&field_0), _mem_pool.get());
        Slice field_1(test_data[i]);
        row.set_field_content(1, reinterpret_cast<char*>(&field_1), _mem_pool.get());
        int32_t field_2 = 10000 + i;
        row.set_field_content(2, reinterpret_cast<char*>(&field_2), _mem_pool.get());
        _rowset_writer->add_row(row);
    }
    _rowset_writer->flush();
    RowsetSharedPtr src_rowset = _rowset_writer->build();
    ASSERT_TRUE(src_rowset != nullptr);
    RowsetId src_rowset_id;
    src_rowset_id.init(10000);
    ASSERT_EQ(src_rowset_id, src_rowset->rowset_id());
    ASSERT_EQ(1024, src_rowset->num_rows());

    // convert
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    create_tablet_meta(&tablet_schema, tablet_meta.get());
    RowsetConverter rowset_converter(tablet_meta);
    RowsetMetaPB dst_rowset_meta_pb;
    FilePathDesc schema_hash_path_desc;
    schema_hash_path_desc.filepath = _schema_hash_path;
    if (dst_type == BETA_ROWSET) {
        ASSERT_EQ(OLAP_SUCCESS,
                  rowset_converter.convert_alpha_to_beta(
                          src_rowset->rowset_meta(), schema_hash_path_desc, &dst_rowset_meta_pb));
    } else {
        ASSERT_EQ(OLAP_SUCCESS,
                  rowset_converter.convert_beta_to_alpha(
                          src_rowset->rowset_meta(), schema_hash_path_desc, &dst_rowset_meta_pb));
    }

    ASSERT_EQ(dst_type, dst_rowset_meta_pb.rowset_type());
    ASSERT_EQ(12345, dst_rowset_meta_pb.tablet_id());
    ASSERT_EQ(1024, dst_rowset_meta_pb.num_rows());

    // read
    RowsetMetaSharedPtr dst_rowset_meta(new RowsetMeta());
    ASSERT_TRUE(dst_rowset_meta->init_from_pb(dst_rowset_meta_pb));
    RowsetSharedPtr dst_rowset;
    ASSERT_EQ(OLAP_SUCCESS, RowsetFactory::create_rowset(&tablet_schema, schema_hash_path_desc,
                                                         dst_rowset_meta, &dst_rowset));

    RowsetReaderSharedPtr dst_rowset_reader;
    ASSERT_EQ(OLAP_SUCCESS, dst_rowset->create_reader(&dst_rowset_reader));
    RowsetReaderContext rowset_reader_context;
    std::set<uint32_t> load_bf_columns;
    std::vector<ColumnPredicate*> predicates;
    Conditions conditions;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    DeleteHandler delete_handler;
    create_rowset_reader_context(&tablet_schema, &return_columns, &delete_handler, &predicates,
                                 &load_bf_columns, &conditions, &rowset_reader_context);
    res = dst_rowset_reader->init(&rowset_reader_context);
    ASSERT_EQ(OLAP_SUCCESS, res);

    RowBlock* row_block = nullptr;
    res = dst_rowset_reader->next_block(&row_block);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1024, row_block->remaining());
    RowCursor row_cursor;
    row_cursor.init(tablet_schema);
    for (int i = 0; i < 1024; ++i) {
        row_block->get_row(i, &row_cursor);
        ASSERT_EQ(i, *(uint32_t*)row_cursor.cell_ptr(0));
        ASSERT_EQ("well" + std::to_string(i), (*(Slice*)row_cursor.cell_ptr(1)).to_string());
        ASSERT_EQ(10000 + i, *(uint32_t*)row_cursor.cell_ptr(2));
    }
}

TEST_F(RowsetConverterTest, TestConvertAlphaRowsetToBeta) {
    process(ALPHA_ROWSET, BETA_ROWSET);
}

TEST_F(RowsetConverterTest, TestConvertBetaRowsetToAlpha) {
    process(ALPHA_ROWSET, BETA_ROWSET);
}

} // namespace doris

int main(int argc, char** argv) {
    doris::StoragePageCache::create_global_cache(1 << 30, 10);
    doris::SegmentLoader::create_global_instance(1000);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

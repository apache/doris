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

#include "olap/rowset/alpha_rowset.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "json2pb/json_to_pb.h"
#include "olap/data_dir.h"
#include "olap/olap_meta.h"
#include "olap/rowset/alpha_rowset_reader.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/storage_engine.h"
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

void set_up() {
    config::path_gc_check = false;
    char buffer[MAX_PATH_LEN];
    ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = std::string(buffer) + "/data_test";
    FileUtils::remove_all(config::storage_root_path);
    ASSERT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());
    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);
    std::string data_path = config::storage_root_path + "/data";
    ASSERT_TRUE(FileUtils::create_dir(data_path).ok());
    std::string shard_path = data_path + "/0";
    ASSERT_TRUE(FileUtils::create_dir(shard_path).ok());
    std::string tablet_path = shard_path + "/12345";
    ASSERT_TRUE(FileUtils::create_dir(tablet_path).ok());
    std::string schema_hash_path = tablet_path + "/1111";
    ASSERT_TRUE(FileUtils::create_dir(schema_hash_path).ok());
}

void tear_down() {
    FileUtils::remove_all(config::storage_root_path);
}

void create_rowset_writer_context(TabletSchema* tablet_schema,
                                  RowsetWriterContext* rowset_writer_context) {
    RowsetId rowset_id;
    rowset_id.init(10000);
    rowset_writer_context->rowset_id = rowset_id;
    rowset_writer_context->tablet_id = 12345;
    rowset_writer_context->tablet_schema_hash = 1111;
    rowset_writer_context->partition_id = 10;
    rowset_writer_context->rowset_type = ALPHA_ROWSET;
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

class AlphaRowsetTest : public testing::Test {
public:
    virtual void SetUp() {
        set_up();
        _mem_tracker.reset(new MemTracker(-1));
        _mem_pool.reset(new MemPool(_mem_tracker.get()));
    }

    virtual void TearDown() { tear_down(); }

private:
    std::unique_ptr<RowsetWriter> _alpha_rowset_writer;
    std::shared_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
};

TEST_F(AlphaRowsetTest, TestAlphaRowsetWriter) {
    TabletSchema tablet_schema;
    create_tablet_schema(AGG_KEYS, &tablet_schema);
    RowsetWriterContext rowset_writer_context;
    create_rowset_writer_context(&tablet_schema, &rowset_writer_context);
    ASSERT_EQ(OLAP_SUCCESS,
              RowsetFactory::create_rowset_writer(rowset_writer_context, &_alpha_rowset_writer));
    RowCursor row;
    OLAPStatus res = row.init(tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, res);

    int32_t field_0 = 10;
    row.set_field_content(0, reinterpret_cast<char*>(&field_0), _mem_pool.get());
    Slice field_1("well");
    row.set_field_content(1, reinterpret_cast<char*>(&field_1), _mem_pool.get());
    int32_t field_2 = 100;
    row.set_field_content(2, reinterpret_cast<char*>(&field_2), _mem_pool.get());
    _alpha_rowset_writer->add_row(row);
    _alpha_rowset_writer->flush();
    RowsetSharedPtr alpha_rowset = _alpha_rowset_writer->build();
    ASSERT_TRUE(alpha_rowset != nullptr);
    RowsetId rowset_id;
    rowset_id.init(10000);
    ASSERT_EQ(rowset_id, alpha_rowset->rowset_id());
    ASSERT_EQ(1, alpha_rowset->num_rows());
}

TEST_F(AlphaRowsetTest, TestAlphaRowsetReader) {
    TabletSchema tablet_schema;
    create_tablet_schema(AGG_KEYS, &tablet_schema);
    RowsetWriterContext rowset_writer_context;
    create_rowset_writer_context(&tablet_schema, &rowset_writer_context);

    ASSERT_EQ(OLAP_SUCCESS,
              RowsetFactory::create_rowset_writer(rowset_writer_context, &_alpha_rowset_writer));

    RowCursor row;
    OLAPStatus res = row.init(tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, res);

    int32_t field_0 = 10;
    row.set_not_null(0);
    row.set_field_content(0, reinterpret_cast<char*>(&field_0), _mem_pool.get());
    Slice field_1("well");
    row.set_not_null(1);
    row.set_field_content(1, reinterpret_cast<char*>(&field_1), _mem_pool.get());
    int32_t field_2 = 100;
    row.set_not_null(2);
    row.set_field_content(2, reinterpret_cast<char*>(&field_2), _mem_pool.get());
    res = _alpha_rowset_writer->add_row(row);
    ASSERT_EQ(OLAP_SUCCESS, res);
    res = _alpha_rowset_writer->flush();
    ASSERT_EQ(OLAP_SUCCESS, res);
    RowsetSharedPtr alpha_rowset = _alpha_rowset_writer->build();
    ASSERT_TRUE(alpha_rowset != nullptr);
    RowsetId rowset_id;
    rowset_id.init(10000);
    ASSERT_EQ(rowset_id, alpha_rowset->rowset_id());
    ASSERT_EQ(1, alpha_rowset->num_rows());
    RowsetReaderSharedPtr rowset_reader;
    res = alpha_rowset->create_reader(&rowset_reader);
    ASSERT_EQ(OLAP_SUCCESS, res);
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    DeleteHandler delete_handler;
    DelPredicateArray predicate_array;
    res = delete_handler.init(tablet_schema, predicate_array, 4);
    ASSERT_EQ(OLAP_SUCCESS, res);
    RowsetReaderContext rowset_reader_context;

    std::set<uint32_t> load_bf_columns;
    std::vector<ColumnPredicate*> predicates;
    Conditions conditions;
    create_rowset_reader_context(&tablet_schema, &return_columns, &delete_handler, &predicates,
                                 &load_bf_columns, &conditions, &rowset_reader_context);
    res = rowset_reader->init(&rowset_reader_context);
    ASSERT_EQ(OLAP_SUCCESS, res);
    RowBlock* row_block = nullptr;
    res = rowset_reader->next_block(&row_block);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, row_block->remaining());
}

TEST_F(AlphaRowsetTest, TestRowCursorWithOrdinal) {
    TabletSchema tablet_schema;
    create_tablet_schema(AGG_KEYS, &tablet_schema);

    RowCursor* row1 = new (std::nothrow) RowCursor(); // 10, "well", 100
    row1->init(tablet_schema);
    int32_t field1_0 = 10;
    row1->set_not_null(0);
    row1->set_field_content(0, reinterpret_cast<char*>(&field1_0), _mem_pool.get());
    Slice field1_1("well");
    row1->set_not_null(1);
    row1->set_field_content(1, reinterpret_cast<char*>(&field1_1), _mem_pool.get());
    int32_t field1_2 = 100;
    row1->set_not_null(2);
    row1->set_field_content(2, reinterpret_cast<char*>(&field1_2), _mem_pool.get());

    RowCursor* row2 = new (std::nothrow) RowCursor(); // 11, "well", 100
    row2->init(tablet_schema);
    int32_t field2_0 = 11;
    row2->set_not_null(0);
    row2->set_field_content(0, reinterpret_cast<char*>(&field2_0), _mem_pool.get());
    Slice field2_1("well");
    row2->set_not_null(1);
    row2->set_field_content(1, reinterpret_cast<char*>(&field2_1), _mem_pool.get());
    int32_t field2_2 = 100;
    row2->set_not_null(2);
    row2->set_field_content(2, reinterpret_cast<char*>(&field2_2), _mem_pool.get());

    RowCursor* row3 = new (std::nothrow) RowCursor(); // 11, "good", 100
    row3->init(tablet_schema);
    int32_t field3_0 = 11;
    row3->set_not_null(0);
    row3->set_field_content(0, reinterpret_cast<char*>(&field3_0), _mem_pool.get());
    Slice field3_1("good");
    row3->set_not_null(1);
    row3->set_field_content(1, reinterpret_cast<char*>(&field3_1), _mem_pool.get());
    int32_t field3_2 = 100;
    row3->set_not_null(2);
    row3->set_field_content(2, reinterpret_cast<char*>(&field3_2), _mem_pool.get());

    std::priority_queue<AlphaMergeContext*, std::vector<AlphaMergeContext*>,
                        AlphaMergeContextComparator>
            queue;
    AlphaMergeContext ctx1;
    ctx1.row_cursor.reset(row1);
    AlphaMergeContext ctx2;
    ctx2.row_cursor.reset(row2);
    AlphaMergeContext ctx3;
    ctx3.row_cursor.reset(row3);

    queue.push(&ctx1);
    queue.push(&ctx2);
    queue.push(&ctx3);

    // should be:
    // row1, row3, row2
    AlphaMergeContext* top1 = queue.top();
    ASSERT_EQ(top1, &ctx1);
    queue.pop();
    AlphaMergeContext* top2 = queue.top();
    ASSERT_EQ(top2, &ctx3);
    queue.pop();
    AlphaMergeContext* top3 = queue.top();
    ASSERT_EQ(top3, &ctx2);
    queue.pop();
    ASSERT_TRUE(queue.empty());
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

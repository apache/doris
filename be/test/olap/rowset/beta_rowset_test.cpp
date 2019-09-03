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
#include <string>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "gtest/gtest.h"
#include "olap/data_dir.h"
#include "olap/row_block.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/row_cursor.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/mem_tracker.h"
#include "runtime/mem_pool.h"
#include "util/slice.h"

using std::string;

namespace doris {

class BetaRowsetTest : public testing::Test {
protected:
    const string kRowsetDir = "./ut_dir/beta_rowset_test";

    void SetUp() override {
        OLAPStatus s;
        if (check_dir_existed(kRowsetDir)) {
            s = remove_all_dir(kRowsetDir);
            ASSERT_EQ(OLAP_SUCCESS, s);
        }
        s = create_dir(kRowsetDir);
        ASSERT_EQ(OLAP_SUCCESS, s);
    }
    void TearDown() override {
        if (check_dir_existed(kRowsetDir)) {
            auto s = remove_all_dir(kRowsetDir);
            ASSERT_EQ(OLAP_SUCCESS, s);
        }
    }

    // (k1 int, k2 varchar(20), k3 int) duplicated key (k1, k2)
    void create_tablet_schema(TabletSchema* tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(DUP_KEYS);
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
        column_1->set_is_nullable(true);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("k2");
        column_2->set_type("INT"); // TODO change to varchar(20) when dict encoding for string is supported
        column_2->set_length(4);
        column_2->set_index_length(4);
        column_2->set_is_nullable(true);
        column_2->set_is_key(true);
        column_2->set_is_nullable(true);
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

    void create_rowset_writer_context(TabletSchema* tablet_schema,
                                      RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(10000);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = 12345;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_type = BETA_ROWSET;
        rowset_writer_context->rowset_path_prefix = kRowsetDir;
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->version.first = 10;
        rowset_writer_context->version.second = 10;
        rowset_writer_context->version_hash = 110;
    }

    void create_and_init_rowset_reader(Rowset* rowset, RowsetReaderContext& context, RowsetReaderSharedPtr* result) {
        auto s = rowset->create_reader(result);
        ASSERT_EQ(OLAP_SUCCESS, s);
        ASSERT_TRUE(*result != nullptr);

        s = (*result)->init(&context);
        ASSERT_EQ(OLAP_SUCCESS, s);
    }
};

TEST_F(BetaRowsetTest, BasicFunctionTest) {
    OLAPStatus s;
    TabletSchema tablet_schema;
    create_tablet_schema(&tablet_schema);

    RowsetSharedPtr rowset;
    const int num_segments = 3;
    const uint32_t rows_per_segment = 4096;
    {   // write `num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context;
        create_rowset_writer_context(&tablet_schema, &writer_context);

        RowsetWriterSharedPtr rowset_writer(new BetaRowsetWriter);
        s = rowset_writer->init(writer_context);
        ASSERT_EQ(OLAP_SUCCESS, s);

        RowCursor input_row;
        input_row.init(tablet_schema);

        // for segment "i", row "rid"
        // k1 := rid*10 + i
        // k2 := k1 * 10
        // k3 := 4096 * i + rid
        for (int i = 0; i < num_segments; ++i) {
            MemTracker mem_tracker(-1);
            MemPool mem_pool(&mem_tracker);
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 10 + i;
                uint32_t k2 = k1 * 10;
                uint32_t k3 = rows_per_segment * i + rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                ASSERT_EQ(OLAP_SUCCESS, s);
            }
            s = rowset_writer->flush();
            ASSERT_EQ(OLAP_SUCCESS, s);
        }

        rowset = rowset_writer->build();
        ASSERT_TRUE(rowset != nullptr);
        ASSERT_EQ(num_segments, rowset->rowset_meta()->num_segments());
        ASSERT_EQ(num_segments * rows_per_segment, rowset->rowset_meta()->num_rows());
    }

    {   // test return ordered results and return k1 and k2
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = &tablet_schema;
        reader_context.need_ordered_result = true;
        std::vector<uint32_t> return_columns = {0, 1};
        reader_context.return_columns = &return_columns;

        RowsetReaderSharedPtr rowset_reader;
        create_and_init_rowset_reader(rowset.get(), reader_context, &rowset_reader);

        RowBlock* output_block;
        uint32_t num_rows_read = 0;
        while ((s = rowset_reader->next_block(&output_block)) == OLAP_SUCCESS) {
            ASSERT_TRUE(output_block != nullptr);
            ASSERT_GT(output_block->row_num(), 0);
            ASSERT_EQ(return_columns, output_block->row_block_info().column_ids);
            // after sort merge segments, k1 will be 0, 1, 2, 10, 11, 12, 20, 21, 22, ..., 40950, 40951, 40952
            for (int i = 0; i < output_block->row_num(); ++i) {
                char* field1 = output_block->field_ptr(i, 0);
                char* field2 = output_block->field_ptr(i, 1);
                // test null bit
                ASSERT_FALSE(*reinterpret_cast<bool*>(field1));
                ASSERT_FALSE(*reinterpret_cast<bool*>(field2));
                uint32_t k1 = *reinterpret_cast<uint32_t*>(field1 + 1);
                uint32_t k2 = *reinterpret_cast<uint32_t*>(field2 + 1);
                ASSERT_EQ(k1 * 10, k2);

                int rid = num_rows_read / 3;
                int seg_id = num_rows_read % 3;
                ASSERT_EQ(rid * 10 + seg_id, k1);
                num_rows_read++;
            }
        }
        EXPECT_EQ(OLAP_ERR_DATA_EOF, s);
        EXPECT_TRUE(output_block == nullptr);
        EXPECT_EQ(rowset->rowset_meta()->num_rows(), num_rows_read);
    }

    {   // test return unordered data and only k3
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = &tablet_schema;
        reader_context.need_ordered_result = false;
        std::vector<uint32_t> return_columns = {2};
        reader_context.return_columns = &return_columns;

        RowsetReaderSharedPtr rowset_reader;
        create_and_init_rowset_reader(rowset.get(), reader_context, &rowset_reader);

        RowBlock* output_block;
        uint32_t num_rows_read = 0;
        while ((s = rowset_reader->next_block(&output_block)) == OLAP_SUCCESS) {
            ASSERT_TRUE(output_block != nullptr);
            ASSERT_GT(output_block->row_num(), 0);
            ASSERT_EQ(return_columns, output_block->row_block_info().column_ids);
            // for unordered result, k3 will be 0, 1, 2, ..., 4096*3-1
            for (int i = 0; i < output_block->row_num(); ++i) {
                char* field3 = output_block->field_ptr(i, 2);
                // test null bit
                ASSERT_FALSE(*reinterpret_cast<bool*>(field3));
                uint32_t k3 = *reinterpret_cast<uint32_t*>(field3 + 1);
                ASSERT_EQ(num_rows_read, k3);
                num_rows_read++;
            }
        }
        EXPECT_EQ(OLAP_ERR_DATA_EOF, s);
        EXPECT_TRUE(output_block == nullptr);
        EXPECT_EQ(rowset->rowset_meta()->num_rows(), num_rows_read);
    }
}

} // namespace doris

int main(int argc, char **argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}


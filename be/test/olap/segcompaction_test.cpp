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

#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "env/env_posix.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/data_dir.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/mem_tracker.h"
#include "util/file_utils.h"
#include "util/slice.h"

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;
StorageEngine* l_engine = nullptr;
static const std::string lTestDir = "./data_test/data/segcompaction_test";

class SegCompactionTest : public testing::Test {
public:
    SegCompactionTest() : _data_dir(std::make_unique<DataDir>(lTestDir)) {
        _data_dir->update_capacity();
    }

    void SetUp() {
        config::enable_segcompaction = true;
        config::enable_storage_vectorization = true;
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;

        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = std::string(buffer) + "/data_test";

        EXPECT_TRUE(FileUtils::remove_all(config::storage_root_path).ok());
        EXPECT_TRUE(FileUtils::create_dir(config::storage_root_path).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(config::storage_root_path, -1);

        doris::EngineOptions options;
        options.store_paths = paths;
        Status s = doris::StorageEngine::open(options, &l_engine);
        EXPECT_TRUE(s.ok()) << s.to_string();

        ExecEnv* exec_env = doris::ExecEnv::GetInstance();
        exec_env->set_storage_engine(l_engine);

        EXPECT_TRUE(FileUtils::create_dir(lTestDir).ok());

        l_engine->start_bg_threads();
    }

    void TearDown() {
        if (l_engine != nullptr) {
            l_engine->stop();
            delete l_engine;
            l_engine = nullptr;
        }
    }

protected:
    OlapReaderStatistics _stats;

    bool check_dir(std::vector<std::string>& vec) {
        std::vector<std::string> result;
        for (const auto& entry : std::filesystem::directory_iterator(lTestDir)) {
            result.push_back(std::filesystem::path(entry.path()).filename());
        }

        LOG(INFO) << "expected ls:" << std::endl;
        for (auto& i : vec) {
            LOG(INFO) << i;
        }
        LOG(INFO) << "acutal ls:" << std::endl;
        for (auto& i : result) {
            LOG(INFO) << i;
        }

        if (result.size() != vec.size()) {
            return false;
        } else {
            for (auto& i : vec) {
                if (std::find(result.begin(), result.end(), i) == result.end()) {
                    return false;
                }
            }
        }
        return true;
    }

    // (k1 int, k2 varchar(20), k3 int) duplicated key (k1, k2)
    void create_tablet_schema(TabletSchemaSPtr tablet_schema) {
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
        column_2->set_type(
                "INT"); // TODO change to varchar(20) when dict encoding for string is supported
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

    void create_rowset_writer_context(int64_t id, TabletSchemaSPtr tablet_schema,
                                      RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(id);
        // rowset_writer_context->data_dir = _data_dir.get();
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = 12345;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_type = BETA_ROWSET;
        rowset_writer_context->rowset_dir = lTestDir;
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->version.first = 10;
        rowset_writer_context->version.second = 10;
    }

    void create_and_init_rowset_reader(Rowset* rowset, RowsetReaderContext& context,
                                       RowsetReaderSharedPtr* result) {
        auto s = rowset->create_reader(result);
        EXPECT_EQ(Status::OK(), s);
        EXPECT_TRUE(*result != nullptr);

        s = (*result)->init(&context);
        EXPECT_EQ(Status::OK(), s);
    }

private:
    std::unique_ptr<DataDir> _data_dir;
};

TEST_F(SegCompactionTest, SegCompactionThenRead) {
    config::enable_segcompaction = true;
    config::enable_storage_vectorization = true;
    Status s;
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    RowsetSharedPtr rowset;
    const int num_segments = 15;
    const uint32_t rows_per_segment = 4096;
    config::segcompaction_small_threshold = 6000; // set threshold above
                                                  // rows_per_segment
    std::vector<uint32_t> segment_num_rows;
    { // write `num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context;
        create_rowset_writer_context(10047, tablet_schema, &writer_context);

        std::unique_ptr<RowsetWriter> rowset_writer;
        s = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        EXPECT_EQ(Status::OK(), s);

        RowCursor input_row;
        input_row.init(tablet_schema);

        // for segment "i", row "rid"
        // k1 := rid*10 + i
        // k2 := k1 * 10
        // k3 := rid
        for (int i = 0; i < num_segments; ++i) {
            MemPool mem_pool;
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + i;
                uint32_t k2 = i;
                uint32_t k3 = rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                EXPECT_EQ(Status::OK(), s);
            }
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
        }

        rowset = rowset_writer->build();
        std::vector<std::string> ls;
        ls.push_back("10047_0.dat");
        ls.push_back("10047_1.dat");
        ls.push_back("10047_2.dat");
        ls.push_back("10047_3.dat");
        ls.push_back("10047_4.dat");
        ls.push_back("10047_5.dat");
        EXPECT_TRUE(check_dir(ls));
    }

    { // read
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = tablet_schema;
        // use this type to avoid cache from other ut
        reader_context.reader_type = READER_CUMULATIVE_COMPACTION;
        reader_context.need_ordered_result = true;
        std::vector<uint32_t> return_columns = {0, 1, 2};
        reader_context.return_columns = &return_columns;
        reader_context.stats = &_stats;

        // without predicates
        {
            RowsetReaderSharedPtr rowset_reader;
            create_and_init_rowset_reader(rowset.get(), reader_context, &rowset_reader);
            RowBlock* output_block;
            uint32_t num_rows_read = 0;
            while ((s = rowset_reader->next_block(&output_block)) == Status::OK()) {
                EXPECT_TRUE(output_block != nullptr);
                EXPECT_GT(output_block->row_num(), 0);
                EXPECT_EQ(0, output_block->pos());
                EXPECT_EQ(output_block->row_num(), output_block->limit());
                EXPECT_EQ(return_columns, output_block->row_block_info().column_ids);
                // after sort merge segments, k1 will be 0, 1, 2, 10, 11, 12, 20, 21, 22, ..., 40950, 40951, 40952
                for (int i = 0; i < output_block->row_num(); ++i) {
                    char* field1 = output_block->field_ptr(i, 0);
                    char* field2 = output_block->field_ptr(i, 1);
                    char* field3 = output_block->field_ptr(i, 2);
                    // test null bit
                    EXPECT_FALSE(*reinterpret_cast<bool*>(field1));
                    EXPECT_FALSE(*reinterpret_cast<bool*>(field2));
                    EXPECT_FALSE(*reinterpret_cast<bool*>(field3));
                    uint32_t k1 = *reinterpret_cast<uint32_t*>(field1 + 1);
                    uint32_t k2 = *reinterpret_cast<uint32_t*>(field2 + 1);
                    uint32_t k3 = *reinterpret_cast<uint32_t*>(field3 + 1);
                    EXPECT_EQ(100 * k3 + k2, k1);

                    num_rows_read++;
                }
            }
            EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_DATA_EOF), s);
            EXPECT_TRUE(output_block == nullptr);
            EXPECT_EQ(rowset->rowset_meta()->num_rows(), num_rows_read);
            EXPECT_TRUE(rowset_reader->get_segment_num_rows(&segment_num_rows).ok());
            size_t total_num_rows = 0;
            //EXPECT_EQ(segment_num_rows.size(), num_segments);
            for (const auto& i : segment_num_rows) {
                total_num_rows += i;
            }
            EXPECT_EQ(total_num_rows, num_rows_read);
        }
    }
}

TEST_F(SegCompactionTest, SegCompactionInterleaveWithBig_ooooOOoOooooooooO) {
    config::enable_segcompaction = true;
    config::enable_storage_vectorization = true;
    Status s;
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    RowsetSharedPtr rowset;
    config::segcompaction_small_threshold = 6000; // set threshold above
                                                  // rows_per_segment
    std::vector<uint32_t> segment_num_rows;
    { // write `num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context;
        create_rowset_writer_context(10048, tablet_schema, &writer_context);

        std::unique_ptr<RowsetWriter> rowset_writer;
        s = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        EXPECT_EQ(Status::OK(), s);

        RowCursor input_row;
        input_row.init(tablet_schema);

        // for segment "i", row "rid"
        // k1 := rid*10 + i
        // k2 := k1 * 10
        // k3 := 4096 * i + rid
        int num_segments = 4;
        uint32_t rows_per_segment = 4096;
        for (int i = 0; i < num_segments; ++i) {
            MemPool mem_pool;
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + i;
                uint32_t k2 = i;
                uint32_t k3 = rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                EXPECT_EQ(Status::OK(), s);
            }
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
        }
        num_segments = 2;
        rows_per_segment = 6400;
        for (int i = 0; i < num_segments; ++i) {
            MemPool mem_pool;
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + i;
                uint32_t k2 = i;
                uint32_t k3 = rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                EXPECT_EQ(Status::OK(), s);
            }
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
        }
        num_segments = 1;
        rows_per_segment = 4096;
        for (int i = 0; i < num_segments; ++i) {
            MemPool mem_pool;
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + i;
                uint32_t k2 = i;
                uint32_t k3 = rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                EXPECT_EQ(Status::OK(), s);
            }
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
        }
        num_segments = 1;
        rows_per_segment = 6400;
        for (int i = 0; i < num_segments; ++i) {
            MemPool mem_pool;
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + i;
                uint32_t k2 = i;
                uint32_t k3 = rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                EXPECT_EQ(Status::OK(), s);
            }
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
        }
        num_segments = 8;
        rows_per_segment = 4096;
        for (int i = 0; i < num_segments; ++i) {
            MemPool mem_pool;
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + i;
                uint32_t k2 = i;
                uint32_t k3 = rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                EXPECT_EQ(Status::OK(), s);
            }
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
        }
        num_segments = 1;
        rows_per_segment = 6400;
        for (int i = 0; i < num_segments; ++i) {
            MemPool mem_pool;
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + i;
                uint32_t k2 = i;
                uint32_t k3 = rid;
                input_row.set_field_content(0, reinterpret_cast<char*>(&k1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&k2), &mem_pool);
                input_row.set_field_content(2, reinterpret_cast<char*>(&k3), &mem_pool);
                s = rowset_writer->add_row(input_row);
                EXPECT_EQ(Status::OK(), s);
            }
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
        }

        rowset = rowset_writer->build();
        std::vector<std::string> ls;
        // ooooOOoOooooooooO
        ls.push_back("10048_0.dat"); // oooo
        ls.push_back("10048_1.dat"); // O
        ls.push_back("10048_2.dat"); // O
        ls.push_back("10048_3.dat"); // o
        ls.push_back("10048_4.dat"); // O
        ls.push_back("10048_5.dat"); // oooooooo
        ls.push_back("10048_6.dat"); // O
        EXPECT_TRUE(check_dir(ls));
    }
}

} // namespace doris

// @brief Test Stub

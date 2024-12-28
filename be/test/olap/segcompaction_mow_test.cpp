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
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "util/slice.h"

namespace doris {
using namespace ErrorCode;

static const uint32_t MAX_PATH_LEN = 1024;
static const uint32_t TABLET_ID = 12345;
static StorageEngine* s_engine = nullptr;
static const std::string lTestDir = "./data_test/data/segcompaction_mow_test";

class SegCompactionMoWTest : public ::testing::TestWithParam<std::string> {
public:
    SegCompactionMoWTest() = default;

    void SetUp() {
        config::enable_segcompaction = true;
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;

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
        Status s = engine->open();
        EXPECT_TRUE(s.ok()) << s.to_string();
        s_engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        s = ThreadPoolBuilder("SegCompactionTaskThreadPool")
                    .set_min_threads(config::segcompaction_num_threads)
                    .set_max_threads(config::segcompaction_num_threads)
                    .build(&s_engine->_seg_compaction_thread_pool);
        EXPECT_TRUE(s.ok()) << s.to_string();

        _data_dir = std::make_unique<DataDir>(*s_engine, lTestDir);
        static_cast<void>(_data_dir->update_capacity());

        EXPECT_TRUE(io::global_local_filesystem()->create_directory(lTestDir).ok());
    }

    void TearDown() {
        config::enable_segcompaction = false;
        ExecEnv* exec_env = doris::ExecEnv::GetInstance();
        s_engine = nullptr;
        exec_env->set_storage_engine(nullptr);
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

    // (k1 int, k2 varchar(20), k3 int) keys (k1, k2)
    void create_tablet_schema(TabletSchemaSPtr tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(UNIQUE_KEYS);
        tablet_schema_pb.set_num_short_key_columns(2);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(5);
        // add seq column so that segcompaction will process delete bitmap
        tablet_schema_pb.set_sequence_col_idx(3);

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

        ColumnPB* v_column = tablet_schema_pb.add_column();
        v_column->set_unique_id(3);
        v_column->set_name(fmt::format("v1"));
        v_column->set_type("INT");
        v_column->set_length(4);
        v_column->set_is_key(false);
        v_column->set_is_nullable(false);
        v_column->set_is_bf_column(false);
        v_column->set_default_value(std::to_string(10));
        v_column->set_aggregation("NONE");

        ColumnPB* seq_column = tablet_schema_pb.add_column();
        seq_column->set_unique_id(4);
        seq_column->set_name(SEQUENCE_COL);
        seq_column->set_type("INT");
        seq_column->set_length(4);
        seq_column->set_is_key(false);
        seq_column->set_is_nullable(false);
        seq_column->set_is_bf_column(false);
        seq_column->set_default_value(std::to_string(10));
        seq_column->set_aggregation("NONE");

        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    // use different id to avoid conflict
    void create_rowset_writer_context(int64_t id, TabletSchemaSPtr tablet_schema,
                                      RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(id);
        // rowset_writer_context->data_dir = _data_dir.get();
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->tablet_id = TABLET_ID;
        rowset_writer_context->tablet_schema_hash = 1111;
        rowset_writer_context->partition_id = 10;
        rowset_writer_context->rowset_type = BETA_ROWSET;
        rowset_writer_context->tablet_path = lTestDir;
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->version.first = 10;
        rowset_writer_context->version.second = 10;

        TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = TABLET_ID;
        static_cast<void>(tablet_meta->set_partition_id(10000));
        tablet_meta->_schema = tablet_schema;
        tablet_meta->_enable_unique_key_merge_on_write = true;
        auto tablet = std::make_shared<Tablet>(*s_engine, tablet_meta, _data_dir.get(), "test_str");
        // tablet->key
        rowset_writer_context->tablet = tablet;
    }

    void create_and_init_rowset_reader(Rowset* rowset, RowsetReaderContext& context,
                                       RowsetReaderSharedPtr* result) {
        auto s = rowset->create_reader(result);
        EXPECT_EQ(Status::OK(), s);
        EXPECT_TRUE(*result != nullptr);

        s = (*result)->init(&context);
        EXPECT_EQ(Status::OK(), s);
    }

    bool check_data_read_with_delete_bitmap(TabletSchemaSPtr tablet_schema,
                                            DeleteBitmapPtr delete_bitmap, RowsetSharedPtr rowset,
                                            int expect_total_rows, int rows_mark_deleted,
                                            bool skip_value_check = false) {
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = tablet_schema;
        // use this type to avoid cache from other ut
        reader_context.reader_type = ReaderType::READER_QUERY;
        reader_context.need_ordered_result = true;
        std::vector<uint32_t> return_columns = {0, 1, 2};
        reader_context.return_columns = &return_columns;
        reader_context.stats = &_stats;
        reader_context.delete_bitmap = delete_bitmap.get();

        std::vector<uint32_t> segment_num_rows;
        Status s;

        // without predicates
        {
            RowsetReaderSharedPtr rowset_reader;
            create_and_init_rowset_reader(rowset.get(), reader_context, &rowset_reader);

            uint32_t num_rows_read = 0;
            bool eof = false;
            while (!eof) {
                std::shared_ptr<vectorized::Block> output_block =
                        std::make_shared<vectorized::Block>(
                                tablet_schema->create_block(return_columns));
                s = rowset_reader->next_block(output_block.get());
                if (s != Status::OK()) {
                    eof = true;
                }
                EXPECT_EQ(return_columns.size(), output_block->columns());
                for (int i = 0; i < output_block->rows(); ++i) {
                    vectorized::ColumnPtr col0 = output_block->get_by_position(0).column;
                    vectorized::ColumnPtr col1 = output_block->get_by_position(1).column;
                    vectorized::ColumnPtr col2 = output_block->get_by_position(2).column;
                    auto field1 = (*col0)[i];
                    auto field2 = (*col1)[i];
                    auto field3 = (*col2)[i];
                    uint32_t k1 = *reinterpret_cast<uint32_t*>((char*)(&field1));
                    uint32_t k2 = *reinterpret_cast<uint32_t*>((char*)(&field2));
                    uint32_t v3 = *reinterpret_cast<uint32_t*>((char*)(&field3));
                    EXPECT_EQ(100 * v3 + k2, k1);
                    if (!skip_value_check) {
                        // all v3%3==0 is deleted in all segments with an even number of ids.
                        EXPECT_TRUE(k2 % 2 != 0 || v3 % 3 != 0);
                    }
                    num_rows_read++;
                }
                output_block->clear();
            }
            EXPECT_EQ(Status::Error<END_OF_FILE>(""), s);
            EXPECT_EQ(rowset->rowset_meta()->num_rows(), expect_total_rows);
            EXPECT_EQ(num_rows_read, expect_total_rows - rows_mark_deleted);
            EXPECT_TRUE(rowset_reader->get_segment_num_rows(&segment_num_rows).ok());
            size_t total_num_rows = 0;
            for (const auto& i : segment_num_rows) {
                total_num_rows += i;
            }
            EXPECT_EQ(total_num_rows, expect_total_rows);
        }
        return true;
    }

private:
    std::unique_ptr<DataDir> _data_dir;
};

TEST_P(SegCompactionMoWTest, SegCompactionThenRead) {
    std::string delete_ratio = GetParam();
    config::enable_segcompaction = true;
    Status s;
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    RowsetSharedPtr rowset;
    const int num_segments = 15;
    const uint32_t rows_per_segment = 4096;
    config::segcompaction_candidate_max_rows = 6000; // set threshold above
                                                     // rows_per_segment
    config::segcompaction_batch_size = 10;
    std::vector<uint32_t> segment_num_rows;
    DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(TABLET_ID);
    uint32_t rows_mark_deleted = 0;
    { // write `num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context;
        int raw_rsid = rand();
        create_rowset_writer_context(raw_rsid, tablet_schema, &writer_context);
        RowsetIdUnorderedSet rsids;
        std::vector<RowsetSharedPtr> rowset_ptrs;
        writer_context.mow_context =
                std::make_shared<MowContext>(1, 1, rsids, rowset_ptrs, delete_bitmap);
        auto rowset_id = writer_context.rowset_id;

        auto res = RowsetFactory::create_rowset_writer(*s_engine, writer_context, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();
        EXPECT_EQ(Status::OK(), s);
        // for segment "i", row "rid"
        // k1 := rid*10 + i
        // k2 := k1 * 10
        // k3 := rid
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + i;
                uint32_t k2 = i;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                if (delete_ratio == "full") { // delete all data
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, i, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                } else {
                    // mark delete every 3 rows, for segments that seg_id is even number
                    if (i % 2 == 0 && rid % 3 == 0) {
                        writer_context.mow_context->delete_bitmap->add(
                                {rowset_id, i, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                        rows_mark_deleted++;
                    }
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            sleep(1);
        }

        size_t total_cardinality1 = 0;
        for (auto entry : delete_bitmap->delete_bitmap) {
            total_cardinality1 += entry.second.cardinality();
        }
        if (delete_ratio == "full") {
            EXPECT_EQ(num_segments, delete_bitmap->delete_bitmap.size());
        } else {
            EXPECT_EQ(num_segments / 2 + num_segments % 2, delete_bitmap->delete_bitmap.size());
        }
        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        std::vector<std::string> ls;
        ls.push_back(fmt::format("{}_0.dat", raw_rsid));
        ls.push_back(fmt::format("{}_1.dat", raw_rsid));
        ls.push_back(fmt::format("{}_2.dat", raw_rsid));
        ls.push_back(fmt::format("{}_3.dat", raw_rsid));
        ls.push_back(fmt::format("{}_4.dat", raw_rsid));
        ls.push_back(fmt::format("{}_5.dat", raw_rsid));
        ls.push_back(fmt::format("{}_6.dat", raw_rsid));
        EXPECT_TRUE(check_dir(ls));
        // 7 segments plus 1 sentinel mark
        size_t total_cardinality2 = 0;
        for (auto entry : delete_bitmap->delete_bitmap) {
            if (std::get<1>(entry.first) == DeleteBitmap::INVALID_SEGMENT_ID) {
                continue;
            }
            total_cardinality2 += entry.second.cardinality();
        }
        if (delete_ratio == "full") {
            // 7 segments + 1 sentinel mark
            EXPECT_EQ(8, delete_bitmap->delete_bitmap.size());
        } else {
            EXPECT_EQ(5, delete_bitmap->delete_bitmap.size());
        }
        EXPECT_EQ(total_cardinality1, total_cardinality2);
    }

    EXPECT_TRUE(check_data_read_with_delete_bitmap(tablet_schema, delete_bitmap, rowset,
                                                   num_segments * rows_per_segment,
                                                   rows_mark_deleted));
}

TEST_F(SegCompactionMoWTest, SegCompactionInterleaveWithBig_ooooOOoOooooooooO) {
    config::enable_segcompaction = true;
    Status s;
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    RowsetSharedPtr rowset;
    config::segcompaction_candidate_max_rows = 6000; // set threshold above
                                                     // rows_per_segment
    DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(TABLET_ID);
    uint32_t rows_mark_deleted = 0;
    uint32_t total_written_rows = 0;
    std::vector<uint32_t> segment_num_rows;
    { // write `num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context;
        create_rowset_writer_context(20048, tablet_schema, &writer_context);
        RowsetIdUnorderedSet rsids;
        std::vector<RowsetSharedPtr> rowset_ptrs;
        writer_context.mow_context =
                std::make_shared<MowContext>(1, 1, rsids, rowset_ptrs, delete_bitmap);
        auto rowset_id = writer_context.rowset_id;

        auto res = RowsetFactory::create_rowset_writer(*s_engine, writer_context, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();
        EXPECT_EQ(Status::OK(), s);

        // for segment "i", row "rid"
        // k1 := rid*10 + i
        // k2 := k1 * 10
        // k3 := 4096 * i + rid
        int num_segments = 4;
        uint32_t rows_per_segment = 4096;
        int segid = 0;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            segid++;
            total_written_rows += rows_per_segment;
        }
        num_segments = 2;
        rows_per_segment = 6400;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            segid++;
            total_written_rows += rows_per_segment;
        }
        num_segments = 1;
        rows_per_segment = 4096;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            segid++;
            total_written_rows += rows_per_segment;
        }
        num_segments = 1;
        rows_per_segment = 6400;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            segid++;
            total_written_rows += rows_per_segment;
        }
        num_segments = 8;
        rows_per_segment = 4096;
        std::map<uint32_t, uint32_t> unique_keys;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                // generate some duplicate rows, segment compaction will merge them
                int rand_i = rand() % (num_segments - 3);
                uint32_t k1 = rid * 100 + rand_i;
                uint32_t k2 = rand_i;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows
                if (rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                }
                unique_keys.emplace(k1, rid);
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            sleep(1);
            segid++;
        }
        // these 8 segments should be compacted to 1 segment finally
        // so the finally written rows should be the unique rows after compaction
        total_written_rows += unique_keys.size();
        for (auto entry : unique_keys) {
            if (entry.second % 3 == 0) {
                rows_mark_deleted++;
            }
        }

        num_segments = 1;
        rows_per_segment = 6400;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            sleep(1);
            segid++;
            total_written_rows += rows_per_segment;
        }

        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        std::vector<std::string> ls;
        // ooooOOoOooooooooO
        ls.push_back("20048_0.dat"); // oooo
        ls.push_back("20048_1.dat"); // O
        ls.push_back("20048_2.dat"); // O
        ls.push_back("20048_3.dat"); // o
        ls.push_back("20048_4.dat"); // O
        ls.push_back("20048_5.dat"); // oooooooo
        ls.push_back("20048_6.dat"); // O
        EXPECT_TRUE(check_dir(ls));
        EXPECT_EQ(6, delete_bitmap->delete_bitmap.size());
    }
    EXPECT_TRUE(check_data_read_with_delete_bitmap(tablet_schema, delete_bitmap, rowset,
                                                   total_written_rows, rows_mark_deleted, true));
}

TEST_F(SegCompactionMoWTest, SegCompactionInterleaveWithBig_OoOoO) {
    config::enable_segcompaction = true;
    Status s;
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    RowsetSharedPtr rowset;
    config::segcompaction_candidate_max_rows = 6000; // set threshold above
    config::segcompaction_batch_size = 5;
    std::vector<uint32_t> segment_num_rows;
    DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(TABLET_ID);
    uint32_t rows_mark_deleted = 0;
    uint32_t total_written_rows = 0;
    { // write `num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context;
        create_rowset_writer_context(20049, tablet_schema, &writer_context);
        RowsetIdUnorderedSet rsids;
        std::vector<RowsetSharedPtr> rowset_ptrs;
        writer_context.mow_context =
                std::make_shared<MowContext>(1, 1, rsids, rowset_ptrs, delete_bitmap);
        auto rowset_id = writer_context.rowset_id;

        auto res = RowsetFactory::create_rowset_writer(*s_engine, writer_context, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();
        EXPECT_EQ(Status::OK(), s);

        // for segment "i", row "rid"
        // k1 := rid*10 + i
        // k2 := k1 * 10
        // k3 := 4096 * i + rid
        int num_segments = 1;
        uint32_t rows_per_segment = 6400;
        int segid = 0;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            segid++;
            total_written_rows += rows_per_segment;
        }
        num_segments = 1;
        rows_per_segment = 4096;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            segid++;
            total_written_rows += rows_per_segment;
        }
        num_segments = 1;
        rows_per_segment = 6400;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            segid++;
            total_written_rows += rows_per_segment;
        }
        num_segments = 1;
        rows_per_segment = 4096;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            segid++;
            total_written_rows += rows_per_segment;
        }
        num_segments = 1;
        rows_per_segment = 6400;
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + segid;
                uint32_t k2 = segid;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (segid % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, segid, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            sleep(1);
            segid++;
            total_written_rows += rows_per_segment;
        }

        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        std::vector<std::string> ls;
        ls.push_back("20049_0.dat"); // O
        ls.push_back("20049_1.dat"); // o
        ls.push_back("20049_2.dat"); // O
        ls.push_back("20049_3.dat"); // o
        ls.push_back("20049_4.dat"); // O
        EXPECT_TRUE(check_dir(ls));
    }

    EXPECT_TRUE(check_data_read_with_delete_bitmap(tablet_schema, delete_bitmap, rowset,
                                                   total_written_rows, rows_mark_deleted));
}

TEST_F(SegCompactionMoWTest, SegCompactionNotTrigger) {
    config::enable_segcompaction = true;
    Status s;
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    create_tablet_schema(tablet_schema);

    RowsetSharedPtr rowset;
    const int num_segments = 8;
    const uint32_t rows_per_segment = 4096;
    config::segcompaction_candidate_max_rows = 6000; // set threshold above
                                                     // rows_per_segment
    config::segcompaction_batch_size = 10;
    std::vector<uint32_t> segment_num_rows;
    DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(TABLET_ID);
    uint32_t rows_mark_deleted = 0;
    { // write `num_segments * rows_per_segment` rows to rowset
        RowsetWriterContext writer_context;
        create_rowset_writer_context(20050, tablet_schema, &writer_context);
        RowsetIdUnorderedSet rsids;
        std::vector<RowsetSharedPtr> rowset_ptrs;
        writer_context.mow_context =
                std::make_shared<MowContext>(1, 1, rsids, rowset_ptrs, delete_bitmap);
        auto rowset_id = writer_context.rowset_id;

        auto res = RowsetFactory::create_rowset_writer(*s_engine, writer_context, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();
        EXPECT_EQ(Status::OK(), s);
        // for segment "i", row "rid"
        // k1 := rid*10 + i
        // k2 := k1 * 10
        // k3 := rid
        for (int i = 0; i < num_segments; ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rows_per_segment; ++rid) {
                uint32_t k1 = rid * 100 + i;
                uint32_t k2 = i;
                uint32_t k3 = rid;
                uint32_t seq = 0;
                columns[0]->insert_data((const char*)&k1, sizeof(k1));
                columns[1]->insert_data((const char*)&k2, sizeof(k2));
                columns[2]->insert_data((const char*)&k3, sizeof(k3));
                columns[3]->insert_data((const char*)&seq, sizeof(seq));
                // mark delete every 3 rows, for segments that seg_id is even number
                if (i % 2 == 0 && rid % 3 == 0) {
                    writer_context.mow_context->delete_bitmap->add(
                            {rowset_id, i, DeleteBitmap::TEMP_VERSION_COMMON}, rid);
                    rows_mark_deleted++;
                }
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_EQ(Status::OK(), s);
            sleep(1);
        }

        EXPECT_EQ(num_segments / 2 + num_segments % 2, delete_bitmap->delete_bitmap.size());
        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        std::vector<std::string> ls;
        ls.push_back("20050_0.dat");
        ls.push_back("20050_1.dat");
        ls.push_back("20050_2.dat");
        ls.push_back("20050_3.dat");
        ls.push_back("20050_4.dat");
        ls.push_back("20050_5.dat");
        ls.push_back("20050_6.dat");
        ls.push_back("20050_7.dat");
        EXPECT_TRUE(check_dir(ls));
        EXPECT_EQ(num_segments / 2 + num_segments % 2, delete_bitmap->delete_bitmap.size());

        EXPECT_FALSE(static_cast<BetaRowsetWriter*>(rowset_writer.get())->is_segcompacted());
    }

    EXPECT_TRUE(check_data_read_with_delete_bitmap(tablet_schema, delete_bitmap, rowset,
                                                   num_segments * rows_per_segment,
                                                   rows_mark_deleted));
}

INSTANTIATE_TEST_SUITE_P(Params, SegCompactionMoWTest,
                         ::testing::ValuesIn(std::vector<std::string> {"partial", "full"}));

} // namespace doris

// @brief Test Stub

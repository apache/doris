
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
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "gutil/stringprintf.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "olap/delete_handler.h"
#include "olap/field.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/rowid_conversion.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/olap/vertical_merge_iterator.h"

namespace doris {
using namespace ErrorCode;
namespace vectorized {

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* k_engine = nullptr;

class VerticalCompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        config::vertical_compaction_max_row_source_memory_mb = 1;
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + kTestDir;

        EXPECT_TRUE(io::global_local_filesystem()->delete_and_create_directory(absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_directory(absolute_dir + "/tablet_path")
                            .ok());

        _data_dir = new DataDir(absolute_dir, 100000000);
        static_cast<void>(_data_dir->init());

        doris::EngineOptions options;
        k_engine = new StorageEngine(options);
        ExecEnv::GetInstance()->set_storage_engine(k_engine);
    }
    void TearDown() override {
        SAFE_DELETE(_data_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        if (k_engine != nullptr) {
            k_engine->stop();
            delete k_engine;
            k_engine = nullptr;
            ExecEnv::GetInstance()->set_storage_engine(nullptr);
        }
    }

    TabletSchemaSPtr create_schema(KeysType keys_type = DUP_KEYS, bool without_key = false) {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(keys_type);
        tablet_schema_pb.set_num_short_key_columns(without_key ? 0 : 1);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("c1");
        column_1->set_type("INT");
        column_1->set_is_key(!without_key);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(false);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("c2");
        column_2->set_type("INT");
        column_2->set_length(4);
        column_2->set_index_length(4);
        column_2->set_is_nullable(true);
        column_2->set_is_key(false);
        column_2->set_is_nullable(false);
        column_2->set_is_bf_column(false);

        // unique table must contains the DELETE_SIGN column
        if (keys_type == UNIQUE_KEYS) {
            ColumnPB* column_3 = tablet_schema_pb.add_column();
            column_3->set_unique_id(3);
            column_3->set_name(DELETE_SIGN);
            column_3->set_type("TINYINT");
            column_3->set_length(1);
            column_3->set_index_length(1);
            column_3->set_is_nullable(false);
            column_3->set_is_key(false);
            column_3->set_is_nullable(false);
            column_3->set_is_bf_column(false);
        }

        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    TabletSchemaSPtr create_agg_schema() {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(KeysType::AGG_KEYS);
        tablet_schema_pb.set_num_short_key_columns(1);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("c1");
        column_1->set_type("INT");
        column_1->set_is_key(true);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(false);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("c2");
        column_2->set_type("INT");
        column_2->set_length(4);
        column_2->set_index_length(4);
        column_2->set_is_nullable(true);
        column_2->set_is_key(false);
        column_2->set_is_nullable(false);
        column_2->set_is_bf_column(false);
        column_2->set_aggregation("SUM");

        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    RowsetWriterContext create_rowset_writer_context(TabletSchemaSPtr tablet_schema,
                                                     const SegmentsOverlapPB& overlap,
                                                     uint32_t max_rows_per_segment,
                                                     Version version) {
        static int64_t inc_id = 1000;
        RowsetWriterContext rowset_writer_context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        rowset_writer_context.rowset_id = rowset_id;
        rowset_writer_context.rowset_type = BETA_ROWSET;
        rowset_writer_context.rowset_state = VISIBLE;
        rowset_writer_context.tablet_schema = tablet_schema;
        rowset_writer_context.rowset_dir = absolute_dir + "/tablet_path";
        rowset_writer_context.version = version;
        rowset_writer_context.segments_overlap = overlap;
        rowset_writer_context.max_rows_per_segment = max_rows_per_segment;
        inc_id++;
        return rowset_writer_context;
    }

    void create_and_init_rowset_reader(Rowset* rowset, RowsetReaderContext& context,
                                       RowsetReaderSharedPtr* result) {
        auto s = rowset->create_reader(result);
        EXPECT_TRUE(s.ok());
        EXPECT_TRUE(*result != nullptr);

        s = (*result)->init(&context);
        EXPECT_TRUE(s.ok());
    }

    RowsetSharedPtr create_rowset(
            TabletSchemaSPtr tablet_schema, const SegmentsOverlapPB& overlap,
            std::vector<std::vector<std::tuple<int64_t, int64_t>>> rowset_data, int64_t version) {
        if (overlap == NONOVERLAPPING) {
            for (auto i = 1; i < rowset_data.size(); i++) {
                auto& last_seg_data = rowset_data[i - 1];
                auto& cur_seg_data = rowset_data[i];
                int64_t last_seg_max = std::get<0>(last_seg_data[last_seg_data.size() - 1]);
                int64_t cur_seg_min = std::get<0>(cur_seg_data[0]);
                EXPECT_LT(last_seg_max, cur_seg_min);
            }
        }
        auto writer_context = create_rowset_writer_context(tablet_schema, overlap, UINT32_MAX,
                                                           {version, version});

        std::unique_ptr<RowsetWriter> rowset_writer;
        Status s = RowsetFactory::create_rowset_writer(writer_context, false, &rowset_writer);
        EXPECT_TRUE(s.ok());

        uint32_t num_rows = 0;
        for (int i = 0; i < rowset_data.size(); ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rowset_data[i].size(); ++rid) {
                int32_t c1 = std::get<0>(rowset_data[i][rid]);
                int32_t c2 = std::get<1>(rowset_data[i][rid]);
                columns[0]->insert_data((const char*)&c1, sizeof(c1));
                columns[1]->insert_data((const char*)&c2, sizeof(c2));

                if (tablet_schema->keys_type() == UNIQUE_KEYS) {
                    uint8_t num = 0;
                    columns[2]->insert_data((const char*)&num, sizeof(num));
                }
                num_rows++;
            }
            s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_TRUE(s.ok());
        }

        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        EXPECT_EQ(rowset_data.size(), rowset->rowset_meta()->num_segments());
        EXPECT_EQ(num_rows, rowset->rowset_meta()->num_rows());
        return rowset;
    }

    void init_rs_meta(RowsetMetaSharedPtr& rs_meta, int64_t start, int64_t end) {
        std::string json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "empty": false
        })";
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);
        rs_meta->init_from_pb(rowset_meta_pb);
    }

    RowsetSharedPtr create_delete_predicate(const TabletSchemaSPtr& schema,
                                            DeletePredicatePB del_pred, int64_t version) {
        RowsetMetaSharedPtr rsm(new RowsetMeta());
        init_rs_meta(rsm, version, version);
        RowsetId id;
        id.init(version * 1000);
        rsm->set_rowset_id(id);
        rsm->set_delete_predicate(std::move(del_pred));
        rsm->set_tablet_schema(schema);
        return std::make_shared<BetaRowset>(schema, "", rsm);
    }

    TabletSharedPtr create_tablet(const TabletSchema& tablet_schema,
                                  bool enable_unique_key_merge_on_write) {
        std::vector<TColumn> cols;
        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
        for (auto i = 0; i < tablet_schema.num_columns(); i++) {
            const TabletColumn& column = tablet_schema.column(i);
            TColumn col;
            col.column_type.type = TPrimitiveType::INT;
            col.__set_column_name(column.name());
            col.__set_is_key(column.is_key());
            cols.push_back(col);
            col_ordinal_to_unique_id[i] = column.unique_id();
        }

        TTabletSchema t_tablet_schema;
        t_tablet_schema.__set_short_key_column_count(tablet_schema.num_short_key_columns());
        t_tablet_schema.__set_schema_hash(3333);
        if (tablet_schema.keys_type() == UNIQUE_KEYS) {
            t_tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        } else if (tablet_schema.keys_type() == DUP_KEYS) {
            t_tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
        } else if (tablet_schema.keys_type() == AGG_KEYS) {
            t_tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
        }
        t_tablet_schema.__set_storage_type(TStorageType::COLUMN);
        t_tablet_schema.__set_columns(cols);
        TabletMetaSharedPtr tablet_meta(
                new TabletMeta(2, 2, 2, 2, 2, 2, t_tablet_schema, 2, col_ordinal_to_unique_id,
                               UniqueId(1, 2), TTabletType::TABLET_TYPE_DISK,
                               TCompressionType::LZ4F, 0, enable_unique_key_merge_on_write));

        TabletSharedPtr tablet(new Tablet(tablet_meta, _data_dir));
        static_cast<void>(tablet->init());
        bool exists = false;
        auto res = io::global_local_filesystem()->exists(tablet->tablet_path(), &exists);
        EXPECT_TRUE(res.ok() && !exists);
        res = io::global_local_filesystem()->create_directory(tablet->tablet_path());
        EXPECT_TRUE(res.ok());
        return tablet;
    }

    // all rowset's data are same
    void generate_input_data(
            uint32_t num_input_rowset, uint32_t num_segments, uint32_t rows_per_segment,
            const SegmentsOverlapPB& overlap,
            std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>>& input_data) {
        for (auto i = 0; i < num_input_rowset; i++) {
            std::vector<std::vector<std::tuple<int64_t, int64_t>>> rowset_data;
            for (auto j = 0; j < num_segments; j++) {
                std::vector<std::tuple<int64_t, int64_t>> segment_data;
                for (auto n = 0; n < rows_per_segment; n++) {
                    int64_t c1 = j * rows_per_segment + n;
                    int64_t c2 = c1 + 1;
                    segment_data.emplace_back(c1, c2);
                }
                rowset_data.emplace_back(segment_data);
            }
            input_data.emplace_back(rowset_data);
        }
    }

    void block_create(TabletSchemaSPtr tablet_schema, vectorized::Block* block) {
        block->clear();
        Schema schema(tablet_schema);
        const auto& column_ids = schema.column_ids();
        for (size_t i = 0; i < schema.num_column_ids(); ++i) {
            auto column_desc = schema.column(column_ids[i]);
            auto data_type = Schema::get_data_type_ptr(*column_desc);
            EXPECT_TRUE(data_type != nullptr);
            auto column = data_type->create_column();
            block->insert(vectorized::ColumnWithTypeAndName(std::move(column), data_type,
                                                            column_desc->name()));
        }
    }

private:
    const std::string kTestDir = "/ut_dir/vertical_compaction_test";
    string absolute_dir;
    DataDir* _data_dir = nullptr;
};

TEST_F(VerticalCompactionTest, TestRowSourcesBuffer) {
    RowSourcesBuffer buffer(100, absolute_dir, ReaderType::READER_CUMULATIVE_COMPACTION);
    RowSource s1(0, 0);
    RowSource s2(0, 0);
    RowSource s3(1, 1);
    RowSource s4(1, 0);
    RowSource s5(2, 0);
    RowSource s6(2, 0);
    std::vector<RowSource> tmp_row_source;
    tmp_row_source.emplace_back(s1);
    tmp_row_source.emplace_back(s2);
    tmp_row_source.emplace_back(s3);
    tmp_row_source.emplace_back(s4);
    tmp_row_source.emplace_back(s5);
    tmp_row_source.emplace_back(s6);

    EXPECT_TRUE(buffer.append(tmp_row_source).ok());
    EXPECT_EQ(buffer.total_size(), 6);
    size_t limit = 10;
    static_cast<void>(buffer.flush());
    static_cast<void>(buffer.seek_to_begin());

    int idx = -1;
    while (buffer.has_remaining().ok()) {
        if (++idx == 1) {
            EXPECT_TRUE(buffer.current().agg_flag());
        }
        auto cur = buffer.current().get_source_num();
        auto same = buffer.same_source_count(cur, limit);
        EXPECT_EQ(same, 2);
        buffer.advance(same);
    }

    RowSourcesBuffer buffer1(101, absolute_dir, ReaderType::READER_CUMULATIVE_COMPACTION);
    EXPECT_TRUE(buffer1.append(tmp_row_source).ok());
    EXPECT_TRUE(buffer1.append(tmp_row_source).ok());
    buffer1.set_agg_flag(2, false);
    buffer1.set_agg_flag(4, true);
    static_cast<void>(buffer1.flush());
    static_cast<void>(buffer1.seek_to_begin());
    EXPECT_EQ(buffer1.total_size(), 12);
    idx = -1;
    while (buffer1.has_remaining().ok()) {
        if (++idx == 1) {
            EXPECT_FALSE(buffer1.current().agg_flag());
        }
        if (++idx == 0) {
            EXPECT_TRUE(buffer1.current().agg_flag());
        }
        std::cout << buffer1.buf_idx() << std::endl;
        auto cur = buffer1.current().get_source_num();
        auto same = buffer1.same_source_count(cur, limit);
        EXPECT_EQ(same, 2);
        buffer1.advance(same);
    }
}

TEST_F(VerticalCompactionTest, TestDupKeyVerticalMerge) {
    auto num_input_rowset = 2;
    auto num_segments = 2;
    auto rows_per_segment = 2 * 100 * 1024;
    SegmentsOverlapPB overlap = NONOVERLAPPING;
    std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
    generate_input_data(num_input_rowset, num_segments, rows_per_segment, overlap, input_data);
    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
            }
        }
    }

    TabletSchemaSPtr tablet_schema = create_schema();
    // create input rowset
    vector<RowsetSharedPtr> input_rowsets;
    SegmentsOverlapPB new_overlap = overlap;
    for (auto i = 0; i < num_input_rowset; i++) {
        if (overlap == OVERLAP_UNKNOWN) {
            if (i == 0) {
                new_overlap = NONOVERLAPPING;
            } else {
                new_overlap = OVERLAPPING;
            }
        }
        RowsetSharedPtr rowset = create_rowset(tablet_schema, new_overlap, input_data[i], i);
        input_rowsets.push_back(rowset);
    }
    // create input rowset reader
    vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        ASSERT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    // create output rowset writer
    auto writer_context = create_rowset_writer_context(tablet_schema, NONOVERLAPPING, 3456,
                                                       {0, input_rowsets.back()->end_version()});
    std::unique_ptr<RowsetWriter> output_rs_writer;
    Status s = RowsetFactory::create_rowset_writer(writer_context, true, &output_rs_writer);
    ASSERT_TRUE(s.ok()) << s;

    // merge input rowset
    TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    s = Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, tablet_schema,
                                       input_rs_readers, output_rs_writer.get(), 100, &stats);
    ASSERT_TRUE(s.ok()) << s;
    RowsetSharedPtr out_rowset;
    EXPECT_EQ(Status::OK(), output_rs_writer->build(out_rowset));
    ASSERT_TRUE(out_rowset);

    // create output rowset reader
    RowsetReaderContext reader_context;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = false;
    std::vector<uint32_t> return_columns = {0, 1};
    reader_context.return_columns = &return_columns;
    RowsetReaderSharedPtr output_rs_reader;
    LOG(INFO) << "create rowset reader in test";
    create_and_init_rowset_reader(out_rowset.get(), reader_context, &output_rs_reader);

    // read output rowset data
    vectorized::Block output_block;
    std::vector<std::tuple<int64_t, int64_t>> output_data;
    do {
        block_create(tablet_schema, &output_block);
        s = output_rs_reader->next_block(&output_block);
        auto columns = output_block.get_columns_with_type_and_name();
        EXPECT_EQ(columns.size(), 2);
        for (auto i = 0; i < output_block.rows(); i++) {
            output_data.emplace_back(columns[0].column->get_int(i), columns[1].column->get_int(i));
        }
    } while (s == Status::OK());
    EXPECT_EQ(Status::Error<END_OF_FILE>(""), s);
    EXPECT_EQ(out_rowset->rowset_meta()->num_rows(), output_data.size());
    EXPECT_EQ(output_data.size(), num_input_rowset * num_segments * rows_per_segment);
    std::vector<uint32_t> segment_num_rows;
    EXPECT_TRUE(output_rs_reader->get_segment_num_rows(&segment_num_rows).ok());
    // check vertical compaction result
    for (auto id = 0; id < output_data.size(); id++) {
        LOG(INFO) << "output data: " << std::get<0>(output_data[id]) << " "
                  << std::get<1>(output_data[id]);
    }
    int dst_id = 0;
    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        dst_id = 0;
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
                EXPECT_EQ(std::get<0>(input_data[rs_id][s_id][row_id]),
                          std::get<0>(output_data[dst_id]));
                EXPECT_EQ(std::get<1>(input_data[rs_id][s_id][row_id]),
                          std::get<1>(output_data[dst_id]));
                dst_id += 2;
            }
        }
    }
}

TEST_F(VerticalCompactionTest, TestDupWithoutKeyVerticalMerge) {
    auto num_input_rowset = 2;
    auto num_segments = 2;
    auto rows_per_segment = 2 * 100 * 1024;
    SegmentsOverlapPB overlap = NONOVERLAPPING;
    std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
    generate_input_data(num_input_rowset, num_segments, rows_per_segment, overlap, input_data);
    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
            }
        }
    }

    TabletSchemaSPtr tablet_schema = create_schema(DUP_KEYS, true);
    // create input rowset
    vector<RowsetSharedPtr> input_rowsets;
    SegmentsOverlapPB new_overlap = overlap;
    for (auto i = 0; i < num_input_rowset; i++) {
        if (overlap == OVERLAP_UNKNOWN) {
            if (i == 0) {
                new_overlap = NONOVERLAPPING;
            } else {
                new_overlap = OVERLAPPING;
            }
        }
        RowsetSharedPtr rowset = create_rowset(tablet_schema, new_overlap, input_data[i], i);
        input_rowsets.push_back(rowset);
    }
    // create input rowset reader
    vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    // create output rowset writer
    auto writer_context = create_rowset_writer_context(tablet_schema, NONOVERLAPPING, 3456,
                                                       {0, input_rowsets.back()->end_version()});
    std::unique_ptr<RowsetWriter> output_rs_writer;
    Status s = RowsetFactory::create_rowset_writer(writer_context, true, &output_rs_writer);
    EXPECT_TRUE(s.ok());

    // merge input rowset
    TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    s = Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, tablet_schema,
                                       input_rs_readers, output_rs_writer.get(), 100, &stats);
    ASSERT_TRUE(s.ok()) << s;
    RowsetSharedPtr out_rowset;
    EXPECT_EQ(Status::OK(), output_rs_writer->build(out_rowset));

    // create output rowset reader
    RowsetReaderContext reader_context;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = false;
    std::vector<uint32_t> return_columns = {0, 1};
    reader_context.return_columns = &return_columns;
    RowsetReaderSharedPtr output_rs_reader;
    LOG(INFO) << "create rowset reader in test";
    create_and_init_rowset_reader(out_rowset.get(), reader_context, &output_rs_reader);

    // read output rowset data
    vectorized::Block output_block;
    std::vector<std::tuple<int64_t, int64_t>> output_data;
    do {
        block_create(tablet_schema, &output_block);
        s = output_rs_reader->next_block(&output_block);
        auto columns = output_block.get_columns_with_type_and_name();
        EXPECT_EQ(columns.size(), 2);
        for (auto i = 0; i < output_block.rows(); i++) {
            output_data.emplace_back(columns[0].column->get_int(i), columns[1].column->get_int(i));
        }
    } while (s == Status::OK());
    EXPECT_EQ(Status::Error<END_OF_FILE>(""), s);
    EXPECT_EQ(out_rowset->rowset_meta()->num_rows(), output_data.size());
    EXPECT_EQ(output_data.size(), num_input_rowset * num_segments * rows_per_segment);
    std::vector<uint32_t> segment_num_rows;
    EXPECT_TRUE(output_rs_reader->get_segment_num_rows(&segment_num_rows).ok());
    // check vertical compaction result
    for (auto id = 0; id < output_data.size(); id++) {
        LOG(INFO) << "output data: " << std::get<0>(output_data[id]) << " "
                  << std::get<1>(output_data[id]);
    }
    int dst_id = 0;

    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        dst_id = 0;
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
                EXPECT_EQ(std::get<0>(input_data[rs_id][s_id][row_id]),
                          std::get<0>(output_data[dst_id]));
                EXPECT_EQ(std::get<1>(input_data[rs_id][s_id][row_id]),
                          std::get<1>(output_data[dst_id]));
                dst_id += 1;
            }
        }
    }
}

TEST_F(VerticalCompactionTest, TestUniqueKeyVerticalMerge) {
    auto num_input_rowset = 2;
    auto num_segments = 2;
    auto rows_per_segment = 2 * 100 * 1024;
    SegmentsOverlapPB overlap = NONOVERLAPPING;
    std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
    generate_input_data(num_input_rowset, num_segments, rows_per_segment, overlap, input_data);
    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
            }
        }
    }

    TabletSchemaSPtr tablet_schema = create_schema(UNIQUE_KEYS);
    // create input rowset
    vector<RowsetSharedPtr> input_rowsets;
    SegmentsOverlapPB new_overlap = overlap;
    for (auto i = 0; i < num_input_rowset; i++) {
        if (overlap == OVERLAP_UNKNOWN) {
            if (i == 0) {
                new_overlap = NONOVERLAPPING;
            } else {
                new_overlap = OVERLAPPING;
            }
        }
        RowsetSharedPtr rowset = create_rowset(tablet_schema, new_overlap, input_data[i], i);
        input_rowsets.push_back(rowset);
    }
    // create input rowset reader
    vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    // create output rowset writer
    auto writer_context = create_rowset_writer_context(tablet_schema, NONOVERLAPPING, 3456,
                                                       {0, input_rowsets.back()->end_version()});
    std::unique_ptr<RowsetWriter> output_rs_writer;
    Status s = RowsetFactory::create_rowset_writer(writer_context, true, &output_rs_writer);
    EXPECT_TRUE(s.ok());

    // merge input rowset
    TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    s = Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, tablet_schema,
                                       input_rs_readers, output_rs_writer.get(), 10000, &stats);
    EXPECT_TRUE(s.ok());
    RowsetSharedPtr out_rowset;
    EXPECT_EQ(Status::OK(), output_rs_writer->build(out_rowset));

    // create output rowset reader
    RowsetReaderContext reader_context;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = false;
    std::vector<uint32_t> return_columns = {0, 1};
    reader_context.return_columns = &return_columns;
    RowsetReaderSharedPtr output_rs_reader;
    LOG(INFO) << "create rowset reader in test";
    create_and_init_rowset_reader(out_rowset.get(), reader_context, &output_rs_reader);

    // read output rowset data
    vectorized::Block output_block;
    std::vector<std::tuple<int64_t, int64_t>> output_data;
    do {
        block_create(tablet_schema, &output_block);
        s = output_rs_reader->next_block(&output_block);
        auto columns = output_block.get_columns_with_type_and_name();
        EXPECT_EQ(columns.size(), 2);
        for (auto i = 0; i < output_block.rows(); i++) {
            output_data.emplace_back(columns[0].column->get_int(i), columns[1].column->get_int(i));
        }
    } while (s == Status::OK());
    EXPECT_EQ(Status::Error<END_OF_FILE>(""), s);
    EXPECT_EQ(out_rowset->rowset_meta()->num_rows(), output_data.size());
    EXPECT_EQ(output_data.size(), num_segments * rows_per_segment);
    std::vector<uint32_t> segment_num_rows;
    EXPECT_TRUE(output_rs_reader->get_segment_num_rows(&segment_num_rows).ok());
    // check vertical compaction result
    for (auto id = 0; id < output_data.size(); id++) {
        LOG(INFO) << "output data: " << std::get<0>(output_data[id]) << " "
                  << std::get<1>(output_data[id]);
    }
    int dst_id = 0;
    for (auto s_id = 0; s_id < input_data[0].size(); s_id++) {
        for (auto row_id = 0; row_id < input_data[0][s_id].size(); row_id++) {
            EXPECT_EQ(std::get<0>(input_data[0][s_id][row_id]), std::get<0>(output_data[dst_id]));
            EXPECT_EQ(std::get<1>(input_data[0][s_id][row_id]), std::get<1>(output_data[dst_id]));
            dst_id++;
        }
    }
}

TEST_F(VerticalCompactionTest, TestDupKeyVerticalMergeWithDelete) {
    auto num_input_rowset = 2;
    auto num_segments = 2;
    auto rows_per_segment = 2 * 100 * 1024;
    SegmentsOverlapPB overlap = NONOVERLAPPING;
    std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
    generate_input_data(num_input_rowset, num_segments, rows_per_segment, overlap, input_data);
    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
            }
        }
    }

    TabletSchemaSPtr tablet_schema = create_schema(DUP_KEYS);
    TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
    // create input rowset
    SegmentsOverlapPB new_overlap = overlap;
    std::vector<RowsetSharedPtr> input_rowsets;
    for (auto i = 0; i < num_input_rowset; i++) {
        if (overlap == OVERLAP_UNKNOWN) {
            new_overlap = (i == 0) ? NONOVERLAPPING : OVERLAPPING;
        }
        input_rowsets.push_back(create_rowset(tablet_schema, new_overlap, input_data[i], i));
    }

    // delete data with key < 100
    std::vector<TCondition> conditions;
    TCondition condition;
    condition.column_name = tablet->tablet_schema()->column(0).name();
    condition.condition_op = "<";
    condition.condition_values.clear();
    condition.condition_values.push_back("100");
    conditions.push_back(condition);
    DeletePredicatePB del_pred;
    auto st = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                       &del_pred);
    ASSERT_TRUE(st.ok()) << st;
    input_rowsets.push_back(create_delete_predicate(tablet->tablet_schema(), std::move(del_pred),
                                                    num_input_rowset));

    // create input rowset reader
    vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        ASSERT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    // create output rowset writer
    auto writer_context = create_rowset_writer_context(tablet_schema, NONOVERLAPPING, 3456,
                                                       {0, input_rowsets.back()->end_version()});
    std::unique_ptr<RowsetWriter> output_rs_writer;
    st = RowsetFactory::create_rowset_writer(writer_context, true, &output_rs_writer);
    ASSERT_TRUE(st.ok()) << st;
    // merge input rowset
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    st = Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, tablet_schema,
                                        input_rs_readers, output_rs_writer.get(), 100, &stats);
    ASSERT_TRUE(st.ok()) << st;
    RowsetSharedPtr out_rowset;
    EXPECT_EQ(Status::OK(), output_rs_writer->build(out_rowset));
    ASSERT_TRUE(out_rowset);

    // create output rowset reader
    RowsetReaderContext reader_context;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = false;
    std::vector<uint32_t> return_columns = {0, 1};
    reader_context.return_columns = &return_columns;
    RowsetReaderSharedPtr output_rs_reader;
    LOG(INFO) << "create rowset reader in test";
    create_and_init_rowset_reader(out_rowset.get(), reader_context, &output_rs_reader);

    // read output rowset data
    vectorized::Block output_block;
    std::vector<std::tuple<int64_t, int64_t>> output_data;
    do {
        block_create(tablet_schema, &output_block);
        st = output_rs_reader->next_block(&output_block);
        auto columns = output_block.get_columns_with_type_and_name();
        EXPECT_EQ(columns.size(), 2);
        for (auto i = 0; i < output_block.rows(); i++) {
            output_data.emplace_back(columns[0].column->get_int(i), columns[1].column->get_int(i));
        }
    } while (st.ok());
    EXPECT_TRUE(st.is<END_OF_FILE>()) << st;
    EXPECT_EQ(out_rowset->rowset_meta()->num_rows(), output_data.size());
    EXPECT_EQ(output_data.size(),
              num_input_rowset * num_segments * rows_per_segment - num_input_rowset * 100);
    std::vector<uint32_t> segment_num_rows;
    EXPECT_TRUE(output_rs_reader->get_segment_num_rows(&segment_num_rows).ok());
    // All keys less than 1000 are deleted by delete handler
    for (auto& item : output_data) {
        ASSERT_GE(std::get<0>(item), 100);
    }
}

TEST_F(VerticalCompactionTest, TestDupWithoutKeyVerticalMergeWithDelete) {
    auto num_input_rowset = 2;
    auto num_segments = 2;
    auto rows_per_segment = 2 * 100 * 1024;
    SegmentsOverlapPB overlap = NONOVERLAPPING;
    std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
    generate_input_data(num_input_rowset, num_segments, rows_per_segment, overlap, input_data);
    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
            }
        }
    }

    TabletSchemaSPtr tablet_schema = create_schema(DUP_KEYS, true);
    TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
    // create input rowset
    SegmentsOverlapPB new_overlap = overlap;
    std::vector<RowsetSharedPtr> input_rowsets;
    for (auto i = 0; i < num_input_rowset; i++) {
        if (overlap == OVERLAP_UNKNOWN) {
            new_overlap = (i == 0) ? NONOVERLAPPING : OVERLAPPING;
        }
        input_rowsets.push_back(create_rowset(tablet_schema, new_overlap, input_data[i], i));
    }

    // delete data with key < 100
    std::vector<TCondition> conditions;
    TCondition condition;
    condition.column_name = tablet->tablet_schema()->column(0).name();
    condition.condition_op = "<";
    condition.condition_values.clear();
    condition.condition_values.push_back("100");
    conditions.push_back(condition);
    DeletePredicatePB del_pred;
    auto st = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                       &del_pred);
    ASSERT_TRUE(st.ok()) << st;
    input_rowsets.push_back(create_delete_predicate(tablet->tablet_schema(), std::move(del_pred),
                                                    num_input_rowset));

    // create input rowset reader
    vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        ASSERT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    // create output rowset writer
    auto writer_context = create_rowset_writer_context(tablet_schema, NONOVERLAPPING, 3456,
                                                       {0, input_rowsets.back()->end_version()});
    std::unique_ptr<RowsetWriter> output_rs_writer;
    st = RowsetFactory::create_rowset_writer(writer_context, true, &output_rs_writer);
    ASSERT_TRUE(st.ok()) << st;
    // merge input rowset
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    st = Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, tablet_schema,
                                        input_rs_readers, output_rs_writer.get(), 100, &stats);
    ASSERT_TRUE(st.ok()) << st;
    RowsetSharedPtr out_rowset;
    EXPECT_EQ(Status::OK(), output_rs_writer->build(out_rowset));
    ASSERT_TRUE(out_rowset);

    // create output rowset reader
    RowsetReaderContext reader_context;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = false;
    std::vector<uint32_t> return_columns = {0, 1};
    reader_context.return_columns = &return_columns;
    RowsetReaderSharedPtr output_rs_reader;
    LOG(INFO) << "create rowset reader in test";
    create_and_init_rowset_reader(out_rowset.get(), reader_context, &output_rs_reader);

    // read output rowset data
    vectorized::Block output_block;
    std::vector<std::tuple<int64_t, int64_t>> output_data;
    do {
        block_create(tablet_schema, &output_block);
        st = output_rs_reader->next_block(&output_block);
        auto columns = output_block.get_columns_with_type_and_name();
        EXPECT_EQ(columns.size(), 2);
        for (auto i = 0; i < output_block.rows(); i++) {
            output_data.emplace_back(columns[0].column->get_int(i), columns[1].column->get_int(i));
        }
    } while (st.ok());
    EXPECT_TRUE(st.is<END_OF_FILE>()) << st;
    EXPECT_EQ(out_rowset->rowset_meta()->num_rows(), output_data.size());
    EXPECT_EQ(output_data.size(),
              num_input_rowset * num_segments * rows_per_segment - num_input_rowset * 100);
    std::vector<uint32_t> segment_num_rows;
    EXPECT_TRUE(output_rs_reader->get_segment_num_rows(&segment_num_rows).ok());
    // All keys less than 1000 are deleted by delete handler
    for (auto& item : output_data) {
        ASSERT_GE(std::get<0>(item), 100);
    }
}

TEST_F(VerticalCompactionTest, TestAggKeyVerticalMerge) {
    auto num_input_rowset = 2;
    auto num_segments = 2;
    auto rows_per_segment = 2 * 100 * 1024;
    SegmentsOverlapPB overlap = NONOVERLAPPING;
    std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
    generate_input_data(num_input_rowset, num_segments, rows_per_segment, overlap, input_data);
    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
            }
        }
    }

    TabletSchemaSPtr tablet_schema = create_agg_schema();
    // create input rowset
    vector<RowsetSharedPtr> input_rowsets;
    SegmentsOverlapPB new_overlap = overlap;
    for (auto i = 0; i < num_input_rowset; i++) {
        if (overlap == OVERLAP_UNKNOWN) {
            if (i == 0) {
                new_overlap = NONOVERLAPPING;
            } else {
                new_overlap = OVERLAPPING;
            }
        }
        RowsetSharedPtr rowset = create_rowset(tablet_schema, new_overlap, input_data[i], i);
        input_rowsets.push_back(rowset);
    }
    // create input rowset reader
    vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    // create output rowset writer
    auto writer_context = create_rowset_writer_context(tablet_schema, NONOVERLAPPING, 3456,
                                                       {0, input_rowsets.back()->end_version()});
    std::unique_ptr<RowsetWriter> output_rs_writer;
    Status s = RowsetFactory::create_rowset_writer(writer_context, true, &output_rs_writer);
    EXPECT_TRUE(s.ok());

    // merge input rowset
    TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
    Merger::Statistics stats;
    RowIdConversion rowid_conversion;
    stats.rowid_conversion = &rowid_conversion;
    s = Merger::vertical_merge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, tablet_schema,
                                       input_rs_readers, output_rs_writer.get(), 100, &stats);
    EXPECT_TRUE(s.ok());
    RowsetSharedPtr out_rowset;
    EXPECT_EQ(Status::OK(), output_rs_writer->build(out_rowset));

    // create output rowset reader
    RowsetReaderContext reader_context;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = false;
    std::vector<uint32_t> return_columns = {0, 1};
    reader_context.return_columns = &return_columns;
    RowsetReaderSharedPtr output_rs_reader;
    LOG(INFO) << "create rowset reader in test";
    create_and_init_rowset_reader(out_rowset.get(), reader_context, &output_rs_reader);

    // read output rowset data
    vectorized::Block output_block;
    std::vector<std::tuple<int64_t, int64_t>> output_data;
    do {
        block_create(tablet_schema, &output_block);
        s = output_rs_reader->next_block(&output_block);
        auto columns = output_block.get_columns_with_type_and_name();
        EXPECT_EQ(columns.size(), 2);
        for (auto i = 0; i < output_block.rows(); i++) {
            output_data.emplace_back(columns[0].column->get_int(i), columns[1].column->get_int(i));
        }
    } while (s == Status::OK());
    EXPECT_EQ(Status::Error<END_OF_FILE>(""), s);
    EXPECT_EQ(out_rowset->rowset_meta()->num_rows(), output_data.size());
    EXPECT_EQ(output_data.size(), num_segments * rows_per_segment);
    std::vector<uint32_t> segment_num_rows;
    EXPECT_TRUE(output_rs_reader->get_segment_num_rows(&segment_num_rows).ok());
    // check vertical compaction result
    for (auto id = 0; id < output_data.size(); id++) {
        LOG(INFO) << "output data: " << std::get<0>(output_data[id]) << " "
                  << std::get<1>(output_data[id]);
    }
    int dst_id = 0;
    for (auto s_id = 0; s_id < input_data[0].size(); s_id++) {
        for (auto row_id = 0; row_id < input_data[0][s_id].size(); row_id++) {
            LOG(INFO) << "input data: " << std::get<0>(input_data[0][s_id][row_id]) << " "
                      << std::get<1>(input_data[0][s_id][row_id]);
            EXPECT_EQ(std::get<0>(input_data[0][s_id][row_id]), std::get<0>(output_data[dst_id]));
            EXPECT_EQ(std::get<1>(input_data[0][s_id][row_id]) * 2,
                      std::get<1>(output_data[dst_id]));
            dst_id++;
        }
    }
}

} // namespace vectorized
} // namespace doris

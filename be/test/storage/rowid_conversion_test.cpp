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

#include "storage/rowid_conversion.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>

#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "json2pb/json_to_pb.h"
#include "runtime/exec_env.h"
#include "storage/compaction_task_tracker.h"
#include "storage/delete/delete_handler.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/merger.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;

class TestRowIdConversion : public testing::TestWithParam<std::tuple<KeysType, bool, bool, bool>> {
protected:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + kTestDir;
        auto st = io::global_local_filesystem()->delete_directory(absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_directory(absolute_dir + "/tablet_path")
                            .ok());

        std::vector<StorePath> tmp_paths;
        tmp_paths.emplace_back(absolute_dir, 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(tmp_paths);
        st = tmp_file_dirs->init();
        ASSERT_TRUE(st.ok()) << st;
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        engine_ref = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_tmp_file_dir(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    TabletSchemaSPtr create_schema(KeysType keys_type = DUP_KEYS,
                                   bool with_inverted_index = false) {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(keys_type);
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

        if (with_inverted_index) {
            tablet_schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
            auto* index = tablet_schema_pb.add_index();
            index->set_index_id(1);
            index->set_index_name("c2_idx");
            index->set_index_type(IndexType::INVERTED);
            index->add_col_unique_id(2);
        }

        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    RowsetWriterContext create_rowset_writer_context(TabletSchemaSPtr tablet_schema,
                                                     const SegmentsOverlapPB& overlap,
                                                     uint32_t max_rows_per_segment,
                                                     Version version) {
        // FIXME(plat1ko): If `inc_id` set to 1000, and run `VerticalCompactionTest` before `TestRowIdConversion`,
        //  will `TestRowIdConversion` will fail. There may be some strange global states here.
        static int64_t inc_id = 0;
        RowsetWriterContext rowset_writer_context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        rowset_writer_context.rowset_id = rowset_id;
        rowset_writer_context.rowset_type = BETA_ROWSET;
        rowset_writer_context.rowset_state = VISIBLE;
        rowset_writer_context.tablet_schema = tablet_schema;
        rowset_writer_context.tablet_path = absolute_dir + "/tablet_path";
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
        auto res = RowsetFactory::create_rowset_writer(*engine_ref, writer_context, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        uint32_t num_rows = 0;
        for (int i = 0; i < rowset_data.size(); ++i) {
            Block block = tablet_schema->create_block();
            auto columns = std::move(block).mutate_columns();
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
            block.set_columns(std::move(columns));
            auto s = rowset_writer->add_block(&block);
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
            "partition_id": 10000,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "empty": false
        })";
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rs_meta->init_from_pb(rowset_meta_pb);
    }

    RowsetSharedPtr create_delete_predicate(const TabletSchemaSPtr& schema,
                                            DeletePredicatePB del_pred, int64_t version) {
        RowsetMetaSharedPtr rsm(new RowsetMeta());
        init_rs_meta(rsm, version, version);
        RowsetId id;
        id.init(version);
        rsm->set_rowset_id(id);
        rsm->set_delete_predicate(std::move(del_pred));
        rsm->set_tablet_schema(schema);
        return std::make_shared<BetaRowset>(schema, rsm, "");
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
        }
        t_tablet_schema.__set_storage_type(TStorageType::COLUMN);
        t_tablet_schema.__set_columns(cols);
        TabletMetaSharedPtr tablet_meta(
                new TabletMeta(1, 1, 1, 1, 1, 1, t_tablet_schema, 1, col_ordinal_to_unique_id,
                               UniqueId(1, 2), TTabletType::TABLET_TYPE_DISK,
                               TCompressionType::LZ4F, 0, enable_unique_key_merge_on_write));

        TabletSharedPtr tablet(new Tablet(*engine_ref, tablet_meta, nullptr));
        static_cast<void>(tablet->init());
        return tablet;
    }

    void check_rowid_conversion(KeysType keys_type, bool enable_unique_key_merge_on_write,
                                uint32_t num_input_rowset, uint32_t num_segments,
                                uint32_t rows_per_segment, const SegmentsOverlapPB& overlap,
                                bool has_delete_handler, bool is_vertical_merger) {
        // generate input data
        std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
        generate_input_data(num_input_rowset, num_segments, rows_per_segment, overlap, input_data);

        TabletSchemaSPtr tablet_schema = create_schema(keys_type);
        // create input rowset
        std::vector<RowsetSharedPtr> input_rowsets;
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
        if (has_delete_handler) {
            // delete data with key < 1000
            std::vector<TCondition> conditions;
            TCondition condition;
            condition.column_name = tablet_schema->column(0).name();
            condition.condition_op = "<";
            condition.condition_values.clear();
            condition.condition_values.push_back("1000");
            conditions.push_back(condition);

            DeletePredicatePB del_pred;
            Status st =
                    DeleteHandler::generate_delete_predicate(*tablet_schema, conditions, &del_pred);
            ASSERT_TRUE(st.ok()) << st;
            input_rowsets.push_back(
                    create_delete_predicate(tablet_schema, del_pred, num_input_rowset));
        }

        // create output rowset writer
        auto writer_context = create_rowset_writer_context(
                tablet_schema, NONOVERLAPPING, 3456, {0, input_rowsets.back()->end_version()});

        auto res = RowsetFactory::create_rowset_writer(*engine_ref, writer_context,
                                                       is_vertical_merger);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto output_rs_writer = std::move(res).value();

        // merge input rowset
        TabletSharedPtr tablet = create_tablet(*tablet_schema, enable_unique_key_merge_on_write);

        // create input rowset reader
        std::vector<RowsetReaderSharedPtr> input_rs_readers;
        for (auto& rowset : input_rowsets) {
            RowsetReaderSharedPtr rs_reader;
            EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
            input_rs_readers.push_back(std::move(rs_reader));
        }

        Merger::Statistics stats;
        RowIdConversion rowid_conversion;
        stats.rowid_conversion = &rowid_conversion;
        Status s;
        if (is_vertical_merger) {
            s = Merger::vertical_merge_rowsets(
                    tablet, ReaderType::READER_BASE_COMPACTION, *tablet_schema, input_rs_readers,
                    output_rs_writer.get(), 10000000, num_segments, &stats);
        } else {
            s = Merger::vmerge_rowsets(tablet, ReaderType::READER_BASE_COMPACTION, *tablet_schema,
                                       input_rs_readers, output_rs_writer.get(), &stats);
        }
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
        create_and_init_rowset_reader(out_rowset.get(), reader_context, &output_rs_reader);

        // read output rowset data
        std::vector<std::tuple<int64_t, int64_t>> output_data;
        do {
            Block output_block = tablet_schema->create_block();
            s = output_rs_reader->next_batch(&output_block);
            auto columns = output_block.get_columns_with_type_and_name();
            EXPECT_EQ(columns.size(), 2);
            for (auto i = 0; i < output_block.rows(); i++) {
                output_data.emplace_back(columns[0].column->get_int(i),
                                         columns[1].column->get_int(i));
            }
        } while (s.ok());
        EXPECT_TRUE(s.is<END_OF_FILE>()) << s;
        EXPECT_EQ(out_rowset->rowset_meta()->num_rows(), output_data.size());
        auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(out_rowset);
        std::vector<uint32_t> segment_num_rows;
        OlapReaderStatistics statistics;
        EXPECT_TRUE(beta_rowset->get_segment_num_rows(&segment_num_rows, false, &statistics).ok());
        if (has_delete_handler) {
            // All keys less than 1000 are deleted by delete handler
            for (auto& item : output_data) {
                ASSERT_GE(std::get<0>(item), 1000);
            }
        }

        // check rowid conversion
        uint64_t count = 0;
        for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
            for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
                for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                    RowLocation src(input_rowsets[rs_id]->rowset_id(), s_id, row_id);
                    RowLocation dst;
                    int res = rowid_conversion.get(src, &dst);
                    if (res < 0) {
                        continue;
                    }
                    size_t rowid_in_output_data = dst.row_id;
                    EXPECT_GT(segment_num_rows[dst.segment_id], dst.row_id);
                    for (auto n = 1; n <= dst.segment_id; n++) {
                        rowid_in_output_data += segment_num_rows[n - 1];
                    }
                    EXPECT_EQ(std::get<0>(output_data[rowid_in_output_data]),
                              std::get<0>(input_data[rs_id][s_id][row_id]));
                    EXPECT_EQ(std::get<1>(output_data[rowid_in_output_data]),
                              std::get<1>(input_data[rs_id][s_id][row_id]));
                    count++;
                }
            }
        }
        EXPECT_EQ(count, output_data.size());
    }
    // if overlap == NONOVERLAPPING, all rowsets are non overlapping;
    // if overlap == OVERLAPPING, all rowsets are overlapping;
    // if overlap == OVERLAP_UNKNOWN, the first rowset is non overlapping, the
    // others are overlaping.
    void generate_input_data(
            uint32_t num_input_rowset, uint32_t num_segments, uint32_t rows_per_segment,
            const SegmentsOverlapPB& overlap,
            std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>>& input_data) {
        EXPECT_GE(rows_per_segment, 10);
        EXPECT_GE(num_segments * rows_per_segment, 500);
        bool is_overlap = false;
        for (auto i = 0; i < num_input_rowset; i++) {
            if (overlap == OVERLAPPING) {
                is_overlap = true;
            } else if (overlap == NONOVERLAPPING) {
                is_overlap = false;
            } else {
                if (i == 0) {
                    is_overlap = false;
                } else {
                    is_overlap = true;
                }
            }
            std::vector<std::vector<std::tuple<int64_t, int64_t>>> rowset_data;
            for (auto j = 0; j < num_segments; j++) {
                std::vector<std::tuple<int64_t, int64_t>> segment_data;
                for (auto n = 0; n < rows_per_segment; n++) {
                    int64_t c1 = j * rows_per_segment + n;
                    // There are 500 rows of data overlap between rowsets
                    if (i > 0) {
                        if (is_overlap) {
                            // There are 500 rows of data overlap between rowsets
                            c1 -= 500;
                        } else {
                            ++c1;
                        }
                    }
                    if (is_overlap && j > 0) {
                        // There are 10 rows of data overlap between segments
                        c1 += j * rows_per_segment - 10;
                    }
                    int64_t c2 = c1 + 1;
                    segment_data.emplace_back(c1, c2);
                }
                rowset_data.emplace_back(segment_data);
            }
            input_data.emplace_back(rowset_data);
        }
    }

private:
    const std::string kTestDir = "/ut_dir/rowid_conversion_test";
    std::string absolute_dir;
};

TEST_F(TestRowIdConversion, Basic) {
    // rowset_id, segment_id, row_id
    int input_data[11][3] = {{0, 0, 0}, {0, 0, 1}, {0, 0, 2}, {0, 0, 3}, {0, 1, 0}, {0, 1, 1},
                             {0, 1, 2}, {1, 0, 0}, {1, 0, 1}, {1, 0, 2}, {1, 0, 3}};

    RowsetId src_rowset;
    RowsetId dst_rowset;
    dst_rowset.init(3);

    std::vector<RowLocation> rss_row_ids;
    for (auto i = 0; i < 11; i++) {
        src_rowset.init(input_data[i][0]);
        RowLocation rss_row_id(src_rowset, input_data[i][1], input_data[i][2]);
        rss_row_ids.push_back(rss_row_id);
    }
    RowIdConversion rowid_conversion;
    src_rowset.init(0);
    std::vector<uint32_t> rs0_segment_num_rows = {4, 3};
    auto st = rowid_conversion.init_segment_map(src_rowset, rs0_segment_num_rows);
    EXPECT_EQ(st.ok(), true);
    src_rowset.init(1);
    std::vector<uint32_t> rs1_segment_num_rows = {4};
    st = rowid_conversion.init_segment_map(src_rowset, rs1_segment_num_rows);
    EXPECT_EQ(st.ok(), true);
    rowid_conversion.set_dst_rowset_id(dst_rowset);

    std::vector<uint32_t> dst_segment_num_rows = {4, 3, 4};
    rowid_conversion.add(rss_row_ids, dst_segment_num_rows);

    int res = 0;
    src_rowset.init(0);
    RowLocation src0(src_rowset, 0, 0);
    RowLocation dst0;
    res = rowid_conversion.get(src0, &dst0);

    EXPECT_EQ(dst0.rowset_id, dst_rowset);
    EXPECT_EQ(dst0.segment_id, 0);
    EXPECT_EQ(dst0.row_id, 0);
    EXPECT_EQ(res, 0);

    src_rowset.init(0);
    RowLocation src1(src_rowset, 1, 2);
    RowLocation dst1;
    res = rowid_conversion.get(src1, &dst1);

    EXPECT_EQ(dst1.rowset_id, dst_rowset);
    EXPECT_EQ(dst1.segment_id, 1);
    EXPECT_EQ(dst1.row_id, 2);
    EXPECT_EQ(res, 0);

    src_rowset.init(1);
    RowLocation src2(src_rowset, 0, 3);
    RowLocation dst2;
    res = rowid_conversion.get(src2, &dst2);

    EXPECT_EQ(dst2.rowset_id, dst_rowset);
    EXPECT_EQ(dst2.segment_id, 2);
    EXPECT_EQ(dst2.row_id, 3);
    EXPECT_EQ(res, 0);

    src_rowset.init(1);
    RowLocation src3(src_rowset, 0, 4);
    RowLocation dst3;
    res = rowid_conversion.get(src3, &dst3);
    EXPECT_EQ(res, -1);

    src_rowset.init(100);
    RowLocation src4(src_rowset, 5, 4);
    RowLocation dst4;
    res = rowid_conversion.get(src4, &dst4);
    EXPECT_EQ(res, -1);
}

TEST_F(TestRowIdConversion, SingleRowsetGroupedCompactionRowIdConversionIsComplete) {
    constexpr int64_t num_segments = 5;
    constexpr int64_t rows_per_segment = 1500;
    constexpr int64_t segment_group_size = 2;
    constexpr int32_t schema_version = 1234;
    constexpr int64_t newest_write_timestamp = 123456789;
    constexpr int64_t compaction_level = 2;
    const bool old_enable_compaction_task_tracker = config::enable_compaction_task_tracker;
    Defer restore_config {
            [&] { config::enable_compaction_task_tracker = old_enable_compaction_task_tracker; }};
    config::enable_compaction_task_tracker = true;

    std::vector<std::vector<std::tuple<int64_t, int64_t>>> input_data;
    for (int64_t segment_id = 0; segment_id < num_segments; ++segment_id) {
        std::vector<std::tuple<int64_t, int64_t>> segment_data;
        for (int64_t row_id = 0; row_id < rows_per_segment; ++row_id) {
            int64_t key = segment_id * rows_per_segment + row_id;
            segment_data.emplace_back(key, key + 1);
        }
        input_data.push_back(std::move(segment_data));
    }

    CloudStorageEngine cloud_engine(EngineOptions {});
    for (bool is_vertical : {false, true}) {
        SCOPED_TRACE(is_vertical ? "vertical merge" : "horizontal merge");

        TabletSchemaSPtr tablet_schema = create_schema(UNIQUE_KEYS, true);
        tablet_schema->set_schema_version(schema_version);
        tablet_schema->set_db_id(1000);
        RowsetSharedPtr input_rowset = create_rowset(tablet_schema, OVERLAPPING, input_data, 2);
        ASSERT_TRUE(input_rowset != nullptr);

        TabletSharedPtr local_tablet = create_tablet(*tablet_schema, true);
        auto writer_context = create_rowset_writer_context(
                tablet_schema, NONOVERLAPPING, rows_per_segment, input_rowset->version());
        writer_context.db_id = tablet_schema->db_id();
        writer_context.table_id = local_tablet->table_id();
        writer_context.tablet_id = local_tablet->tablet_id();
        writer_context.index_id = local_tablet->index_id();
        writer_context.partition_id = local_tablet->partition_id();
        writer_context.tablet_schema_hash = local_tablet->schema_hash();
        writer_context.tablet_uid = local_tablet->tablet_uid();
        writer_context.newest_write_timestamp = newest_write_timestamp;
        writer_context.compaction_level = compaction_level;
        writer_context.enable_unique_key_merge_on_write = true;
        auto writer_result =
                RowsetFactory::create_rowset_writer(*engine_ref, writer_context, is_vertical);
        ASSERT_TRUE(writer_result.has_value()) << writer_result.error();

        auto cloud_tablet = std::make_shared<CloudTablet>(
                cloud_engine, std::make_shared<TabletMeta>(*local_tablet->tablet_meta()));
        CloudCumulativeCompaction compaction(cloud_engine, cloud_tablet);
        compaction._input_rowsets = {input_rowset};
        compaction._cur_tablet_schema = tablet_schema;
        compaction._output_rs_writer = std::move(writer_result).value();
        compaction._is_vertical = is_vertical;
        compaction._input_row_num = input_rowset->num_rows();
        compaction._input_rowsets_data_size = input_rowset->data_disk_size();
        compaction._stats.rowid_conversion = compaction._rowid_conversion.get();

        auto* compaction_task_tracker = CompactionTaskTracker::instance();
        CompactionTaskInfo task_info;
        task_info.compaction_id = compaction.compaction_id();
        compaction_task_tracker->register_task(std::move(task_info));
        Defer remove_tracker_task {
                [compaction_task_tracker, compaction_id = compaction.compaction_id()] {
                    compaction_task_tracker->remove_task(compaction_id);
                }};

        Compaction::MergeInputRowsetsResult merge_result;
        merge_result.is_segment_grouped = true;
        merge_result.segment_group_size = segment_group_size;
        ASSERT_TRUE(compaction.do_merge_input_rowsets({}, &merge_result).ok());
        const int64_t segment_group_count =
                (num_segments + segment_group_size - 1) / segment_group_size;
        EXPECT_EQ(merge_result.output_segment_group_count, segment_group_count);
        if (is_vertical) {
            constexpr int32_t default_num_columns_per_group = 5;
            const int32_t num_columns_per_group =
                    config::vertical_compaction_num_columns_per_group !=
                                    default_num_columns_per_group
                            ? config::vertical_compaction_num_columns_per_group
                            : cloud_tablet->tablet_meta()
                                      ->vertical_compaction_num_columns_per_group();
            std::vector<std::vector<uint32_t>> column_groups;
            std::vector<uint32_t> key_group_cluster_key_idxes;
            Merger::vertical_split_columns(*tablet_schema, &column_groups,
                                           &key_group_cluster_key_idxes, num_columns_per_group);

            const auto tracked_tasks = compaction_task_tracker->get_all_tasks();
            const auto task_it = std::find_if(
                    tracked_tasks.begin(), tracked_tasks.end(), [&](const auto& tracked_task) {
                        return tracked_task.compaction_id == compaction.compaction_id();
                    });
            ASSERT_NE(task_it, tracked_tasks.end());
            const int64_t expected_total_groups =
                    static_cast<int64_t>(column_groups.size()) * segment_group_count;
            EXPECT_EQ(task_it->vertical_total_groups, expected_total_groups);
            EXPECT_EQ(task_it->vertical_completed_groups, expected_total_groups);
        }

        RowsetSharedPtr output_rowset;
        ASSERT_EQ(Status::OK(), compaction._output_rs_writer->build(output_rowset));
        ASSERT_TRUE(output_rowset != nullptr);
        compaction._output_rowset = output_rowset;
        compaction.update_output_rowset_after_build(merge_result);
        EXPECT_EQ(compaction._stats.output_rows, input_rowset->num_rows());
        if (is_vertical) {
            EXPECT_GT(output_rowset->num_segments(), merge_result.output_segment_group_count);
        }
        EXPECT_EQ(output_rowset->rowset_meta()->get_num_segment_rows().size(),
                  output_rowset->num_segments());

        RowsetReaderContext reader_context;
        reader_context.tablet_schema = tablet_schema;
        reader_context.need_ordered_result = false;
        std::vector<uint32_t> return_columns = {0, 1};
        reader_context.return_columns = &return_columns;
        RowsetReaderSharedPtr output_reader;
        create_and_init_rowset_reader(output_rowset.get(), reader_context, &output_reader);

        std::vector<std::tuple<int64_t, int64_t>> output_data;
        Status read_status;
        do {
            Block output_block = tablet_schema->create_block(return_columns);
            read_status = output_reader->next_batch(&output_block);
            const auto& columns = output_block.get_columns_with_type_and_name();
            ASSERT_EQ(columns.size(), return_columns.size());
            for (size_t row_id = 0; row_id < output_block.rows(); ++row_id) {
                output_data.emplace_back(columns[0].column->get_int(row_id),
                                         columns[1].column->get_int(row_id));
            }
        } while (read_status.ok());
        ASSERT_TRUE(read_status.is<END_OF_FILE>()) << read_status;
        ASSERT_EQ(output_data.size(), input_rowset->num_rows());

        auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(output_rowset);
        ASSERT_TRUE(beta_rowset != nullptr);
        const auto& rowset_meta = output_rowset->rowset_meta();
        const auto rowset_meta_pb = rowset_meta->get_rowset_pb();
        const auto& segment_num_rows_from_meta = rowset_meta->get_num_segment_rows();

        EXPECT_EQ(rowset_meta_pb.rowset_id_v2(), writer_context.rowset_id.to_string());
        EXPECT_EQ(rowset_meta->rowset_id().to_string(), writer_context.rowset_id.to_string());

        EXPECT_EQ(rowset_meta->rowset_type(), BETA_ROWSET);
        EXPECT_EQ(rowset_meta->rowset_state(), VISIBLE);
        EXPECT_TRUE(rowset_meta->has_version());
        EXPECT_EQ(rowset_meta->version(), input_rowset->version());
        EXPECT_FALSE(rowset_meta->empty());
        EXPECT_EQ(rowset_meta->num_rows(), input_rowset->num_rows());
        EXPECT_EQ(rowset_meta->segments_overlap(), OVERLAPPING);
        EXPECT_TRUE(rowset_meta->is_segments_overlapping());

        ASSERT_TRUE(rowset_meta->tablet_schema() != nullptr);
        EXPECT_EQ(*rowset_meta->tablet_schema(), *tablet_schema);
        EXPECT_TRUE(rowset_meta_pb.has_tablet_schema());
        TabletSchemaPB expected_tablet_schema_pb;
        tablet_schema->to_schema_pb(&expected_tablet_schema_pb);
        EXPECT_EQ(rowset_meta_pb.tablet_schema().SerializeAsString(),
                  expected_tablet_schema_pb.SerializeAsString());
        EXPECT_EQ(rowset_meta_pb.schema_version(), schema_version);
        EXPECT_TRUE(rowset_meta_pb.has_has_variant_type_in_schema());
        EXPECT_FALSE(rowset_meta_pb.has_variant_type_in_schema());

        std::vector<segment_v2::SegmentSharedPtr> output_segments;
        ASSERT_TRUE(beta_rowset->load_segments(&output_segments).ok());
        ASSERT_EQ(rowset_meta->num_segments(), output_segments.size());
        ASSERT_EQ(segment_num_rows_from_meta.size(), output_segments.size());

        const auto& segment_key_bounds_from_meta = rowset_meta->get_segments_key_bounds();
        EXPECT_FALSE(rowset_meta->is_segments_key_bounds_aggregated());
        EXPECT_FALSE(rowset_meta->is_segments_key_bounds_truncated());
        ASSERT_EQ(segment_key_bounds_from_meta.size(), output_segments.size());

        const auto& inverted_index_file_info_from_meta = rowset_meta->inverted_index_file_info();
        EXPECT_TRUE(rowset_meta_pb.enable_inverted_index_file_info());
        ASSERT_EQ(inverted_index_file_info_from_meta.size(), output_segments.size());

        EXPECT_FALSE(rowset_meta_pb.enable_segments_file_size());
        EXPECT_TRUE(rowset_meta_pb.segments_file_size().empty());

        int64_t actual_data_disk_size = 0;
        int64_t actual_index_disk_size = 0;
        int64_t actual_num_rows = 0;
        for (size_t segment_id = 0; segment_id < output_segments.size(); ++segment_id) {
            EXPECT_EQ(output_segments[segment_id]->id(), segment_id);
            EXPECT_EQ(segment_num_rows_from_meta[segment_id],
                      output_segments[segment_id]->num_rows())
                    << "segment_id=" << segment_id;
            EXPECT_EQ(segment_key_bounds_from_meta[segment_id].min_key(),
                      output_segments[segment_id]->min_key())
                    << "segment_id=" << segment_id;
            EXPECT_EQ(segment_key_bounds_from_meta[segment_id].max_key(),
                      output_segments[segment_id]->max_key())
                    << "segment_id=" << segment_id;

            const auto& index_file_info = inverted_index_file_info_from_meta[segment_id];
            ASSERT_TRUE(index_file_info.has_index_size()) << "segment_id=" << segment_id;
            const auto segment_path = output_rowset->segment_path(segment_id);
            ASSERT_TRUE(segment_path.has_value()) << segment_path.error();
            int64_t segment_file_size = 0;
            const auto segment_file_size_status =
                    rowset_meta->fs()->file_size(segment_path.value(), &segment_file_size);
            ASSERT_TRUE(segment_file_size_status.ok()) << segment_file_size_status;
            actual_data_disk_size += segment_file_size;
            actual_num_rows += output_segments[segment_id]->num_rows();

            const auto index_file_path =
                    segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(
                            segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                                    segment_path.value()));
            int64_t index_file_size = 0;
            const auto index_file_size_status =
                    rowset_meta->fs()->file_size(index_file_path, &index_file_size);
            ASSERT_TRUE(index_file_size_status.ok()) << index_file_size_status;
            EXPECT_EQ(index_file_info.index_size(), index_file_size) << "segment_id=" << segment_id;
            actual_index_disk_size += index_file_size;
        }
        EXPECT_EQ(rowset_meta->num_rows(), actual_num_rows);
        EXPECT_EQ(rowset_meta->data_disk_size(), actual_data_disk_size);
        EXPECT_EQ(rowset_meta->index_disk_size(), actual_index_disk_size);
        EXPECT_EQ(rowset_meta->total_disk_size(),
                  rowset_meta->data_disk_size() + rowset_meta->index_disk_size());

        std::vector<uint32_t> output_segment_num_rows;
        OlapReaderStatistics reader_stats;
        ASSERT_TRUE(
                beta_rowset->get_segment_num_rows(&output_segment_num_rows, false, &reader_stats)
                        .ok());

        RowIdConversion& rowid_conversion = *compaction._stats.rowid_conversion;
        EXPECT_EQ(rowid_conversion.get_src_segment_to_id_map().size(), num_segments);
        EXPECT_EQ(rowid_conversion.get_rowid_conversion_map().size(), num_segments);
        EXPECT_EQ(rowid_conversion.get_rowid_conversion_map().size(),
                  rowid_conversion.get_src_segment_to_id_map().size());
        for (int64_t segment_id = 0; segment_id < num_segments; ++segment_id) {
            for (int64_t row_id = 0; row_id < rows_per_segment; ++row_id) {
                RowLocation src(input_rowset->rowset_id(), segment_id, row_id);
                RowLocation dst;
                ASSERT_EQ(rowid_conversion.get(src, &dst), 0)
                        << "segment_id=" << segment_id << ", row_id=" << row_id;
                ASSERT_LT(dst.segment_id, output_segment_num_rows.size());
                ASSERT_LT(dst.row_id, output_segment_num_rows[dst.segment_id]);

                size_t output_row_id = dst.row_id;
                for (uint32_t output_segment_id = 0; output_segment_id < dst.segment_id;
                     ++output_segment_id) {
                    output_row_id += output_segment_num_rows[output_segment_id];
                }
                ASSERT_LT(output_row_id, output_data.size());
                EXPECT_EQ(output_data[output_row_id], input_data[segment_id][row_id]);
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
        Parameters, TestRowIdConversion,
        ::testing::ValuesIn(std::vector<std::tuple<KeysType, bool, bool, bool>> {
                // Parameters: data_type, enable_unique_key_merge_on_write, has_delete_handler, is_vertical_merger
                {DUP_KEYS, false, false, false},
                {UNIQUE_KEYS, false, false, false},
                {UNIQUE_KEYS, true, false, false},
                {DUP_KEYS, false, true, false},
                {UNIQUE_KEYS, false, true, false},
                {UNIQUE_KEYS, true, true, false},
                {UNIQUE_KEYS, false, false, true},
                {UNIQUE_KEYS, true, false, true},
                {DUP_KEYS, false, true, true},
                {UNIQUE_KEYS, false, true, true},
                {UNIQUE_KEYS, true, true, true}}));

TEST_P(TestRowIdConversion, Conversion) {
    KeysType keys_type = std::get<0>(GetParam());
    bool enable_unique_key_merge_on_write = std::get<1>(GetParam());
    bool has_delete_handler = std::get<2>(GetParam());
    bool is_vertical_merger = std::get<3>(GetParam());

    // if num_input_rowset = 2, VCollectIterator::Level1Iterator::_merge = flase
    // if num_input_rowset = 3, VCollectIterator::Level1Iterator::_merge = true
    for (auto num_input_rowset = 2; num_input_rowset <= 3; num_input_rowset++) {
        uint32_t rows_per_segment = 4567;
        // RowsetReader: SegmentIterator
        {
            uint32_t num_segments = 1;
            SegmentsOverlapPB overlap = NONOVERLAPPING;
            check_rowid_conversion(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                                   num_segments, rows_per_segment, overlap, has_delete_handler,
                                   is_vertical_merger);
        }
        // RowsetReader: VMergeIterator
        {
            uint32_t num_segments = 2;
            SegmentsOverlapPB overlap = OVERLAPPING;
            check_rowid_conversion(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                                   num_segments, rows_per_segment, overlap, has_delete_handler,
                                   is_vertical_merger);
        }
        // RowsetReader: VUnionIterator
        {
            uint32_t num_segments = 2;
            SegmentsOverlapPB overlap = NONOVERLAPPING;
            check_rowid_conversion(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                                   num_segments, rows_per_segment, overlap, has_delete_handler,
                                   is_vertical_merger);
        }
        // RowsetReader: VUnionIterator + VMergeIterator
        {
            uint32_t num_segments = 2;
            SegmentsOverlapPB overlap = OVERLAP_UNKNOWN;
            check_rowid_conversion(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                                   num_segments, rows_per_segment, overlap, has_delete_handler,
                                   is_vertical_merger);
        }
    }
}

} // namespace doris

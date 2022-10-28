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

#include "olap/rowid_conversion.h"

#include <gtest/gtest.h>

#include "common/logging.h"
#include "olap/data_dir.h"
#include "olap/delete_handler.h"
#include "olap/merger.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "util/file_utils.h"

namespace doris {

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* k_engine = nullptr;

class TestRowIdConversion : public testing::TestWithParam<std::tuple<KeysType, bool, bool>> {
protected:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + kTestDir;

        if (FileUtils::check_exist(absolute_dir)) {
            EXPECT_TRUE(FileUtils::remove_all(absolute_dir).ok());
        }
        EXPECT_TRUE(FileUtils::create_dir(absolute_dir).ok());
        EXPECT_TRUE(FileUtils::create_dir(absolute_dir + "/tablet_path").ok());
        _data_dir = std::make_unique<DataDir>(absolute_dir);
        _data_dir->update_capacity();
        doris::EngineOptions options;
        k_engine = new StorageEngine(options);
        StorageEngine::_s_instance = k_engine;
    }

    void TearDown() override {
        if (FileUtils::check_exist(absolute_dir)) {
            EXPECT_TRUE(FileUtils::remove_all(absolute_dir).ok());
        }
        if (k_engine != nullptr) {
            k_engine->stop();
            delete k_engine;
            k_engine = nullptr;
        }
    }

    TabletSchemaSPtr create_schema(KeysType keys_type = DUP_KEYS) {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(keys_type);
        tablet_schema_pb.set_num_short_key_columns(2);
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

        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    void create_rowset_writer_context(TabletSchemaSPtr tablet_schema,
                                      const SegmentsOverlapPB& overlap,
                                      uint32_t max_rows_per_segment,
                                      RowsetWriterContext* rowset_writer_context) {
        static int64_t inc_id = 0;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->rowset_type = BETA_ROWSET;
        rowset_writer_context->data_dir = _data_dir.get();
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->rowset_dir = "tablet_path";
        rowset_writer_context->version = Version(inc_id, inc_id);
        rowset_writer_context->segments_overlap = overlap;
        rowset_writer_context->max_rows_per_segment = max_rows_per_segment;
        inc_id++;
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
            std::vector<std::vector<std::tuple<int64_t, int64_t>>> rowset_data) {
        RowsetWriterContext writer_context;
        if (overlap == NONOVERLAPPING) {
            for (auto i = 1; i < rowset_data.size(); i++) {
                auto& last_seg_data = rowset_data[i - 1];
                auto& cur_seg_data = rowset_data[i];
                int64_t last_seg_max = std::get<0>(last_seg_data[last_seg_data.size() - 1]);
                int64_t cur_seg_min = std::get<0>(cur_seg_data[0]);
                EXPECT_LT(last_seg_max, cur_seg_min);
            }
        }
        create_rowset_writer_context(tablet_schema, overlap, UINT32_MAX, &writer_context);

        std::unique_ptr<RowsetWriter> rowset_writer;
        Status s = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        EXPECT_TRUE(s.ok());

        RowCursor input_row;
        input_row.init(tablet_schema);

        uint32_t num_rows = 0;
        for (int i = 0; i < rowset_data.size(); ++i) {
            MemPool mem_pool;
            for (int rid = 0; rid < rowset_data[i].size(); ++rid) {
                uint32_t c1 = std::get<0>(rowset_data[i][rid]);
                uint32_t c2 = std::get<1>(rowset_data[i][rid]);
                input_row.set_field_content(0, reinterpret_cast<char*>(&c1), &mem_pool);
                input_row.set_field_content(1, reinterpret_cast<char*>(&c2), &mem_pool);
                if (tablet_schema->keys_type() == UNIQUE_KEYS) {
                    uint8_t num = 0;
                    input_row.set_field_content(2, reinterpret_cast<char*>(&num), &mem_pool);
                }
                s = rowset_writer->add_row(input_row);
                EXPECT_TRUE(s.ok());
                num_rows++;
            }
            s = rowset_writer->flush();
            EXPECT_TRUE(s.ok());
        }

        RowsetSharedPtr rowset;
        rowset = rowset_writer->build();
        EXPECT_TRUE(rowset != nullptr);
        EXPECT_EQ(rowset_data.size(), rowset->rowset_meta()->num_segments());
        EXPECT_EQ(num_rows, rowset->rowset_meta()->num_rows());
        return rowset;
    }

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        std::string json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "txn_id": 4042,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "num_rows": 3929,
            "total_disk_size": 84699,
            "data_disk_size": 84464,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670,
            "alpha_rowset_extra_meta_pb": {
                "segment_groups": [
                {
                    "segment_group_id": 0,
                    "num_segments": 2,
                    "index_size": 132,
                    "data_size": 576,
                    "num_rows": 5,
                    "zone_maps": [
                    {
                        "min": "MQ==",
                        "max": "NQ==",
                        "null_flag": false
                    },
                    {
                        "min": "MQ==",
                        "max": "Mw==",
                        "null_flag": false
                    },
                    {
                        "min": "J2J1c2gn",
                        "max": "J3RvbSc=",
                        "null_flag": false
                    }
                    ],
                    "empty": false
                }]
            }
        })";
        pb1->init_from_json(json_rowset_meta);
        pb1->set_start_version(start);
        pb1->set_end_version(end);
        pb1->set_creation_time(10000);
    }

    void add_delete_predicate(TabletSharedPtr tablet, DeletePredicatePB& del_pred,
                              int64_t version) {
        RowsetMetaSharedPtr rsm(new RowsetMeta());
        init_rs_meta(rsm, version, version);
        RowsetId id;
        id.init(version * 1000);
        rsm->set_rowset_id(id);
        rsm->set_delete_predicate(del_pred);
        rsm->set_tablet_schema(tablet->tablet_schema());
        RowsetSharedPtr rowset = std::make_shared<BetaRowset>(tablet->tablet_schema(), "", rsm);
        tablet->add_rowset(rowset);
    }

    TabletSharedPtr create_tablet(const TabletSchema& tablet_schema,
                                  bool enable_unique_key_merge_on_write, int64_t version,
                                  bool has_delete_handler) {
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
                               TCompressionType::LZ4F, "", enable_unique_key_merge_on_write));

        TabletSharedPtr tablet(new Tablet(tablet_meta, nullptr));
        tablet->init();
        if (has_delete_handler) {
            // delete data with key < 1000
            std::vector<TCondition> conditions;
            TCondition condition;
            condition.column_name = tablet_schema.column(0).name();
            condition.condition_op = "<";
            condition.condition_values.clear();
            condition.condition_values.push_back("1000");
            conditions.push_back(condition);

            DeletePredicatePB del_pred;
            Status st =
                    DeleteHandler::generate_delete_predicate(tablet_schema, conditions, &del_pred);
            EXPECT_EQ(Status::OK(), st);
            add_delete_predicate(tablet, del_pred, version);
        }
        return tablet;
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

    void check_rowid_conversion(KeysType keys_type, bool enable_unique_key_merge_on_write,
                                uint32_t num_input_rowset, uint32_t num_segments,
                                uint32_t rows_per_segment, const SegmentsOverlapPB& overlap,
                                bool has_delete_handler) {
        // generate input data
        std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
        generate_input_data(num_input_rowset, num_segments, rows_per_segment, overlap, input_data);

        TabletSchemaSPtr tablet_schema = create_schema(keys_type);
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
            RowsetSharedPtr rowset = create_rowset(tablet_schema, new_overlap, input_data[i]);
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
        RowsetWriterContext writer_context;
        create_rowset_writer_context(tablet_schema, NONOVERLAPPING, 3456, &writer_context);
        std::unique_ptr<RowsetWriter> output_rs_writer;
        Status s = RowsetFactory::create_rowset_writer(writer_context, &output_rs_writer);
        EXPECT_TRUE(s.ok());

        // merge input rowset
        TabletSharedPtr tablet =
                create_tablet(*tablet_schema, enable_unique_key_merge_on_write,
                              output_rs_writer->version().first - 1, has_delete_handler);
        Merger::Statistics stats;
        RowIdConversion rowid_conversion;
        stats.rowid_conversion = &rowid_conversion;
        s = Merger::vmerge_rowsets(tablet, READER_BASE_COMPACTION, tablet_schema, input_rs_readers,
                                   output_rs_writer.get(), &stats);
        EXPECT_TRUE(s.ok());
        RowsetSharedPtr out_rowset = output_rs_writer->build();

        // create output rowset reader
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = tablet_schema;
        reader_context.need_ordered_result = false;
        std::vector<uint32_t> return_columns = {0, 1};
        reader_context.return_columns = &return_columns;
        reader_context.is_vec = true;
        RowsetReaderSharedPtr output_rs_reader;
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
                output_data.emplace_back(columns[0].column->get_int(i),
                                         columns[1].column->get_int(i));
            }
        } while (s == Status::OK());
        EXPECT_EQ(Status::OLAPInternalError(OLAP_ERR_DATA_EOF), s);
        EXPECT_EQ(out_rowset->rowset_meta()->num_rows(), output_data.size());
        std::vector<uint32_t> segment_num_rows;
        EXPECT_TRUE(output_rs_reader->get_segment_num_rows(&segment_num_rows).ok());
        if (has_delete_handler) {
            // All keys less than 1000 are deleted by delete handler
            for (auto& item : output_data) {
                EXPECT_GE(std::get<0>(item), 1000);
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
                        c1 += i * num_segments * rows_per_segment - 500;
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
    string absolute_dir;
    std::unique_ptr<DataDir> _data_dir;
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
    rowid_conversion.init_segment_map(src_rowset, rs0_segment_num_rows);
    src_rowset.init(1);
    std::vector<uint32_t> rs1_segment_num_rows = {4};
    rowid_conversion.init_segment_map(src_rowset, rs1_segment_num_rows);
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

INSTANTIATE_TEST_SUITE_P(
        Parameters, TestRowIdConversion,
        ::testing::ValuesIn(std::vector<std::tuple<KeysType, bool, bool>> {
                // Parameters: data_type, enable_unique_key_merge_on_write, has_delete_handler
                {DUP_KEYS, false, false},
                {UNIQUE_KEYS, false, false},
                {UNIQUE_KEYS, true, false},
                {DUP_KEYS, false, true},
                {UNIQUE_KEYS, false, true},
                {UNIQUE_KEYS, true, true}}));

TEST_P(TestRowIdConversion, Conversion) {
    KeysType keys_type = std::get<0>(GetParam());
    bool enable_unique_key_merge_on_write = std::get<1>(GetParam());
    bool has_delete_handler = std::get<2>(GetParam());

    // if num_input_rowset = 2, VCollectIterator::Level1Iterator::_merge = flase
    // if num_input_rowset = 3, VCollectIterator::Level1Iterator::_merge = true
    for (auto num_input_rowset = 2; num_input_rowset <= 3; num_input_rowset++) {
        uint32_t rows_per_segment = 4567;
        // RowsetReader: SegmentIterator
        {
            uint32_t num_segments = 1;
            SegmentsOverlapPB overlap = NONOVERLAPPING;
            std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
            check_rowid_conversion(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                                   num_segments, rows_per_segment, overlap, has_delete_handler);
        }
        // RowsetReader: VMergeIterator
        {
            uint32_t num_segments = 2;
            SegmentsOverlapPB overlap = OVERLAPPING;
            std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
            check_rowid_conversion(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                                   num_segments, rows_per_segment, overlap, has_delete_handler);
        }
        // RowsetReader: VUnionIterator
        {
            uint32_t num_segments = 2;
            SegmentsOverlapPB overlap = NONOVERLAPPING;
            std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
            check_rowid_conversion(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                                   num_segments, rows_per_segment, overlap, has_delete_handler);
        }
        // RowsetReader: VUnionIterator + VMergeIterator
        {
            uint32_t num_segments = 2;
            SegmentsOverlapPB overlap = OVERLAP_UNKNOWN;
            std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
            check_rowid_conversion(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                                   num_segments, rows_per_segment, overlap, has_delete_handler);
        }
    }
}

} // namespace doris

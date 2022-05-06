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

#include "olap/rowset/segment_v2/segment.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <functional>
#include <iostream>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "olap/comparison_predicate.h"
#include "olap/data_dir.h"
#include "olap/fs/block_manager.h"
#include "olap/fs/fs_util.h"
#include "olap/in_list_predicate.h"
#include "olap/olap_common.h"
#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "testutil/test_util.h"
#include "util/file_utils.h"
namespace doris {
namespace segment_v2 {

using std::string;
using std::shared_ptr;

using std::vector;

using ValueGenerator = std::function<void(size_t rid, int cid, int block_id, RowCursorCell& cell)>;

// 0,  1,  2,  3
// 10, 11, 12, 13
// 20, 21, 22, 23
static void DefaultIntGenerator(size_t rid, int cid, int block_id, RowCursorCell& cell) {
    cell.set_not_null();
    *(int*)cell.mutable_cell_ptr() = rid * 10 + cid;
}

static bool column_contains_index(ColumnMetaPB column_meta, ColumnIndexTypePB type) {
    for (int i = 0; i < column_meta.indexes_size(); ++i) {
        if (column_meta.indexes(i).type() == type) {
            return true;
        }
    }
    return false;
}

class SegmentReaderWriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (FileUtils::check_exist(kSegmentDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kSegmentDir).ok());
        }
        EXPECT_TRUE(FileUtils::create_dir(kSegmentDir).ok());
    }

    void TearDown() override {
        if (FileUtils::check_exist(kSegmentDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kSegmentDir).ok());
        }
    }

    TabletSchema create_schema(const std::vector<TabletColumn>& columns,
                               int num_short_key_columns = -1) {
        TabletSchema res;
        int num_key_columns = 0;
        for (auto& col : columns) {
            if (col.is_key()) {
                num_key_columns++;
            }
            res._cols.push_back(col);
        }
        res._num_columns = columns.size();
        res._num_key_columns = num_key_columns;
        res._num_short_key_columns =
                num_short_key_columns != -1 ? num_short_key_columns : num_key_columns;
        res.init_field_index_for_test();
        return res;
    }

    void build_segment(SegmentWriterOptions opts, const TabletSchema& build_schema,
                       const TabletSchema& query_schema, size_t nrows,
                       const ValueGenerator& generator, shared_ptr<Segment>* res) {
        static int seg_id = 0;
        // must use unique filename for each segment, otherwise page cache kicks in and produces
        // the wrong answer (it use (filename,offset) as cache key)
        std::string filename = strings::Substitute("$0/seg_$1.dat", kSegmentDir, seg_id++);
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions block_opts(filename);
        std::string storage_name;
        Status st = fs::fs_util::block_manager(storage_name)->create_block(block_opts, &wblock);
        EXPECT_TRUE(st.ok());
        DataDir data_dir(kSegmentDir);
        data_dir.init();
        SegmentWriter writer(wblock.get(), 0, &build_schema, &data_dir, INT32_MAX, opts);
        st = writer.init(10);
        EXPECT_TRUE(st.ok());

        RowCursor row;
        auto olap_st = row.init(build_schema);
        EXPECT_EQ(Status::OK(), olap_st);

        for (size_t rid = 0; rid < nrows; ++rid) {
            for (int cid = 0; cid < build_schema.num_columns(); ++cid) {
                int row_block_id = rid / opts.num_rows_per_block;
                RowCursorCell cell = row.cell(cid);
                generator(rid, cid, row_block_id, cell);
            }
            EXPECT_TRUE(writer.append_row(row).ok());
        }

        uint64_t file_size, index_size;
        st = writer.finalize(&file_size, &index_size);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(wblock->close().ok());

        FilePathDesc path_desc;
        path_desc.filepath = filename;
        st = Segment::open(path_desc, 0, &query_schema, res);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(nrows, (*res)->num_rows());
    }

private:
    const std::string kSegmentDir = "./ut_dir/segment_test";
};

TEST_F(SegmentReaderWriterTest, normal) {
    TabletSchema tablet_schema = create_schema(
            {create_int_key(1), create_int_key(2), create_int_value(3), create_int_value(4)});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    shared_ptr<Segment> segment;
    build_segment(opts, tablet_schema, tablet_schema, 4096, DefaultIntGenerator, &segment);

    // reader
    {
        Schema schema(tablet_schema);
        OlapReaderStatistics stats;
        // scan all rows
        {
            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            int left = 4096;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                EXPECT_TRUE(iter->next_batch(&block).ok());
                EXPECT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                EXPECT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        EXPECT_FALSE(column_block.is_null(i));
                        EXPECT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i));
                    }
                }
                rowid += rows_read;
            }
        }
        // test seek, key
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(tablet_schema, 2);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = 100;
            }
            {
                auto cell = lower_bound->cell(1);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = 100;
            }

            // upper bound
            std::unique_ptr<RowCursor> upper_bound(new RowCursor());
            upper_bound->init(tablet_schema, 1);
            {
                auto cell = upper_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = 200;
            }

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, upper_bound.get(), true);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            EXPECT_TRUE(iter->next_batch(&block).ok());
            EXPECT_EQ(DEL_NOT_SATISFIED, block.delete_state());
            EXPECT_EQ(11, block.num_rows());
            auto column_block = block.column_block(0);
            for (int i = 0; i < 11; ++i) {
                EXPECT_EQ(100 + i * 10, *(int*)column_block.cell_ptr(i));
            }
        }
        // test seek, key
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = 40970;
            }

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, nullptr, false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            EXPECT_TRUE(iter->next_batch(&block).is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }
        // test seek, key (-2, -1)
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = -2;
            }

            std::unique_ptr<RowCursor> upper_bound(new RowCursor());
            upper_bound->init(tablet_schema, 1);
            {
                auto cell = upper_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = -1;
            }

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, upper_bound.get(), false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            EXPECT_TRUE(iter->next_batch(&block).is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }
    }
}

TEST_F(SegmentReaderWriterTest, LazyMaterialization) {
    TabletSchema tablet_schema = create_schema({create_int_key(1), create_int_value(2)});
    ValueGenerator data_gen = [](size_t rid, int cid, int block_id, RowCursorCell& cell) {
        cell.set_not_null();
        if (cid == 0) {
            *(int*)(cell.mutable_cell_ptr()) = rid;
        } else if (cid == 1) {
            *(int*)(cell.mutable_cell_ptr()) = rid * 10;
        }
    };

    {
        shared_ptr<Segment> segment;
        build_segment(SegmentWriterOptions(), tablet_schema, tablet_schema, 100, data_gen,
                      &segment);
        {
            // lazy enabled when predicate is subset of returned columns:
            // select c1, c2 where c2 = 30;
            Schema read_schema(tablet_schema);
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(1, 30));
            const std::vector<ColumnPredicate*> predicates = {predicate.get()};

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.column_predicates = predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            EXPECT_TRUE(segment->new_iterator(read_schema, read_opts, &iter).ok());

            RowBlockV2 block(read_schema, 1024);
            EXPECT_TRUE(iter->next_batch(&block).ok());
            EXPECT_TRUE(iter->is_lazy_materialization_read());
            EXPECT_EQ(1, block.selected_size());
            EXPECT_EQ(99, stats.rows_vec_cond_filtered);
            auto row = block.row(block.selection_vector()[0]);
            EXPECT_EQ("[3,30]", row.debug_string());
        }
        {
            // lazy disabled when all return columns have predicates:
            // select c1, c2 where c1 = 10 and c2 = 100;
            Schema read_schema(tablet_schema);
            std::unique_ptr<ColumnPredicate> p0(new EqualPredicate<int32_t>(0, 10));
            std::unique_ptr<ColumnPredicate> p1(new EqualPredicate<int32_t>(1, 100));
            const std::vector<ColumnPredicate*> predicates = {p0.get(), p1.get()};

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.column_predicates = predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            EXPECT_TRUE(segment->new_iterator(read_schema, read_opts, &iter).ok());

            RowBlockV2 block(read_schema, 1024);
            EXPECT_TRUE(iter->next_batch(&block).ok());
            EXPECT_FALSE(iter->is_lazy_materialization_read());
            EXPECT_EQ(1, block.selected_size());
            EXPECT_EQ(99, stats.rows_vec_cond_filtered);
            auto row = block.row(block.selection_vector()[0]);
            EXPECT_EQ("[10,100]", row.debug_string());
        }
        {
            // lazy disabled when no predicate:
            // select c2
            std::vector<ColumnId> read_cols = {1};
            Schema read_schema(tablet_schema.columns(), read_cols);
            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            EXPECT_TRUE(segment->new_iterator(read_schema, read_opts, &iter).ok());

            RowBlockV2 block(read_schema, 1024);
            EXPECT_TRUE(iter->next_batch(&block).ok());
            EXPECT_FALSE(iter->is_lazy_materialization_read());
            EXPECT_EQ(100, block.selected_size());
            for (int i = 0; i < block.selected_size(); ++i) {
                auto row = block.row(block.selection_vector()[i]);
                EXPECT_EQ(strings::Substitute("[$0]", i * 10), row.debug_string());
            }
        }
    }

    {
        tablet_schema = create_schema({create_int_key(1, true, false, true), create_int_value(2)});
        shared_ptr<Segment> segment;
        SegmentWriterOptions write_opts;
        build_segment(write_opts, tablet_schema, tablet_schema, 100, data_gen, &segment);
        EXPECT_TRUE(column_contains_index(segment->footer().columns(0), BITMAP_INDEX));
        {
            // lazy disabled when all predicates are removed by bitmap index:
            // select c1, c2 where c2 = 30;
            Schema read_schema(tablet_schema);
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(0, 20));
            const std::vector<ColumnPredicate*> predicates = {predicate.get()};

            OlapReaderStatistics stats;
            StorageReadOptions read_opts;
            read_opts.column_predicates = predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            EXPECT_TRUE(segment->new_iterator(read_schema, read_opts, &iter).ok());

            RowBlockV2 block(read_schema, 1024);
            EXPECT_TRUE(iter->next_batch(&block).ok());
            EXPECT_FALSE(iter->is_lazy_materialization_read());
            EXPECT_EQ(1, block.selected_size());
            EXPECT_EQ(99, stats.rows_bitmap_index_filtered);
            EXPECT_EQ(0, stats.rows_vec_cond_filtered);
            auto row = block.row(block.selection_vector()[0]);
            EXPECT_EQ("[20,200]", row.debug_string());
        }
    }
}

TEST_F(SegmentReaderWriterTest, TestIndex) {
    TabletSchema tablet_schema = create_schema({create_int_key(1), create_int_key(2, true, true),
                                                create_int_key(3), create_int_value(4)});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    std::shared_ptr<Segment> segment;
    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    // ...
    // 64k int will generate 4 pages
    build_segment(
            opts, tablet_schema, tablet_schema, 64 * 1024,
            [](size_t rid, int cid, int block_id, RowCursorCell& cell) {
                cell.set_not_null();
                if (rid >= 16 * 1024 && rid < 32 * 1024) {
                    // make second page all rows equal
                    *(int*)cell.mutable_cell_ptr() = 164000 + cid;

                } else {
                    *(int*)cell.mutable_cell_ptr() = rid * 10 + cid;
                }
            },
            &segment);

    // reader with condition
    {
        Schema schema(tablet_schema);
        OlapReaderStatistics stats;
        // test empty segment iterator
        {
            // the first two page will be read by this condition
            TCondition condition;
            condition.__set_column_name("3");
            condition.__set_condition_op("<");
            std::vector<std::string> vals = {"2"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(&tablet_schema);
            EXPECT_EQ(Status::OK(), conditions->append_condition(condition));

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1);

            EXPECT_TRUE(iter->next_batch(&block).is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }
        // scan all rows
        {
            TCondition condition;
            condition.__set_column_name("2");
            condition.__set_condition_op("<");
            std::vector<std::string> vals = {"100"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(&tablet_schema);
            EXPECT_EQ(Status::OK(), conditions->append_condition(condition));

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            // only first page will be read because of zone map
            int left = 16 * 1024;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                EXPECT_TRUE(iter->next_batch(&block).ok());
                EXPECT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                EXPECT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        EXPECT_FALSE(column_block.is_null(i));
                        EXPECT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i))
                                << "rid:" << rid << ", i:" << i;
                    }
                }
                rowid += rows_read;
            }
            EXPECT_EQ(16 * 1024, rowid);
            EXPECT_TRUE(iter->next_batch(&block).is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }
        // test zone map with query predicate an delete predicate
        {
            // the first two page will be read by this condition
            TCondition condition;
            condition.__set_column_name("2");
            condition.__set_condition_op("<");
            std::vector<std::string> vals = {"165000"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(&tablet_schema);
            EXPECT_EQ(Status::OK(), conditions->append_condition(condition));

            // the second page read will be pruned by the following delete predicate
            TCondition delete_condition;
            delete_condition.__set_column_name("2");
            delete_condition.__set_condition_op("=");
            std::vector<std::string> vals2 = {"164001"};
            delete_condition.__set_condition_values(vals2);
            std::shared_ptr<Conditions> delete_conditions(new Conditions());
            delete_conditions->set_tablet_schema(&tablet_schema);
            EXPECT_EQ(Status::OK(), delete_conditions->append_condition(delete_condition));

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();
            read_opts.delete_conditions.push_back(delete_conditions.get());

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            // so the first page will be read because of zone map
            int left = 16 * 1024;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                auto s = iter->next_batch(&block);
                EXPECT_TRUE(s.ok()) << s.to_string();
                EXPECT_EQ(rows_read, block.num_rows());
                EXPECT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        EXPECT_FALSE(column_block.is_null(i));
                        EXPECT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i))
                                << "rid:" << rid << ", i:" << i;
                    }
                }
                rowid += rows_read;
            }
            EXPECT_EQ(16 * 1024, rowid);
            EXPECT_TRUE(iter->next_batch(&block).is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }
        // test bloom filter
        {
            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            TCondition condition;
            condition.__set_column_name("2");
            condition.__set_condition_op("=");
            // 102 is not in page 1
            std::vector<std::string> vals = {"102"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(&tablet_schema);
            EXPECT_EQ(Status::OK(), conditions->append_condition(condition));
            read_opts.conditions = conditions.get();
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            EXPECT_TRUE(iter->next_batch(&block).is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }
    }
}

TEST_F(SegmentReaderWriterTest, estimate_segment_size) {
    size_t num_rows_per_block = 10;

    std::shared_ptr<TabletSchema> tablet_schema(new TabletSchema());
    tablet_schema->_num_columns = 4;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 2;
    tablet_schema->_num_rows_per_row_block = num_rows_per_block;
    tablet_schema->_cols.push_back(create_int_key(1));
    tablet_schema->_cols.push_back(create_int_key(2));
    tablet_schema->_cols.push_back(create_int_key(3));
    tablet_schema->_cols.push_back(create_int_value(4));

    // segment write
    std::string dname = "./ut_dir/segment_write_size";
    FileUtils::remove_all(dname);
    FileUtils::create_dir(dname);

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = dname + "/int_case";
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts(fname);
    std::string storage_name;
    Status st = fs::fs_util::block_manager(storage_name)->create_block(wblock_opts, &wblock);
    EXPECT_TRUE(st.ok()) << st.to_string();
    DataDir data_dir(dname);
    data_dir.init();
    SegmentWriter writer(wblock.get(), 0, tablet_schema.get(), &data_dir, INT32_MAX, opts);
    st = writer.init(10);
    EXPECT_TRUE(st.ok()) << st.to_string();

    RowCursor row;
    auto olap_st = row.init(*tablet_schema);
    EXPECT_EQ(Status::OK(), olap_st);

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    for (int i = 0; i < LOOP_LESS_OR_MORE(1024, 1048576); ++i) {
        for (int j = 0; j < 4; ++j) {
            auto cell = row.cell(j);
            cell.set_not_null();
            *(int*)cell.mutable_cell_ptr() = i * 10 + j;
        }
        writer.append_row(row);
    }

    uint32_t segment_size = writer.estimate_segment_size();
    LOG(INFO) << "estimate segment size is:" << segment_size;

    uint64_t file_size = 0;
    uint64_t index_size;
    EXPECT_TRUE(writer.finalize(&file_size, &index_size).ok());
    EXPECT_TRUE(wblock->close().ok());

    file_size = std::filesystem::file_size(fname);
    LOG(INFO) << "segment file size is:" << file_size;

    EXPECT_NE(segment_size, 0);

    FileUtils::remove_all(dname);
}

TEST_F(SegmentReaderWriterTest, TestDefaultValueColumn) {
    std::vector<TabletColumn> columns = {create_int_key(1), create_int_key(2), create_int_value(3),
                                         create_int_value(4)};
    TabletSchema build_schema = create_schema(columns);

    // add a column with null default value
    {
        std::vector<TabletColumn> read_columns = columns;
        read_columns.push_back(create_int_value(5, OLAP_FIELD_AGGREGATION_SUM, true, "NULL"));
        TabletSchema query_schema = create_schema(read_columns);

        std::shared_ptr<Segment> segment;
        build_segment(SegmentWriterOptions(), build_schema, query_schema, 4096, DefaultIntGenerator,
                      &segment);

        Schema schema(query_schema);
        OlapReaderStatistics stats;
        // scan all rows
        {
            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            int left = 4096;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                EXPECT_TRUE(iter->next_batch(&block).ok());
                EXPECT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                EXPECT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        if (cid == 4) {
                            EXPECT_TRUE(column_block.is_null(i));
                        } else {
                            EXPECT_FALSE(column_block.is_null(i));
                            EXPECT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i));
                        }
                    }
                }
                rowid += rows_read;
            }
        }
    }

    // add a column with non-null default value
    {
        std::vector<TabletColumn> read_columns = columns;
        read_columns.push_back(create_int_value(5, OLAP_FIELD_AGGREGATION_SUM, true, "10086"));
        TabletSchema query_schema = create_schema(read_columns);

        std::shared_ptr<Segment> segment;
        build_segment(SegmentWriterOptions(), build_schema, query_schema, 4096, DefaultIntGenerator,
                      &segment);

        Schema schema(query_schema);
        OlapReaderStatistics stats;
        // scan all rows
        {
            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            int left = 4096;

            int rowid = 0;
            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                EXPECT_TRUE(iter->next_batch(&block).ok());
                EXPECT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        if (cid == 4) {
                            EXPECT_FALSE(column_block.is_null(i));
                            EXPECT_EQ(10086, *(int*)column_block.cell_ptr(i));
                        } else {
                            EXPECT_FALSE(column_block.is_null(i));
                            EXPECT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i));
                        }
                    }
                }
                rowid += rows_read;
            }
        }
    }
}

TEST_F(SegmentReaderWriterTest, TestStringDict) {
    size_t num_rows_per_block = 10;
    auto tracker = std::make_shared<MemTracker>();
    MemPool pool(tracker.get());

    std::shared_ptr<TabletSchema> tablet_schema(new TabletSchema());
    tablet_schema->_num_columns = 4;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 2;
    tablet_schema->_num_rows_per_row_block = num_rows_per_block;
    tablet_schema->_cols.push_back(create_char_key(1));
    tablet_schema->_cols.push_back(create_char_key(2));
    tablet_schema->_cols.push_back(create_varchar_key(3));
    tablet_schema->_cols.push_back(create_varchar_key(4));
    tablet_schema->init_field_index_for_test();

    //    segment write
    std::string dname = "./ut_dir/segment_test";
    FileUtils::create_dir(dname);

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = dname + "/string_case";
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts(fname);
    std::string storage_name;
    Status st = fs::fs_util::block_manager(storage_name)->create_block(wblock_opts, &wblock);
    EXPECT_TRUE(st.ok());
    DataDir data_dir(dname);
    data_dir.init();
    SegmentWriter writer(wblock.get(), 0, tablet_schema.get(), &data_dir, INT32_MAX, opts);
    st = writer.init(10);
    EXPECT_TRUE(st.ok());

    RowCursor row;
    auto olap_st = row.init(*tablet_schema);
    EXPECT_EQ(Status::OK(), olap_st);

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    // convert int to string
    for (int i = 0; i < 4096; ++i) {
        for (int j = 0; j < 4; ++j) {
            auto cell = row.cell(j);
            cell.set_not_null();
            set_column_value_by_type(tablet_schema->_cols[j]._type, i * 10 + j,
                                     (char*)cell.mutable_cell_ptr(), &pool,
                                     tablet_schema->_cols[j]._length);
        }
        Status status = writer.append_row(row);
        EXPECT_TRUE(status.ok());
    }

    uint64_t file_size = 0;
    uint64_t index_size;
    EXPECT_TRUE(writer.finalize(&file_size, &index_size).ok());
    EXPECT_TRUE(wblock->close().ok());

    {
        std::shared_ptr<Segment> segment;
        FilePathDesc path_desc;
        path_desc.filepath = fname;
        st = Segment::open(path_desc, 0, tablet_schema.get(), &segment);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(4096, segment->num_rows());
        Schema schema(*tablet_schema);
        OlapReaderStatistics stats;
        // scan all rows
        {
            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            int left = 4096;
            int rowid = 0;

            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                st = iter->next_batch(&block);
                EXPECT_TRUE(st.ok());
                EXPECT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                EXPECT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        EXPECT_FALSE(column_block.is_null(i));
                        const Slice* actual =
                                reinterpret_cast<const Slice*>(column_block.cell_ptr(i));

                        Slice expect;
                        set_column_value_by_type(tablet_schema->_cols[j]._type, rid * 10 + cid,
                                                 reinterpret_cast<char*>(&expect), &pool,
                                                 tablet_schema->_cols[j]._length);
                        EXPECT_EQ(expect.to_string(), actual->to_string());
                    }
                }
                rowid += rows_read;
            }
        }

        // test seek, key
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(*tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, 40970,
                                         (char*)cell.mutable_cell_ptr(), &pool,
                                         tablet_schema->_cols[0]._length);
            }

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, nullptr, false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            st = iter->next_batch(&block);
            EXPECT_TRUE(st.is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }

        // test seek, key (-2, -1)
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(*tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, -2, (char*)cell.mutable_cell_ptr(),
                                         &pool, tablet_schema->_cols[0]._length);
            }

            std::unique_ptr<RowCursor> upper_bound(new RowCursor());
            upper_bound->init(*tablet_schema, 1);
            {
                auto cell = upper_bound->cell(0);
                cell.set_not_null();
                set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, -1, (char*)cell.mutable_cell_ptr(),
                                         &pool, tablet_schema->_cols[0]._length);
            }

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, upper_bound.get(), false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            st = iter->next_batch(&block);
            EXPECT_TRUE(st.is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }

        // test char zone_map query hit;should read whole page
        {
            TCondition condition;
            condition.__set_column_name("1");
            condition.__set_condition_op(">");
            std::vector<std::string> vals = {"100"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(tablet_schema.get());
            EXPECT_EQ(Status::OK(), conditions->append_condition(condition));

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            int left = 4 * 1024;
            int rowid = 0;

            while (left > 0) {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                st = iter->next_batch(&block);
                EXPECT_TRUE(st.ok());
                EXPECT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                EXPECT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        EXPECT_FALSE(column_block.is_null(i));

                        const Slice* actual =
                                reinterpret_cast<const Slice*>(column_block.cell_ptr(i));
                        Slice expect;
                        set_column_value_by_type(tablet_schema->_cols[j]._type, rid * 10 + cid,
                                                 reinterpret_cast<char*>(&expect), &pool,
                                                 tablet_schema->_cols[j]._length);
                        EXPECT_EQ(expect.to_string(), actual->to_string())
                                << "rid:" << rid << ", i:" << i;
                        ;
                    }
                }
                rowid += rows_read;
            }
            EXPECT_EQ(4 * 1024, rowid);
            st = iter->next_batch(&block);
            EXPECT_TRUE(st.is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }

        // test char zone_map query miss;col < -1
        {
            TCondition condition;
            condition.__set_column_name("1");
            condition.__set_condition_op("<");
            std::vector<std::string> vals = {"-2"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(tablet_schema.get());
            EXPECT_EQ(Status::OK(), conditions->append_condition(condition));

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            st = iter->next_batch(&block);
            EXPECT_TRUE(st.is_end_of_file());
            EXPECT_EQ(0, block.num_rows());
        }
    }

    FileUtils::remove_all(dname);
}

TEST_F(SegmentReaderWriterTest, TestBitmapPredicate) {
    TabletSchema tablet_schema = create_schema({create_int_key(1, true, false, true),
                                                create_int_key(2, true, false, true),
                                                create_int_value(3), create_int_value(4)});

    SegmentWriterOptions opts;
    shared_ptr<Segment> segment;
    build_segment(opts, tablet_schema, tablet_schema, 4096, DefaultIntGenerator, &segment);
    EXPECT_TRUE(column_contains_index(segment->footer().columns(0), BITMAP_INDEX));
    EXPECT_TRUE(column_contains_index(segment->footer().columns(1), BITMAP_INDEX));

    {
        Schema schema(tablet_schema);

        // test where v1=10
        {
            std::vector<ColumnPredicate*> column_predicates;
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(0, 10));
            column_predicates.emplace_back(predicate.get());

            StorageReadOptions read_opts;
            OlapReaderStatistics stats;
            read_opts.column_predicates = column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            EXPECT_TRUE(iter->next_batch(&block).ok());
            EXPECT_EQ(block.num_rows(), 1);
            EXPECT_EQ(read_opts.stats->raw_rows_read, 1);
        }

        // test where v1=10 and v2=11
        {
            std::vector<ColumnPredicate*> column_predicates;
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(0, 10));
            std::unique_ptr<ColumnPredicate> predicate2(new EqualPredicate<int32_t>(1, 11));
            column_predicates.emplace_back(predicate.get());
            column_predicates.emplace_back(predicate2.get());

            StorageReadOptions read_opts;
            OlapReaderStatistics stats;
            read_opts.column_predicates = column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            EXPECT_TRUE(iter->next_batch(&block).ok());
            EXPECT_EQ(block.num_rows(), 1);
            EXPECT_EQ(read_opts.stats->raw_rows_read, 1);
        }

        // test where v1=10 and v2=15
        {
            std::vector<ColumnPredicate*> column_predicates;
            std::unique_ptr<ColumnPredicate> predicate(new EqualPredicate<int32_t>(0, 10));
            std::unique_ptr<ColumnPredicate> predicate2(new EqualPredicate<int32_t>(1, 15));
            column_predicates.emplace_back(predicate.get());
            column_predicates.emplace_back(predicate2.get());

            StorageReadOptions read_opts;
            OlapReaderStatistics stats;
            read_opts.column_predicates = column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            EXPECT_FALSE(iter->next_batch(&block).ok());
            EXPECT_EQ(read_opts.stats->raw_rows_read, 0);
        }

        // test where v1 in (10,20,1)
        {
            std::vector<ColumnPredicate*> column_predicates;
            phmap::flat_hash_set<int32_t> values;
            values.insert(10);
            values.insert(20);
            values.insert(1);
            std::unique_ptr<ColumnPredicate> predicate(
                    new InListPredicate<int32_t>(0, std::move(values)));
            column_predicates.emplace_back(predicate.get());

            StorageReadOptions read_opts;
            OlapReaderStatistics stats;
            read_opts.column_predicates = column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            EXPECT_TRUE(iter->next_batch(&block).ok());
            EXPECT_EQ(read_opts.stats->raw_rows_read, 2);
        }

        // test where v1 not in (10,20)
        {
            std::vector<ColumnPredicate*> column_predicates;
            phmap::flat_hash_set<int32_t> values;
            values.insert(10);
            values.insert(20);
            std::unique_ptr<ColumnPredicate> predicate(
                    new NotInListPredicate<int32_t>(0, std::move(values)));
            column_predicates.emplace_back(predicate.get());

            StorageReadOptions read_opts;
            OlapReaderStatistics stats;
            read_opts.column_predicates = column_predicates;
            read_opts.stats = &stats;

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            Status st;
            do {
                block.clear();
                st = iter->next_batch(&block);
            } while (st.ok());
            EXPECT_EQ(read_opts.stats->raw_rows_read, 4094);
        }
    }
}

TEST_F(SegmentReaderWriterTest, TestBloomFilterIndexUniqueModel) {
    TabletSchema schema =
            create_schema({create_int_key(1), create_int_key(2), create_int_key(3),
                           create_int_value(4, OLAP_FIELD_AGGREGATION_REPLACE, true, "", true)});

    // for not base segment
    SegmentWriterOptions opts1;
    shared_ptr<Segment> seg1;
    build_segment(opts1, schema, schema, 100, DefaultIntGenerator, &seg1);
    EXPECT_TRUE(column_contains_index(seg1->footer().columns(3), BLOOM_FILTER_INDEX));

    // for base segment
    SegmentWriterOptions opts2;
    shared_ptr<Segment> seg2;
    build_segment(opts2, schema, schema, 100, DefaultIntGenerator, &seg2);
    EXPECT_TRUE(column_contains_index(seg2->footer().columns(3), BLOOM_FILTER_INDEX));
}

} // namespace segment_v2
} // namespace doris

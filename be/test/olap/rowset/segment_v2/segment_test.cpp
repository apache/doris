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
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/rowset/segment_v2/segment_iterator.h"

#include <gtest/gtest.h>
#include <iostream>
#include <boost/filesystem.hpp>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/row_cursor.h"
#include "olap/tablet_schema.h"
#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/types.h"
#include "olap/tablet_schema_helper.h"
#include "util/file_utils.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {
namespace segment_v2 {

class SegmentReaderWriterTest : public testing::Test {
public:
    SegmentReaderWriterTest() { }
    virtual ~SegmentReaderWriterTest() {
    }
};

TEST_F(SegmentReaderWriterTest, normal) {

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
    std::string dname = "./ut_dir/segment_test";
    FileUtils::create_dir(dname);

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = dname + "/int_case";
    SegmentWriter writer(fname, 0, tablet_schema.get(), opts);
    auto st = writer.init(10);
    ASSERT_TRUE(st.ok());

    RowCursor row;
    auto olap_st = row.init(*tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    for (int i = 0; i < 4096; ++i) {
        for (int j = 0; j < 4; ++j) {
            auto cell = row.cell(j);
            cell.set_not_null();
            *(int*)cell.mutable_cell_ptr() = i * 10 + j;
        }
        writer.append_row(row);
    }

    uint64_t file_size = 0;
    st = writer.finalize(&file_size);
    ASSERT_TRUE(st.ok());
    // reader
    {
        std::shared_ptr<Segment> segment;
        st = Segment::open(fname, 0, tablet_schema.get(), &segment);
        LOG(INFO) << "segment open, msg=" << st.to_string();
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(4096, segment->num_rows());
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
            while (left > 0)  {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                st = iter->next_batch(&block);
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(BitmapTest(column_block.null_bitmap(), i));
                        ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i));
                    }
                }
                rowid += rows_read;
            }
        }
        // test seek, key
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(*tablet_schema, 2);
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
            upper_bound->init(*tablet_schema, 1);
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
            st = iter->next_batch(&block);
            ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(11, block.num_rows());
            auto column_block = block.column_block(0);
            for (int i = 0; i < 11; ++i) {
                ASSERT_EQ(100 + i * 10, *(int*)column_block.cell_ptr(i));
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
                *(int*)cell.mutable_cell_ptr() = 40970;
            }

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, nullptr, false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            st = iter->next_batch(&block);
            ASSERT_TRUE(st.is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
        // test seek, key (-2, -1)
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(*tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = -2;
            }

            std::unique_ptr<RowCursor> upper_bound(new RowCursor());
            upper_bound->init(*tablet_schema, 1);
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
            st = iter->next_batch(&block);
            ASSERT_TRUE(st.is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
    }
    FileUtils::remove_all(dname);
}

TEST_F(SegmentReaderWriterTest, TestZoneMap) {
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
    std::string dname = "./ut_dir/segment_test";
    FileUtils::create_dir(dname);

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = dname + "/int_case2";
    SegmentWriter writer(fname, 0, tablet_schema.get(), opts);
    auto st = writer.init(10);
    ASSERT_TRUE(st.ok());

    RowCursor row;
    auto olap_st = row.init(*tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    //
    // 64k int will generate 4 pages
    for (int i = 0; i < 64 * 1024; ++i) {
        for (int j = 0; j < 4; ++j) {
            auto cell = row.cell(j);
            cell.set_not_null();
            if (i >= 16 * 1024 && i < 32 * 1024) {
                // make second page all rows equal
                *(int*)cell.mutable_cell_ptr() = 164000 + j;

            } else {
                *(int*)cell.mutable_cell_ptr() = i * 10 + j;
            }
        }
        writer.append_row(row);
    }

    uint64_t file_size = 0;
    st = writer.finalize(&file_size);
    ASSERT_TRUE(st.ok());

    // reader with condition
    {
        std::shared_ptr<Segment> segment;
        st = Segment::open(fname, 0, tablet_schema.get(), &segment);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(64 * 1024, segment->num_rows());
        Schema schema(*tablet_schema);
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
            conditions->set_tablet_schema(tablet_schema.get());
            conditions->append_condition(condition);

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1);

            st = iter->next_batch(&block);
            ASSERT_TRUE(st.is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
        // scan all rows
        {
            TCondition condition;
            condition.__set_column_name("2");
            condition.__set_condition_op("<");
            std::vector<std::string> vals = {"100"};
            condition.__set_condition_values(vals);
            std::shared_ptr<Conditions> conditions(new Conditions());
            conditions->set_tablet_schema(tablet_schema.get());
            conditions->append_condition(condition);

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            // only first page will be read because of zone map
            int left = 16 * 1024;

            int rowid = 0;
            while (left > 0)  {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                st = iter->next_batch(&block);
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(BitmapTest(column_block.null_bitmap(), i));
                        ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i)) << "rid:" << rid << ", i:" << i;
                    }
                }
                rowid += rows_read;
            }
            ASSERT_EQ(16 * 1024, rowid);
            st = iter->next_batch(&block);
            ASSERT_TRUE(st.is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
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
            conditions->set_tablet_schema(tablet_schema.get());
            conditions->append_condition(condition);

            // the second page read will be pruned by the following delete predicate
            TCondition delete_condition;
            delete_condition.__set_column_name("2");
            delete_condition.__set_condition_op("=");
            std::vector<std::string> vals2 = {"164001"};
            delete_condition.__set_condition_values(vals2);
            std::shared_ptr<Conditions> delete_conditions(new Conditions());
            delete_conditions->set_tablet_schema(tablet_schema.get());
            delete_conditions->append_condition(delete_condition);

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
            while (left > 0)  {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                st = iter->next_batch(&block);
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(rows_read, block.num_rows());
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(BitmapTest(column_block.null_bitmap(), i));
                        ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i)) << "rid:" << rid << ", i:" << i;
                    }
                }
                rowid += rows_read;
            }
            ASSERT_EQ(16 * 1024, rowid);
            st = iter->next_batch(&block);
            ASSERT_TRUE(st.is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }
    }
    FileUtils::remove_all(dname);
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
    FileUtils::create_dir(dname);

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = dname + "/int_case";
    SegmentWriter writer(fname, 0, tablet_schema.get(), opts);
    auto st = writer.init(10);
    ASSERT_TRUE(st.ok());

    RowCursor row;
    auto olap_st = row.init(*tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    for (int i = 0; i < 1048576; ++i) {
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
    st = writer.finalize(&file_size);

    ASSERT_TRUE(st.ok());

    file_size = boost::filesystem::file_size(fname);
    LOG(INFO) << "segment file size is:" << file_size;

    ASSERT_NE(segment_size, 0);

    float error = 0;
    if (segment_size > file_size) {
        error = (segment_size - file_size) * 1.0 / segment_size;
    } else {
        error = (file_size - segment_size) * 1.0 / segment_size;
    }

    ASSERT_LT(error, 0.30);

    FileUtils::remove_all(dname);
}

TEST_F(SegmentReaderWriterTest, TestDefaultValueColumn) {
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
    std::string dname = "./ut_dir/segment_test";
    FileUtils::create_dir(dname);

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = dname + "/int_case";
    SegmentWriter writer(fname, 0, tablet_schema.get(), opts);
    auto st = writer.init(10);
    ASSERT_TRUE(st.ok());

    RowCursor row;
    auto olap_st = row.init(*tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    for (int i = 0; i < 4096; ++i) {
        for (int j = 0; j < 4; ++j) {
            auto cell = row.cell(j);
            cell.set_not_null();
            *(int*)cell.mutable_cell_ptr() = i * 10 + j;
        }
        writer.append_row(row);
    }

    uint64_t file_size = 0;
    st = writer.finalize(&file_size);
    ASSERT_TRUE(st.ok());

    // add a column with null default value
    {
        std::shared_ptr<TabletSchema> new_tablet_schema_1(new TabletSchema());
        new_tablet_schema_1->_num_columns = 5;
        new_tablet_schema_1->_num_key_columns = 3;
        new_tablet_schema_1->_num_short_key_columns = 2;
        new_tablet_schema_1->_num_rows_per_row_block = num_rows_per_block;
        new_tablet_schema_1->_cols.push_back(create_int_key(1));
        new_tablet_schema_1->_cols.push_back(create_int_key(2));
        new_tablet_schema_1->_cols.push_back(create_int_key(3));
        new_tablet_schema_1->_cols.push_back(create_int_value(4));
        new_tablet_schema_1->_cols.push_back(
            create_int_value(5, OLAP_FIELD_AGGREGATION_SUM, true, "NULL"));

        std::shared_ptr<Segment> segment;
        st = Segment::open(fname, 0, new_tablet_schema_1.get(), &segment);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(4096, segment->num_rows());
        Schema schema(*new_tablet_schema_1);
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
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        if (cid == 4) {
                            ASSERT_TRUE(BitmapTest(column_block.null_bitmap(), i));
                        } else {
                            ASSERT_FALSE(BitmapTest(column_block.null_bitmap(), i));
                            ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i));
                        }
                    }
                }
                rowid += rows_read;
            }
        }
    }

    // add a column with non-null default value
    {
        std::shared_ptr<TabletSchema> new_tablet_schema_1(new TabletSchema());
        new_tablet_schema_1->_num_columns = 5;
        new_tablet_schema_1->_num_key_columns = 3;
        new_tablet_schema_1->_num_short_key_columns = 2;
        new_tablet_schema_1->_num_rows_per_row_block = num_rows_per_block;
        new_tablet_schema_1->_cols.push_back(create_int_key(1));
        new_tablet_schema_1->_cols.push_back(create_int_key(2));
        new_tablet_schema_1->_cols.push_back(create_int_key(3));
        new_tablet_schema_1->_cols.push_back(create_int_value(4));
        new_tablet_schema_1->_cols.push_back(create_int_value(5, OLAP_FIELD_AGGREGATION_SUM, true, "10086"));

        std::shared_ptr<Segment> segment;
        st = Segment::open(fname, 0, new_tablet_schema_1.get(), &segment);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(4096, segment->num_rows());
        Schema schema(*new_tablet_schema_1);
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
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        if (cid == 4) {
                            ASSERT_FALSE(BitmapTest(column_block.null_bitmap(), i));
                            ASSERT_EQ(10086, *(int*)column_block.cell_ptr(i));
                        } else {
                            ASSERT_FALSE(BitmapTest(column_block.null_bitmap(), i));
                            ASSERT_EQ(rid * 10 + cid, *(int*)column_block.cell_ptr(i));
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
    MemTracker tracker;
    MemPool pool(&tracker);

    std::shared_ptr<TabletSchema> tablet_schema(new TabletSchema());
    tablet_schema->_num_columns = 4;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 2;
    tablet_schema->_num_rows_per_row_block = num_rows_per_block;
    tablet_schema->_cols.push_back(create_char_key(1));
    tablet_schema->_cols.push_back(create_char_key(2));
    tablet_schema->_cols.push_back(create_varchar_key(3));
    tablet_schema->_cols.push_back(create_varchar_key(4));

    //    segment write
    std::string dname = "./ut_dir/segment_test";
    FileUtils::create_dir(dname);

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = dname + "/string_case";

    SegmentWriter writer(fname, 0, tablet_schema.get(), opts);
    auto st = writer.init(10);
    ASSERT_TRUE(st.ok());

    RowCursor row;
    auto olap_st = row.init(*tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    // convert int to string
    for (int i = 0; i < 4096; ++i) {
        for (int j = 0; j < 4; ++j) {
            auto cell = row.cell(j);
            cell.set_not_null();
            set_column_value_by_type(tablet_schema->_cols[j]._type, i * 10 + j, (char*)cell.mutable_cell_ptr(), &pool, tablet_schema->_cols[j]._length);
        }
        Status status = writer.append_row(row);
        ASSERT_TRUE(status.ok());
    }

    uint64_t file_size = 0;
    st = writer.finalize(&file_size);
    ASSERT_TRUE(st.ok());

    {
        std::shared_ptr<Segment> segment;
        st = Segment::open(fname, 0, tablet_schema.get(), &segment);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(4096, segment->num_rows());
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

            while (left > 0)  {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                st = iter->next_batch(&block);
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(BitmapTest(column_block.null_bitmap(), i));
                        const Slice* actual = reinterpret_cast<const Slice*>(column_block.cell_ptr(i));

                        Slice expect;
                        set_column_value_by_type(tablet_schema->_cols[j]._type, rid * 10 + cid, reinterpret_cast<char*>(&expect), &pool, tablet_schema->_cols[j]._length);
                        ASSERT_EQ(expect.to_string(), actual->to_string());
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
                set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, 40970, (char*)cell.mutable_cell_ptr(), &pool, tablet_schema->_cols[0]._length);
            }

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, nullptr, false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            st = iter->next_batch(&block);
            ASSERT_TRUE(st.is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }

        // test seek, key (-2, -1)
        {
            // lower bound
            std::unique_ptr<RowCursor> lower_bound(new RowCursor());
            lower_bound->init(*tablet_schema, 1);
            {
                auto cell = lower_bound->cell(0);
                cell.set_not_null();
                set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, -2, (char*)cell.mutable_cell_ptr(), &pool, tablet_schema->_cols[0]._length);
            }

            std::unique_ptr<RowCursor> upper_bound(new RowCursor());
            upper_bound->init(*tablet_schema, 1);
            {
                auto cell = upper_bound->cell(0);
                cell.set_not_null();
                set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, -1, (char*)cell.mutable_cell_ptr(), &pool, tablet_schema->_cols[0]._length);
            }

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.key_ranges.emplace_back(lower_bound.get(), false, upper_bound.get(), false);
            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 100);
            st = iter->next_batch(&block);
            ASSERT_TRUE(st.is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
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
            conditions->append_condition(condition);

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);
            int left = 4 * 1024;
            int rowid = 0;

            while (left > 0)  {
                int rows_read = left > 1024 ? 1024 : left;
                block.clear();
                st = iter->next_batch(&block);
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(DEL_NOT_SATISFIED, block.delete_state());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.schema()->column_ids().size(); ++j) {
                    auto cid = block.schema()->column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        ASSERT_FALSE(BitmapTest(column_block.null_bitmap(), i));

                        const Slice* actual = reinterpret_cast<const Slice*>(column_block.cell_ptr(i));
                        Slice expect;
                        set_column_value_by_type(tablet_schema->_cols[j]._type, rid * 10 + cid, reinterpret_cast<char*>(&expect), &pool, tablet_schema->_cols[j]._length);
                        ASSERT_EQ(expect.to_string(), actual->to_string()) << "rid:" << rid << ", i:" << i;;
                    }
                }
                rowid += rows_read;
            }
            ASSERT_EQ(4 * 1024, rowid);
            st = iter->next_batch(&block);
            ASSERT_TRUE(st.is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
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
            conditions->append_condition(condition);

            StorageReadOptions read_opts;
            read_opts.stats = &stats;
            read_opts.conditions = conditions.get();

            std::unique_ptr<RowwiseIterator> iter;
            segment->new_iterator(schema, read_opts, &iter);

            RowBlockV2 block(schema, 1024);

            st = iter->next_batch(&block);
            ASSERT_TRUE(st.is_end_of_file());
            ASSERT_EQ(0, block.num_rows());
        }

    }

    FileUtils::remove_all(dname);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

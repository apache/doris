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

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/row_cursor.h"
#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/types.h"
#include "util/file_utils.h"

namespace doris {
namespace segment_v2 {

class SegmentReaderWriterTest : public testing::Test {
public:
    SegmentReaderWriterTest() { }
    virtual ~SegmentReaderWriterTest() {
    }
};

TEST_F(SegmentReaderWriterTest, normal) {
    std::vector<ColumnSchemaV2> schema;
    std::vector<FieldInfo> field_infos;
    for (int i = 0; i < 4; ++i) {
        FieldInfo field_info;
        field_info.type = OLAP_FIELD_TYPE_INT;
        field_info.length = 4;
        field_info.is_allow_null = true;
        field_info.unique_id = 0;
        field_info.is_root_column = true;

        field_infos.emplace_back(field_info);
        schema.emplace_back(field_info);
    }

    RowCursor row_cursor;
    auto olap_st = row_cursor.init(field_infos);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    RowBlock row_block(field_infos);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block_info.null_supported = true;
    olap_st = row_block.init(block_info);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    for (int i = 0; i < 1024; ++i) {
        row_block.get_row(i, &row_cursor);
        for (int j = 0; j < 4; ++j) {
            if (j > 2 && ((i + j) % 8) == 0) {
                row_cursor.set_null(j);
            } else {
                row_cursor.set_not_null(j);
                int* field = (int*)row_cursor.get_field_content_ptr(j);
                *field = i * 10 + j;
            }
        }
    }

    // segment write
    std::string dname = "./ut_dir/segment_test";
    FileUtils::create_dir(dname);

    std::string fname = dname + "/int_case";
    SegmentWriter writer(fname, schema, 2, 0, 1024, false);
    auto st = writer.init(10);
    ASSERT_TRUE(st.ok());

    st = writer.write_batch(&row_block, &row_cursor, true);
    ASSERT_TRUE(st.ok());

    uint32_t file_size = 0;
    st = writer.finalize(&file_size);
    ASSERT_TRUE(st.ok());
    // reader
    {
        std::shared_ptr<Segment> segment(new Segment(fname, schema, 2, 0));
        st = segment->open();
        LOG(INFO) << "segment open, msg=" << st.to_string();
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(1024, segment->num_rows());
        // scan all rows
        {
            std::unique_ptr<SegmentIterator> iter;
            st = segment->new_iterator(&iter);
            ASSERT_TRUE(st.ok());

            StorageReadOptions opts;
            opts.column_ids.push_back(0);
            opts.column_ids.push_back(2);

            st = iter->init(opts);
            ASSERT_TRUE(st.ok());

            Arena arena;
            RowBlockV2 block(schema, opts.column_ids, 100, &arena);

            int left = 1024;

            int rowid = 0;
            while (left > 0)  {
                int rows_read = left > 100 ? 100 : left;
                st = iter->next_batch(&block);
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(rows_read, block.num_rows());
                left -= rows_read;

                for (int j = 0; j < block.column_ids().size(); ++j) {
                    auto cid = block.column_ids()[j];
                    auto column_block = block.column_block(j);
                    for (int i = 0; i < rows_read; ++i) {
                        int rid = rowid + i;
                        if (cid > 2 && ((rid + cid) % 8) == 0) {
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
        // test seek, key
        {
            std::unique_ptr<SegmentIterator> iter;
            st = segment->new_iterator(&iter);
            ASSERT_TRUE(st.ok());

            StorageReadOptions opts;
            opts.column_ids.push_back(0);
            opts.column_ids.push_back(1);

            // lower bound
            opts.lower_bound.reset(new RowCursor());
            RowCursor* lower_bound = opts.lower_bound.get();
            lower_bound->init(field_infos, 1);
            lower_bound->set_not_null(0);
            *(int*)lower_bound->get_field_content_ptr(0) = 100;
            opts.include_lower_bound = false;

            // upper bound
            opts.upper_bound.reset(new RowCursor());
            RowCursor* upper_bound = opts.upper_bound.get();
            upper_bound->init(field_infos, 1);
            upper_bound->set_not_null(0);
            *(int*)upper_bound->get_field_content_ptr(0) = 200;
            opts.include_upper_bound = true;

            st = iter->init(opts);
            LOG(INFO) << "iterator init msg=" << st.to_string();
            ASSERT_TRUE(st.ok());

            Arena arena;
            RowBlockV2 block(schema, opts.column_ids, 100, &arena);
            st = iter->next_batch(&block);
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(10, block.num_rows());
            auto column_block = block.column_block(0);
            for (int i = 0; i < 10; ++i) {
                ASSERT_EQ(110 + i * 10, *(int*)column_block.cell_ptr(i));
            }
        }
        // test seek, key
        {
            std::unique_ptr<SegmentIterator> iter;
            st = segment->new_iterator(&iter);
            ASSERT_TRUE(st.ok());

            StorageReadOptions opts;
            opts.column_ids.push_back(0);
            opts.column_ids.push_back(1);

            // lower bound
            opts.lower_bound.reset(new RowCursor());
            RowCursor* lower_bound = opts.lower_bound.get();
            lower_bound->init(field_infos, 1);
            lower_bound->set_not_null(0);
            *(int*)lower_bound->get_field_content_ptr(0) = 10250;
            opts.include_lower_bound = false;

            st = iter->init(opts);
            LOG(INFO) << "iterator init msg=" << st.to_string();
            ASSERT_TRUE(st.ok());

            Arena arena;
            RowBlockV2 block(schema, opts.column_ids, 100, &arena);
            st = iter->next_batch(&block);
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(0, block.num_rows());
        }
    }
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


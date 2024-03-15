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

#include "olap/delete_bitmap_calculator.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/primary_key_index.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "runtime/exec_env.h"

namespace doris {
using namespace ErrorCode;

static std::string kSegmentDir = "./ut_dir/delete_bitmap_calculator_test";
static RowsetId rowset_id {0};

using Generator = std::function<void(size_t rid, int cid, RowCursorCell& cell)>;

static TabletColumnPtr create_int_sequence_value(int32_t id, bool is_nullable = true,
                                                 bool is_bf_column = false,
                                                 bool has_bitmap_index = false) {
    TabletColumnPtr column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_INT;
    column->_is_key = false;
    column->_is_nullable = is_nullable;
    column->_length = 4;
    column->_index_length = 4;
    column->_is_bf_column = is_bf_column;
    column->_has_bitmap_index = has_bitmap_index;
    column->set_name(SEQUENCE_COL);
    return column;
}

class DeleteBitmapCalculatorTest : public testing::Test {
public:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kSegmentDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kSegmentDir);
        ASSERT_TRUE(st.ok()) << st;
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<StorageEngine>(EngineOptions {}));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kSegmentDir).ok());
    }

    TabletSchemaSPtr create_schema(const std::vector<TabletColumnPtr>& columns,
                                   KeysType keys_type = UNIQUE_KEYS) {
        TabletSchemaSPtr res = std::make_shared<TabletSchema>();

        for (auto& col : columns) {
            res->append_column(*col);
        }
        res->_keys_type = keys_type;
        return res;
    }

    void build_segment(SegmentWriterOptions opts, TabletSchemaSPtr build_schema, size_t segment_id,
                       TabletSchemaSPtr query_schema, size_t nrows, Generator generator,
                       std::shared_ptr<Segment>* res) {
        std::string filename = fmt::format("{}_{}.dat", rowset_id.to_string(), segment_id);
        std::string path = fmt::format("{}/{}", kSegmentDir, filename);
        auto fs = io::global_local_filesystem();

        io::FileWriterPtr file_writer;
        Status st = fs->create_file(path, &file_writer);
        EXPECT_TRUE(st.ok());
        SegmentWriter writer(file_writer.get(), segment_id, build_schema, nullptr, nullptr,
                             INT32_MAX, opts, nullptr);
        st = writer.init();
        EXPECT_TRUE(st.ok());

        RowCursor row;
        auto olap_st = row.init(build_schema);
        EXPECT_EQ(Status::OK(), olap_st);

        for (size_t rid = 0; rid < nrows; ++rid) {
            for (int cid = 0; cid < build_schema->num_columns(); ++cid) {
                RowCursorCell cell = row.cell(cid);
                generator(rid, cid, cell);
            }
            EXPECT_TRUE(writer.append_row(row).ok());
        }

        uint64_t file_size, index_size;
        st = writer.finalize(&file_size, &index_size);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(file_writer->close().ok());

        EXPECT_NE("", writer.min_encoded_key().to_string());
        EXPECT_NE("", writer.max_encoded_key().to_string());

        st = segment_v2::Segment::open(fs, path, segment_id, rowset_id, query_schema,
                                       io::FileReaderOptions {}, res);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(nrows, (*res)->num_rows());
    }

    void run_test(size_t const num_segments, size_t const max_rows_per_segment,
                  size_t const num_key_columns, bool has_sequence_col,
                  size_t const num_value_columns, int const random_seed, int const min_value,
                  int const max_value) {
        SegmentWriterOptions opts;
        opts.enable_unique_key_merge_on_write = true;

        size_t const num_columns = num_key_columns + has_sequence_col + num_value_columns;
        size_t const seq_col_idx = has_sequence_col ? num_key_columns : -1;

        std::vector<TabletColumnPtr> columns;

        for (int i = 0; i < num_key_columns; ++i) {
            columns.emplace_back(create_int_key(i));
        }
        if (has_sequence_col) {
            columns.emplace_back(create_int_sequence_value(num_key_columns));
        }
        for (int i = 0; i < num_value_columns; ++i) {
            columns.emplace_back(create_int_value(num_key_columns + has_sequence_col));
        }

        TabletSchemaSPtr tablet_schema = create_schema(columns, UNIQUE_KEYS);

        std::mt19937 rng(random_seed);
        std::uniform_int_distribution<int> gen(min_value, max_value);

        std::vector<std::shared_ptr<segment_v2::Segment>> segments(num_segments);
        std::vector<std::vector<std::vector<int>>> datas(num_segments);
        std::map<std::pair<size_t, size_t>, std::vector<int>> data_map;
        // each flat_data of data will be a tuple of (column1, column2, ..., segment_id, row_id)
        std::vector<std::vector<int>> flat_data;
        size_t seq_counter = 0;

        // Generate random data, ensuring that there are no identical keys within each segment
        // and the keys within each segment are ordered.
        // Also, ensure that the sequence values are not equal.
        for (size_t sid = 0; sid < num_segments; ++sid) {
            auto& segment_data = datas[sid];
            for (size_t rid = 0; rid < max_rows_per_segment; ++rid) {
                std::vector<int> row;
                for (size_t cid = 0; cid < num_columns; ++cid) {
                    if (cid == seq_col_idx) {
                        row.emplace_back(++seq_counter);
                    } else {
                        row.emplace_back(gen(rng));
                    }
                }
                segment_data.emplace_back(row);
            }
            std::sort(segment_data.begin(), segment_data.end());
            segment_data.erase(
                    std::unique(segment_data.begin(), segment_data.end(),
                                [&](std::vector<int> const& lhs, std::vector<int> const& rhs) {
                                    return std::vector<int>(lhs.begin(),
                                                            lhs.begin() + num_key_columns) ==
                                           std::vector<int>(rhs.begin(),
                                                            rhs.begin() + num_key_columns);
                                }),
                    segment_data.end());
            for (size_t rid = 0; rid < segment_data.size(); ++rid) {
                data_map[{sid, rid}] = segment_data[rid];
                auto row = segment_data[rid];
                row.emplace_back(sid);
                row.emplace_back(rid);
                flat_data.emplace_back(row);
            }
        }

        // Construct segments using the data generated before.
        for (size_t sid = 0; sid < num_segments; ++sid) {
            auto& segment = segments[sid];
            std::vector<int> row_data;
            auto generator = [&](size_t rid, int cid, RowCursorCell& cell) {
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = data_map[{sid, rid}][cid];
            };
            build_segment(opts, tablet_schema, sid, tablet_schema, datas[sid].size(), generator,
                          &segment);
        }

        // find the location of rows to be deleted using `MergeIndexDeleteBitmapCalculator`
        // and the result is `result1`
        MergeIndexDeleteBitmapCalculator calculator;
        size_t seq_col_len = 0;
        if (has_sequence_col) {
            seq_col_len = tablet_schema->column(tablet_schema->sequence_col_idx()).length() + 1;
        }

        ASSERT_TRUE(calculator.init(rowset_id, segments, seq_col_len).ok());
        DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(0);
        ASSERT_TRUE(calculator.calculate_all(delete_bitmap).ok());

        std::set<std::pair<size_t, size_t>> result1;
        for (auto [bitmap_key, row_ids] : delete_bitmap->delete_bitmap) {
            auto segment_id = std::get<1>(bitmap_key);
            for (auto row_id : row_ids) {
                result1.emplace(segment_id, row_id);
            }
        }

        // find the location of rows to be deleted using naive algorithm
        // and the result is `result2`
        std::set<std::pair<size_t, size_t>> result2;
        std::sort(flat_data.begin(), flat_data.end(),
                  [&](std::vector<int> const& lhs, std::vector<int> const& rhs) -> bool {
                      for (size_t cid = 0; cid < num_key_columns; ++cid) {
                          if (lhs[cid] != rhs[cid]) {
                              return lhs[cid] < rhs[cid];
                          }
                      }
                      return has_sequence_col ? lhs[seq_col_idx] > rhs[seq_col_idx]
                                              : lhs[num_columns] > rhs[num_columns];
                  });

        for (size_t i = 1; i < flat_data.size(); ++i) {
            bool to_delete = true;
            for (size_t cid = 0; cid < num_key_columns; ++cid) {
                if (flat_data[i][cid] != flat_data[i - 1][cid]) {
                    to_delete = false;
                }
            }
            if (to_delete) {
                result2.emplace(flat_data[i][num_columns], flat_data[i][num_columns + 1]);
            }
        }

        LOG(INFO) << fmt::format("result1.size(): {}, result2.size(): {}", result1.size(),
                                 result2.size());
        // if result1 is equal to result2,
        // we assume the result of `MergeIndexDeleteBitmapCalculator` is correct.
        ASSERT_EQ(result1, result2);
    }
};

TEST_F(DeleteBitmapCalculatorTest, no_sequence_column) {
    run_test(2, 10, 2, false, 1, 4933, 1, 3);
    run_test(4, 100, 2, false, 1, 4933, 1, 15);
    run_test(10, 1000, 2, false, 1, 4933, 1, 50);
    run_test(10, 8192, 2, false, 1, 4933, 1, 100);
}

TEST_F(DeleteBitmapCalculatorTest, has_sequence_column) {
    run_test(2, 10, 2, true, 1, 4933, 1, 3);
    run_test(4, 100, 2, true, 1, 4933, 1, 15);
    run_test(10, 1000, 2, true, 1, 4933, 1, 50);
    run_test(10, 8192, 2, true, 1, 4933, 1, 100);
}

} // namespace doris

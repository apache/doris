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

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/fs_file_cache_storage.h"
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
namespace fs = std::filesystem;

static std::string zySegmentDir = "./ut_dir/open_segment_test";
static RowsetId rowset_id {0};
static fs::path caches_dir = fs::current_path() / "open_segment_cache_test";
static std::string cache_base_path = caches_dir / "cache1" / "";

using Generator = std::function<void(size_t rid, int cid, RowCursorCell& cell)>;

class OpenSegmentTest : public testing::Test {
public:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(zySegmentDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(zySegmentDir);
        ASSERT_TRUE(st.ok()) << st;
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<StorageEngine>(EngineOptions {}));

        std::vector<StorePath> store_paths = {
                StorePath("./", 1024 * 1024 * 1024, TStorageMedium::HDD),
                StorePath("./", 2048 * 1024 * 1024, TStorageMedium::SSD)};

        std::vector<StorePath> spill_store_paths = {
                StorePath("./", 512 * 1024 * 1024, TStorageMedium::HDD)};

        std::set<std::string> broken_paths = {"./", "./"};

        // 调用 init 方法
        st = ExecEnv::init(doris::ExecEnv::GetInstance(), store_paths, spill_store_paths,
                           broken_paths);
        if (st != Status::OK()) {
            std::cerr << "failed to init doris storage engine, res=" << st;
        }
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(zySegmentDir).ok());
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
        std::string path = fmt::format("{}/{}", zySegmentDir, filename);
        auto fs = io::global_local_filesystem();

        io::FileWriterPtr file_writer;
        Status st = fs->create_file(path, &file_writer);
        EXPECT_TRUE(st.ok());
        SegmentWriter writer(file_writer.get(), segment_id, build_schema, nullptr, nullptr, opts);
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
        EXPECT_FALSE(st.ok());
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
    }
};

TEST_F(OpenSegmentTest, parse_footer_error_non_cloud) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    io::FileCacheSettings settings;
    settings.index_queue_size = 30;
    settings.index_queue_elements = 5;
    settings.capacity = 30;
    settings.max_file_block_size = 30;
    settings.max_query_cache_size = 30;
    Status status = io::FileCacheFactory::instance()->create_file_cache(cache_base_path, settings);
    ASSERT_TRUE(status.ok());

    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] { sp->clear_all_call_backs(); }};
    sp->set_call_back("Segment::parse_footer:magic_number_corruption", [&](auto&& args) {
        if (auto p = std::any_cast<uint8_t*>(args[0])) {
            memset(p, 0, 12);
        } else {
            std::cerr << "Failed to cast std::any to uint8_t*" << std::endl;
        }
    });
    sp->enable_processing();
    run_test(2, 10, 2, false, 1, 4933, 1, 3);
    ASSERT_FALSE(fs::exists(cache_base_path + "/error_log/"));
}

} // namespace doris

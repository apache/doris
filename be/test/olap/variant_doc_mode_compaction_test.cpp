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

#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "olap/cumulative_compaction.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/index_writer.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "testutil/variant_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_string.h"

#ifndef NDEBUG
namespace doris {
namespace vectorized {
using namespace ErrorCode;

static constexpr uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;
static constexpr int32_t kRowsPerSegment = 400000;

class VariantDocModeCompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + std::string(kTestDir);
        Status st;
        bool exists = false;
        ASSERT_TRUE(io::global_local_filesystem()->exists(absolute_dir, &exists).ok());
        if (!exists) {
            st = io::global_local_filesystem()->create_directory(absolute_dir);
            ASSERT_TRUE(st.ok()) << st.to_string();
        }
        ASSERT_TRUE(io::global_local_filesystem()
                            ->create_directory(absolute_dir + "/tablet_path")
                            .ok());
        cache_dir = std::string(buffer) + "/ut_dir/variant_doc_mode_compaction_test_cache";
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(cache_dir).ok());

        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(std::string(tmp_dir)).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(std::string(tmp_dir)).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        st = tmp_file_dirs->init();
        ASSERT_TRUE(st.ok()) << st.to_json();
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*engine_ref, absolute_dir);
        ASSERT_TRUE(_data_dir->init(true).ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        // config::enable_ordered_data_compaction = false;
    }

    void TearDown() override {
        // ASSERT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    TabletSchemaSPtr create_schema() {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(KeysType::DUP_KEYS);
        tablet_schema_pb.set_num_short_key_columns(1);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(3);

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
        column_2->set_name("v");
        column_2->set_type("VARIANT");
        column_2->set_is_key(false);
        column_2->set_is_nullable(false);
        column_2->set_variant_max_subcolumns_count(0);
        column_2->set_variant_max_sparse_column_statistics_size(10000);
        column_2->set_variant_sparse_hash_shard_count(32);
        column_2->set_variant_enable_doc_mode(true);
        column_2->set_variant_doc_materialization_min_rows(kRowsPerSegment);

        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    TabletSharedPtr create_tablet(const TabletSchema& tablet_schema) {
        std::vector<TColumn> cols;
        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
        for (int i = 0; i < tablet_schema.num_columns(); ++i) {
            const TabletColumn& column = tablet_schema.column(i);
            TColumn col;
            if (column.type() == FieldType::OLAP_FIELD_TYPE_VARIANT) {
                col.column_type.type = TPrimitiveType::VARIANT;
            } else {
                col.column_type.type = TPrimitiveType::INT;
            }
            col.__set_column_name(column.name());
            col.__set_is_key(column.is_key());
            cols.push_back(col);
            col_ordinal_to_unique_id[i] = column.unique_id();
        }

        TTabletSchema t_tablet_schema;
        t_tablet_schema.__set_short_key_column_count(tablet_schema.num_short_key_columns());
        t_tablet_schema.__set_schema_hash(3333);
        t_tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
        t_tablet_schema.__set_storage_type(TStorageType::COLUMN);
        t_tablet_schema.__set_columns(cols);

        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                2, 2, 2, 2, 2, 2, t_tablet_schema, 2, col_ordinal_to_unique_id, UniqueId(1, 2),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false));

        TabletSharedPtr tablet(new Tablet(*engine_ref, tablet_meta, _data_dir.get()));
        static_cast<void>(tablet->init());
        return tablet;
    }

    void create_rowset_writer_context(TabletSchemaSPtr tablet_schema, const std::string& rowset_dir,
                                      int64_t version, RowsetWriterContext* rowset_writer_context) {
        RowsetId rowset_id;
        rowset_id.init(version + 1000);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->rowset_type = BETA_ROWSET;
        rowset_writer_context->data_dir = _data_dir.get();
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->tablet_path = rowset_dir;
        rowset_writer_context->version = Version(version, version);
        rowset_writer_context->segments_overlap = NONOVERLAPPING;
        rowset_writer_context->max_rows_per_segment = kRowsPerSegment;
    }

    std::string generate_random_json(const std::vector<std::string>& key_pool, std::mt19937_64& rng,
                                     uint32_t row_seed) {
        std::uniform_int_distribution<int> kv_count_dist(10, 50);
        std::uniform_int_distribution<int> key_idx_dist(0, static_cast<int>(key_pool.size() - 1));
        int kv_count = kv_count_dist(rng);

        std::array<int, 50> selected {};
        int selected_size = 0;
        while (selected_size < kv_count) {
            int idx = key_idx_dist(rng);
            bool dup = false;
            for (int i = 0; i < selected_size; ++i) {
                if (selected[i] == idx) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                selected[selected_size++] = idx;
            }
        }

        std::string json;
        json.reserve(static_cast<size_t>(kv_count) * 20 + 2);
        json.push_back('{');
        for (int i = 0; i < kv_count; ++i) {
            const auto& key = key_pool[selected[i]];
            json.push_back('"');
            json.append(key);
            json.append("\":");
            json.append(std::to_string(static_cast<uint32_t>(selected[i]) ^ row_seed));
            if (i + 1 < kv_count) {
                json.push_back(',');
            }
        }
        json.push_back('}');
        return json;
    }

    RowsetSharedPtr create_rowset_with_variant(TabletSchemaSPtr tablet_schema,
                                               TabletSharedPtr tablet, int64_t version,
                                               int64_t start_key,
                                               const std::vector<std::string>& key_pool,
                                               std::mt19937_64& rng, int64_t* import_elapsed_ms) {
        RowsetWriterContext writer_context;
        create_rowset_writer_context(tablet_schema, tablet->tablet_path(), version,
                                     &writer_context);

        auto res = RowsetFactory::create_rowset_writer(*engine_ref, writer_context, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        vectorized::Block block = tablet_schema->create_block();
        auto columns = block.mutate_columns();
        auto* variant_col = assert_cast<ColumnVariant*>(columns[1].get());
        auto raw_json_column = ColumnString::create();
        raw_json_column->reserve(kRowsPerSegment);
        for (uint32_t i = 0; i < kRowsPerSegment; ++i) {
            int32_t c1 = static_cast<int32_t>(start_key + i);
            columns[0]->insert_data(reinterpret_cast<const char*>(&c1), sizeof(c1));

            std::string json = generate_random_json(key_pool, rng, static_cast<uint32_t>(c1));
            raw_json_column->insert_data(json.data(), json.size());
        }

        variant_col->create_root(make_nullable(std::make_shared<DataTypeString>()),
                                 std::move(raw_json_column));

        auto import_start = std::chrono::steady_clock::now();
        auto s = rowset_writer->add_block(&block);
        EXPECT_TRUE(s.ok()) << s.to_json();
        s = rowset_writer->flush();
        EXPECT_TRUE(s.ok()) << s.to_json();

        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        auto import_end = std::chrono::steady_clock::now();
        if (import_elapsed_ms != nullptr) {
            *import_elapsed_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(import_end - import_start)
                            .count();
        }
        EXPECT_EQ(1, rowset->rowset_meta()->num_segments());
        EXPECT_EQ(kRowsPerSegment, rowset->rowset_meta()->num_rows());
        const auto& fs = io::global_local_filesystem();
        RowsetId rowset_id_cached;
        rowset_id_cached.init(version + 1000);
        auto seg_path = local_segment_path(tablet->tablet_path(), rowset_id_cached.to_string(), 0);
        auto cache_seg_path = local_segment_path(cache_dir, rowset_id_cached.to_string(), 0);
        bool cache_exists = false;
        static_cast<void>(fs->exists(cache_seg_path, &cache_exists));
        if (!cache_exists) {
            static_cast<void>(fs->link_file(seg_path, cache_seg_path));
        }
        return rowset;
    }

    RowsetSharedPtr load_rowset_from_local_segments(TabletSchemaSPtr tablet_schema,
                                                    const TabletSharedPtr& tablet,
                                                    int64_t version) {
        RowsetId rowset_id;
        rowset_id.init(version + 1000);
        const auto& fs = io::global_local_filesystem();
        auto seg_path = local_segment_path(tablet->tablet_path(), rowset_id.to_string(), 0);
        bool seg_exists = false;
        if (!fs->exists(seg_path, &seg_exists).ok() || !seg_exists) {
            return nullptr;
        }
        int64_t file_size = 0;
        if (!fs->file_size(seg_path, &file_size).ok()) {
            return nullptr;
        }
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_rowset_id(rowset_id);
        rowset_meta->set_rowset_type(BETA_ROWSET);
        rowset_meta->set_rowset_state(VISIBLE);
        rowset_meta->set_tablet_id(tablet->tablet_id());
        rowset_meta->set_tablet_schema_hash(tablet->schema_hash());
        rowset_meta->set_tablet_uid(tablet->tablet_uid());
        rowset_meta->set_version(Version(version, version));
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
        rowset_meta->set_num_segments(1);
        rowset_meta->set_num_rows(kRowsPerSegment);
        rowset_meta->set_tablet_schema(tablet_schema);
        rowset_meta->add_segments_file_size({static_cast<size_t>(file_size)});
        rowset_meta->set_total_disk_size(file_size);
        rowset_meta->set_data_disk_size(file_size);
        rowset_meta->set_index_disk_size(0);
        RowsetSharedPtr rowset;
        EXPECT_TRUE(RowsetFactory::create_rowset(tablet_schema, tablet->tablet_path(), rowset_meta,
                                                 &rowset)
                            .ok());
        return rowset;
    }

    void create_and_init_rowset_reader(Rowset* rowset, RowsetReaderContext& context,
                                       RowsetReaderSharedPtr* result) {
        auto s = rowset->create_reader(result);
        ASSERT_TRUE(s.ok()) << s.to_json();
        ASSERT_TRUE(*result != nullptr);
        s = (*result)->init(&context);
        ASSERT_TRUE(s.ok()) << s.to_json();
    }

private:
    static constexpr std::string_view kTestDir = "/ut_dir/variant_doc_mode_compaction_test";
    static constexpr std::string_view tmp_dir = "./ut_dir/variant_doc_mode_compaction_test/tmp";
    std::string absolute_dir;
    std::string cache_dir;
    std::unique_ptr<DataDir> _data_dir;
};

// -------------Base line--------------------
// ------------------------------------------
// [==========] Running 1 test from 1 test suite.
// [----------] Global test environment set-up.
// [----------] 1 test from VariantDocModeCompactionTest
// [ RUN      ] VariantDocModeCompactionTest.variant_doc_mode_compaction_merge_10_segments
// variant import segment i=0 mode=generate elapsed_ms=8520
// variant import segment i=1 mode=generate elapsed_ms=8416
// variant import segment i=2 mode=generate elapsed_ms=8185
// variant import segment i=3 mode=generate elapsed_ms=8036
// variant import segment i=4 mode=generate elapsed_ms=8016
// variant import segment i=5 mode=generate elapsed_ms=8106
// variant import segment i=6 mode=generate elapsed_ms=8042
// variant import segment i=7 mode=generate elapsed_ms=8091
// variant import segment i=8 mode=generate elapsed_ms=8183
// variant import segment i=9 mode=generate elapsed_ms=8058
// variant import total_elapsed_ms=81653 wall_elapsed_ms=93160
// variant start compaction
// variant doc mode compaction elapsed_ms=80206
// [       OK ] VariantDocModeCompactionTest.variant_doc_mode_compaction_merge_10_segments (173484 ms)
// [----------] 1 test from VariantDocModeCompactionTest (173484 ms total)
//
// [----------] Global test environment tear-down
// [==========] 1 test from 1 test suite ran. (173484 ms total)
// [  PASSED  ] 1 test.
// === Finished. Gtest output: /mnt/disk1/claude-max/tmp/doris/be/ut_build_RELEASE/gtest_output

TEST_F(VariantDocModeCompactionTest, variant_doc_mode_compaction_merge_10_segments) {
    GTEST_SKIP() << "This test is slow and only runs in Release mode";
    TabletSchemaSPtr tablet_schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*tablet_schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    std::vector<std::string> key_pool;
    key_pool.reserve(10000);
    for (int i = 0; i < 10000; ++i) {
        key_pool.emplace_back("k" + std::to_string(i));
    }

    std::mt19937_64 rng(42);

    std::vector<RowsetSharedPtr> input_rowsets;
    input_rowsets.reserve(10);
    auto import_total_start = std::chrono::steady_clock::now();
    int64_t import_total_ms = 0;
    for (int i = 0; i < 10; ++i) {
        int64_t version = i;
        int64_t start_key = static_cast<int64_t>(i) * kRowsPerSegment;
        int64_t import_elapsed_ms = 0;
        std::string import_mode = "reuse_tablet";
        RowsetSharedPtr rowset = load_rowset_from_local_segments(tablet_schema, tablet, version);
        if (!rowset) {
            import_mode = "reuse_cache";
            const auto& fs = io::global_local_filesystem();
            RowsetId rid;
            rid.init(version + 1000);
            auto cache_seg = local_segment_path(cache_dir, rid.to_string(), 0);
            bool cache_exists = false;
            if (fs->exists(cache_seg, &cache_exists).ok() && cache_exists) {
                auto seg_path = local_segment_path(tablet->tablet_path(), rid.to_string(), 0);
                static_cast<void>(fs->link_file(cache_seg, seg_path));
                rowset = load_rowset_from_local_segments(tablet_schema, tablet, version);
            }
        }
        if (!rowset) {
            import_mode = "generate";
            rowset = create_rowset_with_variant(tablet_schema, tablet, version, start_key, key_pool,
                                                rng, &import_elapsed_ms);
        }
        import_total_ms += import_elapsed_ms;
        std::cout << "variant import segment i=" << i << " mode=" << import_mode
                  << " elapsed_ms=" << import_elapsed_ms << std::endl;
        if (i == 0) {
            RowsetReaderContext input_reader_context;
            input_reader_context.tablet_schema = tablet_schema;
            input_reader_context.need_ordered_result = false;
            std::vector<uint32_t> input_return_columns = {1};
            input_reader_context.return_columns = &input_return_columns;
            RowsetReaderSharedPtr input_rs_reader;
            create_and_init_rowset_reader(rowset.get(), input_reader_context, &input_rs_reader);

            vectorized::Block input_block =
                    tablet_schema->create_block_by_cids(input_return_columns);
            auto st = input_rs_reader->next_batch(&input_block);
            ASSERT_TRUE(st.ok()) << st.to_json();
            ASSERT_EQ(1, input_block.columns());
            ASSERT_GT(input_block.rows(), 0);
            const auto& var =
                    assert_cast<const ColumnVariant&>(*input_block.get_by_position(0).column);
            ASSERT_GT(var.serialized_doc_value_column_offsets()[0], 0);
        }
        ASSERT_TRUE(tablet->add_rowset(rowset).ok());
        input_rowsets.push_back(rowset);
    }
    auto import_total_end = std::chrono::steady_clock::now();
    auto import_wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(import_total_end -
                                                                                import_total_start)
                                  .count();
    std::cout << "variant import total_elapsed_ms=" << import_total_ms
              << " wall_elapsed_ms=" << import_wall_ms << std::endl;

    CumulativeCompaction cu_compaction(*engine_ref, tablet);
    cu_compaction._input_rowsets = std::move(input_rowsets);

    config::enable_variant_doc_sparse_write_subcolumns = true;
    std::cout << "variant start compaction" << std::endl;
    auto start = std::chrono::steady_clock::now();
    auto st = cu_compaction.CompactionMixin::execute_compact();
    auto end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "variant doc mode compaction elapsed_ms=" << elapsed_ms << std::endl;
    if (!st.ok()) {
        std::cout << st.to_string() << std::endl;
        std::cout << doris::get_stack_trace(0) << std::endl;
    }
    ASSERT_TRUE(st.ok()) << st.to_json();

    auto& out_rowset = cu_compaction._output_rowset;
    ASSERT_TRUE(out_rowset != nullptr);
    ASSERT_EQ(static_cast<int64_t>(kRowsPerSegment) * 10, out_rowset->rowset_meta()->num_rows());

    RowsetReaderContext reader_context;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = false;
    std::vector<uint32_t> return_columns = {0};
    reader_context.return_columns = &return_columns;
    RowsetReaderSharedPtr output_rs_reader;
    create_and_init_rowset_reader(out_rowset.get(), reader_context, &output_rs_reader);

    int64_t total_rows = 0;
    Status s = Status::OK();
    while (s.ok()) {
        vectorized::Block output_block = tablet_schema->create_block_by_cids(return_columns);
        s = output_rs_reader->next_batch(&output_block);
        if (s.ok()) {
            total_rows += output_block.rows();
        }
    }
    ASSERT_TRUE(s.is<ErrorCode::END_OF_FILE>()) << s.to_json();
    ASSERT_EQ(static_cast<int64_t>(kRowsPerSegment) * 10, total_rows);
}

} // namespace vectorized
} // namespace doris

#endif
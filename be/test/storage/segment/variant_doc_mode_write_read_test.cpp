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

// Tests for variant doc mode write/read/compaction covering all branches of
// build_subcolumn_write_plan and execute_doc_write_pipeline.
//
// ── Segment type definitions ──────────────────────────────────────────
//
//   Segment A — Pure doc_value, NO materialized subcolumns.
//               Produced when: num_rows < min_rows AND unique_paths >= max_count.
//               Footer: doc_value buckets > 0, subcolumn_count == 0.
//
//   Segment B — Downgraded to subcolumn-only, NO doc_value.
//               Produced when: unique_paths < max_count (regardless of num_rows).
//               Footer: doc_value buckets == 0, subcolumn_count > 0.
//
//   Segment C — Materialized subcolumns + doc_value.
//               Produced when: num_rows >= min_rows AND unique_paths >= max_count.
//               Footer: doc_value buckets > 0, subcolumn_count > 0.
//
// ── Decision tree (build_subcolumn_write_plan) ────────────────────────
//
//   num_rows < min_rows?
//   ├─ YES: paths < max_count? → YES: downgrade (Segment B) ... branch ①②
//   │                          → NO:  doc_value only (Seg A) ... branch ③
//   └─ NO:  materialize, then entries < max_count?
//           → YES: downgrade (Segment B) ... branch ④⑤
//           → NO:  subcolumn + doc_value (Segment C) ... branch ⑥⑦
//
//   Odd branches (①③⑤⑦) use sparse materialization, even (②④⑥) use dense.
//
// ── Parameters ────────────────────────────────────────────────────────
//
//   max_count  = variant_max_subcolumns_count (downgrade threshold)
//   min_rows   = variant_doc_materialization_min_rows
//   paths      = number of unique JSON paths in the data

#include <gen_cpp/Types_types.h>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "gtest/gtest.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/compaction/cumulative_compaction.h"
#include "storage/data_dir.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/variant/variant_statistics.h"
#include "testutil/variant_doc_mode_test_util.h"

using namespace doris;
using namespace doris::variant_doc_mode_test;

namespace doris {

class VariantDocModeWriteReadTest : public testing::Test {
public:
    void SetUp() override {
        char buffer[1024];
        EXPECT_NE(getcwd(buffer, sizeof(buffer)), nullptr);
        _current_dir = std::string(buffer);
        _absolute_dir = _current_dir + "/ut_dir/variant_doc_mode_test";
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        std::string tmp = "./ut_dir/tmp_doc_mode";
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(tmp, 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory("./ut_dir/tmp_doc_mode").ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

protected:
    void init_tablet(const TabletSchemaSPtr& schema, int64_t tablet_id) {
        _schema = schema;
        TabletMetaSharedPtr tablet_meta(new TabletMeta(schema));
        schema->set_external_segment_meta_used_default(true);
        tablet_meta->_tablet_id = tablet_id;
        _tablet = std::make_shared<Tablet>(*_engine_ref, tablet_meta, _data_dir.get());
        EXPECT_TRUE(_tablet->init().ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    }

    Status write_segment(const ColumnPtr& variant_col, size_t num_rows, int segment_id,
                         segment_v2::SegmentFooterPB* footer, std::string* file_path) {
        return write_variant_segment(_schema, variant_col, num_rows, _tablet->tablet_path(), "0",
                                     segment_id, footer, file_path);
    }

    TabletSchemaSPtr _schema;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    TabletSharedPtr _tablet;
    std::string _absolute_dir;
    std::string _current_dir;
};

// ============================================================
// Write tests: verify segment footer structure for all 7 branches
// (see decision tree above for branch numbering)
// ============================================================

// Branch ①: num_rows(50) < min_rows(1000), paths(5+3=8) < max_count(10), dense → Segment B
TEST_F(VariantDocModeWriteReadTest, write_below_min_rows_few_paths_dense) {
    auto saved = config::enable_variant_doc_sparse_write_subcolumns;
    config::enable_variant_doc_sparse_write_subcolumns = false;

    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/1000);
    init_tablet(schema, 1001);

    std::vector<std::string> jsons;
    for (int i = 0; i < 50; ++i) {
        jsons.push_back(generate_json(5, i * 100));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), 50, 0, &footer, &file_path).ok());

    assert_segment_type_B(footer);
    auto s = inspect_footer(footer);
    EXPECT_EQ(s.subcolumn_count, 8); // 5 flat + 3 obj children

    config::enable_variant_doc_sparse_write_subcolumns = saved;
}

// Branch ②: num_rows(50) < min_rows(1000), paths(5+3=8) < max_count(10), sparse → Segment B
TEST_F(VariantDocModeWriteReadTest, write_below_min_rows_few_paths_sparse) {
    auto saved = config::enable_variant_doc_sparse_write_subcolumns;
    config::enable_variant_doc_sparse_write_subcolumns = true;

    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/1000);
    init_tablet(schema, 1002);

    std::vector<std::string> jsons;
    for (int i = 0; i < 50; ++i) {
        jsons.push_back(generate_json(5, i * 100));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), 50, 0, &footer, &file_path).ok());

    assert_segment_type_B(footer);
    auto s = inspect_footer(footer);
    EXPECT_EQ(s.subcolumn_count, 8); // 5 flat + 3 obj children

    config::enable_variant_doc_sparse_write_subcolumns = saved;
}

// Branch ③: num_rows(50) < min_rows(1000), paths(15+3=18) >= max_count(10) → Segment A
TEST_F(VariantDocModeWriteReadTest, write_below_min_rows_many_paths) {
    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/1000);
    init_tablet(schema, 1003);

    std::vector<std::string> jsons;
    for (int i = 0; i < 50; ++i) {
        jsons.push_back(generate_json(15, i * 100));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), 50, 0, &footer, &file_path).ok());

    assert_segment_type_A(footer);
}

// Branch ④: num_rows(200) >= min_rows(0), paths(5+3=8) < max_count(10), dense → Segment B
TEST_F(VariantDocModeWriteReadTest, write_above_min_rows_few_paths_dense) {
    auto saved = config::enable_variant_doc_sparse_write_subcolumns;
    config::enable_variant_doc_sparse_write_subcolumns = false;

    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/0);
    init_tablet(schema, 1004);

    std::vector<std::string> jsons;
    for (int i = 0; i < 200; ++i) {
        jsons.push_back(generate_json(5, i * 100));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), 200, 0, &footer, &file_path).ok());

    assert_segment_type_B(footer);
    auto s = inspect_footer(footer);
    EXPECT_EQ(s.subcolumn_count, 8); // 5 flat + 3 obj children

    config::enable_variant_doc_sparse_write_subcolumns = saved;
}

// Branch ⑤: num_rows(200) >= min_rows(0), paths(5+3=8) < max_count(10), sparse → Segment B
TEST_F(VariantDocModeWriteReadTest, write_above_min_rows_few_paths_sparse) {
    auto saved = config::enable_variant_doc_sparse_write_subcolumns;
    config::enable_variant_doc_sparse_write_subcolumns = true;

    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/0);
    init_tablet(schema, 1005);

    std::vector<std::string> jsons;
    for (int i = 0; i < 200; ++i) {
        jsons.push_back(generate_json(5, i * 100));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), 200, 0, &footer, &file_path).ok());

    assert_segment_type_B(footer);
    auto s = inspect_footer(footer);
    EXPECT_EQ(s.subcolumn_count, 8); // 5 flat + 3 obj children

    config::enable_variant_doc_sparse_write_subcolumns = saved;
}

// Branch ⑥: num_rows(200) >= min_rows(0), paths(15+3=18) >= max_count(10), dense → Segment C
TEST_F(VariantDocModeWriteReadTest, write_above_min_rows_many_paths_dense) {
    auto saved = config::enable_variant_doc_sparse_write_subcolumns;
    config::enable_variant_doc_sparse_write_subcolumns = false;

    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/0);
    init_tablet(schema, 1006);

    std::vector<std::string> jsons;
    for (int i = 0; i < 200; ++i) {
        jsons.push_back(generate_json(15, i * 100));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), 200, 0, &footer, &file_path).ok());

    assert_segment_type_C(footer);
    auto s = inspect_footer(footer);
    EXPECT_EQ(s.subcolumn_count, 18); // 15 flat + 3 obj children

    config::enable_variant_doc_sparse_write_subcolumns = saved;
}

// Branch ⑦: num_rows(200) >= min_rows(0), paths(15+3=18) >= max_count(10), sparse → Segment C
TEST_F(VariantDocModeWriteReadTest, write_above_min_rows_many_paths_sparse) {
    auto saved = config::enable_variant_doc_sparse_write_subcolumns;
    config::enable_variant_doc_sparse_write_subcolumns = true;

    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/0);
    init_tablet(schema, 1007);

    std::vector<std::string> jsons;
    for (int i = 0; i < 200; ++i) {
        jsons.push_back(generate_json(15, i * 100));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), 200, 0, &footer, &file_path).ok());

    assert_segment_type_C(footer);
    auto s = inspect_footer(footer);
    EXPECT_EQ(s.subcolumn_count, 18); // 15 flat + 3 obj children

    config::enable_variant_doc_sparse_write_subcolumns = saved;
}

// ============================================================
// Multi-bucket write/read: doc_hash_shard_count > 1
// ============================================================

// Segment A with 2 buckets: num_rows(50) < min_rows(1000), paths(15+3=18) >= max_count(10)
TEST_F(VariantDocModeWriteReadTest, write_read_multi_bucket_segment_A) {
    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/1000,
                                         /*doc_hash_shard_count=*/2);
    init_tablet(schema, 5001);

    constexpr int kRows = 50;
    constexpr int kFlatKeys = 12;
    constexpr int kObjChildren = 3;
    std::vector<std::string> jsons;
    for (int i = 0; i < kRows; ++i) {
        jsons.push_back(generate_json(kFlatKeys, i * 100, kObjChildren));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), kRows, 0, &footer, &file_path).ok());

    // Segment A: doc_value only, no subcolumns
    assert_segment_type_A(footer);
    auto s = inspect_footer(footer);
    EXPECT_EQ(s.doc_value_bucket_count, 2) << "Should have 2 doc_value buckets";

    // SELECT *
    {
        std::vector<std::string> rows;
        ASSERT_TRUE(read_all_rows(footer, file_path, schema, &rows).ok());
        EXPECT_EQ(rows.size(), kRows);
        EXPECT_NE(rows[0].find("\"k0\""), std::string::npos);
        EXPECT_NE(rows[0].find("\"obj\""), std::string::npos);
    }

    // SELECT v['k0']
    {
        std::vector<std::string> values;
        ASSERT_TRUE(read_subcolumn(footer, file_path, schema, "k0", &values).ok());
        EXPECT_EQ(values.size(), kRows);
    }

    // SELECT v['obj']
    {
        std::vector<std::string> values;
        ASSERT_TRUE(read_subcolumn(footer, file_path, schema, "obj", &values).ok());
        EXPECT_EQ(values.size(), kRows);
        EXPECT_NE(values[0].find("f0"), std::string::npos);
    }
}

// Segment C with 3 buckets: num_rows(200) >= min_rows(0), paths(15+3=18) >= max_count(10)
TEST_F(VariantDocModeWriteReadTest, write_read_multi_bucket_segment_C) {
    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/0,
                                         /*doc_hash_shard_count=*/3);
    init_tablet(schema, 5002);

    constexpr int kRows = 200;
    constexpr int kFlatKeys = 12;
    constexpr int kObjChildren = 3;
    std::vector<std::string> jsons;
    for (int i = 0; i < kRows; ++i) {
        jsons.push_back(generate_json(kFlatKeys, i * 100, kObjChildren));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), kRows, 0, &footer, &file_path).ok());

    // Segment C: subcolumns + doc_value
    assert_segment_type_C(footer);
    auto s = inspect_footer(footer);
    EXPECT_EQ(s.doc_value_bucket_count, 3) << "Should have 3 doc_value buckets";
    EXPECT_EQ(s.subcolumn_count, 15); // 12 flat + 3 obj children

    // SELECT *
    {
        std::vector<std::string> rows;
        ASSERT_TRUE(read_all_rows(footer, file_path, schema, &rows).ok());
        EXPECT_EQ(rows.size(), kRows);
        EXPECT_NE(rows[0].find("\"k0\""), std::string::npos);
        EXPECT_NE(rows[0].find("\"obj\""), std::string::npos);
    }

    // SELECT v['k0']
    {
        std::vector<std::string> values;
        ASSERT_TRUE(read_subcolumn(footer, file_path, schema, "k0", &values).ok());
        EXPECT_EQ(values.size(), kRows);
    }

    // SELECT v['obj']
    {
        std::vector<std::string> values;
        ASSERT_TRUE(read_subcolumn(footer, file_path, schema, "obj", &values).ok());
        EXPECT_EQ(values.size(), kRows);
        EXPECT_NE(values[0].find("f0"), std::string::npos);
    }
}

// ============================================================
// Read tests: one test per segment type, each covering four query patterns:
//   1. SELECT *           — read all rows (RootColumnIterator)
//   2. SELECT v['k0']     — leaf scalar key
//   3. SELECT v['obj']    — intermediate node (nested object)
//   4. SELECT v['nonexistent'] — missing key → null
// ============================================================

// Segment A: pure doc_value, no materialized subcolumns.
TEST_F(VariantDocModeWriteReadTest, read_segment_A) {
    // paths = 12 flat keys + 3 obj children = 15 total >= max_count(10)
    // num_rows(50) < min_rows(1000) → Segment A
    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/10,
                                         /*doc_materialization_min_rows=*/1000);
    init_tablet(schema, 2001);

    constexpr int kRows = 50;
    constexpr int kFlatKeys = 12;
    constexpr int kObjChildren = 3;
    std::vector<std::string> jsons;
    for (int i = 0; i < kRows; ++i) {
        jsons.push_back(generate_json(kFlatKeys, i * 100, kObjChildren));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), kRows, 0, &footer, &file_path).ok());
    assert_segment_type_A(footer);

    // SELECT *
    {
        std::vector<std::string> rows;
        ASSERT_TRUE(read_all_rows(footer, file_path, schema, &rows).ok());
        EXPECT_EQ(rows.size(), kRows);
        EXPECT_NE(rows[0].find("\"k0\""), std::string::npos);
        EXPECT_NE(rows[0].find("\"obj\""), std::string::npos);
    }

    // SELECT v['k0'] (leaf key from doc_value)
    {
        std::vector<std::string> values;
        ASSERT_TRUE(read_subcolumn(footer, file_path, schema, "k0", &values).ok());
        EXPECT_EQ(values.size(), kRows);
    }

    // SELECT v['obj'] (nested object from doc_value)
    {
        std::vector<std::string> values;
        ASSERT_TRUE(read_subcolumn(footer, file_path, schema, "obj", &values).ok());
        EXPECT_EQ(values.size(), kRows);
        EXPECT_NE(values[0].find("f0"), std::string::npos);
        EXPECT_NE(values[0].find("f" + std::to_string(kObjChildren - 1)), std::string::npos);
    }

    // SELECT v['nonexistent']
    {
        std::vector<std::string> values;
        auto st = read_subcolumn(footer, file_path, schema, "nonexistent", &values);
        if (st.ok()) {
            EXPECT_EQ(values.size(), kRows);
            for (const auto& v : values) {
                EXPECT_TRUE(v.empty() || v == "NULL" || v == "null") << "expected null, got: " << v;
            }
        }
    }
}

// ============================================================
// Segment B: downgraded to subcolumn-only, no doc_value.
// ============================================================

TEST_F(VariantDocModeWriteReadTest, read_segment_B) {
    // 5 flat keys + 3 obj children = 8 paths < max_count(20) → downgrade
    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/20,
                                         /*doc_materialization_min_rows=*/0);
    init_tablet(schema, 3001);

    constexpr int kRows = 200;
    constexpr int kFlatKeys = 5;
    constexpr int kObjChildren = 3;
    std::vector<std::string> jsons;
    for (int i = 0; i < kRows; ++i) {
        jsons.push_back(generate_json(kFlatKeys, i * 100, kObjChildren));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), kRows, 0, &footer, &file_path).ok());
    assert_segment_type_B(footer);

    // SELECT *
    {
        std::vector<std::string> rows;
        try {
            ASSERT_TRUE(read_all_rows(footer, file_path, schema, &rows).ok());
        } catch (const std::exception& e) {
            FAIL() << "read_all_rows threw: " << e.what();
        }
        EXPECT_EQ(rows.size(), kRows);
        EXPECT_NE(rows[0].find("\"k0\""), std::string::npos);
        EXPECT_NE(rows[0].find("\"obj\""), std::string::npos);
    }

    // SELECT v['k0'] (hits materialized leaf subcolumn)
    {
        std::vector<std::string> values;
        auto st = read_subcolumn(footer, file_path, schema, "k0", &values);
        ASSERT_TRUE(st.ok()) << "read_subcolumn(k0) failed: " << st.to_json();
        EXPECT_EQ(values.size(), kRows);
    }

    // SELECT v['obj'] (hierarchical: merge child subcolumns obj.f0..f2)
    {
        std::vector<std::string> values;
        auto st = read_subcolumn(footer, file_path, schema, "obj", &values);
        ASSERT_TRUE(st.ok()) << "read_subcolumn(obj) failed: " << st.to_json();
        EXPECT_EQ(values.size(), kRows);
        EXPECT_NE(values[0].find("f0"), std::string::npos);
        EXPECT_NE(values[0].find("f" + std::to_string(kObjChildren - 1)), std::string::npos);
    }

    // SELECT v['nonexistent']
    {
        std::vector<std::string> values;
        auto st = read_subcolumn(footer, file_path, schema, "nonexistent", &values);
        // nonexistent path may return error — this is expected reader behavior
        if (st.ok()) {
            EXPECT_EQ(values.size(), kRows);
            for (const auto& v : values) {
                EXPECT_TRUE(v.empty() || v == "NULL" || v == "null") << "expected null, got: " << v;
            }
        }
    }
}

// ============================================================
// Segment C: materialized subcolumns + doc_value.
// ============================================================

TEST_F(VariantDocModeWriteReadTest, read_segment_C) {
    // 5 flat keys + 3 obj children = 8 paths >= max_count(3) → Segment C
    auto schema = create_doc_mode_schema(/*max_subcolumns_count=*/3,
                                         /*doc_materialization_min_rows=*/0);
    init_tablet(schema, 4001);

    constexpr int kRows = 200;
    constexpr int kFlatKeys = 5;
    constexpr int kObjChildren = 3;
    std::vector<std::string> jsons;
    for (int i = 0; i < kRows; ++i) {
        jsons.push_back(generate_json(kFlatKeys, i * 100, kObjChildren));
    }
    auto variant = create_doc_value_variant(jsons);

    segment_v2::SegmentFooterPB footer;
    std::string file_path;
    ASSERT_TRUE(write_segment(variant->get_ptr(), kRows, 0, &footer, &file_path).ok());
    assert_segment_type_C(footer);

    // SELECT *
    {
        std::vector<std::string> rows;
        try {
            ASSERT_TRUE(read_all_rows(footer, file_path, schema, &rows).ok());
        } catch (const std::exception& e) {
            FAIL() << "read_all_rows threw: " << e.what();
        }
        EXPECT_EQ(rows.size(), kRows);
        EXPECT_NE(rows[0].find("\"k0\""), std::string::npos);
        EXPECT_NE(rows[0].find("\"obj\""), std::string::npos);
    }

    // SELECT v['k0'] (materialized subcolumn leaf)
    {
        std::vector<std::string> values;
        try {
            ASSERT_TRUE(read_subcolumn(footer, file_path, schema, "k0", &values).ok());
        } catch (const std::exception& e) {
            FAIL() << "read_subcolumn(k0) threw: " << e.what();
        }
        EXPECT_EQ(values.size(), kRows);
    }

    // SELECT v['obj'] (hierarchical: merge materialized child subcolumns)
    {
        std::vector<std::string> values;
        try {
            ASSERT_TRUE(read_subcolumn(footer, file_path, schema, "obj", &values).ok());
        } catch (const std::exception& e) {
            FAIL() << "read_subcolumn(obj) threw: " << e.what();
        }
        EXPECT_EQ(values.size(), kRows);
        EXPECT_NE(values[0].find("f0"), std::string::npos);
        EXPECT_NE(values[0].find("f" + std::to_string(kObjChildren - 1)), std::string::npos);
    }

    // SELECT v['nonexistent']
    {
        std::vector<std::string> values;
        auto st = read_subcolumn(footer, file_path, schema, "nonexistent", &values);
        if (st.ok()) {
            EXPECT_EQ(values.size(), kRows);
            for (const auto& v : values) {
                EXPECT_TRUE(v.empty() || v == "NULL" || v == "null") << "expected null, got: " << v;
            }
        }
    }
}

// ============================================================
// Mixed Compaction Tests: all A×B×C segment type combinations
// ============================================================
//
// Schema: max_subcolumns_count=10, doc_materialization_min_rows=100
//   Type A (pure doc_value):        50 rows, 15 paths → rows<min, paths>=max
//   Type B (subcolumn-only):        50 rows,  5 paths → paths<max → downgrade
//   Type C (subcolumn + doc_value): 200 rows, 15 paths → rows>=min, paths>=max
//
// Expected compaction outputs:
//   A+A(100r,15p)→C  A+B(100r,15p)→C  A+C(250r,15p)→C
//   B+B(100r,5p)→B   B+C(250r,15p)→C  C+C(400r,15p)→C
//   A+B+C(300r,15p)→C

static StorageEngine* g_compact_engine = nullptr;

class VariantDocModeMixedCompactionTest : public testing::Test {
public:
    static constexpr int kA_Rows = 50, kA_Keys = 12, kA_Obj = 3;  // 15 paths
    static constexpr int kB_Rows = 50, kB_Keys = 5, kB_Obj = 0;   // 5 paths
    static constexpr int kC_Rows = 200, kC_Keys = 12, kC_Obj = 3; // 15 paths

    void SetUp() override {
        // Guard against config pollution from other tests (e.g.
        // SegmentsKeyBoundsTruncationTest sets min_segment_size=1 and never
        // restores it), which would enable ordered compaction for our tiny
        // segments and cause build_basic_info to be called twice.
        config::enable_ordered_data_compaction = false;

        char buf[1024];
        EXPECT_NE(getcwd(buf, sizeof(buf)), nullptr);
        _cur_dir = std::string(buf);
        _abs_dir = _cur_dir + "/ut_dir/variant_doc_compact";
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_abs_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_abs_dir).ok());
        EXPECT_TRUE(
                io::global_local_filesystem()->create_directory(_abs_dir + "/tablet_path").ok());

        std::string tmp = _cur_dir + "/ut_dir/tmp_doc_compact";
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(tmp, 1024000000);
        auto tmp_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_dirs));

        EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        g_compact_engine = engine.get();
        _data_dir = std::make_unique<DataDir>(*g_compact_engine, _abs_dir);
        EXPECT_TRUE(_data_dir->init(true).ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_abs_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()
                            ->delete_directory(_cur_dir + "/ut_dir/tmp_doc_compact")
                            .ok());
        g_compact_engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

protected:
    TabletSchemaSPtr make_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(KeysType::DUP_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_next_column_unique_id(3);

        auto* c1 = pb.add_column();
        c1->set_unique_id(1);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(true);
        c1->set_length(4);
        c1->set_index_length(4);
        c1->set_is_nullable(false);
        c1->set_is_bf_column(false);

        auto* v = pb.add_column();
        v->set_unique_id(2);
        v->set_name("v");
        v->set_type("VARIANT");
        v->set_is_key(false);
        v->set_is_nullable(false);
        v->set_variant_max_subcolumns_count(10);
        v->set_variant_max_sparse_column_statistics_size(10000);
        v->set_variant_sparse_hash_shard_count(0);
        v->set_variant_enable_doc_mode(true);
        v->set_variant_doc_materialization_min_rows(100);
        v->set_variant_doc_hash_shard_count(1);
        v->set_variant_enable_nested_group(false);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    TabletSharedPtr make_tablet(const TabletSchemaSPtr& schema) {
        std::vector<TColumn> cols;
        std::unordered_map<uint32_t, uint32_t> uid_map;
        for (int i = 0; i < schema->num_columns(); ++i) {
            const auto& column = schema->column(i);
            TColumn tc;
            tc.column_type.type = (column.type() == FieldType::OLAP_FIELD_TYPE_VARIANT)
                                          ? TPrimitiveType::VARIANT
                                          : TPrimitiveType::INT;
            tc.__set_column_name(column.name());
            tc.__set_is_key(column.is_key());
            cols.push_back(tc);
            uid_map[i] = column.unique_id();
        }
        TTabletSchema ts;
        ts.__set_short_key_column_count(schema->num_short_key_columns());
        ts.__set_schema_hash(3333);
        ts.__set_keys_type(TKeysType::DUP_KEYS);
        ts.__set_storage_type(TStorageType::COLUMN);
        ts.__set_columns(cols);

        auto meta = std::make_shared<TabletMeta>(2, 2, 2, 2, 2, 2, ts, 2, uid_map, UniqueId(1, 2),
                                                 TTabletType::TABLET_TYPE_DISK,
                                                 TCompressionType::LZ4F, 0, false);
        auto tablet = std::make_shared<Tablet>(*g_compact_engine, meta, _data_dir.get());
        static_cast<void>(tablet->init());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        return tablet;
    }

    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
                                 int64_t version, int num_rows, int flat_keys, int obj_children) {
        RowsetWriterContext ctx;
        RowsetId rid;
        rid.init(version + 1000);
        ctx.rowset_id = rid;
        ctx.rowset_type = BETA_ROWSET;
        ctx.data_dir = _data_dir.get();
        ctx.rowset_state = VISIBLE;
        ctx.tablet_schema = schema;
        ctx.tablet_path = tablet->tablet_path();
        ctx.version = Version(version, version);
        ctx.segments_overlap = NONOVERLAPPING;
        ctx.max_rows_per_segment = 10000;
        ctx.write_type = DataWriteType::TYPE_COMPACTION;

        auto res = RowsetFactory::create_rowset_writer(*g_compact_engine, ctx, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();

        Block block = schema->create_block();
        auto columns = block.mutate_columns();
        auto raw_json = ColumnString::create();
        for (int i = 0; i < num_rows; ++i) {
            int32_t c1 = static_cast<int32_t>(version * 10000 + i);
            columns[0]->insert_data(reinterpret_cast<const char*>(&c1), sizeof(c1));
            auto json = generate_json(flat_keys, c1 * 100, obj_children);
            raw_json->insert_data(json.data(), json.size());
        }

        auto* vc = assert_cast<ColumnVariant*>(columns[1].get());
        ParseConfig cfg;
        cfg.deprecated_enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        variant_util::parse_json_to_variant(*vc, *raw_json, cfg);

        Block key_block, val_block;
        key_block.insert(block.get_by_position(0));
        val_block.insert(block.get_by_position(1));
        std::vector<uint32_t> key_ids = {0}, val_ids = {1};

        auto s = writer->add_columns(&key_block, key_ids, true, num_rows, false);
        EXPECT_TRUE(s.ok()) << s.to_json();
        s = writer->flush_columns(true);
        EXPECT_TRUE(s.ok()) << s.to_json();
        s = writer->add_columns(&val_block, val_ids, false, num_rows, false);
        EXPECT_TRUE(s.ok()) << s.to_json();
        s = writer->flush_columns(false);
        EXPECT_TRUE(s.ok()) << s.to_json();
        s = writer->final_flush();
        EXPECT_TRUE(s.ok()) << s.to_json();

        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), writer->build(rowset));
        EXPECT_EQ(1, rowset->rowset_meta()->num_segments());
        EXPECT_EQ(num_rows, rowset->rowset_meta()->num_rows());
        return rowset;
    }

    RowsetSharedPtr write_A(const TabletSchemaSPtr& s, const TabletSharedPtr& t, int64_t v) {
        return write_rowset(s, t, v, kA_Rows, kA_Keys, kA_Obj);
    }
    RowsetSharedPtr write_B(const TabletSchemaSPtr& s, const TabletSharedPtr& t, int64_t v) {
        return write_rowset(s, t, v, kB_Rows, kB_Keys, kB_Obj);
    }
    RowsetSharedPtr write_C(const TabletSchemaSPtr& s, const TabletSharedPtr& t, int64_t v) {
        return write_rowset(s, t, v, kC_Rows, kC_Keys, kC_Obj);
    }

    RowsetSharedPtr do_compaction(const TabletSharedPtr& tablet,
                                  std::vector<RowsetSharedPtr> inputs) {
        CumulativeCompaction cu(*g_compact_engine, tablet);
        cu._input_rowsets = std::move(inputs);
        auto st = cu.CompactionMixin::execute_compact();
        EXPECT_TRUE(st.ok()) << st.to_json();
        return cu._output_rowset;
    }

    bool check_doc_value(const RowsetSharedPtr& rs) {
        SegmentCacheHandle cache;
        auto st = SegmentLoader::instance()->load_segments(std::static_pointer_cast<BetaRowset>(rs),
                                                           &cache);
        if (!st.ok()) return false;
        for (const auto& seg : cache.get_segments()) {
            std::shared_ptr<segment_v2::ColumnReader> reader;
            OlapReaderStatistics stats;
            st = seg->get_column_reader(2, &reader, &stats);
            if (!st.ok() || !reader) continue;
            auto* vr = dynamic_cast<segment_v2::VariantColumnReader*>(reader.get());
            if (!vr) continue;
            st = vr->load_external_meta_once();
            if (!st.ok()) continue;
            return vr->get_stats()->has_doc_value_column_non_null_size();
        }
        return false;
    }

    std::vector<std::string> read_json(const RowsetSharedPtr& rs, const TabletSchemaSPtr& schema) {
        RowsetReaderContext rctx;
        rctx.tablet_schema = schema;
        rctx.need_ordered_result = false;
        std::vector<uint32_t> cids = {0, 1};
        rctx.return_columns = &cids;

        RowsetReaderSharedPtr reader;
        EXPECT_TRUE(rs->create_reader(&reader).ok());
        EXPECT_TRUE(reader->init(&rctx).ok());

        auto dt = std::make_shared<DataTypeVariant>();
        std::vector<std::string> out;
        Status st = Status::OK();
        while (st.ok()) {
            Block blk = schema->create_block_by_cids(cids);
            st = reader->next_batch(&blk);
            if (st.ok()) {
                const auto& vcol = *blk.get_by_position(1).column;
                for (size_t i = 0; i < blk.rows(); ++i) {
                    out.push_back(dt->to_string(vcol, i));
                }
            }
        }
        EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st.to_json();
        return out;
    }

    struct Spec {
        int64_t ver;
        int rows;
        int keys;
        int obj;
    };

    void verify(const RowsetSharedPtr& output, const TabletSchemaSPtr& schema, bool expect_dv,
                const std::vector<Spec>& specs) {
        int total = 0;
        for (auto& sp : specs) total += sp.rows;

        // 1. Row count
        EXPECT_EQ(output->rowset_meta()->num_rows(), total);

        // 2. Segment type
        EXPECT_EQ(check_doc_value(output), expect_dv) << "has_doc_value: expected=" << expect_dv;

        // 3. Read all JSON (SELECT *)
        auto jsons = read_json(output, schema);
        ASSERT_EQ(static_cast<int>(jsons.size()), total);

        // Helper: check key:value as complete token (not substring of larger number)
        auto json_has = [](const std::string& json, const std::string& key, int64_t val) {
            std::string pat = "\"" + key + "\":" + std::to_string(val);
            auto pos = json.find(pat);
            if (pos == std::string::npos) return false;
            size_t end = pos + pat.size();
            return end >= json.size() || json[end] == ',' || json[end] == '}' || json[end] == ' ';
        };

        // SELECT * — every row should have k0
        for (const auto& j : jsons) {
            EXPECT_TRUE(j.find("\"k0\"") != std::string::npos)
                    << "Missing k0: " << j.substr(0, 200);
        }

        // Per-version verification: SELECT v['key'], SELECT v['obj']
        for (auto& sp : specs) {
            int32_t c1_first = static_cast<int32_t>(sp.ver * 10000);
            int64_t k0_first = static_cast<int64_t>(c1_first) * 100;

            // Find and verify first row of this version
            bool found = false;
            for (const auto& j : jsons) {
                if (!json_has(j, "k0", k0_first)) continue;
                found = true;

                // SELECT v['obj'] — nested object
                if (sp.obj > 0) {
                    EXPECT_TRUE(j.find("\"obj\"") != std::string::npos)
                            << "v=" << sp.ver << " row missing obj: " << j.substr(0, 200);
                }

                // SELECT v['k{last}'] — last flat key
                if (sp.keys > 1) {
                    int last = sp.keys - 1;
                    EXPECT_TRUE(json_has(j, "k" + std::to_string(last), k0_first + last))
                            << "v=" << sp.ver << " missing k" << last << ": " << j.substr(0, 200);
                }
                break;
            }
            EXPECT_TRUE(found) << "First row of v=" << sp.ver << " not found (k0=" << k0_first
                               << ")";

            // Verify last row of this version
            int32_t c1_last = static_cast<int32_t>(sp.ver * 10000 + sp.rows - 1);
            int64_t k0_last = static_cast<int64_t>(c1_last) * 100;
            bool found_last = std::any_of(jsons.begin(), jsons.end(), [&](const std::string& j) {
                return json_has(j, "k0", k0_last);
            });
            EXPECT_TRUE(found_last)
                    << "Last row of v=" << sp.ver << " not found (k0=" << k0_last << ")";
        }

        // SELECT v['nonexistent'] — no row should have this key
        for (const auto& j : jsons) {
            EXPECT_TRUE(j.find("\"nonexistent\"") == std::string::npos);
        }
    }

    std::unique_ptr<DataDir> _data_dir;
    std::string _abs_dir;
    std::string _cur_dir;
};

// ------- A+A → C (100 rows, 15 paths, has doc_value) -------
TEST_F(VariantDocModeMixedCompactionTest, compact_A_A) {
    auto schema = make_schema();
    auto tablet = make_tablet(schema);
    auto r1 = write_A(schema, tablet, 2);
    auto r2 = write_A(schema, tablet, 3);
    ASSERT_TRUE(check_doc_value(r1)) << "Input r1 should be type A";
    ASSERT_TRUE(check_doc_value(r2)) << "Input r2 should be type A";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kA_Rows, kA_Keys, kA_Obj}, {3, kA_Rows, kA_Keys, kA_Obj}});
}

// ------- A+B → C (100 rows, 15 paths, has doc_value) -------
TEST_F(VariantDocModeMixedCompactionTest, compact_A_B) {
    auto schema = make_schema();
    auto tablet = make_tablet(schema);
    auto r1 = write_A(schema, tablet, 2);
    auto r2 = write_B(schema, tablet, 3);
    ASSERT_TRUE(check_doc_value(r1)) << "Input r1 should be type A";
    ASSERT_FALSE(check_doc_value(r2)) << "Input r2 should be type B";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kA_Rows, kA_Keys, kA_Obj}, {3, kB_Rows, kB_Keys, kB_Obj}});
}

// ------- A+C → C (250 rows, 15 paths, has doc_value) -------
TEST_F(VariantDocModeMixedCompactionTest, compact_A_C) {
    auto schema = make_schema();
    auto tablet = make_tablet(schema);
    auto r1 = write_A(schema, tablet, 2);
    auto r2 = write_C(schema, tablet, 3);
    ASSERT_TRUE(check_doc_value(r1)) << "Input r1 should be type A";
    ASSERT_TRUE(check_doc_value(r2)) << "Input r2 should be type C";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kA_Rows, kA_Keys, kA_Obj}, {3, kC_Rows, kC_Keys, kC_Obj}});
}

// ------- B+B → B (100 rows, 5 paths, NO doc_value) -------
TEST_F(VariantDocModeMixedCompactionTest, compact_B_B) {
    auto schema = make_schema();
    auto tablet = make_tablet(schema);
    auto r1 = write_B(schema, tablet, 2);
    auto r2 = write_B(schema, tablet, 3);
    ASSERT_FALSE(check_doc_value(r1)) << "Input r1 should be type B";
    ASSERT_FALSE(check_doc_value(r2)) << "Input r2 should be type B";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/false,
           {{2, kB_Rows, kB_Keys, kB_Obj}, {3, kB_Rows, kB_Keys, kB_Obj}});
}

// ------- B+C → C (250 rows, 15 paths, has doc_value) -------
TEST_F(VariantDocModeMixedCompactionTest, compact_B_C) {
    auto schema = make_schema();
    auto tablet = make_tablet(schema);
    auto r1 = write_B(schema, tablet, 2);
    auto r2 = write_C(schema, tablet, 3);
    ASSERT_FALSE(check_doc_value(r1)) << "Input r1 should be type B";
    ASSERT_TRUE(check_doc_value(r2)) << "Input r2 should be type C";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kB_Rows, kB_Keys, kB_Obj}, {3, kC_Rows, kC_Keys, kC_Obj}});
}

// ------- C+C → C (400 rows, 15 paths, has doc_value) -------
TEST_F(VariantDocModeMixedCompactionTest, compact_C_C) {
    auto schema = make_schema();
    auto tablet = make_tablet(schema);
    auto r1 = write_C(schema, tablet, 2);
    auto r2 = write_C(schema, tablet, 3);
    ASSERT_TRUE(check_doc_value(r1)) << "Input r1 should be type C";
    ASSERT_TRUE(check_doc_value(r2)) << "Input r2 should be type C";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kC_Rows, kC_Keys, kC_Obj}, {3, kC_Rows, kC_Keys, kC_Obj}});
}

// ------- A+B+C → C (300 rows, 15 paths, has doc_value) -------
TEST_F(VariantDocModeMixedCompactionTest, compact_A_B_C) {
    auto schema = make_schema();
    auto tablet = make_tablet(schema);
    auto r1 = write_A(schema, tablet, 2);
    auto r2 = write_B(schema, tablet, 3);
    auto r3 = write_C(schema, tablet, 4);
    ASSERT_TRUE(check_doc_value(r1)) << "Input r1 should be type A";
    ASSERT_FALSE(check_doc_value(r2)) << "Input r2 should be type B";
    ASSERT_TRUE(check_doc_value(r3)) << "Input r3 should be type C";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    ASSERT_TRUE(tablet->add_rowset(r3).ok());
    auto out = do_compaction(tablet, {r1, r2, r3});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kA_Rows, kA_Keys, kA_Obj},
            {3, kB_Rows, kB_Keys, kB_Obj},
            {4, kC_Rows, kC_Keys, kC_Obj}});
}

// ============================================================
// Index + field_pattern tests
// ============================================================

TabletSchemaSPtr make_schema_with_index(const std::string& field_pattern = "") {
    TabletSchemaPB pb;
    pb.set_keys_type(KeysType::DUP_KEYS);
    pb.set_num_short_key_columns(1);
    pb.set_num_rows_per_row_block(1024);
    pb.set_next_column_unique_id(3);

    auto* c1 = pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(true);
    c1->set_length(4);
    c1->set_index_length(4);
    c1->set_is_nullable(false);

    auto* v = pb.add_column();
    v->set_unique_id(2);
    v->set_name("v");
    v->set_type("VARIANT");
    v->set_is_key(false);
    v->set_is_nullable(false);
    v->set_variant_max_subcolumns_count(10);
    v->set_variant_max_sparse_column_statistics_size(10000);
    v->set_variant_sparse_hash_shard_count(0);
    v->set_variant_enable_doc_mode(true);
    v->set_variant_doc_materialization_min_rows(100);
    v->set_variant_doc_hash_shard_count(1);
    v->set_variant_enable_nested_group(false);

    if (!field_pattern.empty()) {
        // field_pattern requires typed sub_columns on the variant column.
        // Each sub_column defines the glob pattern (as name) and storage type.
        auto* typed_child = v->add_children_columns();
        typed_child->set_unique_id(-1);
        typed_child->set_name(field_pattern);
        typed_child->set_type("BIGINT");
        typed_child->set_is_key(false);
        typed_child->set_is_nullable(true);
        typed_child->set_length(8);
        typed_child->set_pattern_type(PatternTypePB::MATCH_NAME_GLOB);
    }

    pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

    auto* idx = pb.add_index();
    idx->set_index_id(10000);
    idx->set_index_name("idx_v");
    idx->set_index_type(IndexType::INVERTED);
    idx->add_col_unique_id(2);
    if (!field_pattern.empty()) {
        (*idx->mutable_properties())["field_pattern"] = field_pattern;
    }

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(pb);
    return schema;
}

int count_indexed_subcolumns(const RowsetSharedPtr& rs) {
    int total = 0;
    auto storage_format = rs->tablet_schema()->get_inverted_index_storage_format();
    for (int seg = 0; seg < rs->num_segments(); ++seg) {
        const auto& seg_path = rs->segment_path(seg);
        if (!seg_path.has_value()) continue;
        std::string idx_path_prefix(
                InvertedIndexDescriptor::get_index_file_path_prefix(seg_path.value()));
        auto idx_reader = std::make_shared<IndexFileReader>(rs->rowset_meta()->fs(),
                                                            idx_path_prefix, storage_format);
        auto st = idx_reader->init();
        if (!st.ok()) continue;
        auto dirs = idx_reader->get_all_directories();
        if (dirs.has_value()) {
            total += static_cast<int>(dirs.value().size());
        }
    }
    return total;
}

// ------- B+B with inverted index (no field_pattern) -------
// B has 5 flat keys (k0-k4), no obj → 5 subcolumns, all inherit index
TEST_F(VariantDocModeMixedCompactionTest, compact_B_B_with_inverted_index) {
    auto schema = make_schema_with_index();
    auto tablet = make_tablet(schema);
    auto r1 = write_B(schema, tablet, 2);
    auto r2 = write_B(schema, tablet, 3);
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/false, {{2, kB_Rows, kB_Keys, 0}, {3, kB_Rows, kB_Keys, 0}});
    // B has 5 subcolumns (k0-k4), all should have indexes
    EXPECT_EQ(count_indexed_subcolumns(out), kB_Keys)
            << "B+B: all " << kB_Keys << " subcolumns should have indexes";
}

// ------- B+C with inverted index -------
// B: 5 keys, C: 12 keys + 3 obj children → merged 15 unique paths, all indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_B_C_with_inverted_index) {
    auto schema = make_schema_with_index();
    auto tablet = make_tablet(schema);
    auto r1 = write_B(schema, tablet, 2);
    auto r2 = write_C(schema, tablet, 3);
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kB_Rows, kB_Keys, 0}, {3, kC_Rows, kC_Keys, kC_Obj}});
    // Merged: k0-k11 (12) + obj.f0-f2 (3) = 15, all indexed
    EXPECT_EQ(count_indexed_subcolumns(out), kC_Keys + kC_Obj)
            << "B+C: all " << (kC_Keys + kC_Obj) << " subcolumns should have indexes";
}

// ------- A+B with inverted index (A has no subcolumns, no index) -------
// A: 12 keys + 3 obj (doc_value only, 0 indexed), B: 5 keys (5 indexed)
// After compaction: 15 subcolumns, all indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_A_B_with_inverted_index) {
    auto schema = make_schema_with_index();
    auto tablet = make_tablet(schema);
    auto r1 = write_A(schema, tablet, 2);
    auto r2 = write_B(schema, tablet, 3);
    EXPECT_EQ(count_indexed_subcolumns(r1), 0) << "Segment A should have no indexed subcolumns";
    EXPECT_EQ(count_indexed_subcolumns(r2), kB_Keys)
            << "Segment B should have " << kB_Keys << " indexed subcolumns";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kA_Rows, kA_Keys, kA_Obj}, {3, kB_Rows, kB_Keys, 0}});
    // After compaction, A's data materialized → all 15 subcolumns indexed
    EXPECT_EQ(count_indexed_subcolumns(out), kA_Keys + kA_Obj)
            << "A+B compact: all " << (kA_Keys + kA_Obj) << " subcolumns should have indexes";
}

// ------- B+B with field_pattern "k*" -------
// B has 5 flat keys (k0-k4), all match "k*" → 5 indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_B_B_with_field_pattern) {
    auto schema = make_schema_with_index("k*");
    auto tablet = make_tablet(schema);
    auto r1 = write_B(schema, tablet, 2);
    auto r2 = write_B(schema, tablet, 3);
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/false, {{2, kB_Rows, kB_Keys, 0}, {3, kB_Rows, kB_Keys, 0}});
    // B has only k-prefixed keys, all match "k*"
    EXPECT_EQ(count_indexed_subcolumns(out), kB_Keys)
            << "field_pattern k*: all " << kB_Keys << " subcolumns should match";
}

// ------- A+B+C with field_pattern "k*" -------
// Merged: k0-k11 (12 match "k*") + obj.f0-f2 (3 don't match) = 12 indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_A_B_C_with_field_pattern) {
    auto schema = make_schema_with_index("k*");
    auto tablet = make_tablet(schema);
    auto r1 = write_A(schema, tablet, 2);
    auto r2 = write_B(schema, tablet, 3);
    auto r3 = write_C(schema, tablet, 4);
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    ASSERT_TRUE(tablet->add_rowset(r3).ok());
    auto out = do_compaction(tablet, {r1, r2, r3});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kA_Rows, kA_Keys, kA_Obj},
            {3, kB_Rows, kB_Keys, 0},
            {4, kC_Rows, kC_Keys, kC_Obj}});
    // k0-k11 match "k*" (12), obj.f0-f2 don't match (0) → 12 indexed
    EXPECT_EQ(count_indexed_subcolumns(out), kA_Keys)
            << "field_pattern k*: only " << kA_Keys << " k-prefixed subcolumns should be indexed";
}

// ------- A+A with inverted index (both pure doc_value, 0 indexed subcolumns) -------
// A: 12 keys + 3 obj (doc_value only, 0 indexed each)
// After compaction: 100 rows, 15 paths >= max_count(10) → Segment C, all 15 indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_A_A_with_inverted_index) {
    auto schema = make_schema_with_index();
    auto tablet = make_tablet(schema);
    auto r1 = write_A(schema, tablet, 2);
    auto r2 = write_A(schema, tablet, 3);
    EXPECT_EQ(count_indexed_subcolumns(r1), 0) << "Segment A should have no indexed subcolumns";
    EXPECT_EQ(count_indexed_subcolumns(r2), 0) << "Segment A should have no indexed subcolumns";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kA_Rows, kA_Keys, kA_Obj}, {3, kA_Rows, kA_Keys, kA_Obj}});
    // After compaction, both A's data materialized → all 15 subcolumns indexed
    EXPECT_EQ(count_indexed_subcolumns(out), kA_Keys + kA_Obj)
            << "A+A compact: all " << (kA_Keys + kA_Obj) << " subcolumns should have indexes";
}

// ------- A+C with inverted index -------
// A: 12+3=15 paths (doc_value only, 0 indexed), C: 12+3=15 paths (all indexed)
// After compaction: 250 rows, 15 paths → Segment C, all 15 indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_A_C_with_inverted_index) {
    auto schema = make_schema_with_index();
    auto tablet = make_tablet(schema);
    auto r1 = write_A(schema, tablet, 2);
    auto r2 = write_C(schema, tablet, 3);
    EXPECT_EQ(count_indexed_subcolumns(r1), 0) << "Segment A should have no indexed subcolumns";
    EXPECT_GT(count_indexed_subcolumns(r2), 0) << "Segment C should have indexed subcolumns";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kA_Rows, kA_Keys, kA_Obj}, {3, kC_Rows, kC_Keys, kC_Obj}});
    EXPECT_EQ(count_indexed_subcolumns(out), kA_Keys + kA_Obj)
            << "A+C compact: all " << (kA_Keys + kA_Obj) << " subcolumns should have indexes";
}

// ------- C+C with inverted index -------
// Both C segments: 12+3=15 paths, all indexed
// After compaction: 400 rows, 15 paths → Segment C, all 15 indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_C_C_with_inverted_index) {
    auto schema = make_schema_with_index();
    auto tablet = make_tablet(schema);
    auto r1 = write_C(schema, tablet, 2);
    auto r2 = write_C(schema, tablet, 3);
    EXPECT_GT(count_indexed_subcolumns(r1), 0) << "Segment C should have indexed subcolumns";
    EXPECT_GT(count_indexed_subcolumns(r2), 0) << "Segment C should have indexed subcolumns";
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kC_Rows, kC_Keys, kC_Obj}, {3, kC_Rows, kC_Keys, kC_Obj}});
    EXPECT_EQ(count_indexed_subcolumns(out), kC_Keys + kC_Obj)
            << "C+C compact: all " << (kC_Keys + kC_Obj) << " subcolumns should have indexes";
}

// ------- A+C with field_pattern "obj.f*" -------
// Only obj.f0, obj.f1, obj.f2 match → 3 indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_A_C_with_field_pattern_obj) {
    auto schema = make_schema_with_index("obj.f*");
    auto tablet = make_tablet(schema);
    auto r1 = write_A(schema, tablet, 2);
    auto r2 = write_C(schema, tablet, 3);
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kA_Rows, kA_Keys, kA_Obj}, {3, kC_Rows, kC_Keys, kC_Obj}});
    // Only obj.f0-f2 match "obj.f*" → 3 indexed
    EXPECT_EQ(count_indexed_subcolumns(out), kA_Obj)
            << "field_pattern obj.f*: only " << kA_Obj << " obj children should be indexed";
}

// ------- C+C with field_pattern "k*" -------
// C: 12 flat keys match "k*", 3 obj children don't → 12 indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_C_C_with_field_pattern) {
    auto schema = make_schema_with_index("k*");
    auto tablet = make_tablet(schema);
    auto r1 = write_C(schema, tablet, 2);
    auto r2 = write_C(schema, tablet, 3);
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/true,
           {{2, kC_Rows, kC_Keys, kC_Obj}, {3, kC_Rows, kC_Keys, kC_Obj}});
    EXPECT_EQ(count_indexed_subcolumns(out), kC_Keys)
            << "field_pattern k*: only " << kC_Keys << " k-prefixed subcolumns should be indexed";
}

// ------- B+B with field_pattern "nonexistent*" -------
// No subcolumn paths match → 0 indexed
TEST_F(VariantDocModeMixedCompactionTest, compact_B_B_with_field_pattern_no_match) {
    auto schema = make_schema_with_index("nonexistent*");
    auto tablet = make_tablet(schema);
    auto r1 = write_B(schema, tablet, 2);
    auto r2 = write_B(schema, tablet, 3);
    ASSERT_TRUE(tablet->add_rowset(r1).ok());
    ASSERT_TRUE(tablet->add_rowset(r2).ok());
    auto out = do_compaction(tablet, {r1, r2});
    ASSERT_NE(out, nullptr);
    verify(out, schema, /*expect_dv=*/false, {{2, kB_Rows, kB_Keys, 0}, {3, kB_Rows, kB_Keys, 0}});
    EXPECT_EQ(count_indexed_subcolumns(out), 0)
            << "field_pattern nonexistent*: no subcolumns should match";
}

} // namespace doris

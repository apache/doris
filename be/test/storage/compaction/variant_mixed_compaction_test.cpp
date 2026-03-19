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

// End-to-end compaction tests for mixed variant doc-value / subcolumn formats.
// Covers: dynamic parsing downgrade, writer finalize three-branch,
//         aggregate_variant_extended_info format detection,
//         get_extended_compaction_schema three-case schema construction.

#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <random>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_string.h"
#include "exec/common/variant_util.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/compaction/cumulative_compaction.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/variant/variant_statistics.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

#ifndef NDEBUG
namespace doris {
using namespace ErrorCode;

static constexpr uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;
// Small row count for fast tests
static constexpr int32_t kRowsPerSegment = 100;

class VariantMixedCompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        ASSERT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + std::string(kTestDir);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(absolute_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()
                            ->create_directory(absolute_dir + "/tablet_path")
                            .ok());

        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(std::string(tmp_dir)).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(std::string(tmp_dir)).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        auto st = tmp_file_dirs->init();
        ASSERT_TRUE(st.ok()) << st.to_json();
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*engine_ref, absolute_dir);
        ASSERT_TRUE(_data_dir->init(true).ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    // Create doc-mode variant schema with configurable bucket count
    TabletSchemaSPtr create_schema(int doc_hash_shard_count = 2) {
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
        column_2->set_variant_max_subcolumns_count(2048);
        column_2->set_variant_max_sparse_column_statistics_size(10000);
        column_2->set_variant_enable_doc_mode(true);
        column_2->set_variant_doc_materialization_min_rows(kRowsPerSegment);
        column_2->set_variant_doc_hash_shard_count(doc_hash_shard_count);

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
                                      int64_t version, RowsetWriterContext* ctx) {
        RowsetId rowset_id;
        rowset_id.init(version + 1000);
        ctx->rowset_id = rowset_id;
        ctx->rowset_type = BETA_ROWSET;
        ctx->data_dir = _data_dir.get();
        ctx->rowset_state = VISIBLE;
        ctx->tablet_schema = tablet_schema;
        ctx->tablet_path = rowset_dir;
        ctx->version = Version(version, version);
        ctx->segments_overlap = NONOVERLAPPING;
        ctx->max_rows_per_segment = kRowsPerSegment;
    }

    // Generate JSON with specified number of unique keys.
    // num_keys >= 2048 → doc-value mode (not downgraded)
    // num_keys < 2048  → subcolumn mode (downgraded)
    std::string generate_json(int num_keys, int32_t c1) {
        std::string json = "{";
        for (int k = 0; k < num_keys; ++k) {
            if (k > 0) json += ",";
            json += "\"k" + std::to_string(k) + "\":" + std::to_string(c1 * 100 + k);
        }
        json += "}";
        return json;
    }

    // Create a rowset where each row has `num_keys` unique keys.
    // If num_keys >= 2048, sampling will keep doc-value mode.
    // If num_keys < 2048, sampling will downgrade to subcolumn mode.
    RowsetSharedPtr create_rowset(TabletSchemaSPtr tablet_schema, TabletSharedPtr tablet,
                                  int64_t version, int num_keys) {
        RowsetWriterContext writer_context;
        create_rowset_writer_context(tablet_schema, tablet->tablet_path(), version,
                                     &writer_context);

        auto res = RowsetFactory::create_rowset_writer(*engine_ref, writer_context, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        Block block = tablet_schema->create_block();
        auto columns = block.mutate_columns();
        auto raw_json_column = ColumnString::create();
        raw_json_column->reserve(kRowsPerSegment);

        for (int i = 0; i < kRowsPerSegment; ++i) {
            int32_t c1 = static_cast<int32_t>(version * kRowsPerSegment + i);
            columns[0]->insert_data(reinterpret_cast<const char*>(&c1), sizeof(c1));
            std::string json = generate_json(num_keys, c1);
            raw_json_column->insert_data(json.data(), json.size());
        }

        auto* variant_col = assert_cast<ColumnVariant*>(columns[1].get());
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        // set max subcolumns count properly to trigger the sampling downgrade
        cfg.max_subcolumns_count = 2048;
        variant_util::parse_json_to_variant(*variant_col, *raw_json_column, cfg);

        auto s = rowset_writer->add_block(&block);
        EXPECT_TRUE(s.ok()) << s.to_json();
        s = rowset_writer->flush();
        EXPECT_TRUE(s.ok()) << s.to_json();

        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        EXPECT_EQ(1, rowset->rowset_meta()->num_segments());
        EXPECT_EQ(kRowsPerSegment, rowset->rowset_meta()->num_rows());
        return rowset;
    }

    // Check segment storage format via VariantColumnReader statistics
    bool check_segment_is_doc_value(const RowsetSharedPtr& rs) {
        SegmentCacheHandle segment_cache;
        auto st = SegmentLoader::instance()->load_segments(std::static_pointer_cast<BetaRowset>(rs),
                                                           &segment_cache);
        if (!st.ok()) return false;

        for (const auto& segment : segment_cache.get_segments()) {
            std::shared_ptr<segment_v2::ColumnReader> column_reader;
            OlapReaderStatistics stats;
            st = segment->get_column_reader(2, &column_reader, &stats);
            if (!st.ok() || !column_reader) continue;

            auto* variant_reader =
                    dynamic_cast<segment_v2::VariantColumnReader*>(column_reader.get());
            if (!variant_reader) continue;
            st = variant_reader->load_external_meta_once();
            if (!st.ok()) continue;
            return variant_reader->get_stats()->has_doc_value_column_non_null_size();
        }
        return false;
    }

    // Run compaction on input rowsets and return the output rowset
    RowsetSharedPtr run_compaction(TabletSharedPtr tablet,
                                   std::vector<RowsetSharedPtr> input_rowsets) {
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);

        auto st = cu_compaction.CompactionMixin::execute_compact();
        EXPECT_TRUE(st.ok()) << st.to_json();

        return cu_compaction._output_rowset;
    }

    // Read and count all rows from a rowset
    int64_t count_rows(const RowsetSharedPtr& rowset, const TabletSchemaSPtr& schema) {
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = schema;
        reader_context.need_ordered_result = false;
        std::vector<uint32_t> return_columns = {0};
        reader_context.return_columns = &return_columns;

        RowsetReaderSharedPtr rs_reader;
        auto s = rowset->create_reader(&rs_reader);
        EXPECT_TRUE(s.ok()) << s.to_json();
        s = rs_reader->init(&reader_context);
        EXPECT_TRUE(s.ok()) << s.to_json();

        int64_t total_rows = 0;
        Status st = Status::OK();
        while (st.ok()) {
            Block output_block = schema->create_block_by_cids(return_columns);
            st = rs_reader->next_batch(&output_block);
            if (st.ok()) {
                total_rows += output_block.rows();
            }
        }
        EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st.to_json();
        return total_rows;
    }

    // Read all rows from a rowset and return the variant column's JSON strings.
    std::vector<std::string> read_variant_json(const RowsetSharedPtr& rowset,
                                               const TabletSchemaSPtr& schema) {
        RowsetReaderContext reader_context;
        reader_context.tablet_schema = schema;
        reader_context.need_ordered_result = false;
        std::vector<uint32_t> return_columns = {0, 1};
        reader_context.return_columns = &return_columns;

        RowsetReaderSharedPtr rs_reader;
        auto s = rowset->create_reader(&rs_reader);
        EXPECT_TRUE(s.ok()) << s.to_json();
        s = rs_reader->init(&reader_context);
        EXPECT_TRUE(s.ok()) << s.to_json();

        auto variant_to_string = [](const IColumn& column) -> ColumnPtr {
            auto data_type = std::make_shared<DataTypeVariant>();
            auto string_type = std::make_shared<DataTypeString>();
            ColumnPtr string_col = string_type->create_column();
            auto* mutable_string_col = assert_cast<ColumnString*>(string_col->assume_mutable().get());
            for (size_t i = 0; i < column.size(); ++i) {
                std::string s = data_type->to_string(column, i);
                mutable_string_col->insert_data(s.data(), s.size());
            }
            return string_col;
        };

        std::vector<std::string> results;
        Status st = Status::OK();
        while (st.ok()) {
            Block output_block = schema->create_block_by_cids(return_columns);
            st = rs_reader->next_batch(&output_block);
            if (st.ok()) {
                const auto& variant_col = *output_block.get_by_position(1).column;
                // Convert Variant to String column using DataTypeVariant::to_string
                auto string_col = variant_to_string(variant_col);
                for (size_t i = 0; i < output_block.rows(); ++i) {
                    Field f;
                    string_col->get(i, f);
                    if (f.get_type() == PrimitiveType::TYPE_STRING) {
                        results.push_back(f.get<TYPE_STRING>());
                    } else {
                        results.push_back("<non-string>");
                    }
                }
            }
        }
        EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st.to_json();
        return results;
    }

    void verify_json_output(const std::vector<std::string>& json_rows,
                            const std::vector<int>& rowset_versions,
                            const std::vector<int>& rowset_num_keys) {
        ASSERT_EQ(json_rows.size(), kRowsPerSegment * rowset_versions.size());
        for (size_t v_idx = 0; v_idx < rowset_versions.size(); ++v_idx) {
            int version = rowset_versions[v_idx];
            int num_keys = rowset_num_keys[v_idx];
            for (int i = 0; i < kRowsPerSegment; ++i) {
                int row_offset = v_idx * kRowsPerSegment + i;
                int32_t c1 = version * kRowsPerSegment + i;
                const std::string& json = json_rows[row_offset];
                
                // Assert k0 exists and has value c1 * 100 + 0
                std::string exp_k0 = "\"k0\":" + std::to_string(c1 * 100);
                EXPECT_TRUE(json.find(exp_k0) != std::string::npos)
                        << "Row " << row_offset << " (v=" << version << ", i=" << i
                        << ") missing k0 in: " << json.substr(0, 200);

                // Assert k(num_keys-1) exists
                if (num_keys > 0) {
                    std::string exp_last_key = "\"k" + std::to_string(num_keys - 1) + "\":" +
                                               std::to_string(c1 * 100 + num_keys - 1);
                    EXPECT_TRUE(json.find(exp_last_key) != std::string::npos)
                            << "Row " << row_offset << " missing last_key in: " << json.substr(0, 200);
                }
            }
        }
    }

    // Create a rowset with heterogeneous, complex boundary JSON structures
    RowsetSharedPtr create_complex_rowset(TabletSchemaSPtr tablet_schema, TabletSharedPtr tablet,
                                          int64_t version, int num_keys) {
        RowsetWriterContext writer_context;
        create_rowset_writer_context(tablet_schema, tablet->tablet_path(), version,
                                     &writer_context);

        auto res = RowsetFactory::create_rowset_writer(*engine_ref, writer_context, true);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        Block block = tablet_schema->create_block();
        auto columns = block.mutate_columns();
        auto raw_json_column = ColumnString::create();
        raw_json_column->reserve(kRowsPerSegment);

        for (int i = 0; i < kRowsPerSegment; ++i) {
            int32_t c1 = static_cast<int32_t>(version * kRowsPerSegment + i);
            columns[0]->insert_data(reinterpret_cast<const char*>(&c1), sizeof(c1));
            
            std::string json;
            if (i % 5 == 0) {
                // Empty JSON
                json = "{}";
            } else if (i % 5 == 1) {
                // Nested JSON
                json = "{\"nested\": {\"key\": " + std::to_string(c1) + "}, \"arr\": [1, 2, 3]}";
            } else if (i % 5 == 2) {
                // Sparse fields (simulate dynamic schema changes with unique keys per row)
                json = "{";
                for (int k = 0; k < 5; ++k) {
                    if (k > 0) json += ",";
                    json += "\"sparse_" + std::to_string(c1) + "_" + std::to_string(k) + "\":" + std::to_string(c1);
                }
                json += "}";
            } else if (i % 5 == 3) {
                // Null and scalar values
                json = "{\"val\": null, \"bool_val\": true, \"str_val\": \"text_" + std::to_string(c1) + "\"}";
            } else {
                // Standard paths from our generation
                json = generate_json(num_keys, c1);
            }
            raw_json_column->insert_data(json.data(), json.size());
        }

        auto* variant_col = assert_cast<ColumnVariant*>(columns[1].get());
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        // set max subcolumns count properly to trigger the sampling downgrade
        cfg.max_subcolumns_count = 2048;
        variant_util::parse_json_to_variant(*variant_col, *raw_json_column, cfg);

        auto s = rowset_writer->add_block(&block);
        EXPECT_TRUE(s.ok()) << s.to_json();
        s = rowset_writer->flush();
        EXPECT_TRUE(s.ok()) << s.to_json();

        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        EXPECT_EQ(1, rowset->rowset_meta()->num_segments());
        EXPECT_EQ(kRowsPerSegment, rowset->rowset_meta()->num_rows());
        return rowset;
    }

private:
    static constexpr std::string_view kTestDir = "/ut_dir/variant_mixed_compaction_test";
    static constexpr std::string_view tmp_dir = "./ut_dir/variant_mixed_compaction_test/tmp";
    std::string absolute_dir;
    std::unique_ptr<DataDir> _data_dir;
};

// Test 1: All segments use doc-value mode (many keys ≥ 2048).
// Verifies: aggregate detects has_doc_value_segments=true,
//           schema generates doc buckets,
//           writer produces doc-value output.
TEST_F(VariantMixedCompactionTest, all_doc_value_segments) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    constexpr int kNumKeys = 2100; // > 2048, stays in doc mode

    auto rs0 = create_rowset(schema, tablet, 0, kNumKeys);
    auto rs1 = create_rowset(schema, tablet, 1, kNumKeys);

    // Verify both segments are doc-value format
    EXPECT_TRUE(check_segment_is_doc_value(rs0)) << "rs0 should be doc-value";
    EXPECT_TRUE(check_segment_is_doc_value(rs1)) << "rs1 should be doc-value";

    ASSERT_TRUE(tablet->add_rowset(rs0).ok());
    ASSERT_TRUE(tablet->add_rowset(rs1).ok());

    auto output = run_compaction(tablet, {rs0, rs1});
    ASSERT_TRUE(output != nullptr);
    EXPECT_EQ(kRowsPerSegment * 2, output->rowset_meta()->num_rows());

    // Verify output segment is also doc-value
    EXPECT_TRUE(check_segment_is_doc_value(output)) << "output should be doc-value";

    // Verify row count
    EXPECT_EQ(kRowsPerSegment * 2, count_rows(output, schema));

    // Verify detailed JSON content after compaction
    auto json_rows = read_variant_json(output, schema);
    verify_json_output(json_rows, {0, 1}, {kNumKeys, kNumKeys});
}

// Test 2: All segments are downgraded to subcolumn mode (few keys < 2048).
// Verifies: aggregate detects has_doc_value_segments=false (all downgraded),
//           schema uses get_compaction_subcolumns_from_data_types (all materialized),
//           writer finalize uses subcolumn-only branch.
TEST_F(VariantMixedCompactionTest, all_downgraded_subcolumn_segments) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    constexpr int kNumKeys = 10; // < 2048, triggers downgrade

    auto rs0 = create_rowset(schema, tablet, 0, kNumKeys);
    auto rs1 = create_rowset(schema, tablet, 1, kNumKeys);

    // Verify both segments are NOT doc-value (downgraded to subcolumn)
    EXPECT_FALSE(check_segment_is_doc_value(rs0)) << "rs0 should be subcolumn-only";
    EXPECT_FALSE(check_segment_is_doc_value(rs1)) << "rs1 should be subcolumn-only";

    ASSERT_TRUE(tablet->add_rowset(rs0).ok());
    ASSERT_TRUE(tablet->add_rowset(rs1).ok());

    auto output = run_compaction(tablet, {rs0, rs1});
    ASSERT_TRUE(output != nullptr);
    EXPECT_EQ(kRowsPerSegment * 2, output->rowset_meta()->num_rows());

    // Verify output segment is also subcolumn-only (all downgraded path)
    EXPECT_FALSE(check_segment_is_doc_value(output))
            << "output should be subcolumn (all-downgraded path)";

    // Verify row count
    EXPECT_EQ(kRowsPerSegment * 2, count_rows(output, schema));

    // Verify detailed JSON content after compaction
    auto json_rows = read_variant_json(output, schema);
    verify_json_output(json_rows, {0, 1}, {kNumKeys, kNumKeys});
}

// Test 3: Mixed segments — one doc-value and one subcolumn (downgraded).
// The SubcolumnToDocCompactIterator should convert subcolumn data to
// doc-value format during compaction.
// Verifies: aggregate detects has_doc_value_segments=true (mixed),
//           schema generates doc buckets,
//           SubcolumnToDocCompactIterator converts subcolumn data → doc-value.
TEST_F(VariantMixedCompactionTest, mixed_doc_and_subcolumn_segments) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    // rs0: doc-value segment (many keys)
    auto rs0 = create_rowset(schema, tablet, 0, 2100);
    EXPECT_TRUE(check_segment_is_doc_value(rs0)) << "rs0 should be doc-value";

    // rs1: subcolumn-only segment (few keys, downgraded)
    auto rs1 = create_rowset(schema, tablet, 1, 10);
    EXPECT_FALSE(check_segment_is_doc_value(rs1)) << "rs1 should be subcolumn-only";

    ASSERT_TRUE(tablet->add_rowset(rs0).ok());
    ASSERT_TRUE(tablet->add_rowset(rs1).ok());

    auto output = run_compaction(tablet, {rs0, rs1});
    ASSERT_TRUE(output != nullptr);
    EXPECT_EQ(kRowsPerSegment * 2, output->rowset_meta()->num_rows());

    // Output should be doc-value format (mixed → doc-value output)
    EXPECT_TRUE(check_segment_is_doc_value(output)) << "output should be doc-value in mixed mode";

    // Verify row count
    EXPECT_EQ(kRowsPerSegment * 2, count_rows(output, schema));

    // Verify detailed JSON content after mixed compaction
    // rs0 is version 0 (2100 keys), rs1 is version 1 (10 keys)
    auto json_rows = read_variant_json(output, schema);
    verify_json_output(json_rows, {0, 1}, {2100, 10});
}

// Test 4: Mixed in reversed order — subcolumn first, then doc-value.
// Verifies that the order of input rowsets doesn't matter.
TEST_F(VariantMixedCompactionTest, mixed_reversed_order) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    // rs0: subcolumn-only segment first
    auto rs0 = create_rowset(schema, tablet, 0, 10);
    EXPECT_FALSE(check_segment_is_doc_value(rs0)) << "rs0 should be subcolumn-only";

    // rs1: doc-value segment second
    auto rs1 = create_rowset(schema, tablet, 1, 2100);
    EXPECT_TRUE(check_segment_is_doc_value(rs1)) << "rs1 should be doc-value";

    ASSERT_TRUE(tablet->add_rowset(rs0).ok());
    ASSERT_TRUE(tablet->add_rowset(rs1).ok());

    auto output = run_compaction(tablet, {rs0, rs1});
    ASSERT_TRUE(output != nullptr);
    EXPECT_EQ(kRowsPerSegment * 2, output->rowset_meta()->num_rows());

    // Output should be doc-value format
    EXPECT_TRUE(check_segment_is_doc_value(output))
            << "output should be doc-value (reversed input order)";

    // Verify row count
    EXPECT_EQ(kRowsPerSegment * 2, count_rows(output, schema));

    // Verify detailed JSON content after reversed mixed compaction
    // rs0 is version 0 (10 keys), rs1 is version 1 (2100 keys)
    auto json_rows = read_variant_json(output, schema);
    // Note: rowsets are compacted by version order, so output should be sorted by version
    // version 0 has 10 keys, version 1 has 2100 keys
    verify_json_output(json_rows, {0, 1}, {10, 2100});
}

// Test 5: End-to-end full-path test for all doc-value segments.
// Write rowset with known JSON → read back via rowset reader → verify JSON content.
TEST_F(VariantMixedCompactionTest, e2e_doc_value_read_json) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    constexpr int kNumKeys = 2100; // >= 2048, stays in doc mode

    auto rs = create_rowset(schema, tablet, 0, kNumKeys);
    ASSERT_TRUE(check_segment_is_doc_value(rs)) << "segment should be doc-value";

    // Read back and verify JSON content
    auto json_rows = read_variant_json(rs, schema);
    ASSERT_EQ(json_rows.size(), kRowsPerSegment);

    for (int i = 0; i < kRowsPerSegment; ++i) {
        const std::string& json = json_rows[i];
        // k0 = i*100+0, k1 = i*100+1, k5 = i*100+5
        EXPECT_TRUE(json.find("\"k0\":" + std::to_string(i * 100)) != std::string::npos)
                << "Row " << i << " missing k0 in: " << json.substr(0, 200);
        EXPECT_TRUE(json.find("\"k1\":" + std::to_string(i * 100 + 1)) != std::string::npos)
                << "Row " << i << " missing k1 in: " << json.substr(0, 200);
        EXPECT_TRUE(json.find("\"k5\":" + std::to_string(i * 100 + 5)) != std::string::npos)
                << "Row " << i << " missing k5 in: " << json.substr(0, 200);
        // Check last key to ensure all keys are preserved
        std::string last_key = "\"k" + std::to_string(kNumKeys - 1) + "\":" +
                               std::to_string(i * 100 + kNumKeys - 1);
        EXPECT_TRUE(json.find(last_key) != std::string::npos)
                << "Row " << i << " missing last key in: " << json.substr(0, 200);
    }
}

// Test 6: End-to-end full-path test for all downgraded subcolumn segments.
// Write rowset with known JSON → read back via rowset reader → verify JSON content.
TEST_F(VariantMixedCompactionTest, e2e_subcolumn_read_json) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    constexpr int kNumKeys = 10; // < 2048, triggers downgrade to subcolumn

    auto rs = create_rowset(schema, tablet, 0, kNumKeys);
    ASSERT_FALSE(check_segment_is_doc_value(rs)) << "segment should be subcolumn-only";

    // Read back and verify JSON content
    auto json_rows = read_variant_json(rs, schema);
    ASSERT_EQ(json_rows.size(), kRowsPerSegment);

    for (int i = 0; i < kRowsPerSegment; ++i) {
        const std::string& json = json_rows[i];
        EXPECT_TRUE(json.find("\"k0\":" + std::to_string(i * 100)) != std::string::npos)
                << "Row " << i << " missing k0 in: " << json;
        EXPECT_TRUE(json.find("\"k1\":" + std::to_string(i * 100 + 1)) != std::string::npos)
                << "Row " << i << " missing k1 in: " << json;
        EXPECT_TRUE(json.find("\"k5\":" + std::to_string(i * 100 + 5)) != std::string::npos)
                << "Row " << i << " missing k5 in: " << json;
        std::string last_key = "\"k" + std::to_string(kNumKeys - 1) + "\":" +
                               std::to_string(i * 100 + kNumKeys - 1);
        EXPECT_TRUE(json.find(last_key) != std::string::npos)
                << "Row " << i << " missing last key in: " << json;
    }
}

// Test 7: Complex boundary cases compaction (merge nested/sparse/empty).
TEST_F(VariantMixedCompactionTest, e2e_mixed_complex_boundary_cases) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    // rs0 is subcolumn-only (10 keys)
    auto rs0 = create_rowset(schema, tablet, 0, 10);
    // rs1 is complex boundary cases (sparse, empty, nested, null, normal)
    auto rs1 = create_complex_rowset(schema, tablet, 1, 10);
    // rs2 is doc-value (2100 keys)
    auto rs2 = create_rowset(schema, tablet, 2, 2100);

    ASSERT_TRUE(tablet->add_rowset(rs0).ok());
    ASSERT_TRUE(tablet->add_rowset(rs1).ok());
    ASSERT_TRUE(tablet->add_rowset(rs2).ok());

    auto output = run_compaction(tablet, {rs0, rs1, rs2});
    ASSERT_TRUE(output != nullptr);
    EXPECT_EQ(kRowsPerSegment * 3, output->rowset_meta()->num_rows());

    // Verify detailed JSON content
    auto json_rows = read_variant_json(output, schema);
    ASSERT_EQ(json_rows.size(), kRowsPerSegment * 3);

    // Verify rowset 1 (complex boundaries) which starts at offset kRowsPerSegment
    for (int i = 0; i < kRowsPerSegment; ++i) {
        int row_offset = kRowsPerSegment + i;
        int32_t c1 = 1 * kRowsPerSegment + i;
        const std::string& json = json_rows[row_offset];
        if (i % 5 == 0) {
            EXPECT_EQ(json, "{}");
        } else if (i % 5 == 1) {
            EXPECT_TRUE(json.find("\"nested\":{\"key\":" + std::to_string(c1) + "}") != std::string::npos) << json;
            EXPECT_TRUE(json.find("\"arr\":[1, 2, 3]") != std::string::npos) << json;
        } else if (i % 5 == 2) {
            EXPECT_TRUE(json.find("\"sparse_" + std::to_string(c1) + "_0\":" + std::to_string(c1)) != std::string::npos) << json;
        } else if (i % 5 == 3) {
            // Nulls are omitted by the Variant stringifier, booleans are cast/stringified to 1/0
            EXPECT_TRUE(json.find("\"bool_val\":1") != std::string::npos) << json;
            EXPECT_TRUE(json.find("\"str_val\":\"text_" + std::to_string(c1) + "\"") != std::string::npos) << json;
        } else {
            EXPECT_TRUE(json.find("\"k0\":" + std::to_string(c1 * 100)) != std::string::npos) << json;
        }
    }
}

// Test 8: Type conflict across rowsets — same key "x" holds int in one rowset, string in another.
// This tests that compaction preserves type diversity correctly and does not corrupt data.
TEST_F(VariantMixedCompactionTest, e2e_type_conflict_across_rowsets) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    // rs0: key "x" is int, few keys → subcolumn mode
    {
        RowsetWriterContext ctx;
        create_rowset_writer_context(schema, tablet->tablet_path(), 0, &ctx);
        auto res = RowsetFactory::create_rowset_writer(*engine_ref, ctx, true);
        ASSERT_TRUE(res.has_value());
        auto writer = std::move(res).value();

        Block block = schema->create_block();
        auto columns = block.mutate_columns();
        auto raw_json = ColumnString::create();
        for (int i = 0; i < kRowsPerSegment; ++i) {
            int32_t c1 = i;
            columns[0]->insert_data(reinterpret_cast<const char*>(&c1), sizeof(c1));
            // x is an integer
            std::string json = "{\"x\":" + std::to_string(i * 10) + ",\"shared\":" + std::to_string(i) + "}";
            raw_json->insert_data(json.data(), json.size());
        }
        auto* vc = assert_cast<ColumnVariant*>(columns[1].get());
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        cfg.max_subcolumns_count = 2048;
        variant_util::parse_json_to_variant(*vc, *raw_json, cfg);
        ASSERT_TRUE(writer->add_block(&block).ok());
        ASSERT_TRUE(writer->flush().ok());
        RowsetSharedPtr rs;
        ASSERT_EQ(Status::OK(), writer->build(rs));
        ASSERT_TRUE(tablet->add_rowset(rs).ok());
    }

    // rs1: key "x" is string, few keys → subcolumn mode
    {
        RowsetWriterContext ctx;
        create_rowset_writer_context(schema, tablet->tablet_path(), 1, &ctx);
        auto res = RowsetFactory::create_rowset_writer(*engine_ref, ctx, true);
        ASSERT_TRUE(res.has_value());
        auto writer = std::move(res).value();

        Block block = schema->create_block();
        auto columns = block.mutate_columns();
        auto raw_json = ColumnString::create();
        for (int i = 0; i < kRowsPerSegment; ++i) {
            int32_t c1 = kRowsPerSegment + i;
            columns[0]->insert_data(reinterpret_cast<const char*>(&c1), sizeof(c1));
            // x is a string now!
            std::string json = "{\"x\":\"str_" + std::to_string(i) + "\",\"shared\":" + std::to_string(i + 1000) + "}";
            raw_json->insert_data(json.data(), json.size());
        }
        auto* vc = assert_cast<ColumnVariant*>(columns[1].get());
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        cfg.max_subcolumns_count = 2048;
        variant_util::parse_json_to_variant(*vc, *raw_json, cfg);
        ASSERT_TRUE(writer->add_block(&block).ok());
        ASSERT_TRUE(writer->flush().ok());
        RowsetSharedPtr rs;
        ASSERT_EQ(Status::OK(), writer->build(rs));
        ASSERT_TRUE(tablet->add_rowset(rs).ok());
    }

    auto input = tablet->get_rowset_by_version({0, 0});
    auto input2 = tablet->get_rowset_by_version({1, 1});
    ASSERT_TRUE(input != nullptr);
    ASSERT_TRUE(input2 != nullptr);

    auto output = run_compaction(tablet, {input, input2});
    ASSERT_TRUE(output != nullptr);
    EXPECT_EQ(kRowsPerSegment * 2, output->rowset_meta()->num_rows());

    auto json_rows = read_variant_json(output, schema);
    ASSERT_EQ(json_rows.size(), kRowsPerSegment * 2);

    // Verify rs0 rows: x should be int
    for (int i = 0; i < kRowsPerSegment; ++i) {
        const auto& json = json_rows[i];
        EXPECT_TRUE(json.find("\"x\":" + std::to_string(i * 10)) != std::string::npos
                    || json.find("\"x\":" + std::to_string(i * 10) + ".") != std::string::npos)
                << "Row " << i << ": " << json;
        EXPECT_TRUE(json.find("\"shared\":" + std::to_string(i)) != std::string::npos)
                << "Row " << i << ": " << json;
    }

    // Verify rs1 rows: x should be string
    for (int i = 0; i < kRowsPerSegment; ++i) {
        const auto& json = json_rows[kRowsPerSegment + i];
        EXPECT_TRUE(json.find("\"x\":\"str_" + std::to_string(i) + "\"") != std::string::npos)
                << "Row " << (kRowsPerSegment + i) << ": " << json;
        EXPECT_TRUE(json.find("\"shared\":" + std::to_string(i + 1000)) != std::string::npos)
                << "Row " << (kRowsPerSegment + i) << ": " << json;
    }
}

// Test 9: Five-way mixed compaction — alternating doc/sub/doc/sub/doc.
// Tests that compaction handles many input rowsets with mixed formats correctly.
TEST_F(VariantMixedCompactionTest, e2e_five_way_mixed_compaction) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    std::vector<RowsetSharedPtr> rowsets;
    std::vector<int> versions;
    std::vector<int> num_keys_vec;
    for (int v = 0; v < 5; ++v) {
        int num_keys = (v % 2 == 0) ? 2100 : 10; // alternate doc / sub
        auto rs = create_rowset(schema, tablet, v, num_keys);

        if (num_keys >= 2048) {
            EXPECT_TRUE(check_segment_is_doc_value(rs)) << "rs" << v << " should be doc-value";
        } else {
            EXPECT_FALSE(check_segment_is_doc_value(rs)) << "rs" << v << " should be subcolumn";
        }

        ASSERT_TRUE(tablet->add_rowset(rs).ok());
        rowsets.push_back(rs);
        versions.push_back(v);
        num_keys_vec.push_back(num_keys);
    }

    auto output = run_compaction(tablet, rowsets);
    ASSERT_TRUE(output != nullptr);
    EXPECT_EQ(kRowsPerSegment * 5, output->rowset_meta()->num_rows());

    // Output should be doc-value (mixed → doc-value)
    EXPECT_TRUE(check_segment_is_doc_value(output)) << "5-way mixed output should be doc-value";

    // Verify full JSON content
    auto json_rows = read_variant_json(output, schema);
    verify_json_output(json_rows, versions, num_keys_vec);
}

// Test 10: Exact threshold boundary — exactly 2048 unique keys.
// This is the boundary between subcolumn mode and doc-value mode.
TEST_F(VariantMixedCompactionTest, e2e_exact_threshold_2048) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    // Exactly 2048 keys: should NOT downgrade (unique_paths.size() < 2048 is false)
    auto rs_at = create_rowset(schema, tablet, 0, 2048);
    EXPECT_TRUE(check_segment_is_doc_value(rs_at)) << "exactly 2048 keys → doc-value (not <2048)";

    // 2047 keys: should downgrade
    auto rs_below = create_rowset(schema, tablet, 1, 2047);
    EXPECT_FALSE(check_segment_is_doc_value(rs_below)) << "2047 keys < 2048 → subcolumn";

    ASSERT_TRUE(tablet->add_rowset(rs_at).ok());
    ASSERT_TRUE(tablet->add_rowset(rs_below).ok());

    auto output = run_compaction(tablet, {rs_at, rs_below});
    ASSERT_TRUE(output != nullptr);
    EXPECT_EQ(kRowsPerSegment * 2, output->rowset_meta()->num_rows());
    EXPECT_TRUE(check_segment_is_doc_value(output)) << "mixed → doc-value output";

    auto json_rows = read_variant_json(output, schema);
    verify_json_output(json_rows, {0, 1}, {2048, 2047});
}

// Test 11: Cascaded compaction — compact twice.
// output_1 = compact(rs0, rs1), then output_2 = compact(output_1, rs2).
// Tests that compacted output can be re-compacted correctly.
TEST_F(VariantMixedCompactionTest, e2e_cascaded_compaction) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    // Round 1: compact(doc-value, subcolumn)
    auto rs0 = create_rowset(schema, tablet, 0, 2100);
    auto rs1 = create_rowset(schema, tablet, 1, 10);
    ASSERT_TRUE(tablet->add_rowset(rs0).ok());
    ASSERT_TRUE(tablet->add_rowset(rs1).ok());

    auto output1 = run_compaction(tablet, {rs0, rs1});
    ASSERT_TRUE(output1 != nullptr);
    EXPECT_EQ(kRowsPerSegment * 2, output1->rowset_meta()->num_rows());
    EXPECT_TRUE(check_segment_is_doc_value(output1)) << "round-1 output should be doc-value";

    // Verify round-1 content
    auto json_round1 = read_variant_json(output1, schema);
    verify_json_output(json_round1, {0, 1}, {2100, 10});

    // Round 2: compact(output1, new_subcolumn_rowset)
    auto rs2 = create_rowset(schema, tablet, 2, 5);
    ASSERT_TRUE(tablet->add_rowset(output1).ok());
    ASSERT_TRUE(tablet->add_rowset(rs2).ok());

    auto output2 = run_compaction(tablet, {output1, rs2});
    ASSERT_TRUE(output2 != nullptr);
    EXPECT_EQ(kRowsPerSegment * 3, output2->rowset_meta()->num_rows());

    // Verify round-2 content: should contain all 3 original rowsets' data
    auto json_round2 = read_variant_json(output2, schema);
    verify_json_output(json_round2, {0, 1, 2}, {2100, 10, 5});
}

// Test 12: Disjoint key sets across rowsets.
// rs0 has keys {a0, a1, ...}, rs1 has keys {b0, b1, ...} — completely non-overlapping.
// After compaction, each row should only have its own keys, not the other rowset's.
TEST_F(VariantMixedCompactionTest, e2e_disjoint_keys_compaction) {
    TabletSchemaSPtr schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*schema);
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());

    // rs0: keys are "a0", "a1", ..., "a9" (subcolumn mode)
    {
        RowsetWriterContext ctx;
        create_rowset_writer_context(schema, tablet->tablet_path(), 0, &ctx);
        auto res = RowsetFactory::create_rowset_writer(*engine_ref, ctx, true);
        ASSERT_TRUE(res.has_value());
        auto writer = std::move(res).value();

        Block block = schema->create_block();
        auto columns = block.mutate_columns();
        auto raw_json = ColumnString::create();
        for (int i = 0; i < kRowsPerSegment; ++i) {
            int32_t c1 = i;
            columns[0]->insert_data(reinterpret_cast<const char*>(&c1), sizeof(c1));
            std::string json = "{";
            for (int k = 0; k < 10; ++k) {
                if (k > 0) json += ",";
                json += "\"a" + std::to_string(k) + "\":" + std::to_string(i * 10 + k);
            }
            json += "}";
            raw_json->insert_data(json.data(), json.size());
        }
        auto* vc = assert_cast<ColumnVariant*>(columns[1].get());
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        cfg.max_subcolumns_count = 2048;
        variant_util::parse_json_to_variant(*vc, *raw_json, cfg);
        ASSERT_TRUE(writer->add_block(&block).ok());
        ASSERT_TRUE(writer->flush().ok());
        RowsetSharedPtr rs;
        ASSERT_EQ(Status::OK(), writer->build(rs));
        ASSERT_TRUE(tablet->add_rowset(rs).ok());
    }

    // rs1: keys are "b0", "b1", ..., "b9" (subcolumn mode, completely disjoint)
    {
        RowsetWriterContext ctx;
        create_rowset_writer_context(schema, tablet->tablet_path(), 1, &ctx);
        auto res = RowsetFactory::create_rowset_writer(*engine_ref, ctx, true);
        ASSERT_TRUE(res.has_value());
        auto writer = std::move(res).value();

        Block block = schema->create_block();
        auto columns = block.mutate_columns();
        auto raw_json = ColumnString::create();
        for (int i = 0; i < kRowsPerSegment; ++i) {
            int32_t c1 = kRowsPerSegment + i;
            columns[0]->insert_data(reinterpret_cast<const char*>(&c1), sizeof(c1));
            std::string json = "{";
            for (int k = 0; k < 10; ++k) {
                if (k > 0) json += ",";
                json += "\"b" + std::to_string(k) + "\":" + std::to_string(i * 100 + k);
            }
            json += "}";
            raw_json->insert_data(json.data(), json.size());
        }
        auto* vc = assert_cast<ColumnVariant*>(columns[1].get());
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        cfg.max_subcolumns_count = 2048;
        variant_util::parse_json_to_variant(*vc, *raw_json, cfg);
        ASSERT_TRUE(writer->add_block(&block).ok());
        ASSERT_TRUE(writer->flush().ok());
        RowsetSharedPtr rs;
        ASSERT_EQ(Status::OK(), writer->build(rs));
        ASSERT_TRUE(tablet->add_rowset(rs).ok());
    }

    auto in0 = tablet->get_rowset_by_version({0, 0});
    auto in1 = tablet->get_rowset_by_version({1, 1});
    auto output = run_compaction(tablet, {in0, in1});
    ASSERT_TRUE(output != nullptr);
    EXPECT_EQ(kRowsPerSegment * 2, output->rowset_meta()->num_rows());

    auto json_rows = read_variant_json(output, schema);
    ASSERT_EQ(json_rows.size(), kRowsPerSegment * 2);

    // rs0 rows should have "a" keys but NOT "b" keys
    for (int i = 0; i < kRowsPerSegment; ++i) {
        const auto& json = json_rows[i];
        EXPECT_TRUE(json.find("\"a0\":" + std::to_string(i * 10)) != std::string::npos)
                << "Row " << i << " missing a0: " << json;
        EXPECT_TRUE(json.find("\"a9\":" + std::to_string(i * 10 + 9)) != std::string::npos)
                << "Row " << i << " missing a9: " << json;
        // Should NOT contain "b" keys
        EXPECT_TRUE(json.find("\"b0\"") == std::string::npos)
                << "Row " << i << " unexpected b0: " << json;
    }

    // rs1 rows should have "b" keys but NOT "a" keys
    for (int i = 0; i < kRowsPerSegment; ++i) {
        const auto& json = json_rows[kRowsPerSegment + i];
        EXPECT_TRUE(json.find("\"b0\":" + std::to_string(i * 100)) != std::string::npos)
                << "Row " << (kRowsPerSegment + i) << " missing b0: " << json;
        EXPECT_TRUE(json.find("\"b9\":" + std::to_string(i * 100 + 9)) != std::string::npos)
                << "Row " << (kRowsPerSegment + i) << " missing b9: " << json;
        // Should NOT contain "a" keys
        EXPECT_TRUE(json.find("\"a0\"") == std::string::npos)
                << "Row " << (kRowsPerSegment + i) << " unexpected a0: " << json;
    }
}

} // namespace doris

#endif

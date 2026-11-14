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

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <numeric>
#include <optional>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "load/delta_writer/delta_writer_context.h"
#include "load/memtable/memtable.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/sparse_flush_output.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {

class MemTableSparseTest : public testing::Test {
protected:
    void SetUp() override {
        _old_enable = config::enable_sparse_memtable;
        _old_materialize_limit = config::sparse_memtable_materialize_limit_bytes;
        _old_max_rows = config::sparse_memtable_max_rows;

        _resource_ctx = ResourceContext::create_shared();
        _resource_ctx->memory_context()->set_mem_tracker(MemTrackerLimiter::create_shared(
                MemTrackerLimiter::Type::LOAD, "MemTableSparseTest"));
    }

    void TearDown() override {
        config::enable_sparse_memtable = _old_enable;
        config::sparse_memtable_materialize_limit_bytes = _old_materialize_limit;
        config::sparse_memtable_max_rows = _old_max_rows;
    }

    std::shared_ptr<TabletSchema> create_schema(KeysType keys_type, bool keyless = false) {
        TabletSchemaPB pb;
        pb.set_keys_type(keys_type);
        pb.set_num_short_key_columns(keyless ? 0 : 1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_next_column_unique_id(4);

        auto* key = pb.add_column();
        key->set_unique_id(1);
        key->set_name("k");
        key->set_type("INT");
        key->set_is_key(!keyless);
        key->set_is_nullable(false);
        key->set_length(4);
        key->set_index_length(4);
        key->set_aggregation("NONE");

        auto* fixed = pb.add_column();
        fixed->set_unique_id(2);
        fixed->set_name("v_fixed");
        fixed->set_type("BIGINT");
        fixed->set_is_key(false);
        fixed->set_is_nullable(true);
        fixed->set_length(8);
        fixed->set_index_length(8);
        fixed->set_aggregation(keys_type == UNIQUE_KEYS ? "REPLACE" : "NONE");

        auto* string = pb.add_column();
        string->set_unique_id(3);
        string->set_name("v_string");
        string->set_type("STRING");
        string->set_is_key(false);
        string->set_is_nullable(true);
        string->set_length(64);
        string->set_index_length(16);
        string->set_aggregation(keys_type == UNIQUE_KEYS ? "REPLACE" : "NONE");

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    TupleDescriptor* create_tuple() {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto bigint_type = std::make_shared<DataTypeInt64>();
        auto string_type = std::make_shared<DataTypeString>();
        DescriptorTblBuilder builder(&_pool);
        builder.declare_tuple() << std::make_tuple(int_type, "k")
                                << std::make_tuple(std::make_shared<DataTypeNullable>(bigint_type),
                                                   "v_fixed")
                                << std::make_tuple(std::make_shared<DataTypeNullable>(string_type),
                                                   "v_string");
        return builder.build()->get_tuple_descriptor(0);
    }

    TupleDescriptor* create_tuple_with_uids_and_aliases() {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto bigint_type = std::make_shared<DataTypeInt64>();
        auto string_type = std::make_shared<DataTypeString>();
        DescriptorTblBuilder builder(&_pool);
        builder.declare_tuple() << std::make_tuple(int_type, "key_alias", int32_t {1})
                                << std::make_tuple(std::make_shared<DataTypeNullable>(bigint_type),
                                                   "fixed_alias", int32_t {2})
                                << std::make_tuple(std::make_shared<DataTypeNullable>(string_type),
                                                   "string_alias", int32_t {3});
        return builder.build()->get_tuple_descriptor(0);
    }

    Block create_block(TupleDescriptor* tuple, size_t rows, size_t non_null_every,
                       bool duplicate_keys,
                       size_t dense_after = std::numeric_limits<size_t>::max()) {
        Block block;
        for (const auto* slot : tuple->slots()) {
            block.insert({slot->get_empty_mutable_column(), slot->type(), slot->col_name()});
        }
        {
            auto column_guard = block.mutate_columns_scoped();
            auto& columns = column_guard.mutable_columns();
            auto& fixed = assert_cast<ColumnNullable&>(*columns[1]);
            auto& string = assert_cast<ColumnNullable&>(*columns[2]);
            for (size_t i = 0; i < rows; ++i) {
                const int32_t key = duplicate_keys ? static_cast<int32_t>(i % 7)
                                                   : static_cast<int32_t>(rows - i);
                columns[0]->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
                if (i >= dense_after || (non_null_every != 0 && i % non_null_every == 0)) {
                    const auto value = static_cast<int64_t>(i * 11);
                    fixed.insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
                    const std::string text = "value-" + std::to_string(i);
                    string.insert_data(text.data(), text.size());
                } else {
                    fixed.insert_default();
                    string.insert_default();
                }
            }
        }
        return block;
    }

    std::unique_ptr<Block> run(KeysType keys_type, const Block& input, bool enable_sparse,
                               MemTableStat* stat, bool* need_flush = nullptr,
                               size_t batch_size = 0, bool keyless = false) {
        config::enable_sparse_memtable = enable_sparse;
        auto* tuple = create_tuple();
        auto schema = create_schema(keys_type, keyless);
        SCOPED_ATTACH_TASK(_resource_ctx);
        MemTable memtable(1, schema, &tuple->slots(), tuple, keys_type == UNIQUE_KEYS, nullptr,
                          _resource_ctx);
        if (batch_size == 0) {
            batch_size = input.rows();
        }
        for (size_t offset = 0; offset < input.rows(); offset += batch_size) {
            const size_t rows_in_batch = std::min(batch_size, input.rows() - offset);
            DorisVector<uint32_t> rows(rows_in_batch);
            std::iota(rows.begin(), rows.end(), static_cast<uint32_t>(offset));
            auto status = memtable.insert(&input, TabletAddRowsPayload {.row_idxs = rows});
            EXPECT_TRUE(status.ok()) << status;
        }
        if (need_flush != nullptr) {
            *need_flush = memtable.need_flush();
        }
        MemTableFlushOutput flush_output;
        EXPECT_TRUE(memtable.to_flush_output(&flush_output).ok());
        if (flush_output.sparse_attachment != nullptr) {
            for (size_t cid = 0; cid < flush_output.block->columns(); ++cid) {
                if (!flush_output.sparse_attachment->has_column(cid)) {
                    continue;
                }
                MutableColumnPtr materialized;
                EXPECT_TRUE(flush_output.sparse_attachment
                                    ->materialize(cid, 0, flush_output.block->rows(),
                                                  *flush_output.block->get_by_position(cid).column,
                                                  &materialized)
                                    .ok());
                flush_output.block->get_by_position(cid).column = std::move(materialized);
            }
        }
        *stat += memtable.stat();
        return std::move(flush_output.block);
    }

    std::unique_ptr<Block> run_with_attachment(
            KeysType keys_type, const Block& input,
            std::shared_ptr<const SparseFlushOutput>* sparse_output, bool enable_sparse = true) {
        config::enable_sparse_memtable = enable_sparse;
        auto* tuple = create_tuple();
        auto schema = create_schema(keys_type);
        SCOPED_ATTACH_TASK(_resource_ctx);
        MemTable memtable(1, schema, &tuple->slots(), tuple, keys_type == UNIQUE_KEYS, nullptr,
                          _resource_ctx);
        DorisVector<uint32_t> rows(input.rows());
        std::iota(rows.begin(), rows.end(), 0);
        EXPECT_TRUE(memtable.insert(&input, TabletAddRowsPayload {.row_idxs = rows}).ok());
        MemTableFlushOutput output;
        EXPECT_TRUE(memtable.to_flush_output(&output).ok());
        *sparse_output = output.sparse_attachment;
        return std::move(output.block);
    }

    void expect_same(const Block& lhs, const Block& rhs) {
        ASSERT_EQ(lhs.columns(), rhs.columns());
        ASSERT_EQ(lhs.rows(), rhs.rows());
        for (size_t col = 0; col < lhs.columns(); ++col) {
            for (size_t row = 0; row < lhs.rows(); ++row) {
                EXPECT_EQ(lhs.get_by_position(col).column->compare_at(
                                  row, row, *rhs.get_by_position(col).column, -1),
                          0)
                        << "column=" << col << " row=" << row;
            }
        }
    }

    ObjectPool _pool;
    std::shared_ptr<ResourceContext> _resource_ctx;
    bool _old_enable = false;
    int64_t _old_materialize_limit = 0;
    int64_t _old_max_rows = 0;
};

TEST_F(MemTableSparseTest, SparseThresholdConstantsAndConfigDefaults) {
    static_assert(kSparseMemtableSampleRows == 4096);
    static_assert(kSparseMemtableMaxNonNullRatio == 0.10);
    static_assert(kSparseMemtableMinSavingRatio == 0.30);
    EXPECT_EQ(config::sparse_memtable_materialize_limit_bytes, INT64_C(4294967296));
    EXPECT_EQ(config::sparse_memtable_max_rows, INT64_C(10000000));
}

TEST_F(MemTableSparseTest, SparseFlushOutputMaterializesRanges) {
    SparseFlushOutput::Columns columns(2);
    auto sparse = std::make_unique<SparseFlushOutput::Column>();
    sparse->row_count = 8;
    sparse->rowids = {0, 3, 7};
    auto values = ColumnInt64::create();
    values->insert_value(10);
    values->insert_value(30);
    values->insert_value(70);
    sparse->values = std::move(values);
    columns[1] = std::move(sparse);
    std::shared_ptr<const SparseFlushOutput> output;
    ASSERT_TRUE(SparseFlushOutput::create(std::move(columns), &output).ok());

    auto prototype = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    const auto check_range = [&](size_t pos, size_t rows,
                                 const std::vector<std::optional<int64_t>>& expected) {
        MutableColumnPtr result;
        ASSERT_TRUE(output->materialize(1, pos, rows, *prototype, &result).ok());
        const auto& nullable = assert_cast<const ColumnNullable&>(*result);
        const auto& nested = assert_cast<const ColumnInt64&>(nullable.get_nested_column());
        ASSERT_EQ(result->size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            ASSERT_EQ(nullable.is_null_at(i), !expected[i].has_value()) << i;
            if (expected[i].has_value()) {
                EXPECT_EQ(nested.get_data()[i], *expected[i]) << i;
            }
        }
    };
    check_range(0, 8,
                {10, std::nullopt, std::nullopt, 30, std::nullopt, std::nullopt, std::nullopt, 70});
    check_range(2, 3, {std::nullopt, 30, std::nullopt});
    check_range(7, 1, {70});
    check_range(8, 0, {});
}

TEST_F(MemTableSparseTest, SparseFlushOutputRejectsMalformedInput) {
    auto prototype = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    MutableColumnPtr result;
    SparseFlushOutput::Columns missing_columns(1);
    std::shared_ptr<const SparseFlushOutput> missing;
    ASSERT_TRUE(SparseFlushOutput::create(std::move(missing_columns), &missing).ok());
    EXPECT_FALSE(missing->materialize(0, 0, 0, *prototype, &result).ok());

    SparseFlushOutput::Columns invalid_columns(1);
    auto sparse = std::make_unique<SparseFlushOutput::Column>();
    sparse->row_count = 3;
    sparse->rowids = {2, 1};
    auto values = ColumnInt64::create();
    values->insert_value(20);
    values->insert_value(10);
    sparse->values = std::move(values);
    invalid_columns[0] = std::move(sparse);
    std::shared_ptr<const SparseFlushOutput> invalid;
    EXPECT_FALSE(SparseFlushOutput::create(std::move(invalid_columns), &invalid).ok());
    EXPECT_EQ(invalid, nullptr);

    SparseFlushOutput::Columns valid_columns(1);
    auto valid = std::make_unique<SparseFlushOutput::Column>();
    valid->row_count = 3;
    valid->rowids = {1};
    auto valid_values = ColumnInt64::create();
    valid_values->insert_value(10);
    valid->values = std::move(valid_values);
    valid_columns[0] = std::move(valid);
    ASSERT_TRUE(SparseFlushOutput::create(std::move(valid_columns), &invalid).ok());
    EXPECT_FALSE(invalid->materialize(0, 2, 2, *prototype, &result).ok());
    auto non_nullable = ColumnInt64::create();
    EXPECT_FALSE(invalid->materialize(0, 0, 1, *non_nullable, &result).ok());
    EXPECT_FALSE(result);

    SparseFlushOutput::Columns mismatch(1);
    auto mismatched = std::make_unique<SparseFlushOutput::Column>();
    mismatched->row_count = 2;
    mismatched->rowids = {0};
    mismatched->values = ColumnInt64::create();
    mismatch[0] = std::move(mismatched);
    EXPECT_FALSE(SparseFlushOutput::create(std::move(mismatch), &invalid).ok());
    EXPECT_EQ(invalid, nullptr);
}

TEST_F(MemTableSparseTest, DupKeysSparseOutputPreservesSortedPermutation) {
    auto* tuple = create_tuple();
    auto input = create_block(tuple, kSparseMemtableSampleRows, 20, false);
    MemTableStat dense_stat;
    auto dense = run(DUP_KEYS, input, false, &dense_stat);

    std::shared_ptr<const SparseFlushOutput> attachment;
    auto sparse = run_with_attachment(DUP_KEYS, input, &attachment);
    ASSERT_NE(attachment, nullptr);
    ASSERT_TRUE(attachment->has_column(1));
    ASSERT_TRUE(attachment->has_column(2));
    for (size_t cid : {size_t {1}, size_t {2}}) {
        MutableColumnPtr materialized;
        ASSERT_TRUE(attachment
                            ->materialize(cid, 0, sparse->rows(),
                                          *sparse->get_by_position(cid).column, &materialized)
                            .ok());
        sparse->get_by_position(cid).column = std::move(materialized);
    }
    expect_same(*dense, *sparse);
}

TEST_F(MemTableSparseTest, UniqueDuplicateKeysAndCapabilityGatesUseDenseOutput) {
    auto* tuple = create_tuple();
    auto duplicate_input = create_block(tuple, 140, 20, true);
    std::shared_ptr<const SparseFlushOutput> attachment;
    auto duplicate_output = run_with_attachment(UNIQUE_KEYS, duplicate_input, &attachment);
    EXPECT_EQ(attachment, nullptr);
    EXPECT_EQ(duplicate_output->rows(), 7);

    auto unique_input = create_block(tuple, kSparseMemtableSampleRows, 20, false);
    auto output_gate = run_with_attachment(DUP_KEYS, unique_input, &attachment, false);
    EXPECT_EQ(attachment, nullptr);
    EXPECT_EQ(output_gate->rows(), unique_input.rows());
}

TEST_F(MemTableSparseTest, CapabilityReportsStaticAndDynamicReasons) {
    auto* tuple = create_tuple();
    auto input = create_block(tuple, kSparseMemtableSampleRows, 20, false);
    const auto check = [&](bool enabled, size_t duplicates, SparseFlushCapabilityReason expected) {
        config::enable_sparse_memtable = enabled;
        auto schema = create_schema(UNIQUE_KEYS);
        SCOPED_ATTACH_TASK(_resource_ctx);
        MemTable memtable(1, schema, &tuple->slots(), tuple, true, nullptr, _resource_ctx);
        DorisVector<uint32_t> rows(input.rows());
        std::iota(rows.begin(), rows.end(), 0);
        ASSERT_TRUE(memtable.insert(&input, TabletAddRowsPayload {.row_idxs = rows}).ok());
        EXPECT_EQ(memtable.sparse_flush_capability_for_test(duplicates).reason, expected);
    };
    check(false, 0, SparseFlushCapabilityReason::CONFIG_DISABLED);
    check(true, 0, SparseFlushCapabilityReason::ELIGIBLE);
    check(true, 1, SparseFlushCapabilityReason::UNIQUE_DUPLICATES);
}

TEST_F(MemTableSparseTest, FlushResultIsAtomicForDenseSparseAndError) {
    config::enable_sparse_memtable = true;
    auto* tuple = create_tuple();
    auto schema = create_schema(DUP_KEYS);
    auto input = create_block(tuple, kSparseMemtableSampleRows, 20, false);
    SCOPED_ATTACH_TASK(_resource_ctx);
    MemTable memtable(1, schema, &tuple->slots(), tuple, false, nullptr, _resource_ctx);
    DorisVector<uint32_t> rows(input.rows());
    std::iota(rows.begin(), rows.end(), 0);
    ASSERT_TRUE(memtable.insert(&input, TabletAddRowsPayload {.row_idxs = rows}).ok());
    MemTableFlushOutput sparse;
    ASSERT_TRUE(memtable.to_flush_output(&sparse).ok());
    ASSERT_NE(sparse.block, nullptr);
    EXPECT_NE(sparse.sparse_attachment, nullptr);

    config::enable_sparse_memtable = false;
    MemTable dense_memtable(1, schema, &tuple->slots(), tuple, false, nullptr, _resource_ctx);
    ASSERT_TRUE(dense_memtable.insert(&input, TabletAddRowsPayload {.row_idxs = rows}).ok());
    MemTableFlushOutput dense;
    ASSERT_TRUE(dense_memtable.to_flush_output(&dense).ok());
    ASSERT_NE(dense.block, nullptr);
    EXPECT_EQ(dense.sparse_attachment, nullptr);
    EXPECT_FALSE(dense_memtable.to_flush_output(nullptr).ok());
}

TEST_F(MemTableSparseTest, DenseFallbackClearsReusedSparseOutput) {
    auto* tuple = create_tuple();

    std::shared_ptr<const SparseFlushOutput> attachment;
    auto sparse_input = create_block(tuple, kSparseMemtableSampleRows, 20, false);
    auto sparse_output = run_with_attachment(DUP_KEYS, sparse_input, &attachment);
    ASSERT_NE(attachment, nullptr);
    ASSERT_EQ(sparse_output->rows(), sparse_input.rows());

    auto duplicate_input = create_block(tuple, 140, 20, true);
    auto dense_output = run_with_attachment(UNIQUE_KEYS, duplicate_input, &attachment);
    EXPECT_EQ(attachment, nullptr);
    EXPECT_EQ(dense_output->rows(), 7);
}

TEST_F(MemTableSparseTest, AllNullAndNinetyFivePercentNullMatchDense) {
    auto* tuple = create_tuple();
    for (size_t every : {size_t {0}, size_t {20}}) {
        auto input = create_block(tuple, kSparseMemtableSampleRows, every, false);
        MemTableStat dense_stat;
        MemTableStat sparse_stat;
        auto dense = run(DUP_KEYS, input, false, &dense_stat);
        auto sparse =
                run(DUP_KEYS, input, true, &sparse_stat, nullptr, every == 0 ? 20 : input.rows());
        expect_same(*dense, *sparse);
        EXPECT_EQ(sparse_stat.sparse_encoded_columns, 2);
        EXPECT_GE(sparse_stat.sparse_actual_bytes_at_flush,
                  sparse_stat.sparse_sidecar_bytes_at_flush);
        if (every != 0) {
            EXPECT_GT(sparse_stat.sparse_sidecar_bytes_at_flush, 0);
        }
        if (every == 0) {
            EXPECT_EQ(sparse_stat.sparse_all_null_columns, 2);
        }
    }
}

TEST_F(MemTableSparseTest, DisabledByDefaultKeepsDenseRepresentation) {
    EXPECT_FALSE(_old_enable);
    auto* tuple = create_tuple();
    auto input = create_block(tuple, 200, 20, false);
    MemTableStat stat;
    static_cast<void>(run(DUP_KEYS, input, false, &stat, nullptr, 20));
    EXPECT_EQ(stat.sparse_candidate_columns, 0);
    EXPECT_EQ(stat.sparse_encoded_columns, 0);
    EXPECT_EQ(stat.sparse_fallback_columns, 0);
    EXPECT_EQ(stat.sparse_materialize_bytes, 0);

    MemTableStat unsupported_stat;
    static_cast<void>(run(DUP_KEYS, input, true, &unsupported_stat, nullptr, 20, true));
    EXPECT_EQ(unsupported_stat.sparse_candidate_columns, 0);
}

TEST_F(MemTableSparseTest, EnableConfigurationIsSnapshottedAtConstruction) {
    auto* tuple = create_tuple();
    auto schema = create_schema(DUP_KEYS);
    auto input = create_block(tuple, kSparseMemtableSampleRows, 0, false);
    config::enable_sparse_memtable = true;
    SCOPED_ATTACH_TASK(_resource_ctx);
    MemTable memtable(1, schema, &tuple->slots(), tuple, false, nullptr, _resource_ctx);

    config::enable_sparse_memtable = false;
    DorisVector<uint32_t> first_rows(kSparseMemtableSampleRows);
    std::iota(first_rows.begin(), first_rows.end(), 0);
    ASSERT_TRUE(memtable.insert(&input, TabletAddRowsPayload {first_rows}).ok());
    EXPECT_EQ(memtable.stat().sparse_encoded_columns, 2);
}

TEST_F(MemTableSparseTest, SchemaLookupPrefersUidAndFallsBackToName) {
    config::enable_sparse_memtable = true;
    auto schema = create_schema(DUP_KEYS);
    SCOPED_ATTACH_TASK(_resource_ctx);

    auto* alias_tuple = create_tuple_with_uids_and_aliases();
    auto alias_input = create_block(alias_tuple, kSparseMemtableSampleRows, 0, false);
    MemTable uid_memtable(1, schema, &alias_tuple->slots(), alias_tuple, false, nullptr,
                          _resource_ctx);
    DorisVector<uint32_t> rows(kSparseMemtableSampleRows);
    std::iota(rows.begin(), rows.end(), 0);
    ASSERT_TRUE(uid_memtable.insert(&alias_input, TabletAddRowsPayload {.row_idxs = rows}).ok());
    EXPECT_EQ(uid_memtable.stat().sparse_candidate_columns, 2);
    EXPECT_EQ(uid_memtable.stat().sparse_encoded_columns, 2);

    auto* name_tuple = create_tuple();
    auto name_input = create_block(name_tuple, kSparseMemtableSampleRows, 0, false);
    MemTable name_memtable(2, schema, &name_tuple->slots(), name_tuple, false, nullptr,
                           _resource_ctx);
    ASSERT_TRUE(name_memtable.insert(&name_input, TabletAddRowsPayload {.row_idxs = rows}).ok());
    EXPECT_EQ(name_memtable.stat().sparse_candidate_columns, 2);
    EXPECT_EQ(name_memtable.stat().sparse_encoded_columns, 2);
}

TEST_F(MemTableSparseTest, DenseRatioFallsBackAndSortedOutputMatches) {
    auto* tuple = create_tuple();
    auto input = create_block(tuple, kSparseMemtableSampleRows, 2, false);
    MemTableStat dense_stat;
    MemTableStat sparse_stat;
    auto dense = run(DUP_KEYS, input, false, &dense_stat);
    auto sparse = run(DUP_KEYS, input, true, &sparse_stat);
    expect_same(*dense, *sparse);
    EXPECT_EQ(sparse_stat.sparse_encoded_columns, 0);
    EXPECT_EQ(sparse_stat.sparse_fallback_columns, 2);
}

TEST_F(MemTableSparseTest, UniqueDuplicateKeysMatchDense) {
    auto* tuple = create_tuple();
    auto input = create_block(tuple, kSparseMemtableSampleRows, 20, true);
    MemTableStat dense_stat;
    MemTableStat sparse_stat;
    auto dense = run(UNIQUE_KEYS, input, false, &dense_stat);
    auto sparse = run(UNIQUE_KEYS, input, true, &sparse_stat);
    expect_same(*dense, *sparse);
    EXPECT_EQ(sparse->rows(), 7);
    EXPECT_GT(sparse_stat.merged_rows, 0);
    EXPECT_EQ(sparse_stat.sparse_encoded_columns, 2);
    EXPECT_GT(sparse_stat.sparse_materialize_bytes, 0);
}

TEST_F(MemTableSparseTest, LaterDenseRowsMaterializeAndPermanentlyFallBack) {
    auto* tuple = create_tuple();
    auto input =
            create_block(tuple, kSparseMemtableSampleRows * 2, 0, false, kSparseMemtableSampleRows);
    MemTableStat dense_stat;
    MemTableStat sparse_stat;
    auto dense = run(DUP_KEYS, input, false, &dense_stat, nullptr, kSparseMemtableSampleRows);
    auto sparse = run(DUP_KEYS, input, true, &sparse_stat, nullptr, kSparseMemtableSampleRows);
    expect_same(*dense, *sparse);
    EXPECT_EQ(sparse_stat.sparse_encoded_columns, 2);
    EXPECT_EQ(sparse_stat.sparse_fallback_columns, 2);
    EXPECT_GT(sparse_stat.sparse_materialize_bytes, 0);
}

TEST_F(MemTableSparseTest, MaterializeLimitTriggersFlush) {
    config::sparse_memtable_materialize_limit_bytes = 1;
    auto* tuple = create_tuple();
    auto input = create_block(tuple, kSparseMemtableSampleRows, 20, false);
    MemTableStat stat;
    bool need_flush = false;
    static_cast<void>(run(DUP_KEYS, input, true, &stat, &need_flush));
    EXPECT_TRUE(need_flush);
    EXPECT_GT(stat.sparse_dense_equivalent_bytes, 0);
}

TEST_F(MemTableSparseTest, LegacyBlockConsumerReceivesDenseColumns) {
    auto* tuple = create_tuple();
    auto input = create_block(tuple, kSparseMemtableSampleRows, 20, false);
    MemTableStat dense_stat;
    auto expected = run(DUP_KEYS, input, false, &dense_stat);

    config::enable_sparse_memtable = true;
    auto schema = create_schema(DUP_KEYS);
    SCOPED_ATTACH_TASK(_resource_ctx);
    MemTable memtable(1, schema, &tuple->slots(), tuple, false, nullptr, _resource_ctx);
    TabletAddRowsPayload rows;
    rows.row_idxs.resize(input.rows());
    std::iota(rows.row_idxs.begin(), rows.row_idxs.end(), 0);
    ASSERT_TRUE(memtable.insert(&input, rows).ok());
    ASSERT_EQ(memtable.stat().sparse_encoded_columns, 2);

    std::unique_ptr<Block> output;
    ASSERT_TRUE(memtable.to_block(&output).ok());
    ASSERT_NE(output, nullptr);
    expect_same(*expected, *output);
    EXPECT_GT(memtable.stat().sparse_materialize_bytes, 0);
}

TEST_F(MemTableSparseTest, SparseOutputPreservesAllocatedLsnOrder) {
    config::enable_sparse_memtable = true;
    auto* tuple = create_tuple();
    auto schema = create_schema(DUP_KEYS);
    auto input = create_block(tuple, kSparseMemtableSampleRows, 0, false);
    SCOPED_ATTACH_TASK(_resource_ctx);
    MemTable memtable(1, schema, &tuple->slots(), tuple, false, nullptr, _resource_ctx, true);
    TabletAddRowsPayload rows;
    rows.row_idxs.resize(input.rows());
    rows.allocated_lsns.resize(input.rows());
    std::iota(rows.row_idxs.begin(), rows.row_idxs.end(), 0);
    std::iota(rows.allocated_lsns.begin(), rows.allocated_lsns.end(), 100);
    ASSERT_TRUE(memtable.insert(&input, rows).ok());

    MemTableFlushOutput output;
    ASSERT_TRUE(memtable.to_flush_output(&output).ok());
    ASSERT_NE(output.sparse_attachment, nullptr);
    auto lsns = memtable.allocated_lsns();
    ASSERT_EQ(lsns->size(), input.rows());
    for (size_t row = 0; row < lsns->size(); ++row) {
        EXPECT_EQ((*lsns)[row], static_cast<int64_t>(100 + input.rows() - row - 1));
    }
}

} // namespace doris

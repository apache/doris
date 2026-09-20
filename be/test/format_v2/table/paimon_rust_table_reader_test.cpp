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

#include "format_v2/table/paimon_rust_table_reader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "cctz/time_zone.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "format/table/paimon_rust_predicate_converter.h"
#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/url_coding.h"

namespace doris::format::paimon {
namespace {

ColumnDefinition make_column(const std::string& name, const DataTypePtr& type,
                             bool is_partition_key = false) {
    ColumnDefinition column;
    column.name = name;
    column.type = type->is_nullable() ? type : make_nullable(type);
    column.is_partition_key = is_partition_key;
    return column;
}

TFileRangeDesc make_rust_range() {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_RUST);
    std::string encoded;
    base64_encode("dummy-split-bytes", &encoded);
    paimon_params.__set_paimon_split(encoded);
    paimon_params.__set_paimon_table("/paimon/warehouse/db.db/t");
    paimon_params.__set_db_name("db");
    paimon_params.__set_table_name("t");
    paimon_params.__set_paimon_table_schema_json("{}");
    table_format_params.__set_paimon_params(paimon_params);
    range.__set_table_format_params(table_format_params);
    return range;
}

// A range whose table path carries the given URI scheme — the production
// shape FE ships in paimon_table (PaimonScanNode sets it from the catalog's
// table location).
TFileRangeDesc make_rust_range_with_table_path(const std::string& table_path) {
    auto range = make_rust_range();
    range.table_format_params.paimon_params.__set_paimon_table(table_path);
    return range;
}

} // namespace

class PaimonRustTableReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _query_options.__set_batch_size(3);
        _runtime_state = RuntimeState::create_unique(_query_options, _query_globals);
    }

    Status init_reader_with_count(PaimonRustTableReader* reader,
                                  std::vector<GlobalIndex> count_columns) {
        return reader->init({.projected_columns = {_projected_column},
                             .conjuncts = {},
                             .format = FileFormat::JNI,
                             .scan_params = nullptr,
                             .io_ctx = nullptr,
                             .runtime_state = _runtime_state.get(),
                             .scanner_profile = nullptr,
                             .push_down_agg_type = TPushAggOp::type::COUNT,
                             .push_down_count_columns = std::move(count_columns)});
    }

    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    std::unique_ptr<RuntimeState> _runtime_state;
    ColumnDefinition _projected_column = make_column("k", std::make_shared<DataTypeInt32>());
};

TEST_F(PaimonRustTableReaderTest, ValidatesRustSplit) {
    PaimonRustTableReader reader;

    // Missing paimon_split.
    auto range = make_rust_range();
    range.table_format_params.paimon_params.__isset.paimon_split = false;
    auto status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_split"), std::string::npos) << status;

    // Missing paimon_table (table path).
    range = make_rust_range();
    range.table_format_params.paimon_params.__isset.paimon_table = false;
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_table"), std::string::npos) << status;

    // Missing db_name.
    range = make_rust_range();
    range.table_format_params.paimon_params.__isset.db_name = false;
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing db_name"), std::string::npos) << status;

    // Missing table_name.
    range = make_rust_range();
    range.table_format_params.paimon_params.__isset.table_name = false;
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing table_name"), std::string::npos) << status;

    // Missing paimon_table_schema_json.
    range = make_rust_range();
    range.table_format_params.paimon_params.__isset.paimon_table_schema_json = false;
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_table_schema_json"), std::string::npos)
            << status;

    // A mismatched reader_type is a protocol error.
    range = make_rust_range();
    range.table_format_params.paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_JNI);
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("invalid reader_type"), std::string::npos) << status;

    // A complete range validates cleanly.
    EXPECT_TRUE(reader.TEST_validate_rust_split(make_rust_range()).ok());
}

TEST_F(PaimonRustTableReaderTest, AdaptiveBatchSizeBoundsWideStringReads) {
    const auto fixture = std::filesystem::path(__FILE__).parent_path() /
                         "../../exec/test_data/paimon_rust_batch_size";
    const auto read_file = [](const std::filesystem::path& path) {
        std::ifstream input(path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    };
    auto split = read_file(fixture / "split.bin");
    const auto placeholder = split.find("__BUCKET__");
    ASSERT_NE(placeholder, std::string::npos);
    ASSERT_GE(placeholder, 2);
    const auto bucket = std::filesystem::canonical(fixture / "bucket-0").string();
    ASSERT_LT(bucket.size(), 65536);
    // The Java DataSplit fixture uses a neutral bucket path, encoded with writeUTF.
    const std::string length = {static_cast<char>(bucket.size() >> 8),
                                static_cast<char>(bucket.size() & 0xff)};
    split.replace(placeholder - 2, 2 + std::string("__BUCKET__").size(), length + bucket);
    std::string encoded;
    base64_encode(split, &encoded);
    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    options.all_runtime_filters_applied = true;
    auto& params = options.current_range.table_format_params.paimon_params;
    params.__set_paimon_table(std::filesystem::canonical(fixture).string());
    params.__set_paimon_table_schema_json(read_file(fixture / "schema/schema-0"));
    params.__set_paimon_split(encoded);

    const auto value_column = make_column("v", std::make_shared<DataTypeString>());
    PaimonRustTableReader reader;
    ASSERT_TRUE(reader.init({.projected_columns = {_projected_column, value_column},
                             .conjuncts = {},
                             .format = FileFormat::JNI,
                             .scan_params = nullptr,
                             .io_ctx = nullptr,
                             .runtime_state = _runtime_state.get(),
                             .scanner_profile = nullptr})
                        .ok());
    // Wide values must be bounded before Arrow decoding, including when reusing a table handle.
    for (const size_t requested : {0, 32, 7}) {
        if (requested != 0) {
            reader.set_batch_size(requested);
        }
        const size_t expected = requested == 0 ? 3 : requested;
        auto status = reader.prepare_split(options);
        ASSERT_TRUE(status.ok()) << status;
        Block block({ColumnWithTypeAndName(_projected_column.type->create_column(),
                                           _projected_column.type, "k"),
                     ColumnWithTypeAndName(value_column.type->create_column(), value_column.type,
                                           "v")});
        size_t total = 0;
        bool eos = false;
        while (!eos) {
            status = reader.get_block(&block, &eos);
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_LE(block.rows(), expected);
            const auto& keys = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
            const auto& values =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            for (size_t row = 0; row < block.rows(); ++row) {
                ASSERT_FALSE(keys.is_null_at(row));
                ASSERT_FALSE(values.is_null_at(row));
                EXPECT_EQ(keys.get_nested_column().get_int(row), total + row);
                EXPECT_EQ(values.get_nested_column().get_data_at(row).to_string(),
                          std::string(128 * 1024, 'x'));
            }
            total += block.rows();
        }
        EXPECT_EQ(total, 65);
        ASSERT_TRUE(reader.abort_split().ok());
    }
    ASSERT_TRUE(reader.close().ok());
}

TEST_F(PaimonRustTableReaderTest, ValidatesVectorSplit) {
    PaimonRustTableReader reader;

    auto make_vector_range = [] {
        // Vector mode: paimon_split is deliberately an empty string (FE avoids
        // serializing the same DataSplit twice) and the payload is the serialized
        // BucketVectorSearchSplit.
        auto range = make_rust_range();
        range.table_format_params.paimon_params.__set_paimon_split("");
        TPaimonVectorPayload payload;
        payload.__set_payload_type(TPaimonVectorPayloadType::BUCKET_SPLIT_BYTES);
        payload.__set_bytes("PKVSPLIT-dummy");
        range.table_format_params.paimon_params.__set_vector_payload(payload);
        return range;
    };

    // Missing vector_payload: a protocol error, not a fall-through to the empty
    // paimon_split check (whose error text would point the reader at the wrong field).
    auto range = make_vector_range();
    range.table_format_params.paimon_params.__isset.vector_payload = false;
    auto status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing vector_payload"), std::string::npos) << status;

    // Empty payload bytes are equally a protocol error.
    range = make_vector_range();
    range.table_format_params.paimon_params.vector_payload.bytes.clear();
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing vector_payload"), std::string::npos) << status;

    // A complete vector range validates cleanly -- including the empty paimon_split,
    // which the normal path would reject.
    EXPECT_TRUE(reader.TEST_validate_rust_split(make_vector_range()).ok());
}

// ---------------------------------------------------------------- score -> distance
//
// paimon-rust encodes the distance as a score per metric (see
// convert_distance_to_score in crates/paimon/src/lumina/reader.rs); the reader has
// to undo exactly that so the column means what the Doris function returns. For l2
// the score is 1/(1 + SQUARED distance), hence sqrt(1/score - 1). The reference
// points below are exact in binary floating point.

using ScoreTransform = PaimonRustTableReader::ScoreTransform;
static constexpr auto L2 = ScoreTransform::L2;
static constexpr auto INNER_PRODUCT = ScoreTransform::InnerProduct;

// A one-column block holding `scores` in a (possibly nullable) FLOAT column,
// standing in for what the arrow fill loop leaves in the distance column.
static Block _score_block(const std::vector<float>& scores, bool nullable) {
    DataTypePtr type = std::make_shared<DataTypeFloat32>();
    if (nullable) {
        type = std::make_shared<DataTypeNullable>(type);
    }
    MutableColumnPtr column = type->create_column();
    for (float score : scores) {
        column->insert_data(reinterpret_cast<const char*>(&score), sizeof(float));
    }
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), type, kPaimonSearchDistanceColumn));
    return block;
}

// The float at `row` of the block's only column, whether nullable or not.
static float _score_at(const Block& block, size_t row) {
    const auto& column = block.get_by_position(0).column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
        return assert_cast<const ColumnFloat32&>(nullable->get_nested_column()).get_data()[row];
    }
    return assert_cast<const ColumnFloat32&>(*column).get_data()[row];
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceL2) {
    // score 1.0 -> squared 0 -> distance 0 (an exact hit)
    // score 0.5 -> squared 1 -> distance 1
    // score 0.2 -> squared 4 -> distance 2
    //
    // Keep the score == 1.0 row: it is the legal boundary, sitting immediately next
    // to the score > 1 corruption case below, and 1/1 - 1 is exactly 0 in floating
    // point. A guard phrased as "sqrt(radicand) if radicand > 0, else sentinel"
    // passes every other case here while misreporting exact hits as corrupt.
    Block block = _score_block({1.0F, 0.5F, 0.2F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 3, L2);

    EXPECT_FLOAT_EQ(0.0F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(1.0F, _score_at(block, 1));
    EXPECT_FLOAT_EQ(2.0F, _score_at(block, 2));
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceClampsCatastrophicCancellation) {
    // The lumina kernel computes squared L2 as ||a||^2 + ||b||^2 - 2<a,b>, which can
    // yield a small negative residual when a ~= b, i.e. score slightly ABOVE 1. That
    // is a near-exact hit, not corruption: it clamps to distance 0. Only a score past
    // kScoreAboveOneTolerance is sentineled.
    Block block = _score_block({1.0F + 1e-4F, 1.0F + 5e-2F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 2, L2);

    EXPECT_FLOAT_EQ(0.0F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(PaimonRustTableReader::kCorruptScoreAboveOne, _score_at(block, 1));
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceSkipsRowsBeforeOffset) {
    // Block columns are append-only and a block is reused across get_block calls
    // when a batch is fully filtered out, so rows converted by an earlier call must
    // be left alone. Here rows 0-1 already hold distances; only the two raw scores
    // appended after them may be touched.
    Block block = _score_block({0.0F, 2.0F, 0.5F, 0.2F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, /*row_offset=*/2,
                                                      /*num_rows=*/2, L2);

    // Untouched: converting again would turn distance 2.0 into sqrt(1/2 - 1) garbage.
    EXPECT_FLOAT_EQ(0.0F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(2.0F, _score_at(block, 1));
    // Converted.
    EXPECT_FLOAT_EQ(1.0F, _score_at(block, 2));
    EXPECT_FLOAT_EQ(2.0F, _score_at(block, 3));
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceNullable) {
    // FE declares the score column non-nullable, but the slot decides the actual
    // block column, so the nullable shape must work too: null rows stay null and
    // their payload is not read.
    Block block = _score_block({0.5F, 0.0F, 0.2F}, /*nullable=*/true);
    auto nullable = IColumn::mutate(std::move(block.get_by_position(0).column));
    assert_cast<ColumnNullable*>(nullable.get())->get_null_map_data()[1] = 1;
    block.get_by_position(0).column = std::move(nullable);

    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 3, L2);

    EXPECT_FLOAT_EQ(1.0F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(2.0F, _score_at(block, 2));
    const auto* null_column =
            check_and_get_column<ColumnNullable>(block.get_by_position(0).column.get());
    ASSERT_NE(null_column, nullptr);
    EXPECT_EQ(1, null_column->get_null_map_data()[1]);
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceFlagsOutOfRangeScores) {
    // Unreachable for a well-formed index (d >= 0 implies score in (0, 1]), but the
    // distances come out of the closed-source lumina kernel unclamped. Computing the
    // inverse anyway would give +inf (1/0) and NaN (sqrt of a negative), either of
    // which makes the coordinator's Top-N comparisons ill-defined. Each case maps to
    // its own negative sentinel: negative is unreachable for a real Euclidean
    // distance, stays finite and totally ordered, and tells which way it broke.
    Block block = _score_block({0.0F, -1.0F, 1.5F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 3, L2);

    EXPECT_FLOAT_EQ(PaimonRustTableReader::kCorruptScoreNotPositive, _score_at(block, 0));
    EXPECT_FLOAT_EQ(PaimonRustTableReader::kCorruptScoreNotPositive, _score_at(block, 1));
    EXPECT_FLOAT_EQ(PaimonRustTableReader::kCorruptScoreAboveOne, _score_at(block, 2));
    for (size_t i = 0; i < 3; ++i) {
        const float distance = _score_at(block, i);
        EXPECT_TRUE(std::isfinite(distance)) << "row " << i << " = " << distance;
        // Must be distinguishable from any genuine distance, which is >= 0.
        EXPECT_LT(distance, 0.0F) << "row " << i;
    }
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceEmptyBatch) {
    // An empty batch must be a no-op rather than an out-of-range write.
    Block block = _score_block({0.5F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, /*row_offset=*/1,
                                                      /*num_rows=*/0, L2);
    EXPECT_FLOAT_EQ(0.5F, _score_at(block, 0));
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceInnerProductIsIdentity) {
    // For inner_product the score IS the value the Doris function returns, so every
    // row must survive untouched -- including negative and out-of-(0,1] values, which
    // are legal here (an inner product is unbounded) and must NOT be mistaken for
    // the out-of-range l2 scores that get a corruption sentinel.
    Block block = _score_block({0.5F, 0.0F, -3.25F, 42.0F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 4, INNER_PRODUCT);

    EXPECT_FLOAT_EQ(0.5F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(0.0F, _score_at(block, 1));
    EXPECT_FLOAT_EQ(-3.25F, _score_at(block, 2));
    EXPECT_FLOAT_EQ(42.0F, _score_at(block, 3));
}

// ---------------------------------------------------------------- metric parsing

TEST_F(PaimonRustTableReaderTest, ParseScoreTransformAcceptsSupportedMetrics) {
    ScoreTransform transform = INNER_PRODUCT;
    ASSERT_TRUE(PaimonRustTableReader::parse_score_transform(TVectorMetric::L2, &transform).ok());
    EXPECT_EQ(L2, transform);

    ASSERT_TRUE(PaimonRustTableReader::parse_score_transform(TVectorMetric::DOT_PRODUCT, &transform)
                        .ok());
    EXPECT_EQ(INNER_PRODUCT, transform);
}

TEST_F(PaimonRustTableReaderTest, ParseScoreTransformRejectsUnsupportedMetrics) {
    // Must fail rather than fall back to l2: guessing would sqrt an inner product,
    // or return a cosine score where a distance was promised. v1 does not push
    // cosine down, so seeing it here means FE and BE disagree.
    ScoreTransform transform = L2;
    for (auto metric : {TVectorMetric::COSINE, TVectorMetric::DEFAULT, TVectorMetric::HAMMING}) {
        auto status = PaimonRustTableReader::parse_score_transform(metric, &transform);
        EXPECT_FALSE(status.ok()) << "metric " << static_cast<int>(metric) << " must be rejected";
        EXPECT_NE(status.to_string().find("unsupported distance metric"), std::string::npos)
                << status;
    }
}

// ------------------------------------------------- arrow field -> block column

TEST_F(PaimonRustTableReaderTest, ArrowFieldToBlockColumnRenamesScoreField) {
    // FE names the output column "..._to_dis" because the reader turns the score
    // into a distance, but paimon-rust keeps emitting the Arrow field as
    // "__paimon_search_score". The fill loop silently skips Arrow fields it cannot
    // match, so if this mapping regressed the distance column would come back
    // empty rather than error out -- hence the explicit assertion.
    EXPECT_EQ(kPaimonSearchDistanceColumn,
              PaimonRustTableReader::arrow_field_to_block_column(kPaimonRustScoreField));
    // paimon-rust may return the canonical (any-case) field name.
    EXPECT_EQ(kPaimonSearchDistanceColumn,
              PaimonRustTableReader::arrow_field_to_block_column("__Paimon_Search_Score"));
}

TEST_F(PaimonRustTableReaderTest, ArrowFieldToBlockColumnLowercasesOtherFields) {
    // Ordinary table columns are only case-folded: paimon-rust resolves projections
    // case-insensitively and returns the table's canonical casing, while block
    // columns are FE-normalized to lowercase.
    EXPECT_EQ("is_qt", PaimonRustTableReader::arrow_field_to_block_column("is_QT"));
    EXPECT_EQ("id", PaimonRustTableReader::arrow_field_to_block_column("id"));
    // Not the score field, so it must NOT be rewritten to the distance column.
    EXPECT_EQ("__paimon_search_score_extra",
              PaimonRustTableReader::arrow_field_to_block_column("__paimon_search_score_extra"));
}

TEST_F(PaimonRustTableReaderTest, TableLevelCountEmitsSyntheticRows) {
    // COUNT(*) with a table-level row count takes the base-class metadata path:
    // prepare_split never opens the rust pipeline and get_block emits synthetic rows.
    PaimonRustTableReader reader;
    ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());

    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    options.all_runtime_filters_applied = true;
    options.current_range.table_format_params.__set_table_level_row_count(5);
    ASSERT_TRUE(reader.prepare_split(options).ok());
    EXPECT_TRUE(reader.current_split_uses_metadata_count());

    Block block = Block({ColumnWithTypeAndName(_projected_column.type->create_column(),
                                               _projected_column.type, _projected_column.name)});
    bool eos = false;
    // The base-class count contract emits batches until a call finds remaining==0:
    // batch_size(3) splits 5 rows into 3 + 2, and only the following call reports eos.
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 3);
    EXPECT_FALSE(eos);

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 2);
    EXPECT_FALSE(eos);

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 0);
    EXPECT_TRUE(eos);
}

TEST_F(PaimonRustTableReaderTest, TableLevelCountDisabledByConjuncts) {
    // A row predicate makes the metadata shortcut unsafe: the split must not report a
    // metadata count. It proceeds to the rust pipeline instead, which fails on the dummy
    // split bytes of this test range rather than emitting synthetic count rows.
    PaimonRustTableReader reader;
    ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());

    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    options.all_runtime_filters_applied = true;
    options.current_range.table_format_params.__set_table_level_row_count(5);
    options.conjuncts = VExprContextSPtrs {};
    options.conjuncts->push_back(VExprContext::create_shared(VLiteral::create_shared(
            std::make_shared<DataTypeInt32>(), Field::create_field<TYPE_INT>(1))));

    const auto status = reader.prepare_split(options);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(reader.current_split_uses_metadata_count());

    Block block = Block({ColumnWithTypeAndName(_projected_column.type->create_column(),
                                               _projected_column.type, _projected_column.name)});
    bool eos = false;
    const auto get_block_status = reader.get_block(&block, &eos);
    EXPECT_FALSE(get_block_status.ok());
    EXPECT_NE(get_block_status.to_string().find("paimon-rust reader is not initialized"),
              std::string::npos)
            << get_block_status;
}

TEST_F(PaimonRustTableReaderTest, FailedSplitOpenIsIncludedInRustTotalTime) {
    RuntimeProfile profile("scanner");
    PaimonRustTableReader reader;
    ASSERT_TRUE(reader.init({.projected_columns = {_projected_column},
                             .conjuncts = {},
                             .format = FileFormat::JNI,
                             .scan_params = nullptr,
                             .io_ctx = nullptr,
                             .runtime_state = _runtime_state.get(),
                             .scanner_profile = &profile})
                        .ok());
    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    // The invalid split fails during open, before get_block can contribute any time.
    ASSERT_FALSE(reader.prepare_split(options).ok());
    auto* total = profile.get_counter("PaimonRustReader");
    auto* open = profile.get_counter("OpenSplitTime");
    ASSERT_NE(total, nullptr);
    ASSERT_NE(open, nullptr);
    EXPECT_GT(open->value(), 0);
    EXPECT_GE(total->value(), open->value());
}

TEST_F(PaimonRustTableReaderTest, FillsPartitionConstantsForMissingArrowColumns) {
    PaimonRustTableReader reader;
    const auto data_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto partition_type = make_nullable(std::make_shared<DataTypeString>());
    // The output block matches the projected columns exactly (get_block contract):
    // position 0 is the data column k (absent from the arrow batch -> default fill),
    // position 1 is the partition key dt (materialized from split metadata).
    reader.TEST_set_projected_columns(
            {make_column("k", std::make_shared<DataTypeInt32>()),
             make_column("dt", std::make_shared<DataTypeString>(), /*is_partition_key=*/true)});
    std::map<std::string, Field> partition_values;
    partition_values.emplace("dt", Field::create_field<TYPE_STRING>("2024-01-01"));
    reader.TEST_set_partition_values(std::move(partition_values));

    Block block =
            Block({ColumnWithTypeAndName(data_type->create_column(), data_type, "k"),
                   ColumnWithTypeAndName(partition_type->create_column(), partition_type, "dt")});
    const size_t rows = 4;
    ASSERT_TRUE(reader.TEST_fill_non_arrow_columns(&block, rows).ok());

    // The data column is absent from both the arrow batch and split metadata: filled
    // with defaults.
    EXPECT_EQ(block.get_by_position(0).column->size(), rows);

    // The partition position is a constant column with the split value broadcast
    // to every row.
    const auto& column_with_type = block.get_by_position(1);
    EXPECT_EQ(column_with_type.column->size(), rows);
    const auto* const_column = check_and_get_column<ColumnConst>(*column_with_type.column);
    ASSERT_NE(const_column, nullptr);
    EXPECT_EQ(const_column->size(), rows);
    const auto value_field = const_column->get_field();
    const auto& value = value_field.get<TYPE_STRING>();
    EXPECT_EQ(std::string(value.data(), value.size()), "2024-01-01");
}

TEST_F(PaimonRustTableReaderTest, TruncatesNullablePartitionAndDefaultConstants) {
    _query_options.__set_truncate_char_or_varchar_columns(true);
    _runtime_state = RuntimeState::create_unique(_query_options, _query_globals);
    for (const auto primitive : {PrimitiveType::TYPE_CHAR, PrimitiveType::TYPE_VARCHAR}) {
        for (bool is_null : {false, true}) {
            PaimonRustTableReader reader;
            ASSERT_TRUE(init_reader_with_count(&reader, {}).ok());
            const auto type = make_nullable(std::make_shared<DataTypeString>(3, primitive));
            auto partition = make_column("partition_key", type, true);
            auto default_column = make_column("default_value", type);
            const auto value = is_null ? Field() : Field::create_field<TYPE_STRING>("abcdef");
            default_column.default_expr =
                    VExprContext::create_shared(VLiteral::create_shared(type, value));
            reader.TEST_set_projected_columns({partition, default_column});
            reader.TEST_set_partition_values({{"partition_key", value}});
            Block block({ColumnWithTypeAndName(type->create_column(), type, "partition_key"),
                         ColumnWithTypeAndName(type->create_column(), type, "default_value")});
            ASSERT_TRUE(reader.TEST_fill_non_arrow_columns(&block, 3).ok());
            ASSERT_TRUE(is_column_const(*block.get_by_position(0).column));
            // Split constants wrap nullable data; truncation must preserve both the row count
            // and null map when it materializes that outer wrapper.
            ASSERT_TRUE(reader.TEST_truncate_char_or_varchar_columns(&block).ok());
            ASSERT_EQ(block.rows(), 3);
            for (size_t column = 0; column < 2; ++column) {
                for (size_t row = 0; row < 3; ++row) {
                    Field actual;
                    block.get_by_position(column).column->get(row, actual);
                    if (is_null) {
                        EXPECT_TRUE(actual.is_null());
                    } else {
                        const auto& text = actual.get<TYPE_STRING>();
                        EXPECT_EQ(std::string(text.data(), text.size()), "abc");
                    }
                }
            }
        }
    }
}

TEST_F(PaimonRustTableReaderTest, DirectPathTruncatesBoundedStringColumns) {
    // The pinned rust reader maps paimon CHAR(n)/VARCHAR(n) to lengthless Arrow
    // Utf8, so the direct Arrow path must enforce truncate_char_or_varchar_
    // columns itself: a value written under an old wider schema and read into
    // a column narrowed to VARCHAR(3) truncates to "abc" — the semantics
    // TableReader::finalize_chunk applies on the normal path, which this
    // direct path bypasses. Unbounded STRING columns and non-string columns
    // stay untouched, and with the option off nothing truncates.
    const auto fill_block = [] {
        const auto varchar3 =
                make_nullable(std::make_shared<DataTypeString>(3, PrimitiveType::TYPE_VARCHAR));
        const auto string_type = make_nullable(std::make_shared<DataTypeString>());
        const auto int_type = std::make_shared<DataTypeInt32>();
        Block block = Block({ColumnWithTypeAndName(varchar3->create_column(), varchar3, "v"),
                             ColumnWithTypeAndName(string_type->create_column(), string_type, "s"),
                             ColumnWithTypeAndName(int_type->create_column(), int_type, "k")});
        for (auto [idx, field] : std::initializer_list<std::pair<size_t, Field>> {
                     {0, Field::create_field<TYPE_STRING>("abcdefghij")},
                     {1, Field::create_field<TYPE_STRING>("0123456789ABCDEF")},
                     {2, Field::create_field<TYPE_INT>(1)},
                     {0, Field::create_field<TYPE_STRING>("ab")},
                     {1, Field::create_field<TYPE_STRING>("xyz")},
                     {2, Field::create_field<TYPE_INT>(2)}}) {
            auto column = IColumn::mutate(block.get_by_position(idx).column);
            column->insert(field);
            block.get_by_position(idx).column = std::move(column);
        }
        return block;
    };
    const auto value_at = [](const Block& block, size_t idx, size_t row) {
        Field field;
        block.get_by_position(idx).column->get(row, field);
        const auto& value = field.get<TYPE_STRING>();
        return std::string(value.data(), value.size());
    };

    // Option off (the SetUp default): historical values return unchanged.
    {
        PaimonRustTableReader reader;
        ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());
        Block block = fill_block();
        ASSERT_TRUE(reader.TEST_truncate_char_or_varchar_columns(&block).ok());
        EXPECT_EQ(value_at(block, 0, 0), "abcdefghij");
    }

    // Option on: the VARCHAR(3) column truncates to its declared length; the
    // unbounded STRING column and the INT column stay untouched.
    _query_options.__set_truncate_char_or_varchar_columns(true);
    _runtime_state = RuntimeState::create_unique(_query_options, _query_globals);
    {
        PaimonRustTableReader reader;
        ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());
        Block block = fill_block();
        ASSERT_TRUE(reader.TEST_truncate_char_or_varchar_columns(&block).ok());
        EXPECT_EQ(value_at(block, 0, 0), "abc");
        EXPECT_EQ(value_at(block, 0, 1), "ab");
        EXPECT_EQ(value_at(block, 1, 0), "0123456789ABCDEF");
        EXPECT_EQ(value_at(block, 1, 1), "xyz");
        Field int_field;
        block.get_by_position(2).column->get(0, int_field);
        EXPECT_EQ(int_field.get<TYPE_INT>(), 1);
    }
}

TEST_F(PaimonRustTableReaderTest, MaterializesInSessionTimezone) {
    // TIMESTAMP_LTZ values materialize as session-local civil times: the
    // materialization timezone must come from the session, not a fixed default
    // (epoch 0 reads as 08:00 in a +08:00 session and 00:00 in UTC).
    const auto hour_of_epoch_zero = [](const cctz::time_zone& tz) {
        return tz.lookup(cctz::time_point<cctz::seconds>(std::chrono::seconds(0))).cs.hour();
    };

    PaimonRustTableReader reader;
    _runtime_state->set_timezone("+08:00");
    ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());
    EXPECT_EQ(hour_of_epoch_zero(reader.TEST_ctz()), 8);

    PaimonRustTableReader utc_reader;
    _runtime_state->set_timezone("UTC");
    ASSERT_TRUE(init_reader_with_count(&utc_reader, std::vector<GlobalIndex> {}).ok());
    EXPECT_EQ(hour_of_epoch_zero(utc_reader.TEST_ctz()), 0);
}

TEST_F(PaimonRustTableReaderTest, OptionLogRendersKeysOnly) {
    // Storage-option values must never reach the log: credential keys arrive
    // under many spellings and cases (AWS_SECRET_KEY, AWS_TOKEN,
    // fs.oss.accessKeySecret, s3.secret-key, ...), and a key-name blocklist
    // that misses one alias leaks the value, so the diagnostics rendering
    // prints key names only.
    const std::map<std::string, std::string> options {
            {"AWS_ACCESS_KEY", "admin"},
            {"AWS_SECRET_KEY", "leak-if-logged-1"},
            {"AWS_TOKEN", "leak-if-logged-2"},
            {"fs.oss.accessKeySecret", "leak-if-logged-3"},
            {"s3.access-key", "leak-if-logged-4"},
            {"s3.secret-key", "leak-if-logged-5"},
            {"s3.endpoint", "http://leak-if-logged-6:19001"},
    };
    const std::string rendered = PaimonRustTableReader::TEST_format_options(options);

    // Every key is rendered, no value and no '=' separator ever is.
    EXPECT_NE(rendered.find("AWS_SECRET_KEY"), std::string::npos);
    EXPECT_NE(rendered.find("fs.oss.accessKeySecret"), std::string::npos);
    EXPECT_NE(rendered.find("s3.secret-key"), std::string::npos);
    EXPECT_EQ(rendered.find('='), std::string::npos);
    for (int i = 1; i <= 6; ++i) {
        EXPECT_EQ(rendered.find("leak-if-logged-" + std::to_string(i)), std::string::npos)
                << rendered;
    }
    EXPECT_EQ(rendered.find("admin"), std::string::npos) << rendered;
}

TEST_F(PaimonRustTableReaderTest, MapsAnonymousAndAssumeRoleProviderModes) {
    // The FE storage-properties channel marks anonymous access with
    // AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS and assume-role with
    // AWS_ROLE_ARN / AWS_EXTERNAL_ID; the pinned rust S3 parser reads
    // s3.anonymous (skip_signature) and the s3.assumed.role.* family, so the
    // bridge must map both — without the mapping, anonymous catalogs would
    // consult the ambient credential chain and role-only catalogs would
    // never assume the requested role. The ambient JVM provider modes (ENV,
    // SYSTEM_PROPERTIES, WEB_IDENTITY, CONTAINER, INSTANCE_PROFILE) have no
    // rust equivalent and are gated away from the rust reader on the FE.
    TFileScanRangeParams params;
    params.properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = "ANONYMOUS";
    params.properties["AWS_ROLE_ARN"] = "arn:aws:iam::123:role/reader";
    params.properties["AWS_EXTERNAL_ID"] = "external-123";
    params.properties["AWS_ACCESS_KEY"] = "admin";
    params.properties["AWS_SECRET_KEY"] = "password";
    params.properties["AWS_ENDPOINT"] = "http://127.0.0.1:19001";
    params.properties["AWS_REGION"] = "us-east-1";
    params.properties["use_path_style"] = "true";
    params.__isset.properties = true;

    PaimonRustTableReader reader;
    const auto options = reader.TEST_build_options(
            &params, make_rust_range_with_table_path("s3://bucket/wh/db.db/t"));
    EXPECT_EQ(options.at("s3.anonymous"), "true");
    EXPECT_EQ(options.at("s3.assumed.role.arn"), "arn:aws:iam::123:role/reader");
    EXPECT_EQ(options.at("s3.assumed.role.externalId"), "external-123");
    // Static credentials and connection settings keep mapping (regression
    // for the base remap).
    EXPECT_EQ(options.at("s3.access-key"), "admin");
    EXPECT_EQ(options.at("s3.secret-key"), "password");
    EXPECT_EQ(options.at("s3.endpoint"), "http://127.0.0.1:19001");
    EXPECT_EQ(options.at("s3.region"), "us-east-1");
    EXPECT_EQ(options.at("s3.path-style-access"), "true");
}

TEST_F(PaimonRustTableReaderTest, StaticCredentialsMapWithoutProviderMode) {
    // Default shape: no provider-mode marker, plain static credentials.
    TFileScanRangeParams params;
    params.properties["AWS_ACCESS_KEY"] = "admin";
    params.properties["AWS_SECRET_KEY"] = "password";
    params.__isset.properties = true;

    PaimonRustTableReader reader;
    const auto options = reader.TEST_build_options(
            &params, make_rust_range_with_table_path("s3://bucket/wh/db.db/t"));
    EXPECT_EQ(options.at("s3.access-key"), "admin");
    EXPECT_EQ(options.at("s3.secret-key"), "password");
    EXPECT_EQ(options.count("s3.anonymous"), 0);
}

TEST_F(PaimonRustTableReaderTest, OssSchemeMapsToOssFileIOKeys) {
    // The pinned storage dispatcher selects the parser from the table path's
    // URI scheme: oss:// tables read the OSS parser, which requires
    // fs.oss.endpoint / fs.oss.accessKeyId / fs.oss.accessKeySecret (plus
    // optional fs.oss.securityToken for STS). A production-shaped FE map
    // (OSSProperties emits the AWS_* aliases plus the connection settings)
    // must therefore map to the fs.oss.* family — mapping everything to
    // s3.* leaves OSS catalogs failing to open ("Missing required OSS
    // config: fs.oss.endpoint").
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    TPaimonFileDesc paimon_params;
    paimon_params.__set_paimon_table("oss://bucket/wh/db.db/t");
    table_format_params.__set_paimon_params(paimon_params);
    range.__set_table_format_params(table_format_params);

    TFileScanRangeParams params;
    params.properties["AWS_ENDPOINT"] = "http://oss.internal:8080";
    params.properties["AWS_ACCESS_KEY"] = "oss-admin";
    params.properties["AWS_SECRET_KEY"] = "oss-password";
    params.properties["AWS_TOKEN"] = "oss-sts-token";
    params.properties["AWS_REGION"] = "cn-beijing";
    params.properties["use_path_style"] = "true";
    params.__isset.properties = true;

    PaimonRustTableReader reader;
    const auto options = reader.TEST_build_options(&params, range);
    EXPECT_EQ(options.at("fs.oss.endpoint"), "http://oss.internal:8080");
    EXPECT_EQ(options.at("fs.oss.accessKeyId"), "oss-admin");
    EXPECT_EQ(options.at("fs.oss.accessKeySecret"), "oss-password");
    EXPECT_EQ(options.at("fs.oss.securityToken"), "oss-sts-token");
    // The OSS parser reads no s3.* keys; nothing but the fs.oss.* family may
    // be synthesized for this scheme.
    EXPECT_EQ(options.count("s3.access-key"), 0);
    EXPECT_EQ(options.count("s3.endpoint"), 0);
    EXPECT_EQ(options.count("s3.path-style-access"), 0);
}

TEST_F(PaimonRustTableReaderTest, OssSchemeKeepsNativeFsOssKeysUnmapped) {
    // Native fs.oss.* options (delivered by configs that already speak the
    // paimon-java OSS dialect) must pass through untouched instead of being
    // remapped into the s3.* family.
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    TPaimonFileDesc paimon_params;
    paimon_params.__set_paimon_table("oss://bucket/wh/db.db/t");
    table_format_params.__set_paimon_params(paimon_params);
    range.__set_table_format_params(table_format_params);

    TFileScanRangeParams params;
    params.properties["fs.oss.endpoint"] = "http://oss.internal:8080";
    params.properties["fs.oss.accessKeyId"] = "oss-admin";
    params.properties["fs.oss.accessKeySecret"] = "oss-password";
    params.__isset.properties = true;

    PaimonRustTableReader reader;
    const auto options = reader.TEST_build_options(&params, range);
    EXPECT_EQ(options.at("fs.oss.endpoint"), "http://oss.internal:8080");
    EXPECT_EQ(options.at("fs.oss.accessKeyId"), "oss-admin");
    EXPECT_EQ(options.at("fs.oss.accessKeySecret"), "oss-password");
    EXPECT_EQ(options.count("s3.access-key"), 0);
}

TEST_F(PaimonRustTableReaderTest, S3aSchemeMapsToS3FileIOKeys) {
    // The crate dispatches both s3:// and s3a:// to the S3 parser, so the
    // legacy Hadoop-style spelling must translate to the s3.* family too.
    TFileScanRangeParams params;
    params.properties["AWS_ACCESS_KEY"] = "admin";
    params.properties["AWS_SECRET_KEY"] = "password";
    params.properties["AWS_ENDPOINT"] = "http://127.0.0.1:19001";
    params.__isset.properties = true;

    PaimonRustTableReader reader;
    const auto options = reader.TEST_build_options(
            &params, make_rust_range_with_table_path("s3a://bucket/wh/db.db/t"));
    EXPECT_EQ(options.at("s3.access-key"), "admin");
    EXPECT_EQ(options.at("s3.secret-key"), "password");
    EXPECT_EQ(options.at("s3.endpoint"), "http://127.0.0.1:19001");
}

TEST_F(PaimonRustTableReaderTest, UnverifiedSchemesPassThroughWithoutS3Aliases) {
    // libpaimon_c.a compiles in separate COS, OBS, GCS and Azdls parsers,
    // each reading its own key family (fs.cosn.userinfo.* / fs.obs.* /
    // gcs.* / azure.*), while Doris's FE normalizes those object stores'
    // credentials into the AWS_* aliases (the *Properties storage classes).
    // The bridge only implements the s3.* / fs.oss.* translations, and the
    // FE gates every other scheme to JNI before the split is encoded — so
    // here nothing may be synthesized for them: a production-shaped AWS_*
    // map must pass through untouched instead of being silently translated
    // into s3.* keys its scheme's parser ignores (which would fail the open
    // with a misleading auth error, e.g. "Missing required COS config").
    const std::vector<std::string> gated_schemes {
            "cosn://bucket/wh/db.db/t",  "cos://bucket/wh/db.db/t", "obs://bucket/wh/db.db/t",
            "gs://bucket/wh/db.db/t",    "gcs://bucket/wh/db.db/t", "abfs://bucket/wh/db.db/t",
            "abfss://bucket/wh/db.db/t", "az://bucket/wh/db.db/t",  "azure://bucket/wh/db.db/t"};
    for (const auto& table_path : gated_schemes) {
        TFileScanRangeParams params;
        params.properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = "DEFAULT";
        params.properties["AWS_ACCESS_KEY"] = "admin";
        params.properties["AWS_SECRET_KEY"] = "password";
        params.properties["AWS_ENDPOINT"] = "http://127.0.0.1:19001";
        params.properties["AWS_REGION"] = "us-east-1";
        params.properties["AWS_TOKEN"] = "sts-token";
        params.properties["use_path_style"] = "true";
        params.__isset.properties = true;

        PaimonRustTableReader reader;
        const auto options =
                reader.TEST_build_options(&params, make_rust_range_with_table_path(table_path));
        // No key of the s3.* / fs.oss.* families is synthesized for a scheme
        // whose parser reads neither.
        for (const char* key :
             {"s3.access-key", "s3.secret-key", "s3.session.token", "s3.endpoint", "s3.region",
              "s3.path-style-access", "s3.anonymous", "s3.assumed.role.arn",
              "s3.assumed.role.externalId", "fs.oss.endpoint", "fs.oss.accessKeyId",
              "fs.oss.accessKeySecret"}) {
            EXPECT_EQ(options.count(key), 0) << "scheme " << table_path << " synthesized " << key;
        }
        // The FE-supplied property map passes through untouched.
        EXPECT_EQ(options.at("AWS_ACCESS_KEY"), "admin");
        EXPECT_EQ(options.at("AWS_SECRET_KEY"), "password");
        EXPECT_EQ(options.at("AWS_ENDPOINT"), "http://127.0.0.1:19001");
    }
}

TEST_F(PaimonRustTableReaderTest, HdfsAndLocalSchemesPassThroughHadoopConf) {
    // hdfs:// tables read their hadoop conf as delivered (the native HDFS
    // parser has its own config keys) and credential-free local paths need
    // nothing at all: no AWS_* -> s3.* aliasing may be synthesized for
    // either, even if an S3-shaped property map is present.
    const std::vector<std::string> passthrough_schemes {"hdfs://nn/wh/db.db/t",
                                                        "/paimon/warehouse/db.db/t"};
    for (const auto& table_path : passthrough_schemes) {
        TFileScanRangeParams params;
        params.properties["AWS_ACCESS_KEY"] = "admin";
        params.properties["AWS_SECRET_KEY"] = "password";
        params.properties["AWS_ENDPOINT"] = "http://127.0.0.1:19001";
        params.__isset.properties = true;

        PaimonRustTableReader reader;
        const auto options =
                reader.TEST_build_options(&params, make_rust_range_with_table_path(table_path));
        EXPECT_EQ(options.count("s3.access-key"), 0) << table_path;
        EXPECT_EQ(options.count("s3.secret-key"), 0) << table_path;
        EXPECT_EQ(options.count("s3.endpoint"), 0) << table_path;
        EXPECT_EQ(options.count("fs.oss.accessKeyId"), 0) << table_path;
        EXPECT_EQ(options.at("AWS_ACCESS_KEY"), "admin");
    }
}

} // namespace doris::format::paimon

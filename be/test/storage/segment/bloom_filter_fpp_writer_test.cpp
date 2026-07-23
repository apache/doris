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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/encoding_info.h"
#include "storage/segment/segment_writer.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {

namespace {

constexpr std::string_view kTestDir = "./ut_dir/bloom_filter_fpp_writer_test";
constexpr std::string_view kBloomFilterCreateDebugPoint = "BloomFilterIndexWriter::create";

class ScopedBloomFilterFppDebugPoint {
public:
    explicit ScopedBloomFilterFppDebugPoint(double expected_fpp)
            : _old_enable_debug_points(config::enable_debug_points) {
        config::enable_debug_points = true;
        DebugPoints::instance()->add_with_params(std::string(kBloomFilterCreateDebugPoint),
                                                 {{"fpp", std::to_string(expected_fpp)}});
    }

    ~ScopedBloomFilterFppDebugPoint() {
        DebugPoints::instance()->remove(std::string(kBloomFilterCreateDebugPoint));
        config::enable_debug_points = _old_enable_debug_points;
    }

private:
    bool _old_enable_debug_points;
};

struct ScalarBloomFilterTypeCase {
    const char* name;
    FieldType type;
    int32_t length;
    int32_t precision;
    int32_t frac;
};

int32_t scalar_test_column_length(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        return 16;
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        return 32;
    case FieldType::OLAP_FIELD_TYPE_STRING:
        return 64;
    default:
        return cast_set<int32_t>(field_type_size(type));
    }
}

std::vector<ScalarBloomFilterTypeCase> create_scalar_bloom_filter_type_cases() {
    return {
            {"bool", FieldType::OLAP_FIELD_TYPE_BOOL,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_BOOL), -1, -1},
            {"tinyint", FieldType::OLAP_FIELD_TYPE_TINYINT,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_TINYINT), -1, -1},
            {"smallint", FieldType::OLAP_FIELD_TYPE_SMALLINT,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_SMALLINT), -1, -1},
            {"int", FieldType::OLAP_FIELD_TYPE_INT,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_INT), -1, -1},
            {"unsigned_int", FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT), -1, -1},
            {"bigint", FieldType::OLAP_FIELD_TYPE_BIGINT,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_BIGINT), -1, -1},
            {"largeint", FieldType::OLAP_FIELD_TYPE_LARGEINT,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_LARGEINT), -1, -1},
            {"char", FieldType::OLAP_FIELD_TYPE_CHAR,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_CHAR), -1, -1},
            {"varchar", FieldType::OLAP_FIELD_TYPE_VARCHAR,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_VARCHAR), -1, -1},
            {"string", FieldType::OLAP_FIELD_TYPE_STRING,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_STRING), -1, -1},
            {"date", FieldType::OLAP_FIELD_TYPE_DATE,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_DATE), -1, -1},
            {"datetime", FieldType::OLAP_FIELD_TYPE_DATETIME,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_DATETIME), -1, -1},
            {"decimal_v2", FieldType::OLAP_FIELD_TYPE_DECIMAL,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_DECIMAL), 27, 9},
            {"datev2", FieldType::OLAP_FIELD_TYPE_DATEV2,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_DATEV2), -1, -1},
            {"datetimev2", FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_DATETIMEV2), -1, 6},
            {"timestamptz", FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ), -1, 6},
            {"decimal32", FieldType::OLAP_FIELD_TYPE_DECIMAL32,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_DECIMAL32), 9, 2},
            {"decimal64", FieldType::OLAP_FIELD_TYPE_DECIMAL64,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_DECIMAL64), 18, 4},
            {"decimal128i", FieldType::OLAP_FIELD_TYPE_DECIMAL128I,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_DECIMAL128I), 38, 6},
            {"decimal256", FieldType::OLAP_FIELD_TYPE_DECIMAL256,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_DECIMAL256), 76, 10},
            {"ipv4", FieldType::OLAP_FIELD_TYPE_IPV4,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_IPV4), -1, -1},
            {"ipv6", FieldType::OLAP_FIELD_TYPE_IPV6,
             scalar_test_column_length(FieldType::OLAP_FIELD_TYPE_IPV6), -1, -1},
    };
}

const std::vector<ScalarBloomFilterTypeCase> kScalarBloomFilterTypeCases =
        create_scalar_bloom_filter_type_cases();

std::vector<ScalarBloomFilterTypeCase> create_segment_writer_compatible_type_cases() {
    std::vector<ScalarBloomFilterTypeCase> compatible_type_cases;
    compatible_type_cases.reserve(kScalarBloomFilterTypeCases.size());
    for (const auto& type_case : kScalarBloomFilterTypeCases) {
        // BF itself supports UNSIGNED_INT, but the current block/convertor stack used by
        // SegmentWriter/VerticalSegmentWriter still does not materialize that type end-to-end.
        // Keep its coverage at direct ColumnWriter level instead of failing these integration tests
        // for an unrelated legacy limitation.
        if (type_case.type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT) {
            continue;
        }
        compatible_type_cases.push_back(type_case);
    }
    return compatible_type_cases;
}

const std::vector<ScalarBloomFilterTypeCase> kSegmentWriterCompatibleBloomFilterTypeCases =
        create_segment_writer_compatible_type_cases();

Block create_single_row_default_block(const TabletSchemaSPtr& schema) {
    Block block = schema->create_block();
    for (uint32_t cid = 0; cid < schema->num_columns(); ++cid) {
        auto column = block.get_by_position(cid).column->assert_mutable();
        column->insert_default();
        block.replace_by_position(cid, std::move(column));
    }
    return block;
}

TabletIndexPB create_bloom_filter_index_pb(int64_t index_id, int32_t column_uid, double fpp) {
    TabletIndexPB index_pb;
    index_pb.set_index_id(index_id);
    index_pb.set_index_name("idx_" + std::to_string(column_uid));
    index_pb.set_index_type(IndexType::BLOOMFILTER);
    index_pb.add_col_unique_id(column_uid);
    (*index_pb.mutable_properties())["bloom_filter_fpp"] = std::to_string(fpp);
    return index_pb;
}

io::FileWriterPtr create_file_writer(std::string_view file_name) {
    io::FileWriterPtr file_writer;
    auto path = std::string(kTestDir) + "/" + std::string(file_name);
    auto st = io::global_local_filesystem()->create_file(path, &file_writer);
    EXPECT_TRUE(st.ok()) << st;
    return file_writer;
}

} // namespace

class BloomFilterFppWriterTest : public testing::Test {
protected:
    void SetUp() override {
        auto fs = io::global_local_filesystem();
        auto st = fs->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok() || st.is<ErrorCode::NOT_FOUND>()) << st;
        st = fs->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        DebugPoints::instance()->remove(std::string(kBloomFilterCreateDebugPoint));
        DebugPoints::instance()->clear();
        config::enable_debug_points = false;
    }
};

class ScalarBloomFilterFppWriterTestBase : public BloomFilterFppWriterTest {
protected:
    static TabletSchemaSPtr create_schema_with_scalar_bloom_filter_index(
            const ScalarBloomFilterTypeCase& type_case, double fpp) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::AGG_KEYS);

        auto* key_column = schema_pb.add_column();
        key_column->set_unique_id(0);
        key_column->set_name("k1");
        key_column->set_type("INT");
        key_column->set_is_key(true);
        key_column->set_is_nullable(false);
        key_column->set_length(4);
        key_column->set_index_length(4);
        key_column->set_aggregation("NONE");

        auto* value_column = schema_pb.add_column();
        value_column->set_unique_id(1);
        value_column->set_name(type_case.name);
        value_column->set_type(TabletColumn::get_string_by_field_type(type_case.type));
        value_column->set_is_key(false);
        value_column->set_is_nullable(true);
        value_column->set_length(type_case.length);
        // Use an AGG_KEYS value column so SegmentWriter won't create a zone map for it.
        // That keeps this test focused on BF creation/fpp propagation for the full BF type set.
        value_column->set_aggregation("REPLACE");
        value_column->set_is_bf_column(true);
        if (type_case.precision >= 0) {
            value_column->set_precision(type_case.precision);
        }
        if (type_case.frac >= 0) {
            value_column->set_frac(type_case.frac);
        }

        *schema_pb.add_index() = create_bloom_filter_index_pb(1, 1, fpp);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        return schema;
    }

    static TabletColumn create_scalar_bloom_filter_column(
            const ScalarBloomFilterTypeCase& type_case, int32_t unique_id) {
        TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type_case.type,
                            true, unique_id, type_case.length);
        column.set_name(type_case.name);
        column.set_is_bf_column(true);
        if (type_case.precision >= 0) {
            column.set_precision(type_case.precision);
        }
        if (type_case.frac >= 0) {
            column.set_frac(type_case.frac);
        }
        return column;
    }

    static ColumnMetaPB create_scalar_column_meta(const TabletColumn& column, uint32_t column_id) {
        ColumnMetaPB meta;
        meta.set_column_id(column_id);
        meta.set_unique_id(column.unique_id());
        meta.set_type(static_cast<int32_t>(column.type()));
        meta.set_length(column.length());
        meta.set_encoding(EncodingInfo::resolve_default_encoding(
                TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2, column));
        meta.set_compression(CompressionTypePB::LZ4F);
        meta.set_is_nullable(column.is_nullable());
        meta.set_precision(column.precision());
        meta.set_frac(column.frac());
        return meta;
    }
};

class AllScalarBloomFilterFppWriterTest
        : public ScalarBloomFilterFppWriterTestBase,
          public testing::WithParamInterface<ScalarBloomFilterTypeCase> {};

class SegmentWriterCompatibleBloomFilterFppWriterTest
        : public ScalarBloomFilterFppWriterTestBase,
          public testing::WithParamInterface<ScalarBloomFilterTypeCase> {};

TEST_P(AllScalarBloomFilterFppWriterTest, column_writer_uses_bloom_filter_fpp) {
    const auto& type_case = GetParam();
    constexpr double kExpectedFpp = 0.03;

    auto column = create_scalar_bloom_filter_column(type_case, 1);
    auto meta = create_scalar_column_meta(column, 0);

    ColumnWriterOptions writer_opts;
    writer_opts.meta = &meta;
    writer_opts.need_bloom_filter = true;
    writer_opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    writer_opts.bf_options.fpp = kExpectedFpp;

    auto file_writer = create_file_writer(std::string("column_writer_") + type_case.name + ".dat");
    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(ColumnWriter::create(writer_opts, &column, file_writer.get(), &writer).ok());

    // Cover the full scalar BF type matrix directly at ColumnWriter level, including
    // UNSIGNED_INT which cannot currently flow through the segment writer block/convertor stack.
    ScopedBloomFilterFppDebugPoint debug_point(kExpectedFpp);
    auto st = writer->init();
    ASSERT_TRUE(st.ok()) << st;
}

// SegmentWriter uses the same per-column fpp lookup regardless of scalar type, so this suite
// exercises the full integration path for every writer-compatible BF scalar type.
TEST_P(SegmentWriterCompatibleBloomFilterFppWriterTest, segment_writer_uses_index_level_fpp) {
    const auto& type_case = GetParam();
    constexpr double kExpectedFpp = 0.03;

    auto schema = create_schema_with_scalar_bloom_filter_index(type_case, kExpectedFpp);

    SegmentWriterOptions opts;
    auto file_writer = create_file_writer(std::string("segment_writer_") + type_case.name + ".dat");
    SegmentWriter writer(file_writer.get(), 0, schema, nullptr, nullptr, opts, nullptr);

    ScopedBloomFilterFppDebugPoint debug_point(kExpectedFpp);
    auto st = writer.init();
    ASSERT_TRUE(st.ok()) << st;
}

TEST_P(SegmentWriterCompatibleBloomFilterFppWriterTest,
       vertical_segment_writer_uses_index_level_fpp) {
    const auto& type_case = GetParam();
    constexpr double kExpectedFpp = 0.02;

    auto schema = create_schema_with_scalar_bloom_filter_index(type_case, kExpectedFpp);
    Block block = create_single_row_default_block(schema);

    VerticalSegmentWriterOptions opts;
    RowsetWriterContext rowset_ctx;
    rowset_ctx.write_type = DataWriteType::TYPE_DEFAULT;
    rowset_ctx.tablet_schema = schema;
    opts.rowset_ctx = &rowset_ctx;
    auto file_writer =
            create_file_writer(std::string("vertical_segment_writer_") + type_case.name + ".dat");
    VerticalSegmentWriter writer(file_writer.get(), 0, schema, nullptr, nullptr, opts, nullptr);

    ASSERT_TRUE(writer.init().ok());
    ASSERT_TRUE(writer.batch_block(&block, 0, 1).ok());

    // VerticalSegmentWriter only creates ColumnWriter instances during write_batch(), so use a
    // real one-row block here to exercise the BF fpp propagation path in _create_column_writer().
    ScopedBloomFilterFppDebugPoint debug_point(kExpectedFpp);
    auto st = writer.write_batch();
    ASSERT_TRUE(st.ok()) << st;
}

INSTANTIATE_TEST_SUITE_P(AllScalarBloomFilterTypes, AllScalarBloomFilterFppWriterTest,
                         testing::ValuesIn(kScalarBloomFilterTypeCases),
                         [](const testing::TestParamInfo<ScalarBloomFilterTypeCase>& info) {
                             return info.param.name;
                         });

INSTANTIATE_TEST_SUITE_P(SegmentWriterCompatibleBloomFilterTypes,
                         SegmentWriterCompatibleBloomFilterFppWriterTest,
                         testing::ValuesIn(kSegmentWriterCompatibleBloomFilterTypeCases),
                         [](const testing::TestParamInfo<ScalarBloomFilterTypeCase>& info) {
                             return info.param.name;
                         });

} // namespace doris::segment_v2

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

// SNII scoring must not depend on CommonGrams.
//
// CommonGrams is a phrase-query performance optimization. It needs a SEMANTIC
// view of the collection statistics because its physical postings hold gram
// tokens, so sum_total_term_freq and per-document length are not the numbers
// BM25 wants. That semantic view was introduced inside the CommonGrams segment
// metadata, and the scoring gate was written as "does this segment carry
// CommonGrams metadata" -- which made an ordinary analyzed index unscoreable.
// V1/V2/V3 score the same analyzed, position-enabled index (see regression
// test_bm25_score.groovy), so SNII was the outlier. These cases pin the aligned
// behaviour and SNII's explicit norms requirement.

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_writer.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/olap_common.h"
#include "storage/segment/column_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

using segment_v2::IndexColumnWriter;
using segment_v2::IndexFileReader;
using segment_v2::IndexFileWriter;
using segment_v2::InvertedIndexDescriptor;

namespace {

constexpr const char* kTestDir = "./ut_dir/snii_plain_index_scoring_test";
constexpr int64_t kIndexId = 9101;

// One scalar STRING column, nullable, mirroring an ordinary text table.
TabletSchemaSPtr scalar_schema() {
    auto schema = std::make_shared<TabletSchema>();
    TabletSchemaPB pb;
    pb.set_keys_type(DUP_KEYS);
    schema->init_from_pb(pb);
    TabletColumn col;
    col.set_name("body");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    col.set_length(INT_MAX);
    col.set_is_nullable(true);
    schema->append_column(col);
    return schema;
}

// ARRAY<STRING>. CommonGrams rejects ARRAY outright, so an array text column
// could never reach the scoring tier while scoring rode on CommonGrams.
TabletSchemaSPtr array_schema() {
    auto schema = std::make_shared<TabletSchema>();
    TabletSchemaPB pb;
    pb.set_keys_type(DUP_KEYS);
    schema->init_from_pb(pb);
    TabletColumn array;
    array.set_name("body");
    array.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array.set_length(0);
    array.set_index_length(0);
    array.set_is_nullable(true);
    TabletColumn child;
    child.set_name("body_item");
    child.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    child.set_length(INT_MAX);
    array.add_sub_column(child);
    schema->append_column(array);
    return schema;
}

// An ordinary built-in-parser index. No custom analyzer, no CommonGrams.
TabletIndex plain_index_meta(bool support_phrase) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("plain_idx");
    pb.add_col_unique_id(0);
    (*pb.mutable_properties())["parser"] = "english";
    (*pb.mutable_properties())["lower_case"] = "true";
    (*pb.mutable_properties())["support_phrase"] = support_phrase ? "true" : "false";
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

std::string prefix_for(std::string_view rowset_id) {
    return std::string(InvertedIndexDescriptor::get_index_file_path_prefix(
            local_segment_path(kTestDir, rowset_id, 0)));
}

std::unique_ptr<IndexFileWriter> open_writer(const std::string& prefix,
                                             std::string_view rowset_id) {
    io::FileWriterPtr file_writer;
    EXPECT_TRUE(io::global_local_filesystem()
                        ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                      &file_writer)
                        .ok());
    return std::make_unique<IndexFileWriter>(
            io::global_local_filesystem(), prefix, std::string(rowset_id), 0,
            InvertedIndexStorageFormatPB::SNII, std::move(file_writer));
}

// Writes `docs` through the production scalar path and returns the index prefix.
// A row listed in `null_rows` is written as SQL NULL, so a caller can interleave
// null runs with data runs.
std::string write_scalar_segment(std::string_view rowset_id, const TabletIndex& meta,
                                 const std::vector<std::string>& docs,
                                 const std::set<size_t>& null_rows = {}) {
    const std::string prefix = prefix_for(rowset_id);
    auto inner = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    for (size_t row = 0; row < docs.size(); ++row) {
        const auto& doc = docs[row];
        inner->insert_data(doc.data(), doc.size());
        null_map->insert_value(null_rows.contains(row) ? 1 : 0);
    }
    ColumnPtr column = ColumnNullable::create(std::move(inner), std::move(null_map));
    Block block;
    block.insert({column, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
                  "body"});

    TabletSchemaSPtr schema = scalar_schema();
    auto index_file_writer = open_writer(prefix, rowset_id);
    std::unique_ptr<IndexColumnWriter> builder;
    EXPECT_TRUE(
            IndexColumnWriter::create(&schema->column(0), &builder, index_file_writer.get(), &meta)
                    .ok());

    OlapBlockDataConvertor convertor(schema.get(), {0});
    convertor.set_source_content(&block, 0, block.rows());
    auto [status, accessor] = convertor.convert_column_data(0);
    EXPECT_TRUE(status.ok()) << status;
    // Mirrors ColumnWriter::append_nullable: null runs go to add_nulls(), data
    // runs to add_values(). Both must advance the norms vector by their length.
    const auto* row_null_map = accessor->get_nullmap();
    const auto* data = reinterpret_cast<const uint8_t*>(accessor->get_data());
    size_t offset = 0;
    while (offset < block.rows()) {
        const bool is_null = row_null_map != nullptr && row_null_map[offset] != 0;
        size_t run = 1;
        while (offset + run < block.rows() &&
               ((row_null_map != nullptr && row_null_map[offset + run] != 0) == is_null)) {
            ++run;
        }
        if (is_null) {
            EXPECT_TRUE(builder->add_nulls(static_cast<uint32_t>(run)).ok());
        } else {
            EXPECT_TRUE(builder->add_values("body", data + offset * sizeof(Slice), run).ok());
        }
        offset += run;
    }
    EXPECT_TRUE(builder->finish().ok());
    EXPECT_TRUE(index_file_writer->begin_close().ok());
    EXPECT_TRUE(index_file_writer->finish_close().ok());
    return prefix;
}

class SniiPlainIndexScoring : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
    }
};

} // namespace

// The whole point: an ordinary analyzed SNII index carries scoring data.
TEST_F(SniiPlainIndexScoring, PlainAnalyzedIndexOpensTheScoringStatsProvider) {
    const TabletIndex meta = plain_index_meta(/*support_phrase=*/true);
    // Four documents, 24 tokens total -> avgdl 6.
    const std::string prefix = write_scalar_segment(
            "plain_rs", meta,
            {"alpha beta gamma delta epsilon zeta", "alpha beta gamma delta epsilon zeta",
             "alpha beta gamma delta epsilon zeta", "alpha beta gamma delta epsilon zeta"});

    IndexFileReader reader(io::global_local_filesystem(), prefix,
                           InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(reader.init().ok());
    auto logical = reader.open_snii_index(&meta);
    ASSERT_TRUE(logical.has_value()) << logical.error();

    doris::snii::stats::SniiStatsProvider stats;
    const Status status =
            doris::snii::stats::SniiStatsProvider::open(logical.value().get(), &stats);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_TRUE(stats.has_norms()) << "an analyzed index must persist per-document norms";
    EXPECT_DOUBLE_EQ(stats.avgdl(), 6.0);
    uint64_t df = 0;
    ASSERT_TRUE(stats.doc_freq("alpha", &df).ok());
    EXPECT_EQ(df, 4U);

    uint8_t norm = 0;
    ASSERT_TRUE(stats.encoded_norm(0, &norm).ok());
    EXPECT_EQ(norm, 6U) << "the norm must encode the document's token count";
}

// The riskiest invariant this change touches: norms are per ROW, and a null run
// goes through add_nulls() rather than the token path. One missed push and the
// vector desyncs -- every later document would be scored with a neighbour's length.
TEST_F(SniiPlainIndexScoring, NullRunsKeepOneNormPerDocument) {
    const TabletIndex meta = plain_index_meta(/*support_phrase=*/true);
    // 6 rows: data, NULL, NULL, data, NULL, data -- runs on both sides.
    const std::string prefix = write_scalar_segment(
            "nulls_rs", meta,
            {"alpha beta gamma", "", "", "alpha beta", "", "alpha beta gamma delta"},
            /*null_rows=*/ {1, 2, 4});

    IndexFileReader reader(io::global_local_filesystem(), prefix,
                           InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(reader.init().ok());
    auto logical = reader.open_snii_index(&meta);
    ASSERT_TRUE(logical.has_value()) << logical.error();

    doris::snii::stats::SniiStatsProvider stats;
    const Status status =
            doris::snii::stats::SniiStatsProvider::open(logical.value().get(), &stats);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(stats.has_norms());

    const std::vector<uint64_t> expected {3, 0, 0, 2, 0, 4};
    for (uint32_t docid = 0; docid < expected.size(); ++docid) {
        uint8_t norm = 0;
        ASSERT_TRUE(stats.encoded_norm(docid, &norm).ok()) << "docid " << docid;
        EXPECT_EQ(norm, doris::snii::query::encode_norm(expected[docid]))
                << "norm desynced at docid " << docid;
    }
    uint8_t past_end = 0;
    EXPECT_FALSE(stats.encoded_norm(static_cast<uint32_t>(expected.size()), &past_end).ok())
            << "the norms vector outlives the document count";

    // avgdl divides by ALL rows, the same rows the norms span: 9 tokens / 6 docs.
    EXPECT_EQ(logical.value()->stats().doc_count, 6);
    EXPECT_EQ(logical.value()->stats().sum_total_term_freq, 9);
    EXPECT_DOUBLE_EQ(stats.avgdl(), 1.5);
}

// CommonGrams rejects ARRAY fields, so arrays were unscoreable on SNII while
// V1/V2/V3 scored them.
TEST_F(SniiPlainIndexScoring, PlainAnalyzedArrayIndexOpensTheScoringStatsProvider) {
    const TabletIndex meta = plain_index_meta(/*support_phrase=*/true);
    const std::string prefix = prefix_for("array_rs");

    DataTypePtr item = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr array_type = std::make_shared<DataTypeArray>(item);
    MutableColumnPtr nested = array_type->create_column();
    for (int row = 0; row < 4; ++row) {
        Array value;
        value.push_back(Field::create_field<TYPE_STRING>(std::string("alpha beta")));
        value.push_back(Field::create_field<TYPE_STRING>(std::string("gamma delta")));
        nested->insert(Field::create_field<TYPE_ARRAY>(value));
    }
    auto null_map = ColumnUInt8::create();
    for (int row = 0; row < 4; ++row) {
        null_map->insert_value(0);
    }
    ColumnPtr column = ColumnNullable::create(std::move(nested), std::move(null_map));
    Block block;
    block.insert({column, std::make_shared<DataTypeNullable>(array_type), "body"});

    TabletSchemaSPtr schema = array_schema();
    auto index_file_writer = open_writer(prefix, "array_rs");
    std::unique_ptr<IndexColumnWriter> builder;
    ASSERT_TRUE(
            IndexColumnWriter::create(&schema->column(0), &builder, index_file_writer.get(), &meta)
                    .ok());

    OlapBlockDataConvertor convertor(schema.get(), {0});
    convertor.set_source_content(&block, 0, block.rows());
    auto [status, accessor] = convertor.convert_column_data(0);
    ASSERT_TRUE(status.ok()) << status;
    const auto* data_ptr = reinterpret_cast<const uint64_t*>(accessor->get_data());
    ASSERT_TRUE(
            builder->add_array_values(field_type_size(schema->column(0).get_sub_column(0).type()),
                                      reinterpret_cast<const void*>(data_ptr[2]),
                                      reinterpret_cast<const uint8_t*>(data_ptr[3]),
                                      reinterpret_cast<const uint8_t*>(data_ptr[1]), block.rows())
                    .ok());
    ASSERT_TRUE(builder->add_array_nulls(accessor->get_nullmap(), block.rows()).ok());
    ASSERT_TRUE(builder->finish().ok());
    ASSERT_TRUE(index_file_writer->begin_close().ok());
    ASSERT_TRUE(index_file_writer->finish_close().ok());

    IndexFileReader reader(io::global_local_filesystem(), prefix,
                           InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(reader.init().ok());
    auto logical = reader.open_snii_index(&meta);
    ASSERT_TRUE(logical.has_value()) << logical.error();

    doris::snii::stats::SniiStatsProvider stats;
    const Status open_status =
            doris::snii::stats::SniiStatsProvider::open(logical.value().get(), &stats);
    ASSERT_TRUE(open_status.ok()) << open_status;
    EXPECT_TRUE(stats.has_norms());
    EXPECT_DOUBLE_EQ(stats.avgdl(), 4.0);
}

TEST_F(SniiPlainIndexScoring, OuterNullArrayPayloadDoesNotAffectScoring) {
    const TabletIndex index_meta = plain_index_meta(/*support_phrase=*/true);
    const std::string prefix = prefix_for("outer_null_array_rs");

    DataTypePtr item_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr array_type = std::make_shared<DataTypeArray>(item_type);
    MutableColumnPtr nested = array_type->create_column();
    for (const auto& text : {"alpha beta", "poison poison poison", "gamma"}) {
        Array value;
        value.push_back(Field::create_field<TYPE_STRING>(std::string(text)));
        nested->insert(Field::create_field<TYPE_ARRAY>(value));
    }
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(1);
    null_map->insert_value(0);
    ColumnPtr column = ColumnNullable::create(std::move(nested), std::move(null_map));
    Block block;
    block.insert({column, std::make_shared<DataTypeNullable>(array_type), "body"});

    TabletSchemaSPtr schema = array_schema();
    io::FileWriterPtr data_file_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(fmt::format("{}/outer_null_array.dat", kTestDir),
                                      &data_file_writer)
                        .ok());
    auto index_file_writer = open_writer(prefix, "outer_null_array_rs");

    segment_v2::ColumnMetaPB column_meta;
    column_meta.set_column_id(0);
    column_meta.set_unique_id(0);
    column_meta.set_type(int(FieldType::OLAP_FIELD_TYPE_ARRAY));
    column_meta.set_length(0);
    column_meta.set_encoding(segment_v2::PLAIN_ENCODING);
    column_meta.set_compression(segment_v2::CompressionTypePB::LZ4F);
    column_meta.set_is_nullable(true);
    auto* item_meta = column_meta.add_children_columns();
    item_meta->set_column_id(0);
    item_meta->set_unique_id(0);
    item_meta->set_type(int(FieldType::OLAP_FIELD_TYPE_STRING));
    item_meta->set_length(INT_MAX);
    item_meta->set_encoding(segment_v2::PLAIN_ENCODING);
    item_meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
    item_meta->set_is_nullable(true);

    segment_v2::ColumnWriterOptions writer_options;
    writer_options.meta = &column_meta;
    writer_options.need_inverted_index = true;
    writer_options.inverted_indexes = {&index_meta};
    writer_options.index_file_writer = index_file_writer.get();
    writer_options.file_writer = data_file_writer.get();
    writer_options.compression_type = segment_v2::CompressionTypePB::LZ4F;
    std::unique_ptr<segment_v2::ColumnWriter> writer;
    ASSERT_TRUE(segment_v2::ColumnWriter::create(writer_options, &schema->column(0),
                                                 data_file_writer.get(), &writer)
                        .ok());
    ASSERT_TRUE(writer->init().ok());

    OlapBlockDataConvertor convertor(schema.get(), {0});
    convertor.set_source_content(&block, 0, block.rows());
    auto [convert_status, accessor] = convertor.convert_column_data(0);
    ASSERT_TRUE(convert_status.ok()) << convert_status;
    const auto* array_data = reinterpret_cast<const uint8_t*>(accessor->get_data());
    ASSERT_TRUE(writer->append_nullable(accessor->get_nullmap(), &array_data, block.rows()).ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_inverted_index().ok());
    ASSERT_TRUE(index_file_writer->begin_close().ok());
    ASSERT_TRUE(index_file_writer->finish_close().ok());
    ASSERT_TRUE(data_file_writer->close().ok());

    IndexFileReader reader(io::global_local_filesystem(), prefix,
                           InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(reader.init().ok());
    auto logical = reader.open_snii_index(&index_meta);
    ASSERT_TRUE(logical.has_value()) << logical.error();

    doris::snii::stats::SniiStatsProvider stats;
    ASSERT_TRUE(doris::snii::stats::SniiStatsProvider::open(logical.value().get(), &stats).ok());
    EXPECT_EQ(logical.value()->stats().doc_count, 3);
    EXPECT_EQ(logical.value()->stats().sum_total_term_freq, 3);
    EXPECT_DOUBLE_EQ(stats.avgdl(), 1.0);
    uint64_t poison_df = 0;
    ASSERT_TRUE(stats.doc_freq("poison", &poison_df).ok());
    EXPECT_EQ(poison_df, 0);
    uint8_t null_norm = 0;
    ASSERT_TRUE(stats.encoded_norm(1, &null_norm).ok());
    EXPECT_EQ(null_norm, doris::snii::query::encode_norm(0));
}

// Guard against over-reach: without positions there is no scoring tier, exactly
// as on V1/V2/V3 where is_need_similarity_score() requires support_phrase.
TEST_F(SniiPlainIndexScoring, IndexWithoutPositionsStaysUnscoreable) {
    const TabletIndex meta = plain_index_meta(/*support_phrase=*/false);
    const std::string prefix =
            write_scalar_segment("nophrase_rs", meta, {"alpha beta", "alpha gamma"});

    IndexFileReader reader(io::global_local_filesystem(), prefix,
                           InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(reader.init().ok());
    auto logical = reader.open_snii_index(&meta);
    ASSERT_TRUE(logical.has_value()) << logical.error();
    EXPECT_FALSE(logical.value()->has_positions());

    doris::snii::stats::SniiStatsProvider stats;
    EXPECT_FALSE(doris::snii::stats::SniiStatsProvider::open(logical.value().get(), &stats).ok());
}

} // namespace doris

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
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_leaf_visitor.h"
#include "exprs/function/match.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/index/inverted/snii/snii_index_reader.h"
#include "storage/index/inverted/snii/snii_query_oracle.h"
#include "testutil/index_storage_test_util.h"

namespace doris::segment_v2 {
namespace {

TEST(VariantValuesIndexTest, ScopeIsExactAndCaseInsensitive) {
    for (const auto& [scope, expected] : {std::pair {"values", true},
                                          {"VALUES", true},
                                          {" values ", false},
                                          {"paths", false},
                                          {"", false}}) {
        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        (*pb.mutable_properties())["variant_index_scope"] = scope;
        TabletIndex index;
        index.init_from_pb(pb);
        EXPECT_EQ(index.is_variant_values_index(), expected) << scope;
        pb.set_index_type(IndexType::NGRAM_BF);
        index.init_from_pb(pb);
        EXPECT_FALSE(index.is_variant_values_index());
    }
}

ColumnVariantV2::MutablePtr encode_documents(const std::vector<std::string>& docs) {
    JsonStringToVariantEncoder encoder;
    for (const auto& json : docs) {
        encoder.add_json({json.data(), json.size()});
    }
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(encoder.finish_batch());
    return column;
}

class VariantValuesSegmentTest : public index_storage_test::IndexStorageTestFixture {
protected:
    void check_layout(bool doc, int buckets, int limit, int64_t id);
    void check_match(const ColumnPtr& values, const TabletIndex* meta,
                     const std::shared_ptr<IndexFileReader>& file);
};

// One table specifies final visible leaf text; the fixture also crosses the 4096-row write batch.
// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity): keep the input/expected table and actual segment readback together.
void VariantValuesSegmentTest::check_layout(bool doc, int buckets, int limit, int64_t id) {
    using namespace index_storage_test;
    SCOPED_TRACE(testing::Message()
                 << "doc=" << doc << " buckets=" << buckets << " limit=" << limit);
    VariantColumnSpec variant;
    variant.nullable = true;
    variant.enable_doc_mode = doc;
    variant.max_subcolumns_count = limit;
    variant.sparse_hash_shard_count = buckets;
    variant.doc_hash_shard_count = buckets;
    VariantPathSpec typed;
    typed.path = "n";
    typed.type = FieldType::OLAP_FIELD_TYPE_INT;
    variant.predefined_paths = {typed};
    IndexTabletOptions options;
    options.tablet_id = 120200 + id;
    options.variant_columns = {variant};
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.inverted_indexes = {IndexSpec::column_index(72, "values", 2,
                                                        {{"variant_index_scope", "values"},
                                                         {"parser", "english"},
                                                         {"support_phrase", "false"}})};
    ASSERT_TRUE(create_tablet(options).ok());
    const std::vector<std::pair<std::string, std::vector<std::string>>> cases = {
            {R"({"a":"hidden","a.b":"visible","n":1})", {"visible", "1"}},
            {R"({"a":"kept","array":[1,"hello","hello",{"z":"world"}]})",
             {"kept", "1", "hello", "hello", "world"}},
            {R"({"n":2,"float":42.0,"zero":-0.0})", {"2", "42", "0"}},
            {"{}", {}},
            {"[]", {}},
            {"null", {}},
            {R"("root scalar")", {"root scalar"}},
            {R"({"empty":[],"obj":{"key":"deep"},"flag":true})", {"deep", "true"}},
            // Whole-root DOC assembly retains this input string; ORDINARY applies the typed INT path.
            {R"({"n":"0042","flag":false})", {doc ? "0042" : "42", "false"}},
            // The shared path promotes 42 to DECIMAL16; its indexed text must remain 42.
            {R"({"wide":42})", {"42"}},
            {R"({"wide":18446744073709551615})", {"18446744073709551615"}}};
    std::vector<std::string> docs;
    for (size_t row = 0; row < 4101; ++row) {
        docs.push_back(cases[row % cases.size()].first);
    }
    auto nulls = ColumnUInt8::create();
    nulls->insert_many_defaults(docs.size());
    nulls->get_data()[5] = nulls->get_data()[4096] = 1;
    IndexRowsetSpec spec;
    spec.version = 0;
    spec.max_rows_per_segment = 8192;
    spec.batches.push_back(IndexBatch::single_variant_column(
            ColumnNullable::create(encode_documents(docs), nulls->get_ptr())));
    auto rowset = write_rowset(spec);
    ASSERT_TRUE(rowset.has_value()) << rowset.error();
    IndexReadOptions read_options;
    read_options.collect_variant_values = true;
    auto assembled = read_rowsets({*rowset}, read_options);
    ASSERT_TRUE(assembled.has_value()) << assembled.error();
    const auto& persisted = assembled->variant_values_by_uid.at(2);
    ASSERT_EQ(persisted.size(), docs.size());
    for (size_t row = 0; row < docs.size(); ++row) {
        EXPECT_EQ(persisted[row].has_value(), nulls->get_data()[row] == 0);
        docs[row] = persisted[row].value_or("{}");
    }
    auto read_values = encode_documents(docs);
    const auto view = read_values->read_view();
    std::map<std::string, std::vector<uint32_t>> expected_postings;
    for (size_t row = 0; row < docs.size(); ++row) {
        if (nulls->get_data()[row]) {
            continue;
        }
        auto expected = cases[row % cases.size()].second;
        std::vector<std::string> actual;
        ASSERT_TRUE(for_each_variant_leaf_text(view.value_at(row), [&](std::string_view text) {
                        actual.emplace_back(text);
                        return Status::OK();
                    }).ok());
        std::ranges::sort(actual);
        std::ranges::sort(expected);
        EXPECT_EQ(actual, expected) << "row=" << row;
        std::set<std::string> terms;
        for (const auto& text : expected) {
            std::istringstream input(text);
            for (std::string term; input >> term;) {
                terms.insert(term);
            }
        }
        for (const auto& term : terms) {
            expected_postings[term].push_back(row);
        }
    }
    ASSERT_EQ((*rowset)->num_segments(), 1);
    const auto segment = (*rowset)->segment(0);
    auto path = segment.path();
    ASSERT_TRUE(path.has_value());
    auto file = std::make_shared<IndexFileReader>(
            (*rowset)->rowset_meta()->fs(),
            std::string(InvertedIndexDescriptor::get_index_file_path_prefix(*path)),
            InvertedIndexStorageFormatPB::SNII, segment.inverted_index_file_info());
    ASSERT_TRUE(file->init().ok());
    const auto* meta = (*rowset)->tablet_schema()->inverted_indexs(2).front();
    auto index = file->open_snii_index(meta);
    ASSERT_TRUE(index.has_value()) << index.error();
    EXPECT_EQ((*index)->stats().doc_count, docs.size());
    EXPECT_EQ((*index)->stats().null_count, 2);
    EXPECT_EQ((*index)->stats().indexed_doc_count, docs.size() - 2);
    expected_postings["hidden"] = {};
    expected_postings["key"] = {};
    for (const auto& [term, expected] : expected_postings) {
        std::vector<uint32_t> actual;
        ASSERT_TRUE(snii::query::term_query(**index, term, &actual).ok());
        EXPECT_EQ(actual, expected) << term;
    }
    check_match(ColumnNullable::create(std::move(read_values), nulls->get_ptr()), meta, file);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): compare real SNII ANY/ALL with the residual expression on the same assembled rows.
void VariantValuesSegmentTest::check_match(const ColumnPtr& values, const TabletIndex* meta,
                                           const std::shared_ptr<IndexFileReader>& file) {
    TQueryOptions options;
    options.__set_enable_match_without_inverted_index(true);
    options.__set_enable_inverted_index_query_cache(false);
    options.__set_enable_inverted_index_searcher_cache(false);
    RuntimeState state(options, TQueryGlobals {});
    auto context = FunctionContext::create_context(&state, {}, {});
    auto analyzer = std::make_shared<InvertedIndexAnalyzerCtx>();
    analyzer->parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    InvertedIndexAnalyzerConfig config;
    config.parser_type = analyzer->parser_type;
    analyzer->analyzer = inverted_index::InvertedIndexAnalyzer::create_analyzer(&config);
    context->set_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL, analyzer);
    auto reader = SniiIndexReader::create_shared(meta, file, InvertedIndexReaderType::FULLTEXT,
                                                 values->size(), false);
    OlapReaderStatistics stats;
    io::IOContext io_context;
    auto query_context = std::make_shared<IndexQueryContext>();
    query_context->stats = &stats;
    query_context->io_ctx = &io_context;
    query_context->runtime_state = &state;
    FunctionMatchAny any;
    FunctionMatchAll all;
    for (const FunctionMatchBase* function :
         {static_cast<FunctionMatchBase*>(&any), static_cast<FunctionMatchBase*>(&all)}) {
        for (const std::string query : {"hello world", "42", "true false", "hidden"}) {
            auto text = ColumnString::create();
            text->insert_data(query.data(), query.size());
            Block block;
            block.insert({values, make_nullable(std::make_shared<DataTypeVariantV2>()), "v"});
            block.insert({std::move(text), std::make_shared<DataTypeString>(), "query"});
            auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
            block.insert({result_type->create_column(), result_type, "result"});
            ASSERT_TRUE(function->execute(context.get(), block, {0, 1}, 2, values->size()).ok());
            const auto& result =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
            const auto& data =
                    assert_cast<const ColumnUInt8&>(result.get_nested_column()).get_data();
            std::vector<uint32_t> scanned;
            for (size_t row = 0; row < data.size(); ++row) {
                if (!result.is_null_at(row) && data[row]) {
                    scanned.push_back(row);
                }
            }
            std::shared_ptr<roaring::Roaring> bitmap;
            InvertedIndexQueryCacheHandle null_bitmap;
            ASSERT_TRUE(reader->query_with_null_bitmap(query_context, "v",
                                                       Field::create_field<TYPE_STRING>(query),
                                                       function->get_query_type_from_fn_name(),
                                                       bitmap, &null_bitmap, analyzer.get())
                                .ok());
            ASSERT_NE(bitmap, nullptr);
            EXPECT_EQ(std::vector<uint32_t>(bitmap->begin(), bitmap->end()), scanned) << query;
            ASSERT_NE(null_bitmap.get_bitmap(), nullptr);
            EXPECT_TRUE(null_bitmap.get_bitmap()->contains(5));
            EXPECT_TRUE(null_bitmap.get_bitmap()->contains(4096));
        }
    }
    // A segment can lack this logical index even though its SNII container is present.
    TabletIndexPB pb;
    meta->to_schema_pb(&pb);
    pb.set_index_id(73);
    TabletIndex missing;
    missing.init_from_pb(pb);
    auto missing_reader = SniiIndexReader::create_shared(
            &missing, file, InvertedIndexReaderType::FULLTEXT, values->size(), false);
    std::shared_ptr<roaring::Roaring> bitmap;
    InvertedIndexQueryCacheHandle null_bitmap;
    EXPECT_TRUE(
            missing_reader
                    ->query_with_null_bitmap(query_context, "v",
                                             Field::create_field<TYPE_STRING>(std::string("hello")),
                                             InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap,
                                             &null_bitmap, analyzer.get())
                    .is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>());
}

TEST_F(VariantValuesSegmentTest, FinalValuesAndResidualMatchAcrossLayouts) {
    int64_t id = 0;
    for (bool doc : {false, true}) {
        for (int buckets : {1, 3}) {
            for (int limit : {1, 8}) {
                check_layout(doc, buckets, limit, id++);
            }
        }
    }
}
} // namespace
} // namespace doris::segment_v2

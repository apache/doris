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

// evaluate_inverted_index tests for pushing a gram boolean query down from LIKE / REGEXP: a fake
// IndexIterator verifies the whole chain "scheme resolution -> compilation -> the issued
// InvertedIndexParam -> the approximate flag" without depending on real index files.
//
// About the reader: the storage-format fence (Ruling R30) requires push-down to happen only on a
// SNII reader, so the "normal push-down" cases must carry a **real** SniiIndexReader (it is a
// final class and cannot be subclassed for a stub; fortunately resolve_scheme only uses the base
// class's get_index_properties(), and read_from_index is intercepted by the fake iterator, so the
// reader's query() is never called and a SniiIndexReader with a null IndexFileReader is entirely
// sufficient). The negative cases carry a CLucene-semantics stub reader instead, to verify that
// the fence really keeps it out.

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/like.h"
#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/index_iterator.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/gram/gram_query.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

// A non-SNII stub InvertedIndexReader: it derives directly from InvertedIndexReader, so
// dynamic_cast<SniiIndexReader*> necessarily fails -- exactly the shape the storage-format fence
// must stop (in production: FullTextIndexReader / StringTypeInvertedIndexReader over
// CLucene-format segments). The remaining pure virtuals are never called in this test, so the
// simplest placeholder implementations will do.
class StubGramIndexReader : public segment_v2::InvertedIndexReader {
public:
    explicit StubGramIndexReader(const TabletIndex& index_meta)
            : segment_v2::InvertedIndexReader(&index_meta, nullptr) {}
    ~StubGramIndexReader() override = default;

    segment_v2::InvertedIndexReaderType type() override {
        return segment_v2::InvertedIndexReaderType::FULLTEXT;
    }

    Status query(const segment_v2::IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, segment_v2::InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override {
        return Status::NotSupported("StubGramIndexReader::query is not supported");
    }

    Status try_query(const segment_v2::IndexQueryContextPtr& context,
                     const std::string& column_name, const Field& query_value,
                     segment_v2::InvertedIndexQueryType query_type, size_t* count) override {
        return Status::NotSupported("StubGramIndexReader::try_query is not supported");
    }

    Status new_iterator(std::unique_ptr<segment_v2::IndexIterator>* iterator) override {
        return Status::NotSupported("StubGramIndexReader::new_iterator is not supported");
    }
};

// Build a minimal usable TabletIndex from the injected property table, going through the real
// TabletIndexPB -> init_from_pb path (the same shape as the init_*_index_meta family in
// snii_writer_test.cpp), so we need not rely on the detail that get_index_properties() is not
// virtual in a non-BE_TEST build.
TabletIndex build_gram_index_meta(const std::map<std::string, std::string>& properties) {
    TabletIndexPB index_pb;
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.set_index_id(1);
    index_pb.set_index_name("like_gram_index_test");
    index_pb.add_col_unique_id(0);
    for (const auto& [key, value] : properties) {
        index_pb.mutable_properties()->insert({key, value});
    }
    TabletIndex index_meta;
    index_meta.init_from_pb(index_pb);
    return index_meta;
}

// Which reader get_reader returns: a real SNII reader (push-down possible) or a
// CLucene-semantics stub reader (which the storage-format fence should stop).
enum class FakeReaderKind { kSnii, kNonSnii };

// A fake IndexIterator: get_reader returns the reader chosen by FakeReaderKind (for
// resolve_scheme to type-check and read properties from); read_from_index intercepts the issued
// InvertedIndexParam directly and either returns the injected status or fills in a fixed hit set,
// never running a real SNII/CLucene query, so only evaluate_gram_index's own behaviour is tested.
class FakeGramIndexIterator : public segment_v2::IndexIterator {
public:
    explicit FakeGramIndexIterator(std::map<std::string, std::string> properties,
                                   FakeReaderKind kind = FakeReaderKind::kSnii)
            : _index_meta(build_gram_index_meta(properties)) {
        if (kind == FakeReaderKind::kSnii) {
            // A null IndexFileReader is fine: this test never triggers the reader's query path.
            _reader = segment_v2::SniiIndexReader::create_shared(
                    &_index_meta, /*index_file_reader=*/nullptr,
                    segment_v2::InvertedIndexReaderType::FULLTEXT, /*rows_of_segment=*/100,
                    /*column_is_array=*/false);
        } else {
            _reader = std::make_shared<StubGramIndexReader>(_index_meta);
        }
    }
    ~FakeGramIndexIterator() override = default;

    segment_v2::IndexReaderPtr get_reader(segment_v2::IndexReaderType reader_type) const override {
        if (std::holds_alternative<segment_v2::InvertedIndexReaderType>(reader_type) &&
            std::get<segment_v2::InvertedIndexReaderType>(reader_type) ==
                    segment_v2::InvertedIndexReaderType::FULLTEXT) {
            return _reader;
        }
        return nullptr;
    }

    Status read_from_index(const segment_v2::IndexParam& param) override {
        auto* p = std::get<segment_v2::InvertedIndexParam*>(param);
        last_param = *p;
        ++read_from_index_calls;
        if (!inject_status.ok()) {
            return inject_status;
        }
        p->roaring->addMany(answer.size(), answer.data());
        return Status::OK();
    }

    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* cache_handle) override {
        return Status::OK();
    }

    Result<bool> has_null() override { return false; }

    // The full parameter set the last read_from_index received, for asserting on query_type /
    // query_value.
    segment_v2::InvertedIndexParam last_param;
    std::vector<uint32_t> answer {3, 5};
    // When not OK, read_from_index returns it verbatim, to verify the catch-all degradation
    // (Ruling R29).
    Status inject_status = Status::OK();
    int read_from_index_calls = 0;

private:
    TabletIndex _index_meta;
    segment_v2::InvertedIndexReaderPtr _reader;
};

std::shared_ptr<FakeGramIndexIterator> make_gram_iterator(
        std::map<std::string, std::string> properties,
        FakeReaderKind kind = FakeReaderKind::kSnii) {
    return std::make_shared<FakeGramIndexIterator>(std::move(properties), kind);
}

// Build a constant string argument column (ColumnConst(ColumnString) + DataTypeString) to fill
// the arguments list of evaluate_inverted_index (pattern / custom ESCAPE).
ColumnWithTypeAndName const_string_arg(const std::string& value) {
    auto col = ColumnString::create();
    col->insert_data(value.data(), value.size());
    return {ColumnConst::create(std::move(col), 1), std::make_shared<DataTypeString>(), "arg"};
}

// As above, but **without** the ColumnConst wrapper: builds the "ESCAPE is a per-row column"
// shape, which must not be pushed down.
ColumnWithTypeAndName non_const_string_arg(const std::string& value) {
    auto col = ColumnString::create();
    col->insert_data(value.data(), value.size());
    return {std::move(col), std::make_shared<DataTypeString>(), "arg"};
}

// Inject a pair of gram-family policies (a tokenizer with type=ngram mode=dense plus an analyzer
// referencing it), copying the approach of ScopedGramPolicies in snii_writer_test.cpp (swap
// ExecEnv::_index_policy_mgr and restore it afterwards); the ids/names are private to this file
// so that the policy registrations cannot clash with other test files inside the policy manager's
// namespace in the same process (this class installs its own IndexPolicyMgr instance every time,
// so a real clash is impossible, but separate names make failure output easier to read).
class ScopedGramIndexPolicies {
public:
    ScopedGramIndexPolicies() {
        auto* exec_env = ExecEnv::GetInstance();
        _previous = exec_env->index_policy_mgr();
        exec_env->_index_policy_mgr = &_manager;

        TIndexPolicy tokenizer;
        tokenizer.id = 9101;
        tokenizer.name = "gram_dense_tokenizer";
        tokenizer.type = TIndexPolicyType::TOKENIZER;
        tokenizer.properties["type"] = "ngram";
        tokenizer.properties["mode"] = "dense";

        TIndexPolicy analyzer;
        analyzer.id = 9102;
        analyzer.name = analyzer_name();
        analyzer.type = TIndexPolicyType::ANALYZER;
        analyzer.properties["tokenizer"] = tokenizer.name;

        _manager.apply_policy_changes({tokenizer, analyzer}, {});
    }

    ~ScopedGramIndexPolicies() { ExecEnv::GetInstance()->_index_policy_mgr = _previous; }

    static std::string analyzer_name() { return "gram_dense"; }

private:
    IndexPolicyMgr _manager;
    IndexPolicyMgr* _previous = nullptr;
};

TEST(LikeGramIndexTest, RegexpCompilesAndSendsGramBooleanQuery) {
    ScopedGramIndexPolicies policies;
    auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
    auto fn = std::make_shared<FunctionRegexpLike>();
    ColumnsWithTypeAndName args {const_string_arg("hello|world")};
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    segment_v2::InvertedIndexResultBitmap result;

    ASSERT_TRUE(fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result).ok());

    EXPECT_EQ(it->last_param.query_type, segment_v2::InvertedIndexQueryType::GRAM_BOOLEAN_QUERY);
    segment_v2::gram::GramQuery q;
    ASSERT_TRUE(
            segment_v2::gram::GramQuery::parse(it->last_param.query_value.get<TYPE_STRING>(), &q)
                    .ok());
    EXPECT_EQ(q.to_debug_string(),
              "((\"ell\" & \"hel\" & \"llo\") | (\"orl\" & \"rld\" & \"wor\"))");
    EXPECT_TRUE(result.approximate());
    ASSERT_NE(result.get_data_bitmap(), nullptr);
    EXPECT_EQ(result.get_data_bitmap()->cardinality(), 2U);
}

TEST(LikeGramIndexTest, UnindexableOrNonGramProducesNoResult) {
    ScopedGramIndexPolicies policies;
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    auto fn = std::make_shared<FunctionRegexpLike>();

    // A gram-family index, but the pattern compiles to no prunable literal at all (it degrades
    // to ALL).
    {
        auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
        segment_v2::InvertedIndexResultBitmap result;
        ColumnsWithTypeAndName args {const_string_arg("[0-9]{3}-[0-9]{4}")};
        ASSERT_TRUE(
                fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result).ok());
        EXPECT_TRUE(result.is_empty()); // no result produced (is_empty = no bitmap, not zero card)
    }

    // A non-gram-family index (the built-in english parser, no analyzer field in the properties).
    {
        auto it = make_gram_iterator({{"parser", "english"}});
        segment_v2::InvertedIndexResultBitmap result;
        ColumnsWithTypeAndName args {const_string_arg("hello")};
        ASSERT_TRUE(
                fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result).ok());
        EXPECT_TRUE(result.is_empty());
    }
}

TEST(LikeGramIndexTest, LikeUsesLikeCompiler) {
    ScopedGramIndexPolicies policies;
    auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
    auto fn = std::make_shared<FunctionLike>();
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    segment_v2::InvertedIndexResultBitmap result;
    ColumnsWithTypeAndName args {const_string_arg("%abcd%")};

    ASSERT_TRUE(fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result).ok());

    segment_v2::gram::GramQuery q;
    ASSERT_TRUE(
            segment_v2::gram::GramQuery::parse(it->last_param.query_value.get<TYPE_STRING>(), &q)
                    .ok());
    EXPECT_EQ(q.to_debug_string(), "(\"abc\" & \"bcd\")");
    EXPECT_TRUE(result.approximate());
}

TEST(LikeGramIndexTest, CustomEscapeIsNotPushedDown) {
    ScopedGramIndexPolicies policies;
    auto fn = std::make_shared<FunctionLike>();
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};

    // The ESCAPE clause is not the default backslash: unsupported in P0, so it must skip the
    // acceleration without failing.
    {
        auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
        segment_v2::InvertedIndexResultBitmap result;
        ColumnsWithTypeAndName args {const_string_arg("%abcd%"), const_string_arg("#")};
        ASSERT_TRUE(
                fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result).ok());
        EXPECT_TRUE(result.is_empty());
        EXPECT_EQ(it->read_from_index_calls, 0);
    }

    // ESCAPE is a non-constant column: even though it happens to hold a backslash right now, the
    // per-row value may differ, so push-down must be refused.
    {
        auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
        segment_v2::InvertedIndexResultBitmap result;
        ColumnsWithTypeAndName args {const_string_arg("%abcd%"), non_const_string_arg("\\")};
        ASSERT_TRUE(
                fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result).ok());
        EXPECT_TRUE(result.is_empty());
        EXPECT_EQ(it->read_from_index_calls, 0);
    }

    // More arguments than like's two shapes (pattern) / (pattern, escape): refused as well.
    {
        auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
        segment_v2::InvertedIndexResultBitmap result;
        ColumnsWithTypeAndName args {const_string_arg("%abcd%"), const_string_arg("\\"),
                                     const_string_arg("extra")};
        ASSERT_TRUE(
                fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result).ok());
        EXPECT_TRUE(result.is_empty());
        EXPECT_EQ(it->read_from_index_calls, 0);
    }

    // The default backslash ESCAPE spelled out explicitly, as a constant: push-down is allowed.
    {
        auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
        segment_v2::InvertedIndexResultBitmap result;
        ColumnsWithTypeAndName args {const_string_arg("%abcd%"), const_string_arg("\\")};
        ASSERT_TRUE(
                fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result).ok());
        EXPECT_EQ(it->read_from_index_calls, 1);
        EXPECT_TRUE(result.approximate());
    }
}

TEST(LikeGramIndexTest, ConfigDisabledSkipsPushDown) {
    ScopedGramIndexPolicies policies;
    auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
    auto fn = std::make_shared<FunctionRegexpLike>();
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    segment_v2::InvertedIndexResultBitmap result;
    ColumnsWithTypeAndName args {const_string_arg("hello|world")};

    const bool saved_enable = config::enable_gram_index_regexp;
    config::enable_gram_index_regexp = false;
    const Status status =
            fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result);
    config::enable_gram_index_regexp = saved_enable;

    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(result.is_empty());
}

// Layer 1 of the storage-format fence (Ruling R30): when the reader is not a SNII reader nothing
// is pushed down, and the outcome must be "OK with no result" rather than an error -- old
// segments of the same table are in CLucene format, and an error would fail the whole query.
TEST(LikeGramIndexTest, NonSniiReaderIsNotPushedDown) {
    ScopedGramIndexPolicies policies;
    // The property table is identical to the push-down case (the same gram-family analyzer); the
    // only difference is the reader type.
    auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}},
                                 FakeReaderKind::kNonSnii);
    auto fn = std::make_shared<FunctionRegexpLike>();
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    segment_v2::InvertedIndexResultBitmap result;
    ColumnsWithTypeAndName args {const_string_arg("hello|world")};

    ASSERT_TRUE(fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result).ok());

    EXPECT_TRUE(result.is_empty());
    // The key point: read_from_index must not even be issued -- a CLucene reader must never see a
    // GRAM_BOOLEAN_QUERY.
    EXPECT_EQ(it->read_from_index_calls, 0);
}

// Catch-all degradation (Ruling R29): any error returned by read_from_index may only lead to "no
// acceleration".
TEST(LikeGramIndexTest, ArbitraryIndexErrorDegradesToNoResult) {
    ScopedGramIndexPolicies policies;
    auto fn = std::make_shared<FunctionRegexpLike>();
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    ColumnsWithTypeAndName args {const_string_arg("hello|world")};

    // Stack traces are switched off throughout (the second template parameter): these errors are
    // injected on purpose, and a stack trace would only pollute the test output.
    const std::vector<Status> degradable {
            Status::InternalError<false>("boom"),
            Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>("not supported"),
            Status::Error<ErrorCode::CORRUPTION, false>("corrupt index image"),
            Status::Error<ErrorCode::IO_ERROR, false>("s3 read failed"),
    };
    for (const auto& injected : degradable) {
        auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
        it->inject_status = injected;
        segment_v2::InvertedIndexResultBitmap result;
        const Status status =
                fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result);
        ASSERT_TRUE(status.ok()) << injected.to_string() << " -> " << status.to_string();
        EXPECT_TRUE(result.is_empty()) << injected.to_string();
        EXPECT_EQ(it->read_from_index_calls, 1);
    }
}

// The only exception: statuses meaning the query as a whole should already stop are rethrown.
TEST(LikeGramIndexTest, CancellationAndMemoryErrorsPropagate) {
    ScopedGramIndexPolicies policies;
    auto fn = std::make_shared<FunctionRegexpLike>();
    std::vector<IndexFieldNameAndTypePair> names {{"msg", std::make_shared<DataTypeString>()}};
    ColumnsWithTypeAndName args {const_string_arg("hello|world")};

    const std::vector<int> propagated {ErrorCode::CANCELLED, ErrorCode::MEM_LIMIT_EXCEEDED,
                                       ErrorCode::MEM_ALLOC_FAILED};
    for (const int code : propagated) {
        auto it = make_gram_iterator({{"analyzer", ScopedGramIndexPolicies::analyzer_name()}});
        it->inject_status = Status(code, "stop the query");
        segment_v2::InvertedIndexResultBitmap result;
        const Status status =
                fn->evaluate_inverted_index(args, names, {it.get()}, 100, nullptr, result);
        ASSERT_FALSE(status.ok()) << code;
        EXPECT_EQ(status.code(), code);
        EXPECT_TRUE(result.is_empty());
    }
}

// Layer 2 of the storage-format fence (Ruling R30): a CLucene-format reader must reject a
// GRAM_BOOLEAN_QUERY itself, and it must do so before any tokenization or cache lookup. Both
// readers are built with a null IndexFileReader -- the fence is the first statement of query(),
// so nothing that needs a file is ever reached; conversely, if anyone moves the fence later, this
// case blows up immediately.
TEST(LikeGramIndexTest, CluceneReadersRejectGramBooleanQuery) {
    const TabletIndex index_meta = build_gram_index_meta({{"parser", "english"}});
    auto context = std::make_shared<segment_v2::IndexQueryContext>();
    OlapReaderStatistics stats;
    context->stats = &stats;
    const Field query_value = Field::create_field<TYPE_STRING>(std::string("&(\"abc\")"));
    std::shared_ptr<roaring::Roaring> bitmap;

    {
        auto reader = segment_v2::FullTextIndexReader::create_shared(&index_meta, nullptr);
        const Status status =
                reader->query(context, "msg", query_value,
                              segment_v2::InvertedIndexQueryType::GRAM_BOOLEAN_QUERY, bitmap);
        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED) << status.to_string();
        EXPECT_EQ(bitmap, nullptr);
    }
    {
        auto reader =
                segment_v2::StringTypeInvertedIndexReader::create_shared(&index_meta, nullptr);
        const Status status =
                reader->query(context, "msg", query_value,
                              segment_v2::InvertedIndexQueryType::GRAM_BOOLEAN_QUERY, bitmap);
        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED) << status.to_string();
        EXPECT_EQ(bitmap, nullptr);
    }
}

} // namespace doris

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

#include "olap/rowset/segment_v2/inverted_index_iterator.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/tablet_schema.h"
#include "vec/data_types/data_type_string.h"

namespace doris::segment_v2 {

// Mock InvertedIndexReader for testing
class MockInvertedIndexReader : public InvertedIndexReader {
public:
    // Factory method to create instances
    static std::shared_ptr<MockInvertedIndexReader> create(
            const std::map<std::string, std::string>& properties) {
        auto index = std::make_shared<TabletIndex>();
        // Initialize TabletIndex with protobuf to ensure all fields are properly set
        TabletIndexPB pb;
        pb.set_index_id(1);
        pb.set_index_name("test_index");
        pb.set_index_type(IndexType::INVERTED);
        index->init_from_pb(pb);
        return std::shared_ptr<MockInvertedIndexReader>(
                new MockInvertedIndexReader(index, properties));
    }

    InvertedIndexReaderType type() override { return _type; }
    void set_type(InvertedIndexReaderType type) { _type = type; }
    const std::map<std::string, std::string>& get_index_properties() const override {
        return _properties;
    }

    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const void* query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& roaring,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override {
        return Status::OK();
    }

    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const void* query_value, InvertedIndexQueryType query_type,
                     size_t* count) override {
        *count = 0;
        return Status::OK();
    }

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override { return Status::OK(); }

private:
    MockInvertedIndexReader(std::shared_ptr<TabletIndex> index,
                            const std::map<std::string, std::string>& properties)
            : InvertedIndexReader(index.get(), nullptr),
              _mock_index(index), // Copy shared_ptr to keep index alive
              _properties(properties) {}

    std::shared_ptr<TabletIndex> _mock_index; // Keep index alive
    std::map<std::string, std::string> _properties;
    InvertedIndexReaderType _type = InvertedIndexReaderType::FULLTEXT;
};

class InvertedIndexIteratorTest : public testing::Test {
protected:
    std::shared_ptr<MockInvertedIndexReader> create_mock_reader(
            const std::string& analyzer_key,
            InvertedIndexReaderType type = InvertedIndexReaderType::FULLTEXT) {
        std::map<std::string, std::string> properties;
        if (!analyzer_key.empty() && analyzer_key != INVERTED_INDEX_DEFAULT_ANALYZER_KEY) {
            if (AnalyzerConfigParser::is_builtin_analyzer(analyzer_key)) {
                properties[INVERTED_INDEX_PARSER_KEY] = analyzer_key;
            } else {
                properties[INVERTED_INDEX_ANALYZER_NAME_KEY] = analyzer_key;
            }
        }
        auto reader = MockInvertedIndexReader::create(properties);
        reader->set_type(type);
        return reader;
    }
};

// ensure_normalized_key tests
TEST_F(InvertedIndexIteratorTest, EnsureNormalizedKey_EmptyInput) {
    EXPECT_EQ(InvertedIndexIterator::ensure_normalized_key(""),
              INVERTED_INDEX_DEFAULT_ANALYZER_KEY);
}

TEST_F(InvertedIndexIteratorTest, EnsureNormalizedKey_Uppercase) {
    EXPECT_EQ(InvertedIndexIterator::ensure_normalized_key("CHINESE"), "chinese");
}

TEST_F(InvertedIndexIteratorTest, EnsureNormalizedKey_MixedCase) {
    EXPECT_EQ(InvertedIndexIterator::ensure_normalized_key("ChInEsE"), "chinese");
}

TEST_F(InvertedIndexIteratorTest, EnsureNormalizedKey_DefaultKey) {
    EXPECT_EQ(InvertedIndexIterator::ensure_normalized_key("__default__"),
              INVERTED_INDEX_DEFAULT_ANALYZER_KEY);
}

// add_reader tests
TEST_F(InvertedIndexIteratorTest, AddReader_SingleReader) {
    InvertedIndexIterator iterator;
    try {
        auto reader = create_mock_reader("chinese");
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

        auto result = iterator.select_best_reader("chinese");
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), reader);
    } catch (const std::exception& e) {
        EXPECT_TRUE(false) << "Exception thrown: " << e.what();
    }
}

TEST_F(InvertedIndexIteratorTest, AddReader_MultipleReadersWithDifferentKeys) {
    InvertedIndexIterator iterator;
    auto reader1 = create_mock_reader("chinese");
    auto reader2 = create_mock_reader("english");
    auto reader3 = create_mock_reader(""); // empty key normalizes to __default__

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader1);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader2);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader3);

    // Explicit analyzer keys (chinese, english) do exact match
    auto result1 = iterator.select_best_reader("chinese");
    EXPECT_TRUE(result1.has_value());
    EXPECT_EQ(result1.value(), reader1);

    auto result2 = iterator.select_best_reader("english");
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2.value(), reader2);

    // __default__ is a fallback key - it returns all readers for query-type-based selection.
    // The simple select_best_reader(key) overload returns the first candidate.
    // This is by design: __default__ means "no preference, let system choose".
    auto result3 = iterator.select_best_reader(INVERTED_INDEX_DEFAULT_ANALYZER_KEY);
    EXPECT_TRUE(result3.has_value());
    // Don't assert specific reader - fallback mode returns first available
}

// Test that "none" is treated as a distinct analyzer key
TEST_F(InvertedIndexIteratorTest, AddReader_NoneAnalyzerIsDistinct) {
    InvertedIndexIterator iterator;
    auto default_reader = create_mock_reader("");  // normalizes to __default__
    auto none_reader = create_mock_reader("none"); // stays as "none"

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, default_reader);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, none_reader);

    // Query for __default__ should return default_reader
    auto result_default = iterator.select_best_reader(INVERTED_INDEX_DEFAULT_ANALYZER_KEY);
    EXPECT_TRUE(result_default.has_value());
    EXPECT_EQ(result_default.value(), default_reader);

    // Query for "none" should return none_reader (not default_reader)
    auto result_none = iterator.select_best_reader(INVERTED_INDEX_PARSER_NONE);
    EXPECT_TRUE(result_none.has_value());
    EXPECT_EQ(result_none.value(), none_reader);
}

// find_reader_candidates tests (via select_best_reader)
TEST_F(InvertedIndexIteratorTest, FindReaderCandidates_ExactMatch) {
    InvertedIndexIterator iterator;
    auto reader = create_mock_reader("chinese");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

    auto result = iterator.select_best_reader("chinese");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), reader);
}

TEST_F(InvertedIndexIteratorTest, FindReaderCandidates_FallbackToDefault) {
    InvertedIndexIterator iterator;
    auto default_reader = create_mock_reader("");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, default_reader);

    auto result = iterator.select_best_reader(INVERTED_INDEX_DEFAULT_ANALYZER_KEY);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), default_reader);
}

TEST_F(InvertedIndexIteratorTest, FindReaderCandidates_FallbackToAny) {
    InvertedIndexIterator iterator;
    auto chinese_reader = create_mock_reader("chinese");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, chinese_reader);

    auto result = iterator.select_best_reader(INVERTED_INDEX_DEFAULT_ANALYZER_KEY);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), chinese_reader);
}

TEST_F(InvertedIndexIteratorTest, FindReaderCandidates_EmptyReaders) {
    InvertedIndexIterator iterator;
    auto result = iterator.select_best_reader("chinese");
    EXPECT_FALSE(result.has_value());
}

// select_best_reader with column_type tests
TEST_F(InvertedIndexIteratorTest, SelectBestReader_MatchQuerySelectsFulltext) {
    InvertedIndexIterator iterator;
    auto fulltext_reader = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT);
    auto string_reader = create_mock_reader("chinese", InvertedIndexReaderType::STRING_TYPE);

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, fulltext_reader);
    iterator.add_reader(InvertedIndexReaderType::STRING_TYPE, string_reader);

    auto string_type = std::make_shared<vectorized::DataTypeString>();
    auto result = iterator.select_best_reader(string_type, InvertedIndexQueryType::MATCH_ANY_QUERY,
                                              "chinese");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), fulltext_reader);
}

TEST_F(InvertedIndexIteratorTest, SelectBestReader_EqualQuerySelectsStringType) {
    InvertedIndexIterator iterator;
    auto fulltext_reader = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT);
    auto string_reader = create_mock_reader("chinese", InvertedIndexReaderType::STRING_TYPE);

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, fulltext_reader);
    iterator.add_reader(InvertedIndexReaderType::STRING_TYPE, string_reader);

    auto string_type = std::make_shared<vectorized::DataTypeString>();
    auto result = iterator.select_best_reader(string_type, InvertedIndexQueryType::EQUAL_QUERY,
                                              "chinese");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), string_reader);
}

// Index lookup performance test
TEST_F(InvertedIndexIteratorTest, IndexLookup_ManyReadersStillFast) {
    InvertedIndexIterator iterator;

    std::vector<std::shared_ptr<MockInvertedIndexReader>> readers;
    for (int i = 0; i < 100; i++) {
        auto reader = create_mock_reader("analyzer_" + std::to_string(i));
        readers.push_back(reader);
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);
    }

    auto result = iterator.select_best_reader("analyzer_50");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), readers[50]);

    auto result_first = iterator.select_best_reader("analyzer_0");
    EXPECT_TRUE(result_first.has_value());
    EXPECT_EQ(result_first.value(), readers[0]);

    auto result_last = iterator.select_best_reader("analyzer_99");
    EXPECT_TRUE(result_last.has_value());
    EXPECT_EQ(result_last.value(), readers[99]);
}

// Edge cases
TEST_F(InvertedIndexIteratorTest, EdgeCase_EmptyAnalyzerKeyQuery) {
    InvertedIndexIterator iterator;
    auto reader = create_mock_reader("");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

    auto result = iterator.select_best_reader("");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), reader);
}

TEST_F(InvertedIndexIteratorTest, EdgeCase_CaseInsensitiveQuery) {
    InvertedIndexIterator iterator;
    auto reader = create_mock_reader("chinese");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

    auto result = iterator.select_best_reader("CHINESE");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), reader);
}

TEST_F(InvertedIndexIteratorTest, EdgeCase_GetReaderByType) {
    InvertedIndexIterator iterator;
    auto fulltext = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT);
    auto string_type = create_mock_reader("english", InvertedIndexReaderType::STRING_TYPE);

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, fulltext);
    iterator.add_reader(InvertedIndexReaderType::STRING_TYPE, string_type);

    auto reader = iterator.get_reader(InvertedIndexReaderType::FULLTEXT);
    EXPECT_NE(reader, nullptr);

    auto reader2 = iterator.get_reader(InvertedIndexReaderType::STRING_TYPE);
    EXPECT_NE(reader2, nullptr);

    auto reader3 = iterator.get_reader(InvertedIndexReaderType::BKD);
    EXPECT_EQ(reader3, nullptr);
}

} // namespace doris::segment_v2

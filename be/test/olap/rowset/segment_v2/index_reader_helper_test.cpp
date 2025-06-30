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

#include "olap/rowset/segment_v2/index_reader_helper.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>

#include "common/be_mock_util.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/index_reader.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/tablet_schema.h"

using namespace doris;
using namespace doris::segment_v2;

class MockIndexReader : public IndexReader {
public:
    MockIndexReader(IndexType type, uint64_t id) : _type(type), _id(id) {}

    MOCK_FUNCTION IndexType index_type() override { return _type; }
    MOCK_FUNCTION uint64_t get_index_id() const override { return _id; }
    MOCK_FUNCTION Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override {
        return Status::OK();
    }

private:
    IndexType _type;
    uint64_t _id;
};

class MockInvertedIndexReader : public InvertedIndexReader {
public:
    MockInvertedIndexReader(InvertedIndexReaderType reader_type,
                            const std::map<std::string, std::string>& properties)
            : InvertedIndexReader(get_static_tablet_index(), nullptr),
              _reader_type(reader_type),
              _properties(properties) {}

    MOCK_FUNCTION InvertedIndexReaderType type() override { return _reader_type; }

    MOCK_FUNCTION const std::map<std::string, std::string>& get_index_properties() const override {
        return _properties;
    }

    MOCK_FUNCTION Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                               const void* query_value, InvertedIndexQueryType query_type,
                               std::shared_ptr<roaring::Roaring>& bit_map) override {
        return Status::OK();
    }

    MOCK_FUNCTION Status try_query(const IndexQueryContextPtr& context,
                                   const std::string& column_name, const void* query_value,
                                   InvertedIndexQueryType query_type, uint32_t* count) override {
        return Status::OK();
    }

    MOCK_FUNCTION Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override {
        return Status::OK();
    }

private:
    static const TabletIndex* get_static_tablet_index() {
        static TabletIndex idx_meta;
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test_mock_cache");
        index_meta_pb->add_col_unique_id(1);
        idx_meta.init_from_pb(*index_meta_pb);
        return &idx_meta;
    }

    InvertedIndexReaderType _reader_type;
    std::map<std::string, std::string> _properties;
};

class IndexReaderHelperTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(IndexReaderHelperTest, IsFulltextIndexWithNonInvertedIndexTest) {
    auto ann_reader = std::make_shared<MockIndexReader>(IndexType::ANN, 1);
    auto bitmap_reader = std::make_shared<MockIndexReader>(IndexType::BITMAP, 2);
    auto bloomfilter_reader = std::make_shared<MockIndexReader>(IndexType::BLOOMFILTER, 3);
    auto ngram_reader = std::make_shared<MockIndexReader>(IndexType::NGRAM_BF, 4);

    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(ann_reader));
    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(bitmap_reader));
    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(bloomfilter_reader));
    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(ngram_reader));
}

TEST_F(IndexReaderHelperTest, IsFulltextIndexWithInvertedIndexTest) {
    std::map<std::string, std::string> properties;

    auto fulltext_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::FULLTEXT, properties);
    auto string_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::STRING_TYPE, properties);
    auto bkd_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::BKD, properties);
    auto unknown_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::UNKNOWN, properties);

    EXPECT_TRUE(IndexReaderHelper::is_fulltext_index(fulltext_reader));
    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(string_reader));
    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(bkd_reader));
    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(unknown_reader));
}

TEST_F(IndexReaderHelperTest, IsStringIndexWithNonInvertedIndexTest) {
    auto ann_reader = std::make_shared<MockIndexReader>(IndexType::ANN, 1);
    auto bitmap_reader = std::make_shared<MockIndexReader>(IndexType::BITMAP, 2);

    EXPECT_FALSE(IndexReaderHelper::is_string_index(ann_reader));
    EXPECT_FALSE(IndexReaderHelper::is_string_index(bitmap_reader));
}

TEST_F(IndexReaderHelperTest, IsStringIndexWithInvertedIndexTest) {
    std::map<std::string, std::string> properties;

    auto fulltext_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::FULLTEXT, properties);
    auto string_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::STRING_TYPE, properties);
    auto bkd_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::BKD, properties);
    auto unknown_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::UNKNOWN, properties);

    EXPECT_FALSE(IndexReaderHelper::is_string_index(fulltext_reader));
    EXPECT_TRUE(IndexReaderHelper::is_string_index(string_reader));
    EXPECT_FALSE(IndexReaderHelper::is_string_index(bkd_reader));
    EXPECT_FALSE(IndexReaderHelper::is_string_index(unknown_reader));
}

TEST_F(IndexReaderHelperTest, IsBkdIndexWithNonInvertedIndexTest) {
    auto ann_reader = std::make_shared<MockIndexReader>(IndexType::ANN, 1);
    auto bitmap_reader = std::make_shared<MockIndexReader>(IndexType::BITMAP, 2);
    auto bloomfilter_reader = std::make_shared<MockIndexReader>(IndexType::BLOOMFILTER, 3);

    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(ann_reader));
    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(bitmap_reader));
    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(bloomfilter_reader));
}

TEST_F(IndexReaderHelperTest, IsBkdIndexWithInvertedIndexTest) {
    std::map<std::string, std::string> properties;

    auto fulltext_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::FULLTEXT, properties);
    auto string_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::STRING_TYPE, properties);
    auto bkd_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::BKD, properties);
    auto unknown_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::UNKNOWN, properties);

    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(fulltext_reader));
    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(string_reader));
    EXPECT_TRUE(IndexReaderHelper::is_bkd_index(bkd_reader));
    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(unknown_reader));
}

TEST_F(IndexReaderHelperTest, IsSupportPhraseWithNonInvertedIndexTest) {
    auto ann_reader = std::make_shared<MockIndexReader>(IndexType::ANN, 1);
    auto bitmap_reader = std::make_shared<MockIndexReader>(IndexType::BITMAP, 2);
    auto bloomfilter_reader = std::make_shared<MockIndexReader>(IndexType::BLOOMFILTER, 3);
    auto ngram_reader = std::make_shared<MockIndexReader>(IndexType::NGRAM_BF, 4);

    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(ann_reader));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(bitmap_reader));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(bloomfilter_reader));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(ngram_reader));
}

TEST_F(IndexReaderHelperTest, IsSupportPhraseWithPhraseSupportYesTest) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;

    auto fulltext_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::FULLTEXT, properties);
    auto string_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::STRING_TYPE, properties);
    auto bkd_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::BKD, properties);

    EXPECT_TRUE(IndexReaderHelper::is_support_phrase(fulltext_reader));
    EXPECT_TRUE(IndexReaderHelper::is_support_phrase(string_reader));
    EXPECT_TRUE(IndexReaderHelper::is_support_phrase(bkd_reader));
}

TEST_F(IndexReaderHelperTest, IsSupportPhraseWithPhraseSupportNoTest) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = INVERTED_INDEX_PARSER_PHRASE_SUPPORT_NO;

    auto fulltext_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::FULLTEXT, properties);
    auto string_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::STRING_TYPE, properties);
    auto bkd_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::BKD, properties);

    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(fulltext_reader));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(string_reader));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(bkd_reader));
}

TEST_F(IndexReaderHelperTest, IsSupportPhraseWithEmptyPropertiesTest) {
    std::map<std::string, std::string> properties;

    auto fulltext_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::FULLTEXT, properties);
    auto string_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::STRING_TYPE, properties);
    auto bkd_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::BKD, properties);

    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(fulltext_reader));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(string_reader));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(bkd_reader));
}

TEST_F(IndexReaderHelperTest, IsSupportPhraseWithInvalidValueTest) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = "invalid_value";

    auto fulltext_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::FULLTEXT, properties);
    auto string_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::STRING_TYPE, properties);

    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(fulltext_reader));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(string_reader));
}

TEST_F(IndexReaderHelperTest, IsSupportPhraseWithMixedPropertiesTest) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_ENGLISH;
    properties[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;
    properties[INVERTED_INDEX_PARSER_LOWERCASE_KEY] = INVERTED_INDEX_PARSER_TRUE;

    auto fulltext_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::FULLTEXT, properties);

    EXPECT_TRUE(IndexReaderHelper::is_support_phrase(fulltext_reader));
}

TEST_F(IndexReaderHelperTest, AllMethodsWithNullPointerTest) {
    IndexReaderPtr null_reader = nullptr;

    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(null_reader));
    EXPECT_FALSE(IndexReaderHelper::is_string_index(null_reader));
    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(null_reader));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(null_reader));
}

TEST_F(IndexReaderHelperTest, EdgeCaseWithUnknownReaderTypeTest) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;

    auto unknown_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::UNKNOWN, properties);

    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(unknown_reader));
    EXPECT_FALSE(IndexReaderHelper::is_string_index(unknown_reader));
    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(unknown_reader));
    EXPECT_TRUE(IndexReaderHelper::is_support_phrase(unknown_reader));
}

TEST_F(IndexReaderHelperTest, CombinedMethodsConsistencyTest) {
    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;

    auto fulltext_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::FULLTEXT, properties);
    auto string_reader = std::make_shared<MockInvertedIndexReader>(
            InvertedIndexReaderType::STRING_TYPE, properties);
    auto bkd_reader =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::BKD, properties);

    EXPECT_TRUE(IndexReaderHelper::is_fulltext_index(fulltext_reader));
    EXPECT_FALSE(IndexReaderHelper::is_string_index(fulltext_reader));
    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(fulltext_reader));
    EXPECT_TRUE(IndexReaderHelper::is_support_phrase(fulltext_reader));

    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(string_reader));
    EXPECT_TRUE(IndexReaderHelper::is_string_index(string_reader));
    EXPECT_FALSE(IndexReaderHelper::is_bkd_index(string_reader));
    EXPECT_TRUE(IndexReaderHelper::is_support_phrase(string_reader));

    EXPECT_FALSE(IndexReaderHelper::is_fulltext_index(bkd_reader));
    EXPECT_FALSE(IndexReaderHelper::is_string_index(bkd_reader));
    EXPECT_TRUE(IndexReaderHelper::is_bkd_index(bkd_reader));
    EXPECT_TRUE(IndexReaderHelper::is_support_phrase(bkd_reader));
}

TEST_F(IndexReaderHelperTest, MultipleInvertedIndexTypesTest) {
    std::map<std::string, std::string> properties;

    auto readers = {
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::FULLTEXT,
                                                      properties),
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::STRING_TYPE,
                                                      properties),
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::BKD, properties),
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::UNKNOWN,
                                                      properties)};

    std::vector<bool> expected_fulltext = {true, false, false, false};
    std::vector<bool> expected_string = {false, true, false, false};
    std::vector<bool> expected_bkd = {false, false, true, false};

    int i = 0;
    for (const auto& reader : readers) {
        EXPECT_EQ(IndexReaderHelper::is_fulltext_index(reader), expected_fulltext[i]);
        EXPECT_EQ(IndexReaderHelper::is_string_index(reader), expected_string[i]);
        EXPECT_EQ(IndexReaderHelper::is_bkd_index(reader), expected_bkd[i]);
        i++;
    }
}

TEST_F(IndexReaderHelperTest, PropertiesVariationsTest) {
    std::map<std::string, std::string> props1;
    props1[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = "TRUE";

    std::map<std::string, std::string> props2;
    props2[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = "True";

    std::map<std::string, std::string> props3;
    props3[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = "false";

    std::map<std::string, std::string> props4;
    props4[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = "FALSE";

    auto reader1 =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::FULLTEXT, props1);
    auto reader2 =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::FULLTEXT, props2);
    auto reader3 =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::FULLTEXT, props3);
    auto reader4 =
            std::make_shared<MockInvertedIndexReader>(InvertedIndexReaderType::FULLTEXT, props4);

    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(reader1));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(reader2));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(reader3));
    EXPECT_FALSE(IndexReaderHelper::is_support_phrase(reader4));
}

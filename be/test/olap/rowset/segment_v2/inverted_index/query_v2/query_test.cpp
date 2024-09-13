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

#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/factory.inline.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/node.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/operator.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow-field"
#include <CLucene/util/stringUtil.h>

#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#include "CLucene/store/FSDirectory.h"
#pragma GCC diagnostic pop

#include "common/logging.h"
#include "io/fs/local_file_system.h"

CL_NS_USE(search)
CL_NS_USE(store)

namespace doris::segment_v2 {

class QueryTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/query_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    QueryTest() = default;
    ~QueryTest() override = default;
};

using namespace idx_query_v2;

static Status boolean_query_search(const std::string& name,
                                   const std::shared_ptr<IndexSearcher>& search) {
    BooleanQuery::Builder builder;
    RETURN_IF_ERROR(builder.set_op(OperatorType::OP_AND));
    {
        TQueryOptions options;
        auto roaring = std::make_shared<roaring::Roaring>();
        roaring->add(1);
        roaring->add(3);
        roaring->add(5);
        roaring->add(7);
        roaring->add(9);
        auto clause = DORIS_TRY(QueryFactory::create(QueryType::ROARING_QUERY, roaring));
        RETURN_IF_ERROR(builder.add(clause));
    }
    {
        BooleanQuery::Builder builder1;
        RETURN_IF_ERROR(builder1.set_op(OperatorType::OP_OR));
        {
            TQueryOptions options;
            QueryInfo query_info;
            query_info.field_name = StringUtil::string_to_wstring(name);
            query_info.terms.emplace_back("hm");
            auto clause = DORIS_TRY(
                    QueryFactory::create(QueryType::TERM_QUERY, search, options, query_info));
            RETURN_IF_ERROR(builder1.add(clause));
        }
        {
            TQueryOptions options;
            QueryInfo query_info;
            query_info.field_name = StringUtil::string_to_wstring(name);
            query_info.terms.emplace_back("ac");
            auto clause = DORIS_TRY(
                    QueryFactory::create(QueryType::TERM_QUERY, search, options, query_info));
            RETURN_IF_ERROR(builder1.add(clause));
        }
        auto boolean_query = DORIS_TRY(builder1.build());
        RETURN_IF_ERROR(builder.add(boolean_query));
    }
    auto boolean_query = DORIS_TRY(builder.build());

    auto result = std::make_shared<roaring::Roaring>();
    visit_node(boolean_query, QueryExecute {}, result);
    EXPECT_EQ(result->cardinality(), 3);
    EXPECT_EQ(result->toString(), "{1,7,9}");

    return Status::OK();
}

TEST_F(QueryTest, test_boolean_query) {
    std::string name = "name";

    // write
    {
        std::vector<std::string> datas;
        datas.emplace_back("0 hm");
        datas.emplace_back("1 hm");
        datas.emplace_back("2 hm");
        datas.emplace_back("3 bg");
        datas.emplace_back("4 bg");
        datas.emplace_back("5 bg");
        datas.emplace_back("6 ac");
        datas.emplace_back("7 ac");
        datas.emplace_back("8 ac");
        datas.emplace_back("9 ac");

        auto* analyzer = _CLNEW lucene::analysis::standard95::StandardAnalyzer();
        analyzer->set_stopwords(nullptr);
        auto* indexwriter = _CLNEW lucene::index::IndexWriter(kTestDir.c_str(), analyzer, true);
        indexwriter->setMaxBufferedDocs(100);
        indexwriter->setRAMBufferSizeMB(-1);
        indexwriter->setMaxFieldLength(0x7FFFFFFFL);
        indexwriter->setMergeFactor(1000000000);
        indexwriter->setUseCompoundFile(false);

        auto* char_string_reader = _CLNEW lucene::util::SStringReader<char>;

        auto* doc = _CLNEW lucene::document::Document();
        int32_t field_config = lucene::document::Field::STORE_NO;
        field_config |= lucene::document::Field::INDEX_NONORMS;
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_name = std::wstring(name.begin(), name.end());
        auto* field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
        field->setOmitTermFreqAndPositions(false);
        doc->add(*field);

        for (const auto& data : datas) {
            char_string_reader->init(data.data(), data.size(), false);
            auto* stream = analyzer->reusableTokenStream(field->name(), char_string_reader);
            field->setValue(stream);
            indexwriter->addDocument(doc);
        }

        indexwriter->close();

        _CLLDELETE(indexwriter);
        _CLLDELETE(doc);
        _CLLDELETE(analyzer);
        _CLLDELETE(char_string_reader);
    }

    // query
    {
        auto* dir = FSDirectory::getDirectory(kTestDir.c_str());
        auto* reader = IndexReader::open(dir, 1024 * 1024, true);
        auto search = std::make_shared<IndexSearcher>(reader);

        Status res = boolean_query_search(name, search);
        EXPECT_TRUE(res.ok());

        reader->close();
        _CLLDELETE(reader);
        _CLDECDELETE(dir);
    }
}

} // namespace doris::segment_v2

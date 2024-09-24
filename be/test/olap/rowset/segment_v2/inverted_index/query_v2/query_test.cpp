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
#include "olap/rowset/segment_v2/inverted_index/reader/reader.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"

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
public:
    const std::string kTestDir = "./ut_dir/reader_test";
    const std::string rowset_id = "test_rowset";
    const int64_t seg_id = 1;
    const std::string index_path_prefix = "test_rowset_1";
    std::string test_dir = "";
    const InvertedIndexStorageFormatPB storage_format = InvertedIndexStorageFormatPB::V2;
    const int64_t index_id = 1;
    TabletIndex index_meta;
    std::string local_fs_index_path = "";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;

        TabletIndexPB index_meta_pb;
        index_meta_pb.set_index_id(index_id);
        index_meta.init_from_pb(index_meta_pb);

        test_dir = kTestDir + "/" + index_path_prefix;
        auto fs = io::global_local_filesystem();

        local_fs_index_path = InvertedIndexDescriptor::get_temporary_index_path(
                test_dir, rowset_id, seg_id, index_meta.index_id(), index_meta.get_index_suffix());
        st = io::global_local_filesystem()->delete_directory(local_fs_index_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(local_fs_index_path);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    QueryTest() = default;
    ~QueryTest() override = default;
};

using namespace inverted_index;

static Status boolean_query_search(const std::string& name,
                                   const std::shared_ptr<IndexReader>& reader) {
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
            std::string term = "hm";
            auto clause =
                    DORIS_TRY(QueryFactory::create(QueryType::TERM_QUERY, reader, name, term));
            RETURN_IF_ERROR(builder1.add(clause));
        }
        {
            std::string term = "ac";
            auto clause =
                    DORIS_TRY(QueryFactory::create(QueryType::TERM_QUERY, reader, name, term));
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
    DorisFSDirectory* dir = _CLNEW DorisFSDirectory();
    dir->init(io::global_local_filesystem(), local_fs_index_path.c_str(), nullptr);

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
        auto* indexwriter = _CLNEW lucene::index::IndexWriter(dir, analyzer, true);
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

        auto writer = std::make_unique<InvertedIndexFileWriter>(
                io::global_local_filesystem(), test_dir, rowset_id, seg_id, storage_format);
        InvertedIndexDirectoryMap dir_map;
        dir_map.emplace(std::make_pair(index_id, ""),
                        std::unique_ptr<lucene::store::Directory>(dir));
        auto st = writer->initialize(dir_map);
        EXPECT_TRUE(st.ok());
        st = writer->close();
        EXPECT_TRUE(st.ok());
    }

    // open reader
    {
        auto file_reader = std::make_shared<InvertedIndexFileReader>(io::global_local_filesystem(),
                                                                     test_dir, storage_format);
        auto st = file_reader->init(4096);
        if (!st.ok()) {
            std::cerr << st.msg() << std::endl;
            ASSERT_TRUE(st.ok());
        }
        auto inverted_index_reader =
                inverted_index::InvertedIndexReader::create_unique(&index_meta, file_reader);
        st = inverted_index_reader->init_index_reader();
        if (!st.ok()) {
            std::cerr << st.msg() << std::endl;
            ASSERT_TRUE(st.ok());
        }
        auto reader = inverted_index_reader->get_index_reader();
        // query
        {
            Status res = boolean_query_search(name, reader);
            EXPECT_TRUE(res.ok());
        }
        reader->close();
    }
}

} // namespace doris::segment_v2

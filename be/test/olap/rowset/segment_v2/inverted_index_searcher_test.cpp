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

#include "olap/rowset/segment_v2/inverted_index_searcher.h"

#include <CLucene.h>
#include <CLucene/document/Document.h>
#include <CLucene/document/Field.h>
#include <CLucene/index/IndexWriter.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/FSDirectory.h>
#include <CLucene/store/RAMDirectory.h>
#include <CLucene/util/Misc.h>
#include <CLucene/util/NumericUtils.h>
#include <CLucene/util/bkd/bkd_reader.h>
#include <CLucene/util/bkd/bkd_writer.h>

#include <random>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif

#include <CLucene/analysis/standard95/StandardAnalyzer.h>

#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest_pred_impl.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/local_file_system.h"

namespace doris::segment_v2 {
class InvertedIndexSearcherBuilderFlowTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/inverted_index_searcher_flow_test";

    void SetUp() override {
        _fs = io::global_local_filesystem();
        auto st = _fs->delete_directory(kTestDir);
        st = _fs->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        auto st = _fs->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }
    std::string generateRandomString(int length) {
        static const char alphanum[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
        std::string randomString;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, sizeof(alphanum) - 2);
        for (int i = 0; i < length; ++i) {
            randomString += alphanum[dis(gen)];
        }
        return randomString;
    }

    const int32_t MAX_FIELD_LEN = 0x7FFFFFFFL;
    const int32_t MAX_BUFFER_DOCS = 100000000;
    const int32_t MERGE_FACTOR = 100000000;

protected:
    io::FileSystemSPtr _fs;
};

TEST_F(InvertedIndexSearcherBuilderFlowTest, test_bkd_builder_build_success) {
    const int N = 1024 * 1024;
    lucene::store::Directory* tmp_dir =
            lucene::store::FSDirectory::getDirectory("./ut_dir/TestBKD");
    std::unique_ptr<lucene::store::Directory> dir =
            std::unique_ptr<lucene::store::Directory>(_CL_POINTER(tmp_dir));
    std::shared_ptr<lucene::util::bkd::bkd_writer> w =
            std::make_shared<lucene::util::bkd::bkd_writer>(N, 1, 1, 4, 512, 100.0, N, true);
    w->docs_seen_ = N;

    for (int docID = 0; docID < N; docID++) {
        std::vector<uint8_t> scratch(4);

        if (docID > 500000) {
            lucene::util::NumericUtils::intToSortableBytes(200, scratch, 0);

        } else {
            lucene::util::NumericUtils::intToSortableBytes(100, scratch, 0);
        }
        w->add(scratch.data(), scratch.size(), docID);
    }

    int64_t indexFP;
    {
        std::unique_ptr<lucene::store::IndexOutput> out(dir->createOutput("bkd"));
        std::unique_ptr<lucene::store::IndexOutput> meta_out(dir->createOutput("bkd_meta"));
        std::unique_ptr<lucene::store::IndexOutput> index_out(dir->createOutput("bkd_index"));

        try {
            indexFP = w->finish(out.get(), index_out.get());
            w->meta_finish(meta_out.get(), indexFP, 0);
        } catch (...) {
            ASSERT_TRUE(false) << "BKDIndexSearcherBuilder build error";
        }
    }

    BKDIndexSearcherBuilder builder;
    OptionalIndexSearcherPtr output_searcher;

    auto st = builder.build(dir.get(), output_searcher);
    EXPECT_TRUE(st.ok()) << "BKDIndexSearcherBuilder build error: " << st.msg();
    EXPECT_TRUE(output_searcher.has_value());
    EXPECT_GT(builder.get_reader_size(), 0);
    std::cout << "test_bkd_builder size = " << builder.get_reader_size() << std::endl;
}

TEST_F(InvertedIndexSearcherBuilderFlowTest, test_fulltext_builder) {
    auto* tmp_dir = new lucene::store::RAMDirectory();
    std::unique_ptr<lucene::store::Directory> dir =
            std::unique_ptr<lucene::store::Directory>(_CL_POINTER(tmp_dir));

    lucene::analysis::SimpleAnalyzer<char> sanalyzer;
    lucene::index::IndexWriter w(dir.get(), &sanalyzer, true);
    w.setUseCompoundFile(false);
    w.setMaxBufferedDocs(MAX_BUFFER_DOCS);
    w.setRAMBufferSizeMB(256);
    w.setMaxFieldLength(MAX_FIELD_LEN);
    w.setMergeFactor(MERGE_FACTOR);
    lucene::document::Document doc;
    std::wstring field_name = L"fulltext";
    auto* field = _CLNEW lucene::document::Field(
            field_name.c_str(),
            int(lucene::document::Field::INDEX_TOKENIZED) | int(lucene::document::Field::STORE_NO));
    doc.add(*field);

    for (int i = 0; i <= 2000; i++) {
        std::string value1 = "value1";
        if (i > 0) {
            value1 = generateRandomString(2000);
        }
        auto* stringReader = _CLNEW lucene::util::SStringReader<char>(
                value1.c_str(), strlen(value1.c_str()), false);
        auto* stream = sanalyzer.reusableTokenStream(field_name.c_str(), stringReader);

        field->setValue(stream);
        w.addDocument(&doc);
        _CLDELETE(stringReader);
    }
    doc.clear();
    w.close();
    FulltextIndexSearcherBuilder builder;
    OptionalIndexSearcherPtr output_searcher;
    auto st = builder.build(dir.get(), output_searcher);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_TRUE(output_searcher.has_value());
    auto searcher_variant = *output_searcher;
    EXPECT_TRUE(std::holds_alternative<FulltextIndexSearcherPtr>(searcher_variant));
    auto searcher_ptr = std::get<FulltextIndexSearcherPtr>(searcher_variant);
    EXPECT_NE(searcher_ptr, nullptr);
    EXPECT_GT(builder.get_reader_size(), 0);
    std::cout << "test_fulltext_builder size = " << builder.get_reader_size() << std::endl;
}

TEST_F(InvertedIndexSearcherBuilderFlowTest, test_keyword_builder) {
    auto* tmp_dir = new lucene::store::RAMDirectory();
    std::unique_ptr<lucene::store::Directory> dir =
            std::unique_ptr<lucene::store::Directory>(_CL_POINTER(tmp_dir));

    lucene::analysis::SimpleAnalyzer<char> sanalyzer;
    lucene::index::IndexWriter w(dir.get(), &sanalyzer, true);
    w.setUseCompoundFile(false);
    w.setMaxBufferedDocs(MAX_BUFFER_DOCS);
    w.setRAMBufferSizeMB(256);
    w.setMaxFieldLength(MAX_FIELD_LEN);
    w.setMergeFactor(MERGE_FACTOR);
    lucene::document::Document doc;
    std::wstring field_name = L"keyword";
    auto* field = _CLNEW lucene::document::Field(field_name.c_str(),
                                                 int(lucene::document::Field::INDEX_UNTOKENIZED) |
                                                         int(lucene::document::Field::STORE_NO));
    doc.add(*field);

    for (int i = 0; i <= 2000; i++) {
        std::string value1 = "value1";
        if (i > 0) {
            value1 = generateRandomString(2000);
        }
        field->setValue((char*)value1.c_str(), value1.size());
        w.addDocument(&doc);
    }
    doc.clear();
    w.close();
    FulltextIndexSearcherBuilder builder;
    OptionalIndexSearcherPtr output_searcher;
    auto st = builder.build(dir.get(), output_searcher);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_TRUE(output_searcher.has_value());
    auto searcher_variant = *output_searcher;
    EXPECT_TRUE(std::holds_alternative<FulltextIndexSearcherPtr>(searcher_variant));
    auto searcher_ptr = std::get<FulltextIndexSearcherPtr>(searcher_variant);
    EXPECT_NE(searcher_ptr, nullptr);
    EXPECT_GT(builder.get_reader_size(), 0);
    std::cout << "test_keyword_builder size = " << builder.get_reader_size() << std::endl;
}
} // namespace doris::segment_v2
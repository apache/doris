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

#include "olap/rowset/segment_v2/inverted_index/reader/reader.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow-field"
#include <CLucene/util/stringUtil.h>

#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#include "CLucene/store/FSDirectory.h"
#pragma GCC diagnostic pop

#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"

CL_NS_USE(search)
CL_NS_USE(store)

namespace doris::segment_v2 {

class ReaderTest : public testing::Test {
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

    ReaderTest() = default;
    ~ReaderTest() override = default;
};

using namespace inverted_index;

TEST_F(ReaderTest, test_inverted_index_reader) {
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
        reader->close();
    }
}

} // namespace doris::segment_v2

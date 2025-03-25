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

#include <exception>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/logging.h"
#include "io/fs/local_file_system.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wshadow-field"
#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/index/IndexReader.h>
#include <CLucene/search/query/TermPositionIterator.h>
#include <CLucene/util/stringUtil.h>

#include "CLucene/analysis/Analyzers.h"
#include "CLucene/store/FSDirectory.h"
#pragma GCC diagnostic pop

CL_NS_USE(search)
CL_NS_USE(store)
CL_NS_USE(index)
CL_NS_USE(util)

#define FINALLY(eptr, finallyBlock)       \
    {                                     \
        finallyBlock;                     \
        if (eptr) {                       \
            std::rethrow_exception(eptr); \
        }                                 \
    }

namespace doris::segment_v2 {

class IndexCompactionWriteIndexTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/index_compress_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    IndexCompactionWriteIndexTest() = default;
    ~IndexCompactionWriteIndexTest() override = default;

    static constexpr int32_t doc_count = 100000;
};

int32_t getDaySeed() {
    std::time_t now = std::time(nullptr);
    std::tm* localTime = std::localtime(&now);
    localTime->tm_sec = 0;
    localTime->tm_min = 0;
    localTime->tm_hour = 0;
    return static_cast<int32_t>(std::mktime(localTime) / (60 * 60 * 24));
}

static std::string generateRandomIP() {
    std::string ip_v4;
    ip_v4.append(std::to_string(rand() % 256));
    ip_v4.append(".");
    ip_v4.append(std::to_string(rand() % 256));
    ip_v4.append(".");
    ip_v4.append(std::to_string(rand() % 256));
    ip_v4.append(".");
    ip_v4.append(std::to_string(rand() % 256));
    return ip_v4;
}

static void write_index(const std::string& name, const std::string& path, bool has_prox,
                        const std::vector<std::string>& datas) {
    auto* analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>;
    analyzer->set_stopwords(nullptr);
    auto* indexwriter = _CLNEW lucene::index::IndexWriter(path.c_str(), analyzer, true);
    indexwriter->setRAMBufferSizeMB(512);
    indexwriter->setMaxBufferedDocs(-1);
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
    field->setOmitTermFreqAndPositions(has_prox);
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

static void index_compaction(const std::string& path,
                             std::vector<lucene::store::Directory*> srcDirs,
                             std::vector<lucene::store::Directory*> destDirs, int32_t count) {
    auto* analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>;
    auto* indexwriter = _CLNEW lucene::index::IndexWriter(path.c_str(), analyzer, true);

    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec(
            srcDirs.size(), std::vector<std::pair<uint32_t, uint32_t>>(count));
    int32_t idx = 0;
    int32_t id = 0;
    for (int32_t i = 0; i < count; i++) {
        for (int32_t j = 0; j < srcDirs.size(); j++) {
            if (id == count * destDirs.size()) {
                idx++;
                id = 0;
            }
            trans_vec[j][i] = std::make_pair(idx, id++);
        }
    }

    std::vector<uint32_t> dest_index_docs(destDirs.size());
    for (int32_t i = 0; i < destDirs.size(); i++) {
        dest_index_docs[i] = count * destDirs.size();
    }

    std::exception_ptr eptr;
    try {
        indexwriter->indexCompaction(srcDirs, destDirs, trans_vec, dest_index_docs);
    } catch (...) {
        eptr = std::current_exception();
    }
    FINALLY(eptr, {
        indexwriter->close();
        _CLDELETE(indexwriter);
        _CLDELETE(analyzer);
    })
}

TEST_F(IndexCompactionWriteIndexTest, test_compaction_exception) {
    std::srand(getDaySeed());
    std::string name = "field_name";

    // index v1
    {
        std::string path = kTestDir + "/index1";
        std::vector<std::string> datas;
        for (int32_t i = 0; i < 10; i++) {
            std::string ip_v4 = generateRandomIP();
            datas.emplace_back(ip_v4);
        }
        write_index(name, path, true, datas);
    }

    // index v2
    {
        std::string path = kTestDir + "/index2";
        std::vector<std::string> datas;
        for (int32_t i = 0; i < 10; i++) {
            std::string ip_v4 = generateRandomIP();
            datas.emplace_back(ip_v4);
        }
        write_index(name, path, false, datas);
    }

    // index compaction exception 1
    {
        std::vector<lucene::store::Directory*> srcDirs;
        srcDirs.push_back(FSDirectory::getDirectory(std::string(kTestDir + "/index1").c_str()));
        srcDirs.push_back(FSDirectory::getDirectory(std::string(kTestDir + "/index2").c_str()));
        std::vector<lucene::store::Directory*> destDirs;
        destDirs.push_back(FSDirectory::getDirectory(std::string(kTestDir + "/index4").c_str()));

        std::string path = kTestDir + "/index0";
        try {
            index_compaction(path, srcDirs, destDirs, 10);
        } catch (const CLuceneError& e) {
            EXPECT_EQ(e.number(), CL_ERR_IllegalArgument);
        }
        for (auto& p : srcDirs) {
            p->close();
            _CLDECDELETE(p);
        }
        for (auto& p : destDirs) {
            p->close();
            _CLDECDELETE(p);
        }
    }
}

} // namespace doris::segment_v2
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

#include "olap/rowset/segment_v2/inverted_index/util/reader.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"
#include "util/slice.h"

using namespace lucene::analysis;
using namespace doris::segment_v2::inverted_index;

namespace doris::segment_v2 {

TEST(ReaderTest, ArrayFieldTokenStreamWorkflow) {
    CharFilterMap char_filter_map;
    char_filter_map["char_filter_type"] = "char_replace";
    char_filter_map["char_filter_pattern"] = ",";
    char_filter_map["char_filter_replacement"] = " ";

    // 正确创建 InvertedIndexCtx
    auto inverted_index_ctx = std::make_shared<InvertedIndexCtx>();
    inverted_index_ctx->custom_analyzer = "";
    inverted_index_ctx->parser_type = InvertedIndexParserType::PARSER_STANDARD;
    inverted_index_ctx->parser_mode = "standard";
    inverted_index_ctx->support_phrase = "yes";
    inverted_index_ctx->char_filter_map = char_filter_map;
    inverted_index_ctx->lower_case = "true";
    inverted_index_ctx->stop_words = "";

    auto analyzer = InvertedIndexAnalyzer::create_analyzer(inverted_index_ctx.get());
    ASSERT_NE(analyzer, nullptr);

    std::string test_data = "hello,world,test";
    Slice slice(test_data);

    std::vector<ReaderPtr> keep_readers;
    auto dir = std::make_shared<lucene::store::RAMDirectory>();
    {
        lucene::index::IndexWriter indexwriter(dir.get(), analyzer.get(), true);
        indexwriter.setRAMBufferSizeMB(512);
        indexwriter.setMaxFieldLength(0x7FFFFFFFL);
        indexwriter.setMergeFactor(1000000000);
        indexwriter.setUseCompoundFile(false);
        lucene::document::Document doc;
        std::unique_ptr<lucene::document::Field> new_field;
        for (int i = 0; i < 2; i++) {
            int32_t field_config = lucene::document::Field::STORE_NO;
            field_config |= lucene::document::Field::INDEX_NONORMS;
            field_config |= lucene::document::Field::INDEX_TOKENIZED;
            auto* field = _CLNEW lucene::document::Field(L"name", field_config);
            new_field.reset(field);
            {
                ReaderPtr char_string_reader =
                        InvertedIndexAnalyzer::create_reader(inverted_index_ctx->char_filter_map);
                char_string_reader->init(slice.get_data(), cast_set<int32_t>(slice.get_size()),
                                         false);

                auto* ts = analyzer->tokenStream(new_field->name(), char_string_reader);
                ASSERT_NE(ts, nullptr);

                new_field->setValue(ts, true);
                keep_readers.emplace_back(std::move(char_string_reader));
            }
            doc.add(*new_field.release());
        }
        indexwriter.addDocument(&doc);
        indexwriter.close();
    }
    dir->close();
}

} // namespace doris::segment_v2
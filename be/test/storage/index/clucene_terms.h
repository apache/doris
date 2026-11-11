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

#pragma once

#include <CLucene.h>
#include <CLucene/config/repl_wchar.h>
#include <CLucene/index/IndexReader.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "io/fs/local_file_system.h"
#include "storage/index/index_file_reader.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

// Every term of a CLucene index in term order, each with the documents holding it.
inline std::vector<std::pair<std::string, std::vector<int>>> clucene_terms(
        const std::string& index_path_prefix, InvertedIndexStorageFormatPB storage_format,
        const TabletIndex* index_meta) {
    std::vector<std::pair<std::string, std::vector<int>>> terms;
    IndexFileReader reader(io::global_local_filesystem(), index_path_prefix, storage_format);
    EXPECT_TRUE(reader.init().ok());
    auto directory = reader.open(index_meta);
    EXPECT_TRUE(directory.has_value()) << directory.error();
    if (!directory.has_value()) {
        return terms;
    }
    lucene::index::IndexReader* index_reader =
            lucene::index::IndexReader::open(directory.value().get());
    lucene::index::TermEnum* term_enum = index_reader->terms();
    while (term_enum->next()) {
        lucene::index::Term* term = term_enum->term(false);
        std::vector<int> docs;
        lucene::index::TermDocs* term_docs = index_reader->termDocs(term);
        while (term_docs->next()) {
            docs.push_back(term_docs->doc());
        }
        term_docs->close();
        _CLLDELETE(term_docs);
        terms.emplace_back(lucene_wcstoutf8string(term->text(), term->textLength()),
                           std::move(docs));
    }
    term_enum->close();
    _CLLDELETE(term_enum);
    index_reader->close();
    _CLLDELETE(index_reader);
    return terms;
}

} // namespace doris::segment_v2

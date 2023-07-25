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

#include <CLucene/CLConfig.h>
#include <CLucene/SharedHeader.h>
#include <CLucene/debug/error.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/FSDirectory.h>
#include <gflags/gflags.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "CLucene/config/repl_wchar.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"

using doris::segment_v2::DorisCompoundReader;
using doris::segment_v2::DorisCompoundDirectory;
using namespace lucene::analysis;
using namespace lucene::index;
using namespace lucene::util;
using namespace lucene::search;

DEFINE_string(operation, "show_files", "valid operation: show_files,check_terms,term_query_equal");

DEFINE_string(root_path, "./", "storage root path");
DEFINE_string(idx_file_path, "", "inverted index file path");
DEFINE_string(term, "", "inverted index term to query");
DEFINE_string(column_name, "", "inverted index column_name to query");
//DEFINE_string(nested_file, "", "inverted index nested file name");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris Inverted index file tool.\n";
    ss << "Stop BE first before use this tool.\n";
    ss << "Usage:\n";
    ss << "./index_tool --operation=show_files --root_path=directory "
          "--idx_file_path=path/to/file\n";
    ss << "./index_tool --operation=check_terms_stats --root_path=directory "
          "--idx_file_path=path/to/file\n";
    ss << "./index_tool --operation=term_query_equal --root_path=directory "
          "--idx_file_path=path/to/file --term=term --column_name=column_name\n";
    //ss << "./index_tool --operation=check_nested_file --root_path=directory --idx_file_path=path/to/file --nested_file=nested_file_name\n";
    return ss.str();
}

void SearchFiles(lucene::store::Directory* dir, std::string& field, std::string& token) {
    IndexReader* reader = IndexReader::open(dir);

    IndexReader* newreader = reader->reopen();
    if (newreader != reader) {
        _CLLDELETE(reader);
        reader = newreader;
    }
    IndexSearcher s(reader);

    std::wstring field_ws(field.begin(), field.end());
    std::wstring token_ws(token.begin(), token.end());
    lucene::index::Term* term = _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str());
    auto q = _CLNEW lucene::search::TermQuery(term);
    _CLDECDELETE(term);

    std::vector<uint32_t> result;
    int total = 0;
    s._search(q, [&result, &total](const int32_t docid, const float_t /*score*/) {
        // docid equal to rowid in segment
        result.push_back(docid);
        printf("RowID is %d\n", docid);
        total += 1;
    });
    std::cout << "Term queried count:" << total << std::endl;
    _CLLDELETE(q);

    s.close();
    reader->close();
    _CLLDELETE(reader);
}

void check_terms_stats(lucene::store::Directory* dir) {
    IndexReader* r = IndexReader::open(dir);
    //printf("Statistics for %s\n", directory);
    printf("==================================\n");

    printf("Max Docs: %d\n", r->maxDoc());
    printf("Num Docs: %d\n", r->numDocs());

    int64_t ver = r->getCurrentVersion(dir);
    printf("Current Version: %f\n", (float_t)ver);

    TermEnum* te = r->terms();
    int32_t nterms;
    for (nterms = 0; te->next() == true; nterms++) {
        /* empty */
        std::string token = lucene_wcstoutf8string(te->term()->text(), te->term()->textLength());

        printf("Term: %s ", token.c_str());
        printf("Freq: %d\n", te->docFreq());
    }
    printf("Term count: %d\n\n", nterms);
    _CLLDELETE(te);

    r->close();
    _CLLDELETE(r);
}

int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "show_files") {
        if (FLAGS_idx_file_path == "") {
            std::cout << "no file flag for show " << std::endl;
            return -1;
        }
        auto fs = doris::io::global_local_filesystem();
        try {
            lucene::store::Directory* dir =
                    DorisCompoundDirectory::getDirectory(fs, FLAGS_root_path.c_str());
            auto reader = new DorisCompoundReader(dir, FLAGS_idx_file_path.c_str(), 4096);
            std::vector<std::string> files;
            reader->list(&files);
            for (auto& file : files) {
                std::cout << file << std::endl;
            }
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when show files: " << err.what() << std::endl;
        }
    } else if (FLAGS_operation == "check_terms_stats") {
        if (FLAGS_idx_file_path == "") {
            std::cout << "no file flag for check " << std::endl;
            return -1;
        }
        auto fs = doris::io::global_local_filesystem();
        try {
            lucene::store::Directory* dir =
                    DorisCompoundDirectory::getDirectory(fs, FLAGS_root_path.c_str());
            auto reader = new DorisCompoundReader(dir, FLAGS_idx_file_path.c_str(), 4096);
            check_terms_stats(reader);
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when check_terms_stats: " << err.what() << std::endl;
        }
    } else if (FLAGS_operation == "term_query_equal") {
        if (FLAGS_idx_file_path == "" || FLAGS_term == "" || FLAGS_column_name == "") {
            std::cout << "invalid params for term_query_equal " << std::endl;
            return -1;
        }
        auto fs = doris::io::global_local_filesystem();
        try {
            lucene::store::Directory* dir =
                    DorisCompoundDirectory::getDirectory(fs, FLAGS_root_path.c_str());
            auto reader = new DorisCompoundReader(dir, FLAGS_idx_file_path.c_str(), 4096);
            SearchFiles(reader, FLAGS_column_name, FLAGS_term);
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when check_terms_stats: " << err.what() << std::endl;
        }
    } else {
        std::cout << "invalid operation: " << FLAGS_operation << "\n" << usage << std::endl;
        return -1;
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}
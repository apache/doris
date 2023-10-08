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

#include <CLucene.h>
#include <CLucene/config/repl_wchar.h>
#include <gflags/gflags.h>

#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"

using doris::segment_v2::DorisCompoundReader;
using doris::segment_v2::DorisCompoundDirectory;
using doris::io::FileInfo;
using namespace lucene::analysis;
using namespace lucene::index;
using namespace lucene::util;
using namespace lucene::search;

DEFINE_string(operation, "", "valid operation: show_nested_files,check_terms,term_query");

DEFINE_string(directory, "./", "inverted index file directory");
DEFINE_string(idx_file_name, "", "inverted index file name");
DEFINE_string(idx_file_path, "", "inverted index file path");
DEFINE_string(term, "", "inverted index term to query");
DEFINE_string(column_name, "", "inverted index column_name to query");
DEFINE_string(pred_type, "", "inverted index term query predicate, eq/lt/gt/le/ge/match etc.");
DEFINE_bool(print_row_id, false, "print row id when query terms");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris inverted index file tool.\n";
    ss << "Stop BE first before use this tool.\n";
    ss << "Usage:\n";
    ss << "./index_tool --operation=show_nested_files --idx_file_path=path/to/file\n";
    ss << "./index_tool --operation=check_terms_stats --idx_file_path=path/to/file\n";
    ss << "./index_tool --operation=term_query --directory=directory "
          "--idx_file_name=file --print_row_id --term=term --column_name=column_name "
          "--pred_type=eq/lt/gt/le/ge/match etc\n";
    return ss.str();
}

void search(lucene::store::Directory* dir, std::string& field, std::string& token,
            std::string& pred) {
    IndexReader* reader = IndexReader::open(dir);

    IndexReader* newreader = reader->reopen();
    if (newreader != reader) {
        _CLLDELETE(reader);
        reader = newreader;
    }
    IndexSearcher s(reader);
    std::unique_ptr<lucene::search::Query> query;

    std::wstring field_ws(field.begin(), field.end());
    std::wstring token_ws(token.begin(), token.end());
    lucene::index::Term* term = _CLNEW lucene::index::Term(field_ws.c_str(), token_ws.c_str());
    if (pred == "eq" || pred == "match") {
        query.reset(new lucene::search::TermQuery(term));
    } else if (pred == "lt") {
        query.reset(new lucene::search::RangeQuery(nullptr, term, false));
    } else if (pred == "gt") {
        query.reset(new lucene::search::RangeQuery(term, nullptr, false));
    } else if (pred == "le") {
        query.reset(new lucene::search::RangeQuery(nullptr, term, true));
    } else if (pred == "ge") {
        query.reset(new lucene::search::RangeQuery(term, nullptr, true));
    } else {
        std::cout << "invalid predicate type:" << pred << std::endl;
        exit(-1);
    }
    _CLDECDELETE(term);

    std::vector<uint32_t> result;
    int total = 0;

    s._search(query.get(), [&result, &total](const int32_t docid, const float_t /*score*/) {
        // docid equal to rowid in segment
        result.push_back(docid);
        if (FLAGS_print_row_id) {
            printf("RowID is %d\n", docid);
        }
        total += 1;
    });
    std::cout << "Term queried count:" << total << std::endl;

    s.close();
    reader->close();
    _CLLDELETE(reader);
}

void check_terms_stats(lucene::store::Directory* dir) {
    IndexReader* r = IndexReader::open(dir);

    printf("Max Docs: %d\n", r->maxDoc());
    printf("Num Docs: %d\n", r->numDocs());

    int64_t ver = r->getCurrentVersion(dir);
    printf("Current Version: %f\n", (float_t)ver);

    TermEnum* te = r->terms();
    int32_t nterms;
    for (nterms = 0; te->next(); nterms++) {
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

    if (FLAGS_operation == "show_nested_files") {
        if (FLAGS_idx_file_path == "") {
            std::cout << "no file flag for show " << std::endl;
            return -1;
        }
        std::filesystem::path p(FLAGS_idx_file_path);
        std::string dir_str = p.parent_path().string();
        std::string file_str = p.filename().string();
        auto fs = doris::io::global_local_filesystem();
        try {
            lucene::store::Directory* dir =
                    DorisCompoundDirectory::getDirectory(fs, dir_str.c_str());
            auto reader = new DorisCompoundReader(dir, file_str.c_str(), 4096);
            std::vector<std::string> files;
            std::cout << "Nested files for " << file_str << std::endl;
            std::cout << "==================================" << std::endl;
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
        std::filesystem::path p(FLAGS_idx_file_path);
        std::string dir_str = p.parent_path().string();
        std::string file_str = p.filename().string();
        auto fs = doris::io::global_local_filesystem();
        try {
            lucene::store::Directory* dir =
                    DorisCompoundDirectory::getDirectory(fs, dir_str.c_str());
            auto reader = new DorisCompoundReader(dir, file_str.c_str(), 4096);
            std::cout << "Term statistics for " << file_str << std::endl;
            std::cout << "==================================" << std::endl;
            check_terms_stats(reader);
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when check_terms_stats: " << err.what() << std::endl;
        }
    } else if (FLAGS_operation == "term_query") {
        if (FLAGS_directory == "" || FLAGS_term == "" || FLAGS_column_name == "" ||
            FLAGS_pred_type == "") {
            std::cout << "invalid params for term_query " << std::endl;
            return -1;
        }
        auto fs = doris::io::global_local_filesystem();
        try {
            lucene::store::Directory* dir =
                    DorisCompoundDirectory::getDirectory(fs, FLAGS_directory.c_str());
            if (FLAGS_idx_file_name == "") {
                //try to search from directory's all files
                std::vector<FileInfo> files;
                bool exists = false;
                std::filesystem::path root_dir(FLAGS_directory);
                static_cast<void>(fs->list(root_dir, true, &files, &exists));
                if (!exists) {
                    std::cout << FLAGS_directory << " is not exists" << std::endl;
                    return -1;
                }
                for (auto& f : files) {
                    try {
                        auto file_str = f.file_name;
                        if (!file_str.ends_with(".idx")) {
                            continue;
                        }
                        auto reader = new DorisCompoundReader(dir, file_str.c_str(), 4096);
                        std::cout << "Search " << FLAGS_column_name << ":" << FLAGS_term << " from "
                                  << file_str << std::endl;
                        std::cout << "==================================" << std::endl;
                        search(reader, FLAGS_column_name, FLAGS_term, FLAGS_pred_type);
                    } catch (CLuceneError& err) {
                        std::cerr << "error occurred when search file: " << f.file_name
                                  << ", error:" << err.what() << std::endl;
                    }
                }
            } else {
                auto reader = new DorisCompoundReader(dir, FLAGS_idx_file_name.c_str(), 4096);
                std::cout << "Search " << FLAGS_column_name << ":" << FLAGS_term << " from "
                          << FLAGS_idx_file_name << std::endl;
                std::cout << "==================================" << std::endl;
                try {
                    search(reader, FLAGS_column_name, FLAGS_term, FLAGS_pred_type);
                } catch (CLuceneError& err) {
                    std::cerr << "error occurred when search file: " << FLAGS_idx_file_name
                              << ", error:" << err.what() << std::endl;
                }
            }
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

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
#include <gen_cpp/PaloInternalService_types.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <roaring/roaring.hh>
#include <sstream>
#include <string>
#include <vector>

#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index/query/conjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/query.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"

using doris::segment_v2::DorisCompoundReader;
using doris::segment_v2::DorisCompoundDirectoryFactory;
using doris::io::FileInfo;
using namespace doris::segment_v2;
using namespace lucene::analysis;
using namespace lucene::index;
using namespace lucene::util;
using namespace lucene::search;

DEFINE_string(operation, "",
              "valid operation: show_nested_files,check_terms,term_query,debug_index_compaction");

DEFINE_string(directory, "./", "inverted index file directory");
DEFINE_string(idx_file_name, "", "inverted index file name");
DEFINE_string(idx_file_path, "", "inverted index file path");
DEFINE_string(term, "", "inverted index term to query");
DEFINE_string(column_name, "", "inverted index column_name to query");
DEFINE_string(pred_type, "", "inverted index term query predicate, eq/lt/gt/le/ge/match etc.");
DEFINE_bool(print_row_id, false, "print row id when query terms");
DEFINE_bool(print_doc_id, false, "print doc id when check terms stats");
// only for debug index compaction
DEFINE_int32(idx_id, -1, "inverted index id");
DEFINE_string(src_idx_dirs_file, "", "source segment index files");
DEFINE_string(dest_idx_dirs_file, "", "destination segment index files");
DEFINE_string(dest_seg_num_rows_file, "", "destination segment number of rows");
DEFINE_string(tablet_path, "", "tablet path");
DEFINE_string(trans_vec_file, "", "rowid conversion map file");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris inverted index file tool.\n";
    ss << "Usage:\n";
    ss << "./index_tool --operation=show_nested_files --idx_file_path=path/to/file\n";
    ss << "./index_tool --operation=check_terms_stats --idx_file_path=path/to/file "
          "--print_doc_id\n";
    ss << "./index_tool --operation=term_query --directory=directory "
          "--idx_file_name=file --print_row_id --term=term --column_name=column_name "
          "--pred_type=eq/lt/gt/le/ge/match etc\n";
    ss << "*** debug_index_compaction operation is only for offline debug index compaction, do not "
          "use in production ***\n";
    ss << "./index_tool --operation=debug_index_compaction --idx_id=index_id "
          "--src_idx_dirs_file=path/to/file --dest_idx_dirs_file=path/to/file "
          "--dest_seg_num_rows_file=path/to/file --tablet_path=path/to/tablet "
          "--trans_vec_file=path/to/file\n";
    return ss.str();
}

std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

void search(lucene::store::Directory* dir, std::string& field, std::string& token,
            std::string& pred) {
    IndexReader* reader = IndexReader::open(dir);

    IndexReader* newreader = reader->reopen();
    if (newreader != reader) {
        reader->close();
        _CLDELETE(reader);
        reader = newreader;
    }
    auto s = std::make_shared<IndexSearcher>(reader);
    std::unique_ptr<lucene::search::Query> query;

    std::cout << "version: " << (int32_t)(reader->getIndexVersion()) << std::endl;

    std::wstring field_ws(field.begin(), field.end());
    if (pred == "match_all") {
    } else if (pred == "match_phrase") {
        std::vector<std::string> terms = split(token, '|');
        auto* phrase_query = new lucene::search::PhraseQuery();
        for (auto& term : terms) {
            std::wstring term_ws = StringUtil::string_to_wstring(term);
            auto* t = _CLNEW lucene::index::Term(field_ws.c_str(), term_ws.c_str());
            phrase_query->add(t);
            _CLDECDELETE(t);
        }
        query.reset(phrase_query);
    } else {
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
    }

    int32_t total = 0;
    if (pred == "match_all") {
        roaring::Roaring result;
        std::vector<std::string> terms = split(token, '|');

        doris::TQueryOptions queryOptions;
        ConjunctionQuery query(s, queryOptions);
        query.add(field_ws, terms);
        query.search(result);

        total += result.cardinality();
    } else {
        roaring::Roaring result;
        s->_search(query.get(), [&result](const int32_t docid, const float_t /*score*/) {
            // docid equal to rowid in segment
            result.add(docid);
            if (FLAGS_print_row_id) {
                printf("RowID is %d\n", docid);
            }
        });
        total += result.cardinality();
    }
    std::cout << "Term queried count:" << total << std::endl;

    s->close();
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
        std::string token =
                lucene_wcstoutf8string(te->term(false)->text(), te->term(false)->textLength());

        printf("Term: %s ", token.c_str());
        printf("Freq: %d\n", te->docFreq());
        if (FLAGS_print_doc_id) {
            TermDocs* td = r->termDocs(te->term());
            while (td->next()) {
                printf("DocID: %d ", td->doc());
                printf("TermFreq: %d\n", td->freq());
            }
            _CLLDELETE(td);
        }
    }
    printf("Term count: %d\n\n", nterms);
    te->close();
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
        bool is_exists = false;
        const auto file_path = dir_str + "/" + file_str;
        if (!(fs->exists(file_path, &is_exists).ok()) || !is_exists) {
            std::cerr << "file " << file_path << " not found" << std::endl;
            return -1;
        }
        std::unique_ptr<DorisCompoundReader> reader;
        try {
            reader = std::make_unique<DorisCompoundReader>(
                    DorisCompoundDirectoryFactory::getDirectory(fs, dir_str.c_str()),
                    file_str.c_str(), 4096);
            std::vector<std::string> files;
            std::cout << "Nested files for " << file_str << std::endl;
            std::cout << "==================================" << std::endl;
            reader->list(&files);
            for (auto& file : files) {
                std::cout << file << std::endl;
            }
            reader->close();
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when show files: " << err.what() << std::endl;
            if (reader) {
                reader->close();
            }
            return -1;
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
        bool is_exists = false;
        const auto file_path = dir_str + "/" + file_str;
        if (!(fs->exists(file_path, &is_exists).ok()) || !is_exists) {
            std::cerr << "file " << file_path << " not found" << std::endl;
            return -1;
        }
        std::unique_ptr<DorisCompoundReader> reader;
        try {
            reader = std::make_unique<DorisCompoundReader>(
                    DorisCompoundDirectoryFactory::getDirectory(fs, dir_str.c_str()),
                    file_str.c_str(), 4096);
            std::cout << "Term statistics for " << file_str << std::endl;
            std::cout << "==================================" << std::endl;
            check_terms_stats(reader.get());
            reader->close();
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when check_terms_stats: " << err.what() << std::endl;
            if (reader) {
                reader->close();
            }
            return -1;
        }
    } else if (FLAGS_operation == "term_query") {
        if (FLAGS_directory == "" || FLAGS_term == "" || FLAGS_column_name == "" ||
            FLAGS_pred_type == "") {
            std::cerr << "invalid params for term_query " << std::endl;
            return -1;
        }
        auto fs = doris::io::global_local_filesystem();
        std::unique_ptr<DorisCompoundReader> reader;
        try {
            if (FLAGS_idx_file_name == "") {
                //try to search from directory's all files
                std::vector<FileInfo> files;
                bool exists = false;
                std::filesystem::path root_dir(FLAGS_directory);
                doris::Status status = fs->list(root_dir, true, &files, &exists);
                if (!status.ok()) {
                    std::cerr << "can't search from directory's all files,err : " << status
                              << std::endl;
                    return -1;
                }
                if (!exists) {
                    std::cerr << FLAGS_directory << " is not exists" << std::endl;
                    return -1;
                }
                for (auto& f : files) {
                    try {
                        auto file_str = f.file_name;
                        if (!file_str.ends_with(".idx")) {
                            continue;
                        }
                        reader = std::make_unique<DorisCompoundReader>(
                                DorisCompoundDirectoryFactory::getDirectory(fs, file_str.c_str()),
                                file_str.c_str(), 4096);
                        std::cout << "Search " << FLAGS_column_name << ":" << FLAGS_term << " from "
                                  << file_str << std::endl;
                        std::cout << "==================================" << std::endl;
                        search(reader.get(), FLAGS_column_name, FLAGS_term, FLAGS_pred_type);
                        reader->close();
                    } catch (CLuceneError& err) {
                        std::cerr << "error occurred when search file: " << f.file_name
                                  << ", error:" << err.what() << std::endl;
                        if (reader) {
                            reader->close();
                        }
                        return -1;
                    }
                }
            } else {
                bool is_exists = false;
                auto file_path = FLAGS_directory + "/" + FLAGS_idx_file_name;
                if (!(fs->exists(file_path, &is_exists).ok()) || !is_exists) {
                    std::cerr << "file " << file_path << " not found" << std::endl;
                    return -1;
                }
                auto reader = std::make_unique<DorisCompoundReader>(
                        DorisCompoundDirectoryFactory::getDirectory(fs, FLAGS_directory.c_str()),
                        FLAGS_idx_file_name.c_str(), 4096);
                std::cout << "Search " << FLAGS_column_name << ":" << FLAGS_term << " from "
                          << FLAGS_idx_file_name << std::endl;
                std::cout << "==================================" << std::endl;
                try {
                    search(reader.get(), FLAGS_column_name, FLAGS_term, FLAGS_pred_type);
                    reader->close();
                } catch (CLuceneError& err) {
                    std::cerr << "error occurred when search file: " << FLAGS_idx_file_name
                              << ", error:" << err.what() << std::endl;
                    if (reader) {
                        reader->close();
                    }
                    return -1;
                }
            }
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when check_terms_stats: " << err.what() << std::endl;
            if (reader) {
                reader->close();
            }
            return -1;
        }
    } else if (FLAGS_operation == "debug_index_compaction") {
        // only for debug index compaction, do not use in production
        if (FLAGS_idx_id <= 0 || FLAGS_src_idx_dirs_file == "" || FLAGS_dest_idx_dirs_file == "" ||
            FLAGS_dest_seg_num_rows_file == "" || FLAGS_tablet_path == "" ||
            FLAGS_trans_vec_file == "") {
            std::cout << "invalid params for debug_index_compaction " << std::endl;
            return -1;
        }

        auto fs = doris::io::global_local_filesystem();

        auto read_file_to_json = [&](const std::string& file, std::string& output) {
            doris::io::FileReaderSPtr file_reader;
            doris::Status status = fs->open_file(file, &file_reader);
            if (!status.ok()) {
                std::cout << "read file " << file << " failed" << std::endl;
                return false;
            }
            size_t fsize = file_reader->size();
            if (fsize > 0) {
                output.resize(fsize);
                size_t bytes_read = 0;
                status = file_reader->read_at(0, {output.data(), fsize}, &bytes_read);
            }
            if (!status.ok()) {
                std::cout << "read file " << file << " failed" << std::endl;
                return false;
            }
            return true;
        };

        int32_t index_id = FLAGS_idx_id;
        std::string tablet_path = FLAGS_tablet_path;
        std::string src_index_dirs_string;
        std::string dest_index_dirs_string;
        std::string dest_segment_num_rows_string;
        std::string trans_vec_string;

        if (!read_file_to_json(FLAGS_src_idx_dirs_file, src_index_dirs_string) ||
            !read_file_to_json(FLAGS_dest_idx_dirs_file, dest_index_dirs_string) ||
            !read_file_to_json(FLAGS_dest_seg_num_rows_file, dest_segment_num_rows_string) ||
            !read_file_to_json(FLAGS_trans_vec_file, trans_vec_string)) {
            return -1;
        }
        std::vector<std::string> src_index_files = nlohmann::json::parse(src_index_dirs_string);
        std::vector<std::string> dest_index_files = nlohmann::json::parse(dest_index_dirs_string);
        std::vector<uint32_t> dest_segment_num_rows =
                nlohmann::json::parse(dest_segment_num_rows_string);
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec =
                nlohmann::json::parse(trans_vec_string);
        int src_segment_num = src_index_files.size();
        int dest_segment_num = dest_index_files.size();

        std::string index_writer_path = tablet_path + "/tmp_index_writer";
        lucene::store::Directory* dir =
                DorisCompoundDirectoryFactory::getDirectory(fs, index_writer_path.c_str(), false);
        lucene::analysis::SimpleAnalyzer<char> analyzer;
        auto index_writer = _CLNEW lucene::index::IndexWriter(dir, &analyzer, true /* create */,
                                                              true /* closeDirOnShutdown */);
        std::ostream* infoStream = &std::cout;
        index_writer->setInfoStream(infoStream);
        // get compound directory src_index_dirs
        std::vector<lucene::store::Directory*> src_index_dirs(src_segment_num);
        for (int i = 0; i < src_segment_num; ++i) {
            // format: rowsetId_segmentId_indexId.idx
            std::string src_idx_full_name =
                    src_index_files[i] + "_" + std::to_string(index_id) + ".idx";
            DorisCompoundReader* reader = new DorisCompoundReader(
                    DorisCompoundDirectoryFactory::getDirectory(fs, tablet_path.c_str()),
                    src_idx_full_name.c_str());
            src_index_dirs[i] = reader;
        }

        // get dest idx file paths
        std::vector<lucene::store::Directory*> dest_index_dirs(dest_segment_num);
        for (int i = 0; i < dest_segment_num; ++i) {
            // format: rowsetId_segmentId_columnId
            auto path = tablet_path + "/" + dest_index_files[i] + "_" + std::to_string(index_id);
            dest_index_dirs[i] =
                    DorisCompoundDirectoryFactory::getDirectory(fs, path.c_str(), true);
        }

        index_writer->indexCompaction(src_index_dirs, dest_index_dirs, trans_vec,
                                      dest_segment_num_rows);

        index_writer->close();
        _CLDELETE(index_writer);
        // NOTE: need to ref_cnt-- for dir,
        // when index_writer is destroyed, if closeDir is set, dir will be close
        // _CLDECDELETE(dir) will try to ref_cnt--, when it decreases to 1, dir will be destroyed.
        _CLDECDELETE(dir)
        for (auto d : src_index_dirs) {
            if (d != nullptr) {
                d->close();
                _CLDELETE(d);
            }
        }
        for (auto d : dest_index_dirs) {
            if (d != nullptr) {
                // NOTE: DO NOT close dest dir here, because it will be closed when dest index writer finalize.
                //d->close();
                _CLDELETE(d);
            }
        }

        // delete temporary index_writer_path
        if (!fs->delete_directory(index_writer_path.c_str()).ok()) {
            std::cout << "delete temporary index writer path: " << index_writer_path << " failed."
                      << std::endl;
            return -1;
        }
    } else {
        std::cerr << "invalid operation: " << FLAGS_operation << "\n" << usage << std::endl;
        return -1;
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}
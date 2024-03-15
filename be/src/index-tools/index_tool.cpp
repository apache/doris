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
#include <gen_cpp/olap_file.pb.h>
#include <gflags/gflags.h>

#include <filesystem>
#include <iostream>
#include <memory>
#include <roaring/roaring.hh>
#include <sstream>
#include <string>
#include <vector>

#include "io/fs/file_reader.h"
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include "gutil/strings/strip.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/inverted_index/query/conjunction_query.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"

using doris::segment_v2::DorisCompoundReader;
using doris::segment_v2::DorisFSDirectoryFactory;
using doris::segment_v2::InvertedIndexFileWriter;
using doris::segment_v2::InvertedIndexDescriptor;
using doris::segment_v2::InvertedIndexFileReader;
using doris::io::FileInfo;
using doris::TabletIndex;
using namespace doris::segment_v2;
using namespace lucene::analysis;
using namespace lucene::index;
using namespace lucene::util;
using namespace lucene::search;

DEFINE_string(operation, "", "valid operation: show_nested_files,check_terms,term_query");

DEFINE_string(directory, "./", "inverted index file directory");
DEFINE_string(idx_file_name, "", "inverted index file name");
DEFINE_string(idx_file_path, "", "inverted index file path");
DEFINE_string(data_file_path, "", "inverted index data path");
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
    ss << "./index_tool --operation=write_index_v2 --idx_file_path=path/to/index "
          "--data_file_path=data/to/index\n";
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
        ConjunctionQuery conjunct_query(s, queryOptions);
        conjunct_query.add(field_ws, terms);
        conjunct_query.search(result);

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
                    DorisFSDirectoryFactory::getDirectory(fs, dir_str.c_str()),
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
                    DorisFSDirectoryFactory::getDirectory(fs, dir_str.c_str()),
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
                                DorisFSDirectoryFactory::getDirectory(fs, file_str.c_str()),
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
                reader = std::make_unique<DorisCompoundReader>(
                        DorisFSDirectoryFactory::getDirectory(fs, FLAGS_directory.c_str()),
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
    } else if (FLAGS_operation == "write_index_v2") {
        if (FLAGS_idx_file_path == "") {
            std::cout << "no index path flag for check " << std::endl;
            return -1;
        }
        if (FLAGS_data_file_path == "") {
            std::cout << "no data file flag for check " << std::endl;
            return -1;
        }
        std::string name = "test";
        std::string file_dir = FLAGS_idx_file_path;
        std::string file_name = "test_index_0.dat";
        int64_t index_id = 1;
        std::string index_suffix = "";
        doris::TabletIndexPB index_pb;
        index_pb.set_index_id(index_id);
        index_pb.set_index_suffix_name(index_suffix);
        TabletIndex index_meta;
        index_meta.init_from_pb(index_pb);
        auto index_path = InvertedIndexDescriptor::get_temporary_index_path(
                file_dir + "/" + file_name, index_id, index_suffix);
        std::vector<std::string> datas;
        {
            if (std::filesystem::exists(FLAGS_data_file_path) &&
                std::filesystem::file_size(FLAGS_data_file_path) == 0) {
                std::cerr << "Error: File '" << FLAGS_data_file_path << "' is empty." << std::endl;
                return -1;
            } else {
                std::ifstream ifs;
                std::cout << "prepare to load " << FLAGS_data_file_path << std::endl;

                ifs.open(FLAGS_data_file_path);
                if (!ifs) {
                    std::cerr << "Error: Unable to open file '" << FLAGS_data_file_path << "'."
                              << std::endl;
                    return -1;
                } else {
                    std::string line;
                    while (std::getline(ifs, line)) {
                        datas.emplace_back(line);
                    }
                    ifs.close();
                }
            }
        }

        auto fs = doris::io::global_local_filesystem();
        auto index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                fs, file_dir, file_name, doris::InvertedIndexStorageFormatPB::V2);
        auto st = index_file_writer->open(&index_meta);
        if (!st.has_value()) {
            std::cerr << "InvertedIndexFileWriter init error:" << st.error() << std::endl;
            return -1;
        }
        using T = std::decay_t<decltype(st)>;
        auto dir = std::forward<T>(st).value();
        auto analyzer = _CLNEW lucene::analysis::standard95::StandardAnalyzer();
        // auto analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>();
        auto indexwriter = _CLNEW lucene::index::IndexWriter(dir, analyzer, true, true);
        indexwriter->setRAMBufferSizeMB(512);
        indexwriter->setMaxFieldLength(0x7FFFFFFFL);
        indexwriter->setMergeFactor(100000000);
        indexwriter->setUseCompoundFile(false);

        auto char_string_reader = _CLNEW lucene::util::SStringReader<char>;

        auto doc = _CLNEW lucene::document::Document();
        auto field_config = (int32_t)(lucene::document::Field::STORE_NO);
        field_config |= (int32_t)(lucene::document::Field::INDEX_NONORMS);
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_name = std::wstring(name.begin(), name.end());
        auto field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
        field->setOmitTermFreqAndPositions(false);
        doc->add(*field);

        for (int32_t j = 0; j < 1; j++) {
            for (auto& str : datas) {
                char_string_reader->init(str.data(), str.size(), false);
                auto stream = analyzer->reusableTokenStream(field->name(), char_string_reader);
                field->setValue(stream);

                // field->setValue(str.data(), str.size());

                indexwriter->addDocument(doc);
            }
        }
        indexwriter->close();

        _CLLDELETE(indexwriter);
        _CLLDELETE(doc);
        _CLLDELETE(analyzer);
        _CLLDELETE(char_string_reader);

        auto ret = index_file_writer->close();
        if (!ret.ok()) {
            std::cerr << "InvertedIndexFileWriter close error:" << ret.msg() << std::endl;
            return -1;
        }
    } else if (FLAGS_operation == "show_nested_files_v2") {
        if (FLAGS_idx_file_path == "") {
            std::cout << "no file flag for show " << std::endl;
            return -1;
        }
        std::filesystem::path p(FLAGS_idx_file_path);
        auto dir_path = p.parent_path();
        std::string file_str = StripSuffixString(p.filename().string(), ".idx");
        auto fs = doris::io::global_local_filesystem();
        try {
            auto index_file_reader = std::make_unique<InvertedIndexFileReader>(
                    fs, dir_path, file_str, doris::InvertedIndexStorageFormatPB::V2);
            auto st = index_file_reader->init(4096);
            if (!st.ok()) {
                std::cerr << "InvertedIndexFileReader init error:" << st.msg() << std::endl;
                return -1;
            }
            std::cout << "Nested files for " << file_str << std::endl;
            std::cout << "==================================" << std::endl;
            auto dirs = index_file_reader->get_all_directories();
            for (auto& dir : *dirs) {
                auto index_id = dir.first.first;
                auto index_suffix = dir.first.second;
                std::vector<std::string> files;
                doris::TabletIndexPB index_pb;
                index_pb.set_index_id(index_id);
                index_pb.set_index_suffix_name(index_suffix);
                TabletIndex index_meta;
                index_meta.init_from_pb(index_pb);
                std::cout << "index_id:" << index_id << " index_suffix:" << index_suffix
                          << std::endl;

                CLuceneError err;
                auto ret = index_file_reader->open(&index_meta);
                if (!ret.has_value()) {
                    std::cerr << "InvertedIndexFileReader open error:" << ret.error() << std::endl;
                    return -1;
                }
                using T = std::decay_t<decltype(ret)>;
                auto reader = std::forward<T>(ret).value();
                reader->list(&files);
                for (auto& file : files) {
                    std::cout << file << std::endl;
                }
            }
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when show files: " << err.what() << std::endl;
        }
    } else if (FLAGS_operation == "check_terms_stats_v2") {
        if (FLAGS_idx_file_path == "") {
            std::cout << "no file flag for check " << std::endl;
            return -1;
        }
        std::filesystem::path p(FLAGS_idx_file_path);
        std::string dir_path = p.parent_path();
        std::string file_str = StripSuffixString(p.filename().string(), ".idx");
        auto fs = doris::io::global_local_filesystem();
        try {
            auto index_file_reader = std::make_unique<InvertedIndexFileReader>(
                    fs, dir_path, file_str, doris::InvertedIndexStorageFormatPB::V2);
            auto st = index_file_reader->init(4096);
            if (!st.ok()) {
                std::cerr << "InvertedIndexFileReader init error:" << st.msg() << std::endl;
                return -1;
            }
            std::vector<std::string> files;
            int64_t index_id = 1;
            std::string index_suffix = "";
            doris::TabletIndexPB index_pb;
            index_pb.set_index_id(index_id);
            index_pb.set_index_suffix_name(index_suffix);
            TabletIndex index_meta;
            index_meta.init_from_pb(index_pb);
            auto ret = index_file_reader->open(&index_meta);
            if (!ret.has_value()) {
                std::cerr << "InvertedIndexFileReader open error:" << ret.error() << std::endl;
                return -1;
            }
            using T = std::decay_t<decltype(ret)>;
            auto reader = std::forward<T>(ret).value();
            index_file_reader->debug_file_entries();
            std::cout << "Term statistics for " << file_str << std::endl;
            std::cout << "==================================" << std::endl;
            check_terms_stats(reader.get());
            reader->close();
        } catch (CLuceneError& err) {
            std::cerr << "error occurred when check_terms_stats: " << err.what() << std::endl;
        }
    } else {
        std::cerr << "invalid operation: " << FLAGS_operation << "\n" << usage << std::endl;
        return -1;
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}

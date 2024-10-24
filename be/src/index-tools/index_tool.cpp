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
#include <fstream>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
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

DEFINE_string(operation, "",
              "valid operation: show_nested_files,check_terms,term_query,debug_index_compaction");

DEFINE_string(directory, "./", "inverted index file directory");
DEFINE_string(idx_file_name, "", "inverted index file name");
DEFINE_string(idx_file_path, "", "inverted index file path");
DEFINE_string(data_file_path, "", "inverted index data path");
DEFINE_string(term, "", "inverted index term to query");
DEFINE_string(column_name, "", "inverted index column_name to query");
DEFINE_string(pred_type, "", "inverted index term query predicate, eq/lt/gt/le/ge/match etc.");
DEFINE_bool(print_row_id, false, "print row id when query terms");
DEFINE_bool(print_doc_id, false, "print doc id when check terms stats");
// only for debug index compaction
DEFINE_int64(idx_id, -1, "inverted index id");
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
    ss << "./index_tool --operation=write_index_v2 --idx_file_path=path/to/index "
          "--data_file_path=data/to/index\n";
    ss << "./index_tool --operation=show_nested_files_v2 --idx_file_path=path/to/file\n";
    ss << "./index_tool --operation=check_terms_stats_v2 --idx_file_path=path/to/file "
          "--idx_id=index_id\n";
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
        std::string field = lucene_wcstoutf8string(te->term(false)->field(),
                                                   lenOfString(te->term(false)->field()));

        printf("Field: %s ", field.c_str());
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

std::unique_ptr<DorisCompoundReader> get_compound_reader(std::string file_path) {
    CLuceneError err;
    CL_NS(store)::IndexInput* index_input = nullptr;
    auto ok = DorisFSDirectory::FSIndexInput::open(doris::io::global_local_filesystem(),
                                                   file_path.c_str(), index_input, err, 4096, -1);
    if (!ok) {
        // now index_input = nullptr
        if (err.number() == CL_ERR_FileNotFound) {
            std::cerr << "file " << file_path << " not found" << std::endl;
            exit(-1);
        }
        std::cerr << "file " << file_path << " open error:" << err.what() << std::endl;
        exit(-1);
    }

    return std::make_unique<DorisCompoundReader>(index_input, 4096);
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
            reader = get_compound_reader(file_path);
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
            reader = get_compound_reader(file_path);
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
                std::vector<doris::io::FileInfo> files;
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
                        const auto file_path = FLAGS_directory + "/" + file_str;
                        reader = get_compound_reader(file_path);
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
                reader = get_compound_reader(file_path);
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

        int64_t index_id = FLAGS_idx_id;
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
                DorisFSDirectoryFactory::getDirectory(fs, index_writer_path.c_str(), false);
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
            const auto file_path = tablet_path + "/" + src_idx_full_name;
            auto reader_ptr = get_compound_reader(file_path);
            DorisCompoundReader* reader = reader_ptr.release();
            src_index_dirs[i] = reader;
        }

        // get dest idx file paths
        std::vector<lucene::store::Directory*> dest_index_dirs(dest_segment_num);
        for (int i = 0; i < dest_segment_num; ++i) {
            // format: rowsetId_segmentId_columnId
            auto path = tablet_path + "/" + dest_index_files[i] + "_" + std::to_string(index_id);
            dest_index_dirs[i] = DorisFSDirectoryFactory::getDirectory(fs, path.c_str(), true);
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
    } else if (FLAGS_operation == "write_index_v2") {
        if (FLAGS_idx_file_path == "") {
            std::cout << "no index path flag for check " << std::endl;
            return -1;
        }
        if (FLAGS_data_file_path == "") {
            std::cout << "no data file flag for check " << std::endl;
            return -1;
        }

        const std::string rowset_id = "test_rowset";
        constexpr int seg_id = 0;
        std::string name = "test";
        const std::string& file_dir = FLAGS_idx_file_path;
        constexpr int64_t index_id = 1;
        doris::TabletIndexPB index_pb;
        index_pb.set_index_id(index_id);
        TabletIndex index_meta;
        index_meta.init_from_pb(index_pb);

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
                fs,
                std::string {InvertedIndexDescriptor::get_index_file_path_prefix(
                        doris::local_segment_path(file_dir, rowset_id, seg_id))},
                rowset_id, seg_id, doris::InvertedIndexStorageFormatPB::V2);
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
        auto field_name = StringUtil::string_to_wstring(name);
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
        std::string index_path_prefix = StripSuffixString(FLAGS_idx_file_path, ".idx");
        auto fs = doris::io::global_local_filesystem();
        try {
            auto index_file_reader = std::make_unique<InvertedIndexFileReader>(
                    fs, index_path_prefix, doris::InvertedIndexStorageFormatPB::V2);
            auto st = index_file_reader->init(4096);
            if (!st.ok()) {
                std::cerr << "InvertedIndexFileReader init error:" << st.msg() << std::endl;
                return -1;
            }
            std::cout << "Nested files for " << index_path_prefix << std::endl;
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
        if (FLAGS_idx_file_path == "" || FLAGS_idx_id <= 0) {
            std::cout << "no file flag for check " << std::endl;
            return -1;
        }
        std::string index_path_prefix = StripSuffixString(FLAGS_idx_file_path, ".idx");
        auto fs = doris::io::global_local_filesystem();
        try {
            auto index_file_reader = std::make_unique<InvertedIndexFileReader>(
                    fs, index_path_prefix, doris::InvertedIndexStorageFormatPB::V2);
            auto st = index_file_reader->init(4096);
            if (!st.ok()) {
                std::cerr << "InvertedIndexFileReader init error:" << st.msg() << std::endl;
                return -1;
            }
            std::vector<std::string> files;
            int64_t index_id = FLAGS_idx_id;
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
            std::cout << "Term statistics for " << index_path_prefix << std::endl;
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

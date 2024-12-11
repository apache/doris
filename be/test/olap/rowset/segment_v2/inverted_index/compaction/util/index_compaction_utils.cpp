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

#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <vector>

#include "CLucene/StdHeader.h"
#include "CLucene/config/repl_wchar.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "olap/base_compaction.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_factory.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/storage_engine.h"

namespace doris {

static int64_t inc_id = 1000;
const static std::string expected_output =
        "Max Docs: 2000\n"
        "Num Docs: 2000\n"
        "Field: 1 Term: bad Freq: 196\n"
        "Field: 1 Term: excellent Freq: 227\n"
        "Field: 1 Term: fine Freq: 190\n"
        "Field: 1 Term: good Freq: 197\n"
        "Field: 1 Term: great Freq: 194\n"
        "Field: 1 Term: maybe Freq: 191\n"
        "Field: 1 Term: no Freq: 205\n"
        "Field: 1 Term: ok Freq: 175\n"
        "Field: 1 Term: terrible Freq: 205\n"
        "Field: 1 Term: yes Freq: 220\n"
        "Term count: 10\n\n";
const static std::string expected_delete_output =
        "Max Docs: 1806\n"
        "Num Docs: 1806\n"
        "Field: 1 Term: bad Freq: 196\n"
        "Field: 1 Term: excellent Freq: 227\n"
        "Field: 1 Term: fine Freq: 190\n"
        "Field: 1 Term: good Freq: 197\n"
        "Field: 1 Term: maybe Freq: 191\n"
        "Field: 1 Term: no Freq: 205\n"
        "Field: 1 Term: ok Freq: 175\n"
        "Field: 1 Term: terrible Freq: 205\n"
        "Field: 1 Term: yes Freq: 220\n"
        "Term count: 9\n\n";

using QueryData = std::pair<std::vector<std::string>, std::vector<int>>;

class IndexCompactionUtils {
    struct DataRow {
        int key;
        std::string word;
        std::string url;
        int num;
    };

    static std::vector<DataRow> read_data(const std::string file_name) {
        std::ifstream file(file_name);
        EXPECT_TRUE(file.is_open());

        std::string line;
        std::vector<DataRow> data;

        while (std::getline(file, line)) {
            std::stringstream ss(line);
            std::string item;
            DataRow row;
            EXPECT_TRUE(std::getline(ss, item, ','));
            row.key = std::stoi(item);
            EXPECT_TRUE(std::getline(ss, item, ','));
            row.word = item;
            EXPECT_TRUE(std::getline(ss, item, ','));
            row.url = item;
            EXPECT_TRUE(std::getline(ss, item, ','));
            row.num = std::stoi(item);
            data.emplace_back(std::move(row));
        }

        file.close();
        return data;
    }

    static bool query_bkd(const TabletIndex* index,
                          std::shared_ptr<InvertedIndexFileReader>& inverted_index_file_reader,
                          const std::vector<int>& query_data,
                          const std::vector<int>& query_result) {
        const auto& idx_reader = BkdIndexReader::create_shared(index, inverted_index_file_reader);
        const auto& index_searcher_builder = std::make_unique<BKDIndexSearcherBuilder>();
        auto dir = inverted_index_file_reader->open(index);
        EXPECT_TRUE(dir.has_value());
        auto searcher_result = index_searcher_builder->get_index_searcher(dir.value().release());
        EXPECT_TRUE(searcher_result.has_value());
        auto bkd_searcher = std::get_if<BKDIndexSearcherPtr>(&searcher_result.value());
        EXPECT_TRUE(bkd_searcher != nullptr);
        idx_reader->_type_info = get_scalar_type_info((FieldType)(*bkd_searcher)->type);
        EXPECT_TRUE(idx_reader->_type_info != nullptr);
        idx_reader->_value_key_coder = get_key_coder(idx_reader->_type_info->type());

        for (int i = 0; i < query_data.size(); i++) {
            vectorized::Field param_value = int32_t(query_data[i]);
            std::unique_ptr<segment_v2::InvertedIndexQueryParamFactory> query_param = nullptr;
            EXPECT_TRUE(segment_v2::InvertedIndexQueryParamFactory::create_query_value(
                                PrimitiveType::TYPE_INT, &param_value, query_param)
                                .ok());
            auto result = std::make_shared<roaring::Roaring>();
            EXPECT_TRUE(idx_reader
                                ->invoke_bkd_query(query_param->get_value(),
                                                   InvertedIndexQueryType::EQUAL_QUERY,
                                                   *bkd_searcher, result)
                                .ok());
            EXPECT_EQ(query_result[i], result->cardinality()) << query_data[i];
        }
        return true;
    }

    static bool query_string(const TabletIndex* index,
                             std::shared_ptr<InvertedIndexFileReader>& inverted_index_file_reader,
                             const std::string& column_name,
                             const std::vector<std::string>& query_data,
                             const std::vector<int>& query_result) {
        const auto& idx_reader =
                StringTypeInvertedIndexReader::create_shared(index, inverted_index_file_reader);
        const auto& index_searcher_builder = std::make_unique<FulltextIndexSearcherBuilder>();
        auto dir = inverted_index_file_reader->open(index);
        EXPECT_TRUE(dir.has_value());
        auto searcher_result = index_searcher_builder->get_index_searcher(dir.value().release());
        EXPECT_TRUE(searcher_result.has_value());
        auto string_searcher = std::get_if<FulltextIndexSearcherPtr>(&searcher_result.value());
        EXPECT_TRUE(string_searcher != nullptr);
        std::wstring column_name_ws = StringUtil::string_to_wstring(column_name);

        for (int i = 0; i < query_data.size(); i++) {
            TQueryOptions queryOptions;
            auto query = QueryFactory::create(InvertedIndexQueryType::EQUAL_QUERY, *string_searcher,
                                              queryOptions, nullptr);
            EXPECT_TRUE(query != nullptr);
            InvertedIndexQueryInfo query_info;
            query_info.field_name = column_name_ws;
            query_info.terms.emplace_back(query_data[i]);
            query->add(query_info);
            auto result = std::make_shared<roaring::Roaring>();
            query->search(*result);
            EXPECT_EQ(query_result[i], result->cardinality()) << query_data[i];
        }
        return true;
    }

    static bool query_fulltext(const TabletIndex* index,
                               std::shared_ptr<InvertedIndexFileReader>& inverted_index_file_reader,
                               const std::string& column_name,
                               const std::vector<std::string>& query_data,
                               const std::vector<int>& query_result) {
        const auto& idx_reader =
                FullTextIndexReader::create_shared(index, inverted_index_file_reader);
        const auto& index_searcher_builder = std::make_unique<FulltextIndexSearcherBuilder>();
        auto dir = inverted_index_file_reader->open(index);
        EXPECT_TRUE(dir.has_value());
        auto searcher_result = index_searcher_builder->get_index_searcher(dir.value().release());
        EXPECT_TRUE(searcher_result.has_value());
        auto string_searcher = std::get_if<FulltextIndexSearcherPtr>(&searcher_result.value());
        EXPECT_TRUE(string_searcher != nullptr);
        std::wstring column_name_ws = StringUtil::string_to_wstring(column_name);

        for (int i = 0; i < query_data.size(); i++) {
            TQueryOptions queryOptions;
            auto query = QueryFactory::create(InvertedIndexQueryType::MATCH_ANY_QUERY,
                                              *string_searcher, queryOptions, nullptr);
            EXPECT_TRUE(query != nullptr);
            InvertedIndexQueryInfo query_info;
            query_info.field_name = column_name_ws;
            query_info.terms.emplace_back(query_data[i]);
            query->add(query_info);
            auto result = std::make_shared<roaring::Roaring>();
            query->search(*result);
            EXPECT_EQ(query_result[i], result->cardinality()) << query_data[i];
        }
        return true;
    }

    static void check_terms_stats(lucene::store::Directory* dir, std::ostream& os = std::cout) {
        IndexReader* r = IndexReader::open(dir);

        os << "Max Docs: " << r->maxDoc() << "\n";
        os << "Num Docs: " << r->numDocs() << "\n";

        TermEnum* te = r->terms();
        int32_t nterms;
        for (nterms = 0; te->next(); nterms++) {
            std::string token =
                    lucene_wcstoutf8string(te->term(false)->text(), te->term(false)->textLength());
            std::string field = lucene_wcstoutf8string(te->term(false)->field(),
                                                       lenOfString(te->term(false)->field()));

            os << "Field: " << field << " ";
            os << "Term: " << token << " ";
            os << "Freq: " << te->docFreq() << "\n";
            if (false) {
                TermDocs* td = r->termDocs(te->term());
                while (td->next()) {
                    os << "DocID: " << td->doc() << " ";
                    os << "TermFreq: " << td->freq() << "\n";
                }
                _CLLDELETE(td);
            }
        }
        os << "Term count: " << nterms << "\n\n";
        te->close();
        _CLLDELETE(te);

        r->close();
        _CLLDELETE(r);
    }
    static Status check_idx_file_correctness(lucene::store::Directory* index_reader,
                                             lucene::store::Directory* tmp_index_reader) {
        lucene::index::IndexReader* idx_reader = lucene::index::IndexReader::open(index_reader);
        lucene::index::IndexReader* tmp_idx_reader =
                lucene::index::IndexReader::open(tmp_index_reader);

        // compare numDocs
        if (idx_reader->numDocs() != tmp_idx_reader->numDocs()) {
            return Status::InternalError(
                    "index compaction correctness check failed, numDocs not equal, idx_numDocs={}, "
                    "tmp_idx_numDocs={}",
                    idx_reader->numDocs(), tmp_idx_reader->numDocs());
        }

        lucene::index::TermEnum* term_enum = idx_reader->terms();
        lucene::index::TermEnum* tmp_term_enum = tmp_idx_reader->terms();
        lucene::index::TermDocs* term_docs = nullptr;
        lucene::index::TermDocs* tmp_term_docs = nullptr;

        // iterate TermEnum
        while (term_enum->next() && tmp_term_enum->next()) {
            std::string token = lucene_wcstoutf8string(term_enum->term(false)->text(),
                                                       term_enum->term(false)->textLength());
            std::string field = lucene_wcstoutf8string(
                    term_enum->term(false)->field(), lenOfString(term_enum->term(false)->field()));
            std::string tmp_token = lucene_wcstoutf8string(
                    tmp_term_enum->term(false)->text(), tmp_term_enum->term(false)->textLength());
            std::string tmp_field =
                    lucene_wcstoutf8string(tmp_term_enum->term(false)->field(),
                                           lenOfString(tmp_term_enum->term(false)->field()));
            // compare token and field
            if (field != tmp_field) {
                return Status::InternalError(
                        "index compaction correctness check failed, fields not equal, field={}, "
                        "tmp_field={}",
                        field, field);
            }
            if (token != tmp_token) {
                return Status::InternalError(
                        "index compaction correctness check failed, tokens not equal, token={}, "
                        "tmp_token={}",
                        token, tmp_token);
            }

            // get term's docId and freq
            term_docs = idx_reader->termDocs(term_enum->term(false));
            tmp_term_docs = tmp_idx_reader->termDocs(tmp_term_enum->term(false));

            // compare term's docId and freq
            while (term_docs->next() && tmp_term_docs->next()) {
                if (term_docs->doc() != tmp_term_docs->doc() ||
                    term_docs->freq() != tmp_term_docs->freq()) {
                    return Status::InternalError(
                            "index compaction correctness check failed, docId or freq not equal, "
                            "docId={}, tmp_docId={}, freq={}, tmp_freq={}",
                            term_docs->doc(), tmp_term_docs->doc(), term_docs->freq(),
                            tmp_term_docs->freq());
                }
            }

            // check if there are remaining docs
            if (term_docs->next() || tmp_term_docs->next()) {
                return Status::InternalError(
                        "index compaction correctness check failed, number of docs not equal for "
                        "term={}, tmp_term={}",
                        token, tmp_token);
            }
            if (term_docs) {
                term_docs->close();
                _CLLDELETE(term_docs);
            }
            if (tmp_term_docs) {
                tmp_term_docs->close();
                _CLLDELETE(tmp_term_docs);
            }
        }

        // check if there are remaining terms
        if (term_enum->next() || tmp_term_enum->next()) {
            return Status::InternalError(
                    "index compaction correctness check failed, number of terms not equal");
        }
        if (term_enum) {
            term_enum->close();
            _CLLDELETE(term_enum);
        }
        if (tmp_term_enum) {
            tmp_term_enum->close();
            _CLLDELETE(tmp_term_enum);
        }
        if (idx_reader) {
            idx_reader->close();
            _CLLDELETE(idx_reader);
        }
        if (tmp_idx_reader) {
            tmp_idx_reader->close();
            _CLLDELETE(tmp_idx_reader);
        }
        return Status::OK();
    }

    static Status do_compaction(
            const std::vector<RowsetSharedPtr>& rowsets, StorageEngine* engine_ref,
            const TabletSharedPtr& tablet, bool is_index_compaction, RowsetSharedPtr& rowset_ptr,
            const std::function<void(const BaseCompaction&, const RowsetWriterContext&)>
                    custom_check = nullptr) {
        config::inverted_index_compaction_enable = is_index_compaction;
        // only base compaction can handle delete predicate
        BaseCompaction compaction(*engine_ref, tablet);
        compaction._input_rowsets = std::move(rowsets);
        compaction.build_basic_info();

        std::vector<RowsetReaderSharedPtr> input_rs_readers;
        create_input_rowsets_readers(compaction, input_rs_readers);

        RowsetWriterContext ctx;
        RETURN_IF_ERROR(compaction.construct_output_rowset_writer(ctx));

        compaction._stats.rowid_conversion = compaction._rowid_conversion.get();
        RETURN_IF_ERROR(Merger::vertical_merge_rowsets(
                tablet, compaction.compaction_type(), *(compaction._cur_tablet_schema),
                input_rs_readers, compaction._output_rs_writer.get(), 100000, 5,
                &compaction._stats));

        const auto& dst_writer =
                dynamic_cast<BaseBetaRowsetWriter*>(compaction._output_rs_writer.get());
        check_idx_file_writer_closed(dst_writer, false);

        RETURN_IF_ERROR(compaction.do_inverted_index_compaction());

        RETURN_IF_ERROR(compaction._output_rs_writer->build(compaction._output_rowset));

        check_idx_file_writer_closed(dst_writer, true);

        if (custom_check) {
            custom_check(compaction, ctx);
        }

        rowset_ptr = std::move(compaction._output_rowset);
        return Status::OK();
    }

    static void create_input_rowsets_readers(const BaseCompaction& compaction,
                                             std::vector<RowsetReaderSharedPtr>& input_rs_readers) {
        input_rs_readers.reserve(compaction._input_rowsets.size());
        for (auto& rowset : compaction._input_rowsets) {
            RowsetReaderSharedPtr rs_reader;
            EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
            input_rs_readers.push_back(std::move(rs_reader));
        }
    }

    static void check_idx_file_writer_closed(BaseBetaRowsetWriter* writer, bool closed) {
        for (const auto& [seg_id, idx_file_writer] : writer->inverted_index_file_writers()) {
            EXPECT_EQ(idx_file_writer->_closed, closed);
        }
    }

    static void init_rs_meta(RowsetMetaSharedPtr& rs_meta, int64_t start, int64_t end) {
        std::string json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "partition_id": 10000,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "empty": false
        })";
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rs_meta->init_from_pb(rowset_meta_pb);
    }

    static RowsetSharedPtr create_delete_predicate_rowset(const TabletSchemaSPtr& schema,
                                                          std::string pred, int64_t version) {
        DeletePredicatePB del_pred;
        del_pred.add_sub_predicates(pred);
        del_pred.set_version(1);
        RowsetMetaSharedPtr rsm(new RowsetMeta());
        init_rs_meta(rsm, version, version);
        RowsetId id;
        id.init(version);
        rsm->set_rowset_id(id);
        rsm->set_delete_predicate(std::move(del_pred));
        rsm->set_tablet_schema(schema);
        return std::make_shared<BetaRowset>(schema, rsm, "");
    }

    static void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
                                 const std::string& index_name, int32_t col_unique_id,
                                 const std::string& column_type, const std::string& column_name,
                                 bool parser = false) {
        column_pb->set_unique_id(col_unique_id);
        column_pb->set_name(column_name);
        column_pb->set_type(column_type);
        column_pb->set_is_key(false);
        column_pb->set_is_nullable(true);
        tablet_index->set_index_id(index_id);
        tablet_index->set_index_name(index_name);
        tablet_index->set_index_type(IndexType::INVERTED);
        tablet_index->add_col_unique_id(col_unique_id);
        if (parser) {
            auto* properties = tablet_index->mutable_properties();
            (*properties)[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_UNICODE;
        }
    }

    static void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                                 const std::string& column_type, const std::string& column_name) {
        column_pb->set_unique_id(col_unique_id);
        column_pb->set_name(column_name);
        column_pb->set_type(column_type);
        column_pb->set_is_key(false);
        column_pb->set_is_nullable(true);
    }

    static void construct_index(TabletIndexPB* tablet_index, int64_t index_id,
                                const std::string& index_name, int32_t col_unique_id,
                                bool parser = false) {
        tablet_index->set_index_id(index_id);
        tablet_index->set_index_name(index_name);
        tablet_index->set_index_type(IndexType::INVERTED);
        tablet_index->add_col_unique_id(col_unique_id);
        if (parser) {
            auto* properties = tablet_index->mutable_properties();
            (*properties)[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_UNICODE;
        }
    }

    static void check_meta_and_file(const RowsetSharedPtr& output_rowset,
                                    const TabletSchemaSPtr& tablet_schema,
                                    const std::map<int, QueryData>& query_map) {
        CHECK_EQ(output_rowset->num_segments(), 1);
        // check rowset meta and file
        int seg_id = 0;
        // meta
        const auto& index_info = output_rowset->_rowset_meta->inverted_index_file_info(seg_id);
        EXPECT_TRUE(index_info.has_index_size());
        const auto& fs = output_rowset->_rowset_meta->fs();
        const auto& file_name = fmt::format("{}/{}_{}.idx", output_rowset->tablet_path(),
                                            output_rowset->rowset_id().to_string(), seg_id);
        int64_t file_size = 0;
        EXPECT_TRUE(fs->file_size(file_name, &file_size).ok());
        EXPECT_EQ(index_info.index_size(), file_size);

        // file
        const auto& seg_path = output_rowset->segment_path(seg_id);
        EXPECT_TRUE(seg_path.has_value());
        const auto& index_file_path_prefix =
                InvertedIndexDescriptor::get_index_file_path_prefix(seg_path.value());
        auto inverted_index_file_reader = std::make_shared<InvertedIndexFileReader>(
                fs, std::string(index_file_path_prefix),
                tablet_schema->get_inverted_index_storage_format(), index_info);
        EXPECT_TRUE(inverted_index_file_reader->init().ok());
        const auto& dirs = inverted_index_file_reader->get_all_directories();
        EXPECT_TRUE(dirs.has_value());
        EXPECT_EQ(dirs.value().size(), 4);

        for (const auto& [col_uid, query_data] : query_map) {
            const auto& column = tablet_schema->column_by_uid(col_uid);
            const auto* index = tablet_schema->inverted_index(column);
            EXPECT_TRUE(index != nullptr);

            if (col_uid == 0 || col_uid == 3) {
                // BKD index
                std::vector<int> query_data_int;
                for (const auto& data : query_data.first) {
                    query_data_int.push_back(std::stoi(data));
                }
                EXPECT_TRUE(query_bkd(index, inverted_index_file_reader, query_data_int,
                                      query_data.second));
            } else if (col_uid == 1) {
                // String index
                EXPECT_TRUE(query_string(index, inverted_index_file_reader, std::to_string(col_uid),
                                         query_data.first, query_data.second));
            } else if (col_uid == 2) {
                // Fulltext index
                EXPECT_TRUE(query_fulltext(index, inverted_index_file_reader,
                                           std::to_string(col_uid), query_data.first,
                                           query_data.second));
            }
        }
    }

    static RowsetWriterContext rowset_writer_context(const std::unique_ptr<DataDir>& data_dir,
                                                     const TabletSchemaSPtr& schema,
                                                     const std::string& tablet_path) {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.tablet_path = tablet_path;
        context.version = Version(inc_id, inc_id);
        context.max_rows_per_segment = 200;
        inc_id++;
        return context;
    }

    static void build_rowsets(const std::unique_ptr<DataDir>& data_dir,
                              const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
                              StorageEngine* engine_ref, std::vector<RowsetSharedPtr>& rowsets,
                              const std::vector<std::string>& data_files,
                              const std::function<void(const int32_t&)> custom_check = nullptr) {
        std::vector<std::vector<DataRow>> data;
        for (auto file : data_files) {
            data.emplace_back(read_data(file));
        }
        for (int i = 0; i < data.size(); i++) {
            const auto& res = RowsetFactory::create_rowset_writer(
                    *engine_ref, rowset_writer_context(data_dir, schema, tablet->tablet_path()),
                    false);
            EXPECT_TRUE(res.has_value()) << res.error();
            const auto& rowset_writer = res.value();

            vectorized::Block block = schema->create_block();
            auto columns = block.mutate_columns();
            for (const auto& row : data[i]) {
                vectorized::Field key = int32_t(row.key);
                vectorized::Field v1(row.word);
                vectorized::Field v2(row.url);
                vectorized::Field v3 = int32_t(row.num);
                columns[0]->insert(key);
                columns[1]->insert(v1);
                columns[2]->insert(v2);
                columns[3]->insert(v3);
            }
            EXPECT_TRUE(rowset_writer->add_block(&block).ok());
            EXPECT_TRUE(rowset_writer->flush().ok());
            const auto& dst_writer = dynamic_cast<BaseBetaRowsetWriter*>(rowset_writer.get());

            check_idx_file_writer_closed(dst_writer, true);

            EXPECT_TRUE(rowset_writer->build(rowsets[i]).ok());
            EXPECT_TRUE(tablet->add_rowset(rowsets[i]).ok());
            EXPECT_TRUE(rowsets[i]->num_segments() == 5);

            // check rowset meta and file
            for (int seg_id = 0; seg_id < rowsets[i]->num_segments(); seg_id++) {
                const auto& index_info = rowsets[i]->_rowset_meta->inverted_index_file_info(seg_id);
                EXPECT_TRUE(index_info.has_index_size());
                const auto& fs = rowsets[i]->_rowset_meta->fs();
                const auto& file_name = fmt::format("{}/{}_{}.idx", rowsets[i]->tablet_path(),
                                                    rowsets[i]->rowset_id().to_string(), seg_id);
                int64_t file_size = 0;
                EXPECT_TRUE(fs->file_size(file_name, &file_size).ok());
                EXPECT_EQ(index_info.index_size(), file_size);

                const auto& seg_path = rowsets[i]->segment_path(seg_id);
                EXPECT_TRUE(seg_path.has_value());
                const auto& index_file_path_prefix =
                        InvertedIndexDescriptor::get_index_file_path_prefix(seg_path.value());
                auto inverted_index_file_reader = std::make_shared<InvertedIndexFileReader>(
                        fs, std::string(index_file_path_prefix),
                        schema->get_inverted_index_storage_format(), index_info);
                EXPECT_TRUE(inverted_index_file_reader->init().ok());
                const auto& dirs = inverted_index_file_reader->get_all_directories();
                EXPECT_TRUE(dirs.has_value());
                if (custom_check) {
                    custom_check(dirs.value().size());
                }
            }
        }
    }

    static std::shared_ptr<InvertedIndexFileReader> init_index_file_reader(
            const RowsetSharedPtr& output_rowset, const std::string& seg_path,
            const InvertedIndexStorageFormatPB& index_storage_format) {
        const auto& index_file_path_prefix =
                InvertedIndexDescriptor::get_index_file_path_prefix(seg_path);
        auto inverted_index_file_reader_index = std::make_shared<InvertedIndexFileReader>(
                output_rowset->_rowset_meta->fs(), std::string(index_file_path_prefix),
                index_storage_format);
        auto st = inverted_index_file_reader_index->init();
        EXPECT_TRUE(st.ok()) << st.to_string();

        return inverted_index_file_reader_index;
    }
};

} // namespace doris
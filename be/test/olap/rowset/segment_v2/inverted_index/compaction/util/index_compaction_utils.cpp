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
#include <nlohmann/json.hpp>
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

const static std::string expected_output =
        "Max Docs: 2000\n"
        "Num Docs: 2000\n"
        "Field: v1 Term: bad Freq: 196\n"
        "Field: v1 Term: excellent Freq: 227\n"
        "Field: v1 Term: fine Freq: 190\n"
        "Field: v1 Term: good Freq: 197\n"
        "Field: v1 Term: great Freq: 194\n"
        "Field: v1 Term: maybe Freq: 191\n"
        "Field: v1 Term: no Freq: 205\n"
        "Field: v1 Term: ok Freq: 175\n"
        "Field: v1 Term: terrible Freq: 205\n"
        "Field: v1 Term: yes Freq: 220\n"
        "Term count: 10\n\n";
const static std::string expected_delete_output =
        "Max Docs: 1806\n"
        "Num Docs: 1806\n"
        "Field: v1 Term: bad Freq: 196\n"
        "Field: v1 Term: excellent Freq: 227\n"
        "Field: v1 Term: fine Freq: 190\n"
        "Field: v1 Term: good Freq: 197\n"
        "Field: v1 Term: maybe Freq: 191\n"
        "Field: v1 Term: no Freq: 205\n"
        "Field: v1 Term: ok Freq: 175\n"
        "Field: v1 Term: terrible Freq: 205\n"
        "Field: v1 Term: yes Freq: 220\n"
        "Term count: 9\n\n";

using QueryData = std::pair<std::vector<std::string>, std::vector<int>>;

class IndexCompactionUtils {
    struct DataRow {
        int key;
        std::string word;
        std::string url;
        int num;
    };
    struct WikiDataRow {
        std::string title;
        std::string content;
        std::string redirect;
        std::string space;
    };

    template <typename T>
    static std::vector<T> read_data(const std::string& file_name);

    template <>
    std::vector<DataRow> read_data<DataRow>(const std::string& file_name) {
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

    template <>
    std::vector<WikiDataRow> read_data<WikiDataRow>(const std::string& file_name) {
        std::ifstream file(file_name);
        EXPECT_TRUE(file.is_open());

        std::vector<WikiDataRow> data;
        std::string line;

        while (std::getline(file, line)) {
            if (line.empty()) {
                continue;
            }
            // catch parse exception and continue
            try {
                nlohmann::json j = nlohmann::json::parse(line);
                WikiDataRow row;
                row.title = j.value("title", "null");
                row.content = j.value("content", "null");
                row.redirect = j.value("redirect", "null");
                row.space = j.value("space", "null");

                data.emplace_back(std::move(row));
            } catch (const std::exception& e) {
                std::cout << "parse json error: " << e.what() << std::endl;
                continue;
            }
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
                                              queryOptions);
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
                                              *string_searcher, queryOptions);
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
    static Status check_idx_file_correctness_impl(lucene::index::IndexReader* idx_reader,
                                                  lucene::index::IndexReader* normal_idx_reader) {
        // compare numDocs
        if (idx_reader->numDocs() != normal_idx_reader->numDocs()) {
            return Status::InternalError(
                    "index compaction correctness check failed, numDocs not equal, idx_numDocs={}, "
                    "normal_idx_numDocs={}",
                    idx_reader->numDocs(), normal_idx_reader->numDocs());
        }

        lucene::index::TermEnum* term_enum = idx_reader->terms();
        lucene::index::TermEnum* normal_term_enum = normal_idx_reader->terms();
        lucene::index::TermDocs* term_docs = nullptr;
        lucene::index::TermDocs* normal_term_docs = nullptr;

        // iterate TermEnum
        while (term_enum->next() && normal_term_enum->next()) {
            std::string token = lucene_wcstoutf8string(term_enum->term(false)->text(),
                                                       term_enum->term(false)->textLength());
            std::string field = lucene_wcstoutf8string(
                    term_enum->term(false)->field(), lenOfString(term_enum->term(false)->field()));
            std::string normal_token =
                    lucene_wcstoutf8string(normal_term_enum->term(false)->text(),
                                           normal_term_enum->term(false)->textLength());
            std::string normal_field =
                    lucene_wcstoutf8string(normal_term_enum->term(false)->field(),
                                           lenOfString(normal_term_enum->term(false)->field()));
            // compare token and field
            if (field != normal_field) {
                return Status::InternalError(
                        "index compaction correctness check failed, fields not equal, field={}, "
                        "normal_field={}",
                        field, field);
            }
            if (token != normal_token) {
                return Status::InternalError(
                        "index compaction correctness check failed, tokens not equal, token={}, "
                        "normal_token={}",
                        token, normal_token);
            }

            // get term's docId and freq
            term_docs = idx_reader->termDocs(term_enum->term(false));
            normal_term_docs = normal_idx_reader->termDocs(normal_term_enum->term(false));

            // compare term's docId and freq
            while (term_docs->next() && normal_term_docs->next()) {
                if (term_docs->doc() != normal_term_docs->doc() ||
                    term_docs->freq() != normal_term_docs->freq()) {
                    return Status::InternalError(
                            "index compaction correctness check failed, docId or freq not equal, "
                            "docId={}, normal_docId={}, freq={}, normal_freq={}",
                            term_docs->doc(), normal_term_docs->doc(), term_docs->freq(),
                            normal_term_docs->freq());
                }
            }

            // check if there are remaining docs
            if (term_docs->next() || normal_term_docs->next()) {
                return Status::InternalError(
                        "index compaction correctness check failed, number of docs not equal for "
                        "term={}, normal_term={}",
                        token, normal_token);
            }
            if (term_docs) {
                term_docs->close();
                _CLLDELETE(term_docs);
            }
            if (normal_term_docs) {
                normal_term_docs->close();
                _CLLDELETE(normal_term_docs);
            }
        }

        // check if there are remaining terms
        if (term_enum->next() || normal_term_enum->next()) {
            return Status::InternalError(
                    "index compaction correctness check failed, number of terms not equal");
        }
        if (term_enum) {
            term_enum->close();
            _CLLDELETE(term_enum);
        }
        if (normal_term_enum) {
            normal_term_enum->close();
            _CLLDELETE(normal_term_enum);
        }
        if (idx_reader) {
            idx_reader->close();
            _CLLDELETE(idx_reader);
        }
        if (normal_idx_reader) {
            normal_idx_reader->close();
            _CLLDELETE(normal_idx_reader);
        }
        return Status::OK();
    }

    static Status check_idx_file_correctness(lucene::store::Directory* index_reader,
                                             lucene::store::Directory* normal_index_reader) {
        lucene::index::IndexReader* idx_reader = lucene::index::IndexReader::open(index_reader);
        lucene::index::IndexReader* normal_idx_reader =
                lucene::index::IndexReader::open(normal_index_reader);

        return check_idx_file_correctness_impl(idx_reader, normal_idx_reader);
    }

    static Status check_idx_file_correctness(
            const std::vector<std::unique_ptr<DorisCompoundReader>>& index_readers,
            const std::vector<std::unique_ptr<DorisCompoundReader>>& normal_index_readers) {
        ValueArray<lucene::index::IndexReader*> readers(index_readers.size());
        for (int i = 0; i < index_readers.size(); i++) {
            lucene::index::IndexReader* idx_reader =
                    lucene::index::IndexReader::open(index_readers[i].get());
            readers[i] = idx_reader;
        }
        ValueArray<lucene::index::IndexReader*> normal_readers(normal_index_readers.size());
        for (int i = 0; i < normal_index_readers.size(); i++) {
            lucene::index::IndexReader* normal_idx_reader =
                    lucene::index::IndexReader::open(normal_index_readers[i].get());
            normal_readers[i] = normal_idx_reader;
        }

        auto* idx_reader = new lucene::index::MultiReader(&readers, true);
        auto* normal_idx_reader = new lucene::index::MultiReader(&normal_readers, true);

        return check_idx_file_correctness_impl(idx_reader, normal_idx_reader);
    }

    static Status do_compaction(
            const std::vector<RowsetSharedPtr>& rowsets, StorageEngine* engine_ref,
            const TabletSharedPtr& tablet, bool is_index_compaction, RowsetSharedPtr& rowset_ptr,
            const std::function<void(const BaseCompaction&, const RowsetWriterContext&)>
                    custom_check = nullptr,
            int64_t max_rows_per_segment = 100000) {
        config::inverted_index_compaction_enable = is_index_compaction;
        // control max rows in one block
        config::compaction_batch_size = max_rows_per_segment;
        // only base compaction can handle delete predicate
        BaseCompaction compaction(tablet);
        compaction._input_rowsets = std::move(rowsets);
        compaction.build_basic_info();

        std::vector<RowsetReaderSharedPtr> input_rs_readers;
        create_input_rowsets_readers(compaction, input_rs_readers);

        RowsetWriterContext ctx;
        ctx.max_rows_per_segment = max_rows_per_segment;
        RETURN_IF_ERROR(compaction.construct_output_rowset_writer(ctx, true));

        Merger::Statistics stats;
        RowIdConversion rowid_conversion;
        stats.rowid_conversion = &rowid_conversion;
        RETURN_IF_ERROR(Merger::vertical_merge_rowsets(
                tablet, compaction.compaction_type(), compaction._cur_tablet_schema,
                input_rs_readers, compaction._output_rs_writer.get(), max_rows_per_segment - 1, 5,
                &stats));

        RETURN_IF_ERROR(compaction._output_rs_writer->build(compaction._output_rowset));

        RETURN_IF_ERROR(compaction.do_inverted_index_compaction(ctx, stats));

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
                                                          std::string pred, int64& inc_id,
                                                          const std::string& tablet_path) {
        DeletePredicatePB del_pred;
        del_pred.add_sub_predicates(pred);
        del_pred.set_version(1);
        RowsetMetaSharedPtr rsm(new RowsetMeta());
        init_rs_meta(rsm, inc_id, inc_id);
        RowsetId id;
        id.init(inc_id);
        rsm->set_rowset_id(id);
        rsm->set_delete_predicate(std::move(del_pred));
        rsm->set_tablet_schema(schema);
        inc_id++;
        return std::make_shared<BetaRowset>(schema, tablet_path, rsm);
    }

    static void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
                                 const std::string& index_name, int32_t col_unique_id,
                                 const std::string& column_type, const std::string& column_name,
                                 const std::map<std::string, std::string>& properties =
                                         std::map<std::string, std::string>(),
                                 bool is_key = false) {
        column_pb->set_unique_id(col_unique_id);
        column_pb->set_name(column_name);
        column_pb->set_type(column_type);
        column_pb->set_is_key(is_key);
        column_pb->set_is_nullable(true);
        tablet_index->set_index_id(index_id);
        tablet_index->set_index_name(index_name);
        tablet_index->set_index_type(IndexType::INVERTED);
        tablet_index->add_col_unique_id(col_unique_id);
        if (!properties.empty()) {
            auto* pros = tablet_index->mutable_properties();
            for (const auto& [key, value] : properties) {
                (*pros)[key] = value;
            }
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
                                    const std::map<int, QueryData>& query_map,
                                    const std::string& tablet_path) {
        CHECK_EQ(output_rowset->num_segments(), 1);
        // check rowset meta and file
        const auto& fs = output_rowset->_rowset_meta->fs();
        auto rowset_id = output_rowset->rowset_id();
        auto segment_file_name = rowset_id.to_string() + "_0.dat";
        auto inverted_index_file_reader = std::make_shared<InvertedIndexFileReader>(
                fs, tablet_path, segment_file_name,
                tablet_schema->get_inverted_index_storage_format());

        EXPECT_TRUE(inverted_index_file_reader->init().ok());

        for (const auto& [col_uid, query_data] : query_map) {
            const auto& column = tablet_schema->column_by_uid(col_uid);
            const auto* index = tablet_schema->get_inverted_index(column);
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
                EXPECT_TRUE(query_string(index, inverted_index_file_reader, "v1", query_data.first,
                                         query_data.second));
            } else if (col_uid == 2) {
                // Fulltext index
                EXPECT_TRUE(query_fulltext(index, inverted_index_file_reader, "v2",
                                           query_data.first, query_data.second));
            }
        }
    }

    static RowsetWriterContext rowset_writer_context(const std::unique_ptr<DataDir>& data_dir,
                                                     const TabletSchemaSPtr& schema,
                                                     const std::string& tablet_path, int64& inc_id,
                                                     int64 max_rows_per_segment = 200) {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.rowset_dir = tablet_path;
        context.version = Version(inc_id, inc_id);
        context.max_rows_per_segment = max_rows_per_segment;
        inc_id++;
        return context;
    }

    template <typename T>
    static void build_rowsets(const std::unique_ptr<DataDir>& data_dir,
                              const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
                              StorageEngine* engine_ref, std::vector<RowsetSharedPtr>& rowsets,
                              const std::vector<std::string>& data_files, int64& inc_id,
                              const std::function<void(const int32_t&)> custom_check = nullptr,
                              const bool& is_performance = false,
                              int64 max_rows_per_segment = 200) {
        std::vector<std::vector<T>> data;
        for (const auto& file : data_files) {
            data.emplace_back(read_data<T>(file));
        }
        for (int i = 0; i < data.size(); i++) {
            auto tablet_path = tablet->tablet_path();
            std::unique_ptr<RowsetWriter> rowset_writer;
            Status s = RowsetFactory::create_rowset_writer(
                    rowset_writer_context(data_dir, schema, tablet_path, inc_id,
                                          max_rows_per_segment),
                    true, &rowset_writer);

            vectorized::Block block = schema->create_block();
            auto columns = block.mutate_columns();
            for (const auto& row : data[i]) {
                if constexpr (std::is_same_v<T, DataRow>) {
                    vectorized::Field key = int32_t(row.key);
                    vectorized::Field v1(row.word);
                    vectorized::Field v2(row.url);
                    vectorized::Field v3 = int32_t(row.num);
                    columns[0]->insert(key);
                    columns[1]->insert(v1);
                    columns[2]->insert(v2);
                    columns[3]->insert(v3);
                } else if constexpr (std::is_same_v<T, WikiDataRow>) {
                    vectorized::Field title(row.title);
                    vectorized::Field content(row.content);
                    vectorized::Field redirect(row.redirect);
                    vectorized::Field space(row.space);
                    columns[0]->insert(title);
                    if (is_performance) {
                        columns[1]->insert(content);
                        columns[2]->insert(redirect);
                        columns[3]->insert(space);
                        if (schema->keys_type() == UNIQUE_KEYS) {
                            uint8_t num = 0;
                            columns[4]->insert_data((const char*)&num, sizeof(num));
                        }
                    } else {
                        for (int j = 1; j < 35; j++) {
                            columns[j]->insert(content);
                        }
                        columns[35]->insert(redirect);
                        columns[36]->insert(space);
                        if (schema->keys_type() == UNIQUE_KEYS) {
                            uint8_t num = 0;
                            columns[37]->insert_data((const char*)&num, sizeof(num));
                        }
                    }
                }
            }

            Status st = rowset_writer->add_block(&block);
            EXPECT_TRUE(st.ok()) << st.to_string();
            st = rowset_writer->flush();
            EXPECT_TRUE(st.ok()) << st.to_string();

            st = rowset_writer->build(rowsets[i]);
            EXPECT_TRUE(st.ok()) << st.to_string();
            st = tablet->add_rowset(rowsets[i]);
            EXPECT_TRUE(st.ok()) << st.to_string();
            EXPECT_TRUE(rowsets[i]->num_segments() ==
                        (rowsets[i]->num_rows() / max_rows_per_segment))
                    << rowsets[i]->num_segments();
        }
    }

    static std::shared_ptr<InvertedIndexFileReader> init_index_file_reader(
            const RowsetSharedPtr& output_rowset, const std::string& seg_path,
            const InvertedIndexStorageFormatPB& index_storage_format) {
        auto rowset_id = output_rowset->rowset_id();
        auto segment_file_name = rowset_id.to_string() + "_0.dat";
        auto inverted_index_file_reader = std::make_shared<InvertedIndexFileReader>(
                output_rowset->_rowset_meta->fs(), seg_path, segment_file_name,
                index_storage_format);
        auto st = inverted_index_file_reader->init();
        EXPECT_TRUE(st.ok()) << st.to_string();

        return inverted_index_file_reader;
    }
};

} // namespace doris
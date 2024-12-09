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

#include <iostream>
#include <memory>

#include "CLucene/StdHeader.h"
#include "CLucene/config/repl_wchar.h"
#include "common/status.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "olap/base_compaction.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_query.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_factory.h"
#include "olap/rowset/segment_v2/inverted_index_compaction.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/storage_engine.h"

namespace doris {

using namespace doris::vectorized;

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "/ut_dir/inverted_index_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";
static int64_t inc_id = 1000;

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

static void check_terms_stats(lucene::store::Directory* dir) {
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
        if (false) {
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
static Status check_idx_file_correctness(lucene::store::Directory* index_reader,
                                         lucene::store::Directory* tmp_index_reader) {
    lucene::index::IndexReader* idx_reader = lucene::index::IndexReader::open(index_reader);
    lucene::index::IndexReader* tmp_idx_reader = lucene::index::IndexReader::open(tmp_index_reader);

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
        std::string field = lucene_wcstoutf8string(term_enum->term(false)->field(),
                                                   lenOfString(term_enum->term(false)->field()));
        std::string tmp_token = lucene_wcstoutf8string(tmp_term_enum->term(false)->text(),
                                                       tmp_term_enum->term(false)->textLength());
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

static RowsetSharedPtr do_compaction(std::vector<RowsetSharedPtr> rowsets,
                                     StorageEngine* engine_ref, TabletSharedPtr tablet,
                                     bool is_index_compaction) {
    config::inverted_index_compaction_enable = is_index_compaction;
    // only base compaction can handle delete predicate
    BaseCompaction compaction(tablet);
    compaction._input_rowsets = std::move(rowsets);
    compaction.build_basic_info();

    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    input_rs_readers.reserve(compaction._input_rowsets.size());
    for (auto& rowset : compaction._input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    RowsetWriterContext ctx;
    EXPECT_TRUE(compaction.construct_output_rowset_writer(ctx, true).ok());

    if (is_index_compaction) {
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.size() == 2);
        // col v1
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
        // col v2
        EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));
    }

    Merger::Statistics stats;
    stats.rowid_conversion = compaction._rowid_conversion.get();
    Status st = Merger::vertical_merge_rowsets(
            tablet, compaction.compaction_type(), compaction._cur_tablet_schema, input_rs_readers,
            compaction._output_rs_writer.get(), 100000, 5, &stats);
    EXPECT_TRUE(st.ok()) << st.to_string();

    st = compaction._output_rs_writer->build(compaction._output_rowset);
    EXPECT_TRUE(st.ok()) << st.to_string();

    EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);

    // do index compaction

    if (stats.rowid_conversion && config::inverted_index_compaction_enable &&
        !ctx.columns_to_do_index_compaction.empty()) {
        OlapStopWatch inverted_watch;

        // translation vec
        // <<dest_idx_num, dest_docId>>
        // the first level vector: index indicates src segment.
        // the second level vector: index indicates row id of source segment,
        // value indicates row id of destination segment.
        // <UINT32_MAX, UINT32_MAX> indicates current row not exist.
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec =
                stats.rowid_conversion->get_rowid_conversion_map();

        // source rowset,segment -> index_id
        std::map<std::pair<RowsetId, uint32_t>, uint32_t> src_seg_to_id_map =
                stats.rowid_conversion->get_src_segment_to_id_map();
        // dest rowset id
        RowsetId dest_rowset_id = stats.rowid_conversion->get_dst_rowset_id();
        // dest segment id -> num rows
        std::vector<uint32_t> dest_segment_num_rows;
        Status st = compaction._output_rs_writer->get_segment_num_rows(&dest_segment_num_rows);
        EXPECT_TRUE(st.ok()) << st.to_string();

        auto src_segment_num = src_seg_to_id_map.size();
        auto dest_segment_num = dest_segment_num_rows.size();

        if (dest_segment_num > 0) {
            // src index files
            // format: rowsetId_segmentId
            std::vector<std::string> src_index_files(src_segment_num);
            for (const auto& m : src_seg_to_id_map) {
                std::pair<RowsetId, uint32_t> p = m.first;
                src_index_files[m.second] = p.first.to_string() + "_" + std::to_string(p.second);
            }

            // dest index files
            // format: rowsetId_segmentId
            std::vector<std::string> dest_index_files(dest_segment_num);
            for (int i = 0; i < dest_segment_num; ++i) {
                auto prefix = dest_rowset_id.to_string() + "_" + std::to_string(i);
                dest_index_files[i] = prefix;
            }

            // create index_writer to compaction indexes
            const auto& fs = compaction._output_rowset->rowset_meta()->fs();
            const auto& tablet_path = tablet->tablet_path();

            // src index dirs
            // format: rowsetId_segmentId
            std::vector<std::unique_ptr<InvertedIndexFileReader>> inverted_index_file_readers(
                    src_segment_num);
            for (const auto& m : src_seg_to_id_map) {
                std::pair<RowsetId, uint32_t> p = m.first;
                auto segment_file_name =
                        p.first.to_string() + "_" + std::to_string(p.second) + ".dat";
                auto inverted_index_file_reader = std::make_unique<InvertedIndexFileReader>(
                        fs, tablet_path, segment_file_name,
                        compaction._cur_tablet_schema->get_inverted_index_storage_format());
                bool open_idx_file_cache = false;
                auto st = inverted_index_file_reader->init(config::inverted_index_read_buffer_size,
                                                           open_idx_file_cache);
                EXPECT_TRUE(st.ok()) << st.to_string();
                inverted_index_file_readers[m.second] = std::move(inverted_index_file_reader);
            }

            // dest index files
            // format: rowsetId_segmentId
            std::vector<std::unique_ptr<InvertedIndexFileWriter>> inverted_index_file_writers(
                    dest_segment_num);

            // Some columns have already been indexed
            // key: seg_id, value: inverted index file size
            std::unordered_map<int, int64_t> compacted_idx_file_size;
            for (int seg_id = 0; seg_id < dest_segment_num; ++seg_id) {
                auto prefix = dest_rowset_id.to_string() + "_" + std::to_string(seg_id) + ".dat";
                auto inverted_index_file_reader = std::make_unique<InvertedIndexFileReader>(
                        fs, tablet_path, prefix,
                        compaction._cur_tablet_schema->get_inverted_index_storage_format());
                bool open_idx_file_cache = false;
                st = inverted_index_file_reader->init(config::inverted_index_read_buffer_size,
                                                      open_idx_file_cache);
                if (st.ok()) {
                    auto index_not_need_to_compact =
                            inverted_index_file_reader->get_all_directories();
                    EXPECT_TRUE(index_not_need_to_compact.has_value());
                    // V1: each index is a separate file
                    // V2: all indexes are in a single file
                    if (compaction._cur_tablet_schema->get_inverted_index_storage_format() !=
                        doris::InvertedIndexStorageFormatPB::V1) {
                        int64_t fsize = 0;
                        st = fs->file_size(InvertedIndexDescriptor::get_index_file_name(prefix),
                                           &fsize);
                        EXPECT_TRUE(st.ok()) << st.to_string();
                        compacted_idx_file_size[seg_id] = fsize;
                    }
                    auto inverted_index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                            fs, tablet_path, prefix,
                            compaction._cur_tablet_schema->get_inverted_index_storage_format());
                    st = inverted_index_file_writer->initialize(index_not_need_to_compact.value());
                    EXPECT_TRUE(st.ok()) << st.to_string();
                    inverted_index_file_writers[seg_id] = std::move(inverted_index_file_writer);
                } else if (st.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>()) {
                    auto inverted_index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                            fs, tablet_path, prefix,
                            compaction._cur_tablet_schema->get_inverted_index_storage_format());
                    inverted_index_file_writers[seg_id] = std::move(inverted_index_file_writer);
                    // no index file
                    compacted_idx_file_size[seg_id] = 0;
                } else {
                    EXPECT_TRUE(st.ok()) << st.to_string();
                }
            }

            // we choose the first destination segment name as the temporary index writer path
            // Used to distinguish between different index compaction
            auto index_tmp_path = tablet_path + "/" + dest_rowset_id.to_string() + "_" + "tmp";

            auto error_handler = [&compaction](int64_t index_id, int64_t column_uniq_id) {
                for (auto& rowset : compaction._input_rowsets) {
                    rowset->set_skip_index_compaction(column_uniq_id);
                }
            };

            Status status = Status::OK();
            for (auto&& column_uniq_id : ctx.columns_to_do_index_compaction) {
                auto col = compaction._cur_tablet_schema->column_by_uid(column_uniq_id);
                const auto* index_meta = compaction._cur_tablet_schema->get_inverted_index(col);

                // if index properties are different, index compaction maybe needs to be skipped.
                bool is_continue = false;
                std::optional<std::map<std::string, std::string>> first_properties;
                for (const auto& rowset : compaction._input_rowsets) {
                    const auto* tablet_index = rowset->tablet_schema()->get_inverted_index(col);
                    const auto& properties = tablet_index->properties();
                    if (!first_properties.has_value()) {
                        first_properties = properties;
                    } else {
                        if (properties != first_properties.value()) {
                            error_handler(index_meta->index_id(), column_uniq_id);
                            status = Status::Error<ErrorCode::INVERTED_INDEX_COMPACTION_ERROR>(
                                    "if index properties are different, index compaction needs to "
                                    "be "
                                    "skipped.");
                            is_continue = true;
                            break;
                        }
                    }
                }
                if (is_continue) {
                    continue;
                }

                std::vector<lucene::store::Directory*> dest_index_dirs(dest_segment_num);
                try {
                    std::vector<std::unique_ptr<DorisCompoundReader>> src_idx_dirs(src_segment_num);
                    for (int src_segment_id = 0; src_segment_id < src_segment_num;
                         src_segment_id++) {
                        auto res = inverted_index_file_readers[src_segment_id]->open(index_meta);
                        EXPECT_TRUE(res.has_value());
                        src_idx_dirs[src_segment_id] = std::move(res.value());
                    }
                    for (int dest_segment_id = 0; dest_segment_id < dest_segment_num;
                         dest_segment_id++) {
                        auto ret = inverted_index_file_writers[dest_segment_id]->open(index_meta);
                        // EXPECT_TRUE(res.has_value());
                        dest_index_dirs[dest_segment_id] = ret.value();
                    }
                    auto st = compact_column(index_meta->index_id(), src_idx_dirs, dest_index_dirs,
                                             fs, index_tmp_path, trans_vec, dest_segment_num_rows);
                    if (!st.ok()) {
                        error_handler(index_meta->index_id(), column_uniq_id);
                        status =
                                Status::Error<ErrorCode::INVERTED_INDEX_COMPACTION_ERROR>(st.msg());
                    }
                } catch (CLuceneError& e) {
                    error_handler(index_meta->index_id(), column_uniq_id);
                    status = Status::Error<ErrorCode::INVERTED_INDEX_COMPACTION_ERROR>(e.what());
                }
            }
            uint64_t inverted_index_file_size = 0;
            for (int seg_id = 0; seg_id < dest_segment_num; ++seg_id) {
                auto inverted_index_file_writer = inverted_index_file_writers[seg_id].get();
                if (Status st = inverted_index_file_writer->close(); !st.ok()) {
                    status = Status::Error<ErrorCode::INVERTED_INDEX_COMPACTION_ERROR>(st.msg());
                } else {
                    inverted_index_file_size += inverted_index_file_writer->get_index_file_size();
                    inverted_index_file_size -= compacted_idx_file_size[seg_id];
                }
            }
            EXPECT_TRUE(status.ok()) << status.to_string();

            // index compaction should update total disk size and index disk size
            compaction._output_rowset->rowset_meta()->set_data_disk_size(
                    compaction._output_rowset->data_disk_size() + inverted_index_file_size);
            compaction._output_rowset->rowset_meta()->set_total_disk_size(
                    compaction._output_rowset->data_disk_size() + inverted_index_file_size);
            compaction._output_rowset->rowset_meta()->set_index_disk_size(
                    compaction._output_rowset->index_disk_size() + inverted_index_file_size);

            COUNTER_UPDATE(compaction._output_rowset_data_size_counter,
                           compaction._output_rowset->data_disk_size());
        }
    }

    return compaction._output_rowset;
}

class IndexCompactionDeleteTest : public ::testing::Test {
protected:
    void SetUp() override {
        // absolute dir
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _curreent_dir = std::string(buffer);
        _absolute_dir = _curreent_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        // tmp dir
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        Status st = tmp_file_dirs->init();
        EXPECT_TRUE(st.ok()) << st.to_json();
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // storage engine
        doris::EngineOptions options;
        _engine_ref = new StorageEngine(options);
        _data_dir = std::make_unique<DataDir>(_absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(_engine_ref);

        // tablet_schema
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);

        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "key_index", 0,
                         "INT", "key");
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index", 1,
                         "STRING", "v1");
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10002, "v2_index", 2,
                         "STRING", "v2", true);
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10003, "v3_index", 3, "INT",
                         "v3");

        _tablet_schema.reset(new TabletSchema);
        _tablet_schema->init_from_pb(schema_pb);

        // tablet
        TabletMetaSharedPtr tablet_meta(new TabletMeta());
        tablet_meta->_schema = _tablet_schema;

        _tablet.reset(new Tablet(*_engine_ref, tablet_meta, _data_dir.get()));
        EXPECT_TRUE(_tablet->init().ok());
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());

        if (_engine_ref != nullptr) {
            _engine_ref->stop();
            delete _engine_ref;
            _engine_ref = nullptr;
            ExecEnv::GetInstance()->set_storage_engine(nullptr);
        }
    }

    void init_rs_meta(RowsetMetaSharedPtr& rs_meta, int64_t start, int64_t end) {
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

    RowsetSharedPtr create_delete_predicate_rowset(const TabletSchemaSPtr& schema, std::string pred,
                                                   int64_t version) {
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
        return std::make_shared<BetaRowset>(schema, _tablet->tablet_path(), rsm);
    }

    void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
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

    RowsetWriterContext rowset_writer_context() {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = _data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _tablet_schema;
        context.rowset_dir = _tablet->tablet_path();
        context.version = Version(inc_id, inc_id);
        context.max_rows_per_segment = 200;
        inc_id++;
        return context;
    }

    IndexCompactionDeleteTest() = default;
    ~IndexCompactionDeleteTest() override = default;

private:
    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _curreent_dir;
};

TEST_F(IndexCompactionDeleteTest, delete_index_test) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _curreent_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _curreent_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";

    std::vector<std::vector<DataRow>> data;
    data.emplace_back(read_data(data_file1));
    data.emplace_back(read_data(data_file2));

    std::vector<RowsetSharedPtr> rowsets(data.size());
    for (int i = 0; i < data.size(); i++) {
        std::unique_ptr<RowsetWriter> rowset_writer;
        const auto& res =
                RowsetFactory::create_rowset_writer(rowset_writer_context(), false, &rowset_writer);
        EXPECT_TRUE(res.ok()) << res.to_string();

        Block block = _tablet_schema->create_block();
        auto columns = block.mutate_columns();
        for (const auto& row : data[i]) {
            vectorized::Field key = Int32(row.key);
            vectorized::Field v1(row.word);
            vectorized::Field v2(row.url);
            vectorized::Field v3 = Int32(row.num);
            columns[0]->insert(key);
            columns[1]->insert(v1);
            columns[2]->insert(v2);
            columns[3]->insert(v3);
        }
        Status st = rowset_writer->add_block(&block);
        EXPECT_TRUE(st.ok()) << st.to_string();
        st = rowset_writer->flush();
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(rowset_writer->build(rowsets[i]).ok());
        EXPECT_TRUE(_tablet->add_rowset(rowsets[i]).ok());
        EXPECT_TRUE(rowsets[i]->num_segments() == 5);
    }

    // create delete predicate rowset and add to tablet
    auto delete_rowset = create_delete_predicate_rowset(_tablet_schema, "v1='great'", inc_id++);
    EXPECT_TRUE(_tablet->add_rowset(delete_rowset).ok());
    EXPECT_TRUE(_tablet->rowset_map().size() == 3);
    rowsets.push_back(delete_rowset);
    EXPECT_TRUE(rowsets.size() == 3);

    auto output_rowset_index = do_compaction(rowsets, _engine_ref, _tablet, true);
    int seg_id = 0;
    auto segment_file_name_index =
            fmt::format("{}_{}.dat", output_rowset_index->rowset_id().to_string(), seg_id);
    std::cout << "segment_file_name: " << segment_file_name_index << std::endl;
    auto inverted_index_file_reader_index = std::make_unique<InvertedIndexFileReader>(
            output_rowset_index->_rowset_meta->fs(), _tablet->tablet_path(),
            segment_file_name_index, _tablet_schema->get_inverted_index_storage_format());
    EXPECT_TRUE(inverted_index_file_reader_index->init().ok());

    auto output_rowset_normal = do_compaction(rowsets, _engine_ref, _tablet, false);

    auto segment_file_name_normal =
            fmt::format("{}_{}.dat", output_rowset_index->rowset_id().to_string(), seg_id);
    auto inverted_index_file_reader_normal = std::make_unique<InvertedIndexFileReader>(
            output_rowset_normal->_rowset_meta->fs(), _tablet->tablet_path(),
            segment_file_name_normal, _tablet_schema->get_inverted_index_storage_format());
    EXPECT_TRUE(inverted_index_file_reader_normal->init().ok());

    // check index file terms
    std::string empty_string;
    auto dir_idx_compaction = inverted_index_file_reader_index->_open(10001, empty_string);
    auto dir_normal_compaction = inverted_index_file_reader_normal->_open(10001, empty_string);
    EXPECT_TRUE(dir_idx_compaction.has_value()) << dir_idx_compaction.error();
    EXPECT_TRUE(dir_normal_compaction.has_value()) << dir_normal_compaction.error();
    check_terms_stats(dir_idx_compaction->get());
    check_terms_stats(dir_normal_compaction->get());
    auto st = check_idx_file_correctness(dir_idx_compaction->get(), dir_normal_compaction->get());
    EXPECT_TRUE(st.ok()) << st.to_string();
}

} // namespace doris

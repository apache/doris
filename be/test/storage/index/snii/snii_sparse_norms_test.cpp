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

// End-to-end sparse norms through the production column writer: with the BE config
// enable_snii_sparse_norms on, NULL-heavy scalar and ARRAY columns get the sparse norms section;
// with it off, the same input produces the section every earlier writer produced; BM25 scores are
// identical between the two layouts.

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/check.h"
#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/query/scoring_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

constexpr int64_t kIndexId = 7101;

using ScalarRow = std::optional<std::string>;
using ArrayRow = std::optional<std::vector<std::optional<std::string>>>;

TabletIndex make_meta() {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("sparse_norms");
    pb.add_col_unique_id(0);
    pb.mutable_properties()->insert({"parser", "english"});
    pb.mutable_properties()->insert({"support_phrase", "true"});
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

// Runs `write` with enable_snii_sparse_norms set to `enabled`, then restores the config.
Status with_sparse_norms_config(bool enabled, const std::function<Status()>& write) {
    const bool saved_sparse_norms = config::enable_snii_sparse_norms;
    Defer restore {[&] { config::enable_snii_sparse_norms = saved_sparse_norms; }};
    config::enable_snii_sparse_norms = enabled;
    return write();
}

Status open_writer(const std::string& prefix, std::unique_ptr<IndexFileWriter>* out) {
    const std::string file_path = InvertedIndexDescriptor::get_index_file_path_v2(prefix);
    auto fs = io::global_local_filesystem();
    bool exists = false;
    RETURN_IF_ERROR(fs->exists(file_path, &exists));
    if (exists) {
        RETURN_IF_ERROR(fs->delete_file(file_path));
    }
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(fs->create_file(file_path, &file_writer));
    *out = std::make_unique<IndexFileWriter>(fs, prefix, "sparse_norms_rowset", /*seg_id=*/0,
                                             InvertedIndexStorageFormatPB::SNII,
                                             std::move(file_writer),
                                             /*can_use_ram_dir=*/true, /*tablet_id=*/901);
    return Status::OK();
}

// Mirrors ScalarColumnWriter::append_nullable: runs of non-NULL rows go to add_values, NULL runs
// to add_nulls.
Status write_scalar(const std::string& prefix, const TabletIndex& meta,
                    const std::vector<ScalarRow>& rows) {
    std::unique_ptr<IndexFileWriter> file_writer;
    RETURN_IF_ERROR(open_writer(prefix, &file_writer));
    SniiIndexColumnWriter writer(file_writer.get(), &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    RETURN_IF_ERROR(writer.init());
    std::vector<Slice> batch;
    uint32_t null_run = 0;
    for (const auto& row : rows) {
        if (row.has_value()) {
            if (null_run != 0) {
                RETURN_IF_ERROR(writer.add_nulls(null_run));
                null_run = 0;
            }
            batch.emplace_back(*row);
        } else {
            if (!batch.empty()) {
                RETURN_IF_ERROR(writer.add_values("content", batch.data(), batch.size()));
                batch.clear();
            }
            ++null_run;
        }
    }
    if (null_run != 0) {
        RETURN_IF_ERROR(writer.add_nulls(null_run));
    }
    if (!batch.empty()) {
        RETURN_IF_ERROR(writer.add_values("content", batch.data(), batch.size()));
    }
    RETURN_IF_ERROR(writer.finish());
    RETURN_IF_ERROR(file_writer->begin_close());
    return file_writer->finish_close();
}

// Mirrors ArrayColumnWriter::append_nullable: every row goes to add_array_values (a NULL row may
// keep a nested payload), then add_array_nulls marks the NULL rows. Written in two batches.
Status write_array(const std::string& prefix, const TabletIndex& meta,
                   const std::vector<ArrayRow>& rows, const std::vector<bool>& row_is_null) {
    std::unique_ptr<IndexFileWriter> file_writer;
    RETURN_IF_ERROR(open_writer(prefix, &file_writer));
    SniiIndexColumnWriter writer(file_writer.get(), &meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    RETURN_IF_ERROR(writer.init());
    const size_t split = rows.size() / 2;
    for (const auto& [begin, end] :
         {std::pair<size_t, size_t> {0, split}, std::pair<size_t, size_t> {split, rows.size()}}) {
        std::vector<std::string> storage;
        std::vector<uint8_t> element_nulls;
        std::vector<uint64_t> offsets {0};
        std::vector<uint8_t> null_map;
        for (size_t i = begin; i < end; ++i) {
            if (rows[i].has_value()) {
                for (const auto& element : *rows[i]) {
                    storage.push_back(element.value_or(""));
                    element_nulls.push_back(element.has_value() ? 0 : 1);
                }
            }
            offsets.push_back(storage.size());
            null_map.push_back(row_is_null[i] ? 1 : 0);
        }
        std::vector<Slice> elements;
        elements.reserve(storage.size());
        for (const auto& value : storage) {
            elements.emplace_back(value);
        }
        RETURN_IF_ERROR(writer.add_array_values(
                sizeof(Slice), elements.data(), element_nulls.data(),
                reinterpret_cast<const uint8_t*>(offsets.data()), end - begin));
        RETURN_IF_ERROR(writer.add_array_nulls(null_map.data(), end - begin));
    }
    RETURN_IF_ERROR(writer.finish());
    RETURN_IF_ERROR(file_writer->begin_close());
    return file_writer->finish_close();
}

struct OpenedIndex {
    std::shared_ptr<IndexFileReader> file_reader;
    std::unique_ptr<snii::reader::LogicalIndexReader> index;
    snii::stats::SniiStatsProvider stats;
    snii::format::NormsPodReader norms;
    std::vector<uint8_t> norms_section;
};

Status open_index(const std::string& prefix, const TabletIndex& meta, OpenedIndex* out) {
    out->file_reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                         InvertedIndexStorageFormatPB::SNII);
    RETURN_IF_ERROR(out->file_reader->init());
    auto index = out->file_reader->open_snii_index(&meta);
    if (!index.has_value()) {
        return index.error();
    }
    out->index = std::move(index.value());
    RETURN_IF_ERROR(snii::stats::SniiStatsProvider::open(out->index.get(), &out->stats));
    RETURN_IF_ERROR(out->index->open_norms(&out->norms));

    std::ifstream file(InvertedIndexDescriptor::get_index_file_path_v2(prefix), std::ios::binary);
    const std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(file)),
                                     std::istreambuf_iterator<char>());
    const auto& region = out->index->section_refs().norms;
    DORIS_CHECK_LE(region.offset + region.length, bytes.size());
    out->norms_section.assign(
            bytes.begin() + static_cast<ptrdiff_t>(region.offset),
            bytes.begin() + static_cast<ptrdiff_t>(region.offset + region.length));
    return Status::OK();
}

// Scores every posting of `term` with fixed collection statistics.
Status score_term(const OpenedIndex& opened, const std::string& term,
                  std::vector<snii::query::ScoredDoc>* out) {
    std::vector<uint32_t> docids;
    RETURN_IF_ERROR(snii::query::term_query(*opened.index, term, &docids));
    roaring::Roaring candidates;
    candidates.addMany(docids.size(), docids.data());
    return snii::query::scoring_query_candidates(
            *opened.index, opened.stats, {{.physical_term = term, .idf = 1.7}}, candidates,
            opened.stats.avgdl(), snii::query::Bm25Params {}, out);
}

void expect_same_scores(const OpenedIndex& sparse, const OpenedIndex& dense,
                        const std::string& term, size_t expected_hits) {
    std::vector<snii::query::ScoredDoc> sparse_scores;
    std::vector<snii::query::ScoredDoc> dense_scores;
    Status status = score_term(sparse, term, &sparse_scores);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = score_term(dense, term, &dense_scores);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(sparse_scores.size(), expected_hits);
    ASSERT_EQ(sparse_scores.size(), dense_scores.size());
    for (size_t i = 0; i < sparse_scores.size(); ++i) {
        EXPECT_EQ(sparse_scores[i].docid, dense_scores[i].docid);
        // Bit-identical: both layouts hand the scorer the same norm byte.
        EXPECT_EQ(sparse_scores[i].score, dense_scores[i].score)
                << "docid " << sparse_scores[i].docid;
        EXPECT_GT(sparse_scores[i].score, 0.0);
    }
}

class SniiSparseNormsTest : public ::testing::Test {
protected:
    void SetUp() override {
        _dir = "./ut_dir/snii_sparse_norms_test";
        static_cast<void>(io::global_local_filesystem()->delete_directory(_dir));
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_dir).ok());
    }
    void TearDown() override {
        static_cast<void>(io::global_local_filesystem()->delete_directory(_dir));
    }

    std::string _dir;
};

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST_F(SniiSparseNormsTest, NullHeavyScalarColumnWritesSparseNorms) {
    constexpr uint32_t kRows = 150000;
    std::vector<ScalarRow> rows(kRows);
    std::vector<uint32_t> token_counts(kRows, 0);
    size_t present = 0;
    for (uint32_t row = 0; row < kRows; ++row) {
        // One present row in 100, plus a present stretch; one empty string (present, no token).
        if (row % 100 == 0 || (row >= 70000 && row < 70200) || row == 12345) {
            std::string value = row == 12345 ? "" : "alpha";
            for (uint32_t i = 0; i < row % 4; ++i) {
                value += " beta";
            }
            if (row == 12345) {
                value.clear();
            }
            token_counts[row] = row == 12345 ? 0 : 1 + row % 4;
            rows[row] = std::move(value);
            ++present;
        }
    }
    // The sparse layout is on by default.
    EXPECT_TRUE(config::enable_snii_sparse_norms);
    const TabletIndex meta = make_meta();
    const std::string sparse_prefix = _dir + "/scalar_sparse";
    const std::string dense_prefix = _dir + "/scalar_dense";
    // The same rows written with the config on and off.
    ASSERT_TRUE(with_sparse_norms_config(true, [&] {
                    return write_scalar(sparse_prefix, meta, rows);
                }).ok());
    ASSERT_TRUE(with_sparse_norms_config(false, [&] {
                    return write_scalar(dense_prefix, meta, rows);
                }).ok());

    OpenedIndex sparse;
    OpenedIndex dense;
    Status status = open_index(sparse_prefix, meta, &sparse);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = open_index(dense_prefix, meta, &dense);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ(sparse.index->stats().doc_count, kRows);
    EXPECT_EQ(sparse.index->stats().indexed_doc_count, present);
    EXPECT_EQ(dense.index->stats().indexed_doc_count, present);
    ASSERT_TRUE(sparse.norms.is_sparse());
    ASSERT_FALSE(dense.norms.is_sparse());
    EXPECT_EQ(sparse.norms.present_count(), present);
    EXPECT_EQ(sparse.norms_section[0],
              static_cast<uint8_t>(snii::format::SectionType::kNormsSparse));
    EXPECT_EQ(dense.norms_section[0], static_cast<uint8_t>(snii::format::SectionType::kNormsPod));
    EXPECT_EQ(dense.norms_section.size(), snii::format::dense_norms_section_bytes(kRows));
    EXPECT_LT(sparse.norms_section.size() * 20, dense.norms_section.size());

    // The dense section is the one earlier writers produced: one byte per row, encode_norm(0)
    // for NULL rows.
    std::vector<uint8_t> legacy(kRows);
    for (uint32_t row = 0; row < kRows; ++row) {
        legacy[row] = snii::query::encode_norm(token_counts[row]);
    }
    snii::ByteSink legacy_section;
    snii::format::NormsPodWriter::finish(legacy, &legacy_section);
    EXPECT_EQ(dense.norms_section, legacy_section.buffer());

    for (uint32_t row = 0; row < kRows; ++row) {
        uint8_t sparse_norm = 0;
        const Status sparse_status = sparse.stats.encoded_norm(row, &sparse_norm);
        if (rows[row].has_value()) {
            ASSERT_TRUE(sparse_status.ok()) << row << " " << sparse_status.to_string();
            ASSERT_EQ(sparse_norm, legacy[row]) << row;
        } else {
            ASSERT_TRUE(sparse_status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << row;
        }
        uint8_t dense_norm = 0;
        ASSERT_TRUE(dense.stats.encoded_norm(row, &dense_norm).ok());
        ASSERT_EQ(dense_norm, legacy[row]) << row;
    }
    EXPECT_DOUBLE_EQ(sparse.stats.avgdl(), dense.stats.avgdl());
    expect_same_scores(sparse, dense, "alpha", present - 1);
    // Multiples of 100 are multiples of 4 and carry no "beta"; the stretch adds 150 rows with one.
    expect_same_scores(sparse, dense, "beta", 150);
}

// A NULL ARRAY row that kept its nested payload has postings, so it keeps a norm; NULL rows
// without tokens, empty arrays and arrays of NULL elements behave as documented.
// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST_F(SniiSparseNormsTest, NullableArrayRowsKeepNormsOnlyWithTokens) {
    constexpr uint32_t kRows = 80000;
    std::vector<ArrayRow> rows(kRows);
    std::vector<bool> row_is_null(kRows, true);
    std::vector<uint32_t> expected_null_docids_with_norms;
    for (uint32_t row = 0; row < kRows; ++row) {
        if (row % 50 == 0) {
            // Non-NULL rows: tokens across elements, an empty array, an array of NULL elements.
            row_is_null[row] = false;
            if (row % 150 == 0) {
                rows[row] = std::vector<std::optional<std::string>> {};
            } else if (row % 250 == 0) {
                rows[row] = std::vector<std::optional<std::string>> {std::nullopt, std::nullopt};
            } else {
                rows[row] = std::vector<std::optional<std::string>> {"alpha beta", std::nullopt,
                                                                     "alpha"};
            }
        } else if (row % 997 == 0) {
            // NULL row keeping a nested payload.
            rows[row] = std::vector<std::optional<std::string>> {"alpha gamma"};
            expected_null_docids_with_norms.push_back(row);
        }
    }
    const TabletIndex meta = make_meta();
    const std::string sparse_prefix = _dir + "/array_sparse";
    const std::string dense_prefix = _dir + "/array_dense";
    ASSERT_TRUE(with_sparse_norms_config(true, [&] {
                    return write_array(sparse_prefix, meta, rows, row_is_null);
                }).ok());
    ASSERT_TRUE(with_sparse_norms_config(false, [&] {
                    return write_array(dense_prefix, meta, rows, row_is_null);
                }).ok());

    OpenedIndex sparse;
    OpenedIndex dense;
    Status status = open_index(sparse_prefix, meta, &sparse);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = open_index(dense_prefix, meta, &dense);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(sparse.norms.is_sparse());
    ASSERT_FALSE(dense.norms.is_sparse());
    const uint32_t non_null_rows = kRows / 50;
    EXPECT_EQ(sparse.index->stats().indexed_doc_count, non_null_rows);
    EXPECT_EQ(sparse.norms.present_count(), non_null_rows + expected_null_docids_with_norms.size());

    for (uint32_t row = 0; row < kRows; ++row) {
        uint8_t sparse_norm = 0;
        uint8_t dense_norm = 0;
        ASSERT_TRUE(dense.stats.encoded_norm(row, &dense_norm).ok());
        const Status sparse_status = sparse.stats.encoded_norm(row, &sparse_norm);
        const bool has_tokens =
                rows[row].has_value() && row % 150 != 0 && !(row % 50 == 0 && row % 250 == 0);
        if (!row_is_null[row] || has_tokens) {
            ASSERT_TRUE(sparse_status.ok()) << row << " " << sparse_status.to_string();
            ASSERT_EQ(sparse_norm, dense_norm) << row;
            uint64_t tokens = 0;
            if (has_tokens) {
                tokens = row_is_null[row] ? 2 : 3;
            }
            ASSERT_EQ(dense_norm, snii::query::encode_norm(tokens)) << row;
        } else {
            ASSERT_TRUE(sparse_status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << row;
            ASSERT_EQ(dense_norm, snii::format::kEmptyDocumentNorm) << row;
        }
    }
    // "alpha" postings include the NULL rows that kept tokens; scoring reads their norms.
    size_t alpha_rows = expected_null_docids_with_norms.size();
    for (uint32_t row = 0; row < kRows; row += 50) {
        alpha_rows += (row % 150 != 0 && row % 250 != 0) ? 1 : 0;
    }
    expect_same_scores(sparse, dense, "alpha", alpha_rows);
    expect_same_scores(sparse, dense, "gamma", expected_null_docids_with_norms.size());
}

// Without NULL rows every document carries a norm, so the writer keeps the dense layout whatever
// enable_snii_sparse_norms says, and the two files are identical.
TEST_F(SniiSparseNormsTest, SegmentWithoutNullsIsIdenticalInBothModes) {
    std::vector<ScalarRow> rows;
    for (uint32_t row = 0; row < 5000; ++row) {
        rows.emplace_back(row % 2 == 0 ? "alpha beta" : "gamma");
    }
    const TabletIndex meta = make_meta();
    const std::string adaptive_prefix = _dir + "/no_null_adaptive";
    const std::string dense_prefix = _dir + "/no_null_dense";
    ASSERT_TRUE(with_sparse_norms_config(true, [&] {
                    return write_scalar(adaptive_prefix, meta, rows);
                }).ok());
    ASSERT_TRUE(with_sparse_norms_config(false, [&] {
                    return write_scalar(dense_prefix, meta, rows);
                }).ok());
    auto read_file = [](const std::string& prefix) {
        std::ifstream file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                           std::ios::binary);
        return std::vector<uint8_t>((std::istreambuf_iterator<char>(file)),
                                    std::istreambuf_iterator<char>());
    };
    EXPECT_EQ(read_file(adaptive_prefix), read_file(dense_prefix));
}

} // namespace
} // namespace doris::segment_v2

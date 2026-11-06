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

// Frozen index files. The analysis goldens freeze the tokens; these freeze what the production
// writers make of them, so the paths from the analyzers into the writers are covered too: the SNII
// file bytes, and for V1 and V2, whose files carry timestamps, the terms with their documents and
// positions read back. The digests are in analysis_golden.txt under "write/".

#include <CLucene.h>
#include <CLucene/config/repl_wchar.h>
#include <CLucene/index/IndexReader.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/analysis/golden/golden_configs.h"
#include "storage/index/inverted/analysis/golden/golden_corpus.h"
#include "storage/index/inverted/analysis/golden/golden_environment.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/golden_file.h"
#include "util/hash_util.hpp"
#include "util/slice.h"

namespace doris::segment_v2::inverted_index::golden {

namespace {

constexpr const char* kGoldenFile =
        "./be/test/storage/index/inverted/analysis/golden/analysis_golden.txt";
constexpr const char* kTestDir = "./ut_dir/analysis_write_golden_test";
constexpr const char* kWritePrefix = "write/";
constexpr int64_t kIndexId = 77;

// The dictionary parsers, and a custom chain with every kind of component.
constexpr const char* kConfigs[] = {
        "builtin/chinese/fine_grained/lower_case=unset/stopwords=unset",
        "builtin/ik/ik_max_word/lower_case=unset/stopwords=unset",
        "builtin/icu/lower_case=unset/stopwords=unset",
        "custom/char_replace+standard+word_delimiter+asciifolding+lowercase",
};

struct Format {
    const char* name;
    InvertedIndexStorageFormatPB format;
};

constexpr Format kFormats[] = {
        {.name = "snii", .format = InvertedIndexStorageFormatPB::SNII},
        {.name = "v1", .format = InvertedIndexStorageFormatPB::V1},
        {.name = "v2", .format = InvertedIndexStorageFormatPB::V2},
};

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

void put_u32(std::string* out, uint32_t value) {
    for (int shift = 0; shift < 32; shift += 8) {
        out->push_back(static_cast<char>((value >> shift) & 0xFF));
    }
}

// The index the configuration describes, with positions, so phrase data is written too.
TabletIndex index_meta(const GoldenConfig& config) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("write_golden_idx");
    pb.add_col_unique_id(0);
    for (const auto& [key, value] : config.properties) {
        pb.mutable_properties()->insert({key, value});
    }
    pb.mutable_properties()->insert({"support_phrase", "true"});
    TabletIndex meta;
    meta.init_from_pb(pb);
    return meta;
}

TabletColumn text_column() {
    ColumnPB pb;
    pb.set_unique_id(0);
    pb.set_name("text");
    pb.set_type(TabletColumn::get_string_by_field_type(FieldType::OLAP_FIELD_TYPE_STRING));
    pb.set_is_key(false);
    pb.set_is_nullable(true);
    TabletColumn column;
    column.init_from_pb(pb);
    return column;
}

// What every index gets: the first lines of four families, one batch each, nulls after each.
std::vector<std::vector<std::string>> row_batches(const std::vector<CorpusFamily>& families) {
    std::vector<std::vector<std::string>> batches;
    for (const char* name : {"latin_words", "mixed_zh_en", "cjk_text", "japanese_text"}) {
        const auto family = std::ranges::find_if(
                families, [&](const CorpusFamily& f) { return f.name == name; });
        DORIS_CHECK(family != families.end()) << name;
        DORIS_CHECK(family->lines.size() >= 25) << name;
        batches.emplace_back(family->lines.begin(), family->lines.begin() + 25);
    }
    return batches;
}

// Writes the batches through the production writer factory; returns the index path prefix.
std::string write_index(const std::string& name, InvertedIndexStorageFormatPB format,
                        const TabletIndex& meta,
                        const std::vector<std::vector<std::string>>& batches) {
    const std::string prefix = std::string(kTestDir) + "/" + name;
    const auto fs = io::global_local_filesystem();
    io::FileWriterPtr file_writer;
    if (format != InvertedIndexStorageFormatPB::V1) {
        EXPECT_TRUE(fs->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                    &file_writer)
                            .ok());
    }
    IndexFileWriter index_file_writer(fs, prefix, "write_golden_rowset", /*seg_id=*/0, format,
                                      std::move(file_writer), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/7700);
    const TabletColumn column = text_column();
    std::unique_ptr<IndexColumnWriter> writer;
    const Status created = IndexColumnWriter::create(&column, &writer, &index_file_writer, &meta);
    EXPECT_TRUE(created.ok()) << created.to_string();
    if (!created.ok()) {
        return prefix;
    }
    for (const std::vector<std::string>& batch : batches) {
        std::vector<Slice> slices;
        for (const std::string& row : batch) {
            slices.emplace_back(row);
        }
        EXPECT_TRUE(
                writer->add_values(reinterpret_cast<const uint8_t*>(slices.data()), slices.size())
                        .ok());
        EXPECT_TRUE(writer->add_nulls(2).ok());
    }
    EXPECT_TRUE(writer->finish().ok());
    EXPECT_TRUE(index_file_writer.begin_close().ok());
    EXPECT_TRUE(index_file_writer.finish_close().ok());
    return prefix;
}

struct WriteDigest {
    uint64_t count = 0;
    uint64_t digest = 0;
};

// The bytes of the SNII file.
WriteDigest snii_digest(const std::string& prefix) {
    std::ifstream in(InvertedIndexDescriptor::get_index_file_path_v2(prefix), std::ios::binary);
    EXPECT_TRUE(in.good()) << prefix;
    std::ostringstream bytes;
    bytes << in.rdbuf();
    const std::string file = bytes.str();
    EXPECT_FALSE(file.empty()) << prefix;
    return {.count = file.size(),
            .digest = HashUtil::xxHash64WithSeed(file.data(), file.size(), 0)};
}

// Every term of a V1 or V2 index in term order, each with its documents, frequencies and
// positions; the count is the number of (term, document) postings.
WriteDigest clucene_digest(const std::string& prefix, InvertedIndexStorageFormatPB format,
                           const TabletIndex& meta) {
    WriteDigest result;
    IndexFileReader reader(io::global_local_filesystem(), prefix, format);
    const Status status = reader.init();
    EXPECT_TRUE(status.ok()) << status.to_string();
    auto directory = reader.open(&meta);
    EXPECT_TRUE(directory.has_value()) << directory.error();
    if (!directory.has_value()) {
        return result;
    }
    lucene::index::IndexReader* index_reader =
            lucene::index::IndexReader::open(directory.value().get());
    lucene::index::TermEnum* terms = index_reader->terms();
    std::string encoded;
    while (terms->next()) {
        lucene::index::Term* term = terms->term(false);
        encoded = lucene_wcstoutf8string(term->text(), term->textLength());
        encoded.push_back('\0');
        lucene::index::TermPositions* positions = index_reader->termPositions(term);
        while (positions->next()) {
            put_u32(&encoded, static_cast<uint32_t>(positions->doc()));
            const int32_t freq = positions->freq();
            put_u32(&encoded, static_cast<uint32_t>(freq));
            for (int32_t i = 0; i < freq; ++i) {
                put_u32(&encoded, static_cast<uint32_t>(positions->nextPosition()));
            }
            ++result.count;
        }
        positions->close();
        _CLLDELETE(positions);
        result.digest = HashUtil::xxHash64WithSeed(encoded.data(), encoded.size(), result.digest);
    }
    terms->close();
    _CLLDELETE(terms);
    index_reader->close();
    _CLLDELETE(index_reader);
    return result;
}

} // namespace

class AnalysisWriteGolden : public testing::Test {
protected:
    void SetUp() override {
        const Status status = _environment.init(_configs);
        ASSERT_TRUE(status.ok()) << status.to_string();
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        // Pin what the SNII writer reads from the configuration, as SniiWriterGoldenBytes does,
        // so a changed default does not move the digests.
        _saved = {config::snii_dict_block_zstd_level, config::snii_prx_zstd_level,
                  config::snii_prx_zstd_level_direct_load,
                  config::snii_prx_zstd_min_saving_percent};
        config::snii_dict_block_zstd_level = 3;
        config::snii_prx_zstd_level = 3;
        config::snii_prx_zstd_level_direct_load = 3;
        config::snii_prx_zstd_min_saving_percent = 0;
    }

    void TearDown() override {
        config::snii_dict_block_zstd_level = _saved[0];
        config::snii_prx_zstd_level = _saved[1];
        config::snii_prx_zstd_level_direct_load = _saved[2];
        config::snii_prx_zstd_min_saving_percent = _saved[3];
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    const std::vector<GoldenConfig> _configs = golden_configs();
    GoldenEnvironment _environment;
    std::vector<int32_t> _saved;
};

TEST_F(AnalysisWriteGolden, MatchesTheFrozenDigests) {
    const std::vector<std::vector<std::string>> batches =
            row_batches(default_families(_environment.corpus_sources()));
    test::GoldenFile file(kGoldenFile);
    for (const char* name : kConfigs) {
        const auto config = std::ranges::find_if(
                _configs, [&](const GoldenConfig& c) { return c.name == name; });
        ASSERT_NE(config, _configs.end()) << name;
        const TabletIndex meta = index_meta(*config);
        for (const Format& format : kFormats) {
            const std::string key = std::string(kWritePrefix) + format.name + "/" + config->name;
            const std::string prefix =
                    write_index(std::to_string(&format - kFormats) + "_" +
                                        std::to_string(config - _configs.begin()),
                                format.format, meta, batches);
            const WriteDigest digest = format.format == InvertedIndexStorageFormatPB::SNII
                                               ? snii_digest(prefix)
                                               : clucene_digest(prefix, format.format, meta);
            EXPECT_GT(digest.count, 0) << key;
            file.check(key, "rows", digest.count, digest.digest, describe(*config));
        }
    }
    file.finish([](const std::string& name) { return name.starts_with(kWritePrefix); }, true);
}

} // namespace doris::segment_v2::inverted_index::golden

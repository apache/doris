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

// Where do the bytes of a gram index actually go?
//
// Measured on a cluster, a gram index runs about 1.0x to 1.4x the size of the column data
// it accelerates, and lowering the boundary density or the maximum gram length moves that
// only so far. Deciding whether the remaining size is worth attacking, and where, needs the
// split between the two regions that hold it: the term dictionary (already front-coded,
// with a full-term anchor every anchor_interval entries) and the postings.
//
// This builds real SNII segments over one corpus under several schemes and reports the
// section sizes the writer produced, straight out of the core metadata the reader reads.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

constexpr const char* kDir = "./ut_dir/gram_index_section_sizes";
constexpr int64_t kIndexId = 4801;

void assert_ok(const Status& st) {
    ASSERT_TRUE(st.ok()) << st.to_string();
}

// The region split has to be measured on the vocabulary a real workload produces.
// GRAM_SECTION_CORPUS names a file of one row per line and GRAM_SECTION_ROWS caps how many
// are read; without the variable the case skips rather than fall back to generated rows.
// A generated corpus is not a substitute here: a templated one yields an order of magnitude
// fewer distinct terms than real log text, and the dictionary is sized by exactly that.
std::vector<std::string> LoadCorpusFile(const char* path, size_t max_rows) {
    std::vector<std::string> out;
    std::ifstream in(path);
    std::string line;
    while (out.size() < max_rows && std::getline(in, line)) {
        if (!line.empty()) {
            out.push_back(line);
        }
    }
    return out;
}

TabletIndex MakeIndexMeta(const std::map<std::string, std::string>& props) {
    TabletIndexPB pb;
    pb.set_index_id(kIndexId);
    pb.set_index_name("idx_gram_sections");
    pb.set_index_type(IndexType::INVERTED);
    pb.add_col_unique_id(0);
    auto& m = *pb.mutable_properties();
    for (const auto& [k, v] : props) {
        m[k] = v;
    }
    TabletIndex idx;
    idx.init_from_pb(pb);
    return idx;
}

struct Sections {
    uint64_t dict = 0;
    uint64_t posting = 0;
    uint64_t null_bitmap = 0;
    uint64_t bsbf = 0;
    uint64_t file = 0;
    uint64_t terms = 0;
};

} // namespace

// Prints the region split for a few schemes. The only assertions are structural, so the
// case reports numbers without becoming a tripwire for ordinary size drift.
TEST(GramIndexSectionSizesTest, ReportsDictAndPostingSplit) {
    std::filesystem::remove_all(kDir);
    std::filesystem::create_directories(kDir);

    struct Cfg {
        std::string name;
        std::string mode;
        std::string min_gram;
        std::string max_gram;
        std::string density;
    };
    // density and max_gram act on different regions, so the grid crosses them: density
    // changes how many grams are emitted, which sizes both regions, while max_gram
    // changes how many distinct ones there are, which sizes the dictionary alone.
    std::vector<Cfg> cfgs;
    for (const std::string& d : {"0.25", "0.10", "0.05"}) {
        for (const std::string& m : {"16", "8", "4"}) {
            cfgs.push_back({"sparse d=" + d + " max=" + m, "sparse", "3", m, d});
        }
    }
    cfgs.push_back({"dense min=3", "dense", "3", "16", "0.25"});

    const char* corpus_path = std::getenv("GRAM_SECTION_CORPUS");
    if (corpus_path == nullptr) {
        GTEST_SKIP() << "set GRAM_SECTION_CORPUS to a corpus file (one row per line) to run; "
                        "there is no built-in corpus: a synthetic one is far too templated "
                        "to carry a size conclusion";
    }
    const char* rows_env = std::getenv("GRAM_SECTION_ROWS");
    const size_t max_rows = rows_env ? static_cast<size_t>(std::atoll(rows_env)) : 200000;
    const std::vector<std::string> corpus_rows = LoadCorpusFile(corpus_path, max_rows);
    ASSERT_FALSE(corpus_rows.empty()) << "no rows read from " << corpus_path;
    std::vector<std::optional<std::string>> corpus;
    corpus.reserve(corpus_rows.size());
    uint64_t raw_bytes = 0;
    for (const std::string& r : corpus_rows) {
        raw_bytes += r.size();
        corpus.emplace_back(r);
    }

    printf("\ncorpus: %s, %zu rows, %.2f MB raw\n", corpus_path, corpus.size(),
           static_cast<double>(raw_bytes) / 1048576.0);
    printf("%-24s %10s %10s %10s %7s %7s %9s %9s\n", "scheme", "dict", "posting", "index", "dict%",
           "post%", "terms", "idx/raw");

    int policy_id = 9400;
    for (const Cfg& cfg : cfgs) {
        // SniiIndexColumnWriter takes its gram scheme from the analyzer provider, so the
        // tokenizer parameters have to arrive as a real index policy; properties dropped on
        // the index itself are ignored and every scheme would come out identical.
        IndexPolicyMgr manager;
        auto* exec_env = ExecEnv::GetInstance();
        IndexPolicyMgr* previous = exec_env->index_policy_mgr();
        exec_env->_index_policy_mgr = &manager;

        TIndexPolicy tok;
        tok.id = ++policy_id;
        tok.name = std::string("sec_tok_") + std::to_string(policy_id);
        tok.type = TIndexPolicyType::TOKENIZER;
        tok.properties["type"] = "ngram";
        tok.properties["mode"] = cfg.mode;
        tok.properties["min_gram"] = cfg.min_gram;
        tok.properties["max_gram"] = cfg.max_gram;
        if (cfg.mode == "sparse") {
            tok.properties["density"] = cfg.density;
        }
        TIndexPolicy ana;
        ana.id = ++policy_id;
        ana.name = std::string("sec_ana_") + std::to_string(policy_id);
        ana.type = TIndexPolicyType::ANALYZER;
        ana.properties["tokenizer"] = tok.name;
        manager.apply_policy_changes({tok, ana}, {});

        const TabletIndex index_meta =
                MakeIndexMeta({{"analyzer", ana.name}, {"support_phrase", "false"}});
        const std::string path = std::string(kDir) + "/s" + std::to_string(policy_id) + ".idx";

        io::FileWriterPtr fw;
        assert_ok(io::global_local_filesystem()->create_file(path, &fw));
        IndexFileWriter ifw(io::global_local_filesystem(), path, "gram_sections_rowset",
                            /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII, std::move(fw),
                            /*can_use_ram_dir=*/true, /*tablet_id=*/9301);
        SniiIndexColumnWriter writer(&ifw, &index_meta, FieldType::OLAP_FIELD_TYPE_VARCHAR);
        if (!writer.init().ok()) {
            printf("%-24s  (scheme rejected by the writer, skipped)\n", cfg.name.c_str());
            exec_env->_index_policy_mgr = previous;
            continue;
        }
        std::vector<Slice> slices;
        slices.reserve(corpus.size());
        for (const auto& r : corpus) {
            slices.emplace_back(*r);
        }
        assert_ok(writer.add_values("c1", slices.data(), slices.size()));
        assert_ok(writer.finish());
        assert_ok(ifw.begin_close());
        assert_ok(ifw.finish_close());

        doris::snii::io::LocalFileReader file;
        assert_ok(file.open(path));
        doris::snii::reader::SniiSegmentReader segment;
        assert_ok(doris::snii::reader::SniiSegmentReader::open(&file, &segment));
        doris::snii::reader::LogicalIndexReader index;
        assert_ok(segment.open_index(static_cast<uint64_t>(kIndexId), "", &index));

        Sections s;
        const auto& refs = index.section_refs();
        s.dict = refs.dict_region.length;
        s.posting = refs.posting_region.length;
        s.null_bitmap = refs.null_bitmap.length;
        s.bsbf = refs.bsbf.length;
        s.terms = index.stats().term_count;
        s.file = std::filesystem::file_size(path);

        const double total = static_cast<double>(s.dict + s.posting);
        printf("%-24s %10llu %10llu %10llu %6.1f%% %6.1f%% %9llu %9.3f\n", cfg.name.c_str(),
               static_cast<unsigned long long>(s.dict), static_cast<unsigned long long>(s.posting),
               static_cast<unsigned long long>(s.file),
               total > 0 ? 100.0 * static_cast<double>(s.dict) / total : 0.0,
               total > 0 ? 100.0 * static_cast<double>(s.posting) / total : 0.0,
               static_cast<unsigned long long>(s.terms),
               raw_bytes > 0 ? static_cast<double>(s.file) / static_cast<double>(raw_bytes) : 0.0);

        EXPECT_GT(s.dict, 0U) << cfg.name;
        EXPECT_GT(s.posting, 0U) << cfg.name;
        EXPECT_GT(s.terms, 0U) << cfg.name;
        exec_env->_index_policy_mgr = previous;
    }

    std::filesystem::remove_all(kDir);
}

} // namespace doris::segment_v2

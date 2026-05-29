// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// SPIMI V4 vs V2 inverted-index write-throughput benchmark, on
// Google Benchmark (stable auto-iteration + warmup + stddev,
// unlike the gtest self-timed harness).
//
// Build:   ./build.sh --benchmark
// Run:     be/output/lib/benchmark_test \
//              --benchmark_filter=Spimi \
//              --benchmark_repetitions=5 \
//              --benchmark_report_aggregates_only=true
//
// Data: prefers the real Elasticsearch-rally http_logs corpus
// (1998 World Cup access logs) — the `request` field is the
// fulltext column. Point SPIMI_HTTPLOGS_DIR at the directory of
// `documents-*.json` files (default
// ~/workspace/bin/doris_test/httplogs/data). If unavailable the
// benchmark falls back to a synthetic Zipf fixture so it still
// builds + runs anywhere.

#pragma once

#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <array>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/memory/jemalloc_control.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h" // TmpFileDirs
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/options.h" // StorePath
#include "storage/tablet/tablet_schema.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_writer.h"

namespace doris::segment_v2 {

namespace spimi_bench_detail {

inline constexpr const char* kBenchDir = "/tmp/spimi_gbench";

// Extracts the `"request": "..."` value from one http_logs JSON
// line. Returns empty if not found. Plain substring scan — no
// JSON parser dependency, and the corpus is well-formed.
inline std::string extract_request_field(const std::string& line) {
    static constexpr std::string_view kKey = "\"request\":";
    const size_t k = line.find(kKey);
    if (k == std::string::npos) {
        return {};
    }
    size_t q1 = line.find('"', k + kKey.size());
    if (q1 == std::string::npos) {
        return {};
    }
    const size_t q2 = line.find('"', q1 + 1);
    if (q2 == std::string::npos) {
        return {};
    }
    return line.substr(q1 + 1, q2 - q1 - 1);
}

// Loads up to `max_lines` request strings from the http_logs
// corpus. Reads ONLY a bounded prefix of a SINGLE corpus file
// (the data/ directory is ~32 GB of 1 GB+ files — we never scan
// it all). Falls back to a synthetic Zipf-over-HTTP-paths fixture
// if the corpus directory is missing, so the benchmark is always
// runnable.
//
// `max_lines` is overridable via SPIMI_BENCH_ROWS so a quick smoke
// run can use e.g. 20000 and a heavier run 200000 without
// recompiling.
inline const std::vector<std::string>& httplogs_requests(size_t max_lines) {
    static const std::vector<std::string> cached = [max_lines]() -> std::vector<std::string> {
        size_t cap = max_lines;
        if (const char* rows_env = std::getenv("SPIMI_BENCH_ROWS")) {
            const long v = std::atol(rows_env);
            if (v > 0) {
                cap = static_cast<size_t>(v);
            }
        }
        std::vector<std::string> out;
        out.reserve(cap);
        const char* env = std::getenv("SPIMI_HTTPLOGS_DIR");
        const std::string dir =
                env != nullptr ? env
                               : std::string(std::getenv("HOME") ? std::getenv("HOME") : "") +
                                         "/workspace/bin/doris_test/httplogs/data";
        std::error_code ec;
        if (std::filesystem::is_directory(dir, ec)) {
            // Pick the lexicographically-first .json file and read
            // only its leading `cap` request lines — bounded I/O,
            // never the full 32 GB corpus.
            std::string chosen;
            for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
                if (entry.path().extension() == ".json") {
                    const std::string p = entry.path().string();
                    if (chosen.empty() || p < chosen) {
                        chosen = p;
                    }
                }
            }
            if (!chosen.empty()) {
                std::ifstream in(chosen);
                std::string line;
                while (out.size() < cap && std::getline(in, line)) {
                    std::string req = extract_request_field(line);
                    if (!req.empty()) {
                        out.emplace_back(std::move(req));
                    }
                }
            }
        }
        if (!out.empty()) {
            return out;
        }
        // Synthetic fallback: Zipf over a small bag of HTTP-path
        // tokens, shaped like the real `request` field.
        static const std::array<const char*, 24> kVocab = {
                "GET",     "POST",    "HTTP",    "images",  "english", "html",  "gif",  "jpg",
                "nav",     "bg",      "top",     "bottom",  "news",    "btn",   "off",  "on",
                "splash",  "inet",    "home",    "comp",    "hm",      "nbg",   "backg", "phrase"};
        std::mt19937 rng(0xB17B17BEU);
        std::vector<double> cdf;
        cdf.reserve(kVocab.size());
        double sum = 0;
        for (size_t i = 1; i <= kVocab.size(); ++i) {
            sum += 1.0 / std::pow(static_cast<double>(i), 1.07);
            cdf.push_back(sum);
        }
        for (auto& v : cdf) {
            v /= sum;
        }
        std::uniform_real_distribution<double> u(0.0, 1.0);
        out.reserve(cap);
        for (size_t row = 0; row < cap; ++row) {
            std::string r = "GET /";
            const int toks = 3 + static_cast<int>(rng() % 6);
            for (int t = 0; t < toks; ++t) {
                const double p = u(rng);
                const auto it = std::lower_bound(cdf.begin(), cdf.end(), p);
                r += kVocab[static_cast<size_t>(it - cdf.begin())];
                r += (t + 1 < toks) ? "_" : ".gif";
            }
            r += " HTTP/1.0";
            out.emplace_back(std::move(r));
        }
        return out;
    }();
    return cached;
}

// Loads up to `max_lines` plain-text documents (one per physical
// line, no JSON parse) from a newline-delimited corpus. Used for the
// unicode/CJK reproducer: point SPIMI_UNICODE_CORPUS at a file whose
// each line is one document's fulltext (e.g. the agentlogs `input`
// field extracted to /tmp/agentlogs_input.txt). Falls back to a
// synthetic CJK+ASCII template fixture (shaped like agentlogs) so the
// benchmark still runs anywhere. `max_lines` overridable via
// SPIMI_BENCH_ROWS.
inline const std::vector<std::string>& unicode_corpus_rows(size_t max_lines) {
    static const std::vector<std::string> cached = [max_lines]() -> std::vector<std::string> {
        size_t cap = max_lines;
        if (const char* rows_env = std::getenv("SPIMI_BENCH_ROWS")) {
            const long v = std::atol(rows_env);
            if (v > 0) {
                cap = static_cast<size_t>(v);
            }
        }
        std::vector<std::string> out;
        out.reserve(cap);
        const char* env = std::getenv("SPIMI_UNICODE_CORPUS");
        const std::string path = env != nullptr ? env : "/tmp/agentlogs_input.txt";
        std::error_code ec;
        if (std::filesystem::is_regular_file(path, ec)) {
            std::ifstream in(path);
            std::string line;
            while (out.size() < cap && std::getline(in, line)) {
                if (!line.empty()) {
                    out.emplace_back(std::move(line));
                }
            }
        }
        if (!out.empty()) {
            return out;
        }
        // Synthetic fallback: agentlogs-shaped CJK+ASCII templates.
        static const std::array<const char*, 12> kCjk = {
                "上下文主题", "分类", "应用", "租户", "任务主题", "估算",
                "编码运维", "大上下文回放", "意图", "阶段", "直接计数", "令牌"};
        static const std::array<const char*, 12> kAscii = {
                "token",   "estimate",          "coding_devops", "app_001",
                "tenant",  "archetype",         "intent",        "phase",
                "rid",     "large_context",     "tokens",        "direct_count"};
        std::mt19937 rng(0xA9E27106U);
        out.reserve(cap);
        for (size_t row = 0; row < cap; ++row) {
            std::string r;
            const int segs = 6 + static_cast<int>(rng() % 8);
            for (int s = 0; s < segs; ++s) {
                r += kCjk[rng() % kCjk.size()];
                r += "：";
                r += kAscii[rng() % kAscii.size()];
                r += "_00";
                r += static_cast<char>('0' + (rng() % 9));
                r += "；";
            }
            out.emplace_back(std::move(r));
        }
        return out;
    }();
    return cached;
}

// One-time minimal ExecEnv init the SPIMI write path needs:
// memory tracker (so thread-context allocation accounting doesn't
// deref null) and a tmp-file-dir (IndexFileWriter ctor reads
// ExecEnv::get_tmp_file_dirs()). `benchmark_main`'s BENCHMARK_MAIN
// generates its own `main`, so we lazy-init on first use instead
// of hooking the entry point. Memory tracking is disabled to
// avoid pulling in the full BE runtime.
inline void ensure_env_inited() {
    static const bool inited = [] {
        // Populate config defaults, then explicitly pin the two
        // values the CLucene V2 IndexWriter ctor validates. Without
        // a non-zero ram buffer the V2 writer init throws
        // "ramBufferSize should be > 0.0 MB" and the V2 benchmark
        // would silently no-op — making V2/V4 numbers incomparable.
        // Setting the globals directly is robust regardless of how
        // config::init treats a null conf file in this standalone
        // binary.
        static_cast<void>(config::init(nullptr, false, false, true));
        config::inverted_index_ram_buffer_size = 512;        // MB, production default
        config::inverted_index_max_buffered_docs = -1;       // production default
        // Create the orphan mem-tracker so ThreadMemTrackerMgr::init() succeeds
        // for tracked allocators (faststring, used by the V4 FOR posting tail).
        ExecEnv::GetInstance()->init_mem_tracker();
        ExecEnv::GetInstance()->set_tracking_memory(false);
        auto fs = io::global_local_filesystem();
        static_cast<void>(fs->delete_directory(kBenchDir));
        static_cast<void>(fs->create_directory(kBenchDir));
        std::vector<StorePath> paths;
        paths.emplace_back(kBenchDir, 1024);
        auto tmp_dirs = std::make_unique<TmpFileDirs>(paths);
        static_cast<void>(tmp_dirs->init());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_dirs));
        return true;
    }();
    static_cast<void>(inited);
}

inline TabletSchemaSPtr make_schema() {
    auto schema = std::make_shared<TabletSchema>();
    TabletSchemaPB pb;
    pb.set_keys_type(DUP_KEYS);
    pb.set_num_short_key_columns(1);
    pb.set_num_rows_per_row_block(1024);
    pb.set_compress_kind(COMPRESS_NONE);
    pb.set_next_column_unique_id(3);
    schema->init_from_pb(pb);

    TabletColumn key_col;
    key_col.set_unique_id(0);
    key_col.set_name("k");
    key_col.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    key_col.set_length(4);
    key_col.set_index_length(4);
    key_col.set_is_key(true);
    key_col.set_is_nullable(true);
    schema->append_column(key_col);

    TabletColumn val_col;
    val_col.set_unique_id(1);
    val_col.set_name("request");
    val_col.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
    val_col.set_length(65535);
    val_col.set_is_key(false);
    val_col.set_is_nullable(false);
    schema->append_column(val_col);
    return schema;
}

// Index meta matching the production http_logs DDL: parser=english,
// phrase support on. (Pass parser="basic" via the arg to compare
// the cheaper tokenizer.)
inline TabletIndex make_index_meta(const std::string& parser) {
    auto pb = std::make_unique<TabletIndexPB>();
    pb->set_index_type(IndexType::INVERTED);
    pb->set_index_id(1);
    pb->set_index_name("request_idx");
    pb->clear_col_unique_id();
    pb->add_col_unique_id(1);
    auto* props = pb->mutable_properties();
    (*props)["parser"] = parser;
    (*props)["support_phrase"] = "true";
    TabletIndex idx;
    idx.init_from_pb(*pb);
    return idx;
}

// Peak heap measurement via jemalloc's exact per-thread high-water
// mark (jemalloc 5.3 "thread.peak"). reset_thread_peak() zeroes the
// counter; read_thread_peak() returns the maximum net bytes
// (allocated - deallocated) this thread held at any instant since the
// reset. This is exact, not sampled, and adds no cost inside the timed
// region, so it captures whichever phase actually peaks: V4's full
// SPIMI buffer during add_values, OR V2/CLucene's segment merge during
// finish(). The whole write runs on one benchmark worker thread, so a
// per-thread counter is the correct, fair scope.
//
// The benchmark binary links jemalloc (USE_JEMALLOC=ON); tcmalloc's
// MallocExtension is NOT usable here — it resolves to a weak no-op stub
// because tcmalloc is not the active allocator in this build.
inline void reset_thread_peak() {
#ifdef USE_JEMALLOC
    JemallocControl::action_jemallctl("thread.peak.reset");
#endif
}

inline int64_t read_thread_peak() {
#ifdef USE_JEMALLOC
    return static_cast<int64_t>(JemallocControl::get_jemallctl_value<uint64_t>("thread.peak.read"));
#else
    return JemallocControl::get_tc_metrics("generic.current_allocated_bytes");
#endif
}

// Writes the whole request corpus through one fresh segment writer
// in the given storage format. This is the timed unit of work.
// Returns the writer's exact peak heap high-water (jemalloc
// thread.peak) over the full add_values + finish + close span,
// uniform for V2 (CLucene) and V4 (SPIMI buffer); no module
// self-reporting. Across Google Benchmark's iterations the counter
// takes the max.
inline int64_t write_one_segment(InvertedIndexStorageFormatPB format, const std::string& parser,
                                 const std::vector<std::string>& rows, int64_t run_id) {
    ensure_env_inited();
    // SPIMI write path touches thread-local memory accounting (thread_context()).
    // ensure_env_inited() calls ExecEnv::init_mem_tracker() so the orphan
    // mem-tracker exists; that lets ThreadMemTrackerMgr::init() succeed for
    // tracked allocators like `faststring` (used by the V4 FOR-packed posting
    // tail), matching the production segment-write path. jemalloc thread.peak
    // is independent of Doris tracking, so this doesn't affect the measurement.
    SCOPED_INIT_THREAD_CONTEXT();
    auto fs = io::global_local_filesystem();
    static_cast<void>(fs->create_directory(kBenchDir));
    auto schema = make_schema();
    TabletIndex idx_meta = make_index_meta(parser);

    const std::string fmt_tag =
            (format == InvertedIndexStorageFormatPB::V4) ? "v4" : "v2";
    const std::string rowset_id = std::string("spimi_gb_") + fmt_tag + "_" + parser + "_" +
                                  std::to_string(run_id);
    const int seg_id = 0;
    const std::string seg_path =
            std::string(kBenchDir) + "/" + rowset_id + "_" + std::to_string(seg_id) + ".dat";
    const std::string prefix {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)};
    const std::string idx_path = InvertedIndexDescriptor::get_index_file_path_v2(prefix);

    // Hard-fail on any error: a silently-skipped write would
    // report a meaningless near-zero time and make V2/V4
    // incomparable. CHECK aborts so a broken setup is obvious.
    auto must = [](const Status& st, const char* what) {
        if (!st.ok()) {
            LOG(FATAL) << "spimi bench " << what << " failed: " << st.to_string();
        }
    };
    // Pre-build the Slice vector OUTSIDE the memory window so the
    // input bytes / Slices aren't counted as writer memory.
    std::vector<Slice> slices;
    slices.reserve(rows.size());
    for (const auto& r : rows) {
        slices.emplace_back(r);
    }

    // Reset the per-thread high-water mark right before the writer
    // exists, so the peak reflects only writer-attributable memory
    // (the input Slices/strings were built above, outside the window).
    reset_thread_peak();
    io::FileWriterPtr fw;
    io::FileWriterOptions opts;
    must(fs->create_file(idx_path, &fw, &opts), "create_file");
    // FAIR: both V2 and V4 use the SAME ram-dir setting (env SPIMI_RAM_DIR=0
    // streams both to disk; default keeps both in the RAM directory). Never
    // give one format the disk-streaming escape and not the other.
    const bool can_use_ram_dir = !(std::getenv("SPIMI_RAM_DIR") &&
                                   std::string(std::getenv("SPIMI_RAM_DIR")) == "0");
    auto ifw = std::make_unique<IndexFileWriter>(fs, prefix, rowset_id, seg_id, format,
                                                 std::move(fw), can_use_ram_dir);
    const TabletColumn& column = schema->column(1);
    std::unique_ptr<IndexColumnWriter> writer;
    must(IndexColumnWriter::create(&column, &writer, ifw.get(), &idx_meta), "create_writer");
    constexpr size_t kBatch = 32;
    for (size_t i = 0; i < slices.size(); i += kBatch) {
        const size_t n = std::min(kBatch, slices.size() - i);
        must(writer->add_values("request", slices.data() + i, n), "add_values");
    }
    // finish() flushes the in-memory index to the file. For V2/CLucene
    // this is where buffered segments get merged — a memory peak that a
    // pre-finish sample would miss — so the peak read must come AFTER it.
    must(writer->finish(), "finish");
    must(ifw->begin_close(), "begin_close");
    must(ifw->finish_close(), "finish_close");
    // Exact high-water over the whole add_values + finish + close span.
    const int64_t peak_bytes = read_thread_peak();
    if (std::getenv("SPIMI_IDX_SIZE")) {
        int64_t fsz = 0;
        bool ex = false;
        static_cast<void>(fs->exists(idx_path, &ex));
        if (ex) {
            static_cast<void>(fs->file_size(idx_path, &fsz));
        }
        LOG(INFO) << "[IDX] " << fmt_tag << " idx_bytes=" << fsz;
    }
    // Clean the produced file so the directory doesn't grow across
    // the auto-tuned iteration count Google Benchmark runs.
    static_cast<void>(fs->delete_file(idx_path));
    return peak_bytes > 0 ? peak_bytes : 0;
}

// Tokenize-only baseline: builds the SAME analyzer chain the write
// path uses for `parser`, drives `stream->next()` over every row,
// and discards the tokens (no Intern, no posting append, no
// segment emit). Subtracting this from the V2/V4 end-to-end medians
// isolates the *storage-layer* delta — the work the SPIMI refactor
// actually changes — from the tokenize cost shared by both paths.
inline void tokenize_only(const std::string& parser, const std::vector<std::string>& rows) {
    ensure_env_inited();
    SCOPED_INIT_THREAD_CONTEXT();
    TabletIndex idx_meta = make_index_meta(parser);
    doris::InvertedIndexAnalyzerConfig cfg;
    cfg.analyzer_name = get_analyzer_name_from_properties(idx_meta.properties());
    cfg.parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(idx_meta.properties()));
    cfg.parser_mode = get_parser_mode_string_from_properties(idx_meta.properties());
    cfg.char_filter_map = get_parser_char_filter_map_from_properties(idx_meta.properties());
    cfg.lower_case = get_parser_lowercase_from_properties<true>(idx_meta.properties());
    cfg.stop_words = get_parser_stopwords_from_properties(idx_meta.properties());

    auto reader = inverted_index::InvertedIndexAnalyzer::create_reader(cfg.char_filter_map);
    auto analyzer = inverted_index::InvertedIndexAnalyzer::create_analyzer(&cfg);
    lucene::analysis::Token tok;
    volatile size_t sink = 0;
    for (const auto& r : rows) {
        if (r.empty()) {
            continue;
        }
        reader->init(r.data(), static_cast<int32_t>(r.size()), false);
        // ReaderPtr overload (BasicAnalyzer only implements this one).
        auto* stream = analyzer->reusableTokenStream(L"request", reader);
        stream->reset();
        while (stream->next(&tok) != nullptr) {
            sink += tok.termLength<char>();
        }
    }
    static_cast<void>(sink);
}

} // namespace spimi_bench_detail

// Default 30K real http_logs request lines (overridable via
// SPIMI_BENCH_ROWS). 30K keeps each timed iteration short enough
// (~tens of ms) that Google Benchmark can auto-tune many
// iterations + repetitions for a tight stddev, while staying a
// realistic single-segment write. Reads only the prefix of one
// corpus file — never the full 32 GB data/ directory.
inline const std::vector<std::string>& spimi_bench_rows() {
    return spimi_bench_detail::httplogs_requests(30000);
}

// Reports a `peak_mem_MB` counter = peak writer-attributable live
// bytes (tcmalloc generic.current_allocated_bytes delta sampled at
// the peak moment inside write_one_segment). Allocator-level, so
// V2 and V4 are measured identically — V4's memory-savings claim is
// directly comparable here.
static void run_write_bm(benchmark::State& state, InvertedIndexStorageFormatPB format,
                         const char* parser) {
    const auto& rows = spimi_bench_rows();
    int64_t run = 0;
    int64_t peak_mem = 0;
    for (auto _ : state) {
        peak_mem = std::max(peak_mem,
                            spimi_bench_detail::write_one_segment(format, parser, rows, run++));
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
    state.counters["peak_mem_MB"] =
            benchmark::Counter(static_cast<double>(peak_mem) / (1024.0 * 1024.0));
}

static void BM_SpimiHttplogsWriteV2English(benchmark::State& state) {
    run_write_bm(state, InvertedIndexStorageFormatPB::V2, "english");
}
BENCHMARK(BM_SpimiHttplogsWriteV2English)->Unit(benchmark::kMillisecond)->UseRealTime();

static void BM_SpimiHttplogsWriteV4English(benchmark::State& state) {
    run_write_bm(state, InvertedIndexStorageFormatPB::V4, "english");
}
BENCHMARK(BM_SpimiHttplogsWriteV4English)->Unit(benchmark::kMillisecond)->UseRealTime();

static void BM_SpimiHttplogsWriteV2Basic(benchmark::State& state) {
    run_write_bm(state, InvertedIndexStorageFormatPB::V2, "basic");
}
BENCHMARK(BM_SpimiHttplogsWriteV2Basic)->Unit(benchmark::kMillisecond)->UseRealTime();

static void BM_SpimiHttplogsWriteV4Basic(benchmark::State& state) {
    run_write_bm(state, InvertedIndexStorageFormatPB::V4, "basic");
}
BENCHMARK(BM_SpimiHttplogsWriteV4Basic)->Unit(benchmark::kMillisecond)->UseRealTime();

// ---- Unicode/CJK reproducer (agentlogs `input` field) ----
// parser=unicode on real CJK+English text (~489 B/doc). This is the
// workload where the cluster E2E showed V4 ~1.5x SLOWER than V2,
// unlike ASCII httplogs where V4 wins. Same timed unit + peak_mem_MB
// counter as the httplogs benchmarks.
inline const std::vector<std::string>& spimi_unicode_rows() {
    return spimi_bench_detail::unicode_corpus_rows(30000);
}

static void run_write_bm_unicode(benchmark::State& state, InvertedIndexStorageFormatPB format) {
    const auto& rows = spimi_unicode_rows();
    int64_t run = 0;
    int64_t peak_mem = 0;
    for (auto _ : state) {
        peak_mem = std::max(peak_mem,
                            spimi_bench_detail::write_one_segment(format, "unicode", rows, run++));
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
    state.counters["peak_mem_MB"] =
            benchmark::Counter(static_cast<double>(peak_mem) / (1024.0 * 1024.0));
}

static void BM_SpimiAgentlogsWriteV2Unicode(benchmark::State& state) {
    run_write_bm_unicode(state, InvertedIndexStorageFormatPB::V2);
}
BENCHMARK(BM_SpimiAgentlogsWriteV2Unicode)->Unit(benchmark::kMillisecond)->UseRealTime();

static void BM_SpimiAgentlogsWriteV4Unicode(benchmark::State& state) {
    run_write_bm_unicode(state, InvertedIndexStorageFormatPB::V4);
}
BENCHMARK(BM_SpimiAgentlogsWriteV4Unicode)->Unit(benchmark::kMillisecond)->UseRealTime();

static void BM_SpimiAgentlogsTokenizeUnicode(benchmark::State& state) {
    const auto& rows = spimi_unicode_rows();
    for (auto _ : state) {
        spimi_bench_detail::tokenize_only("unicode", rows);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
}
BENCHMARK(BM_SpimiAgentlogsTokenizeUnicode)->Unit(benchmark::kMillisecond)->UseRealTime();

// Tokenize-only baselines. storage_layer = WriteVx - TokenizeOnly.
// Compute V4-vs-V2 storage ratio as
//   (V4_total - TokenizeOnly) / (V2_total - TokenizeOnly).
static void BM_SpimiHttplogsTokenizeEnglish(benchmark::State& state) {
    const auto& rows = spimi_bench_rows();
    for (auto _ : state) {
        spimi_bench_detail::tokenize_only("english", rows);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
}
BENCHMARK(BM_SpimiHttplogsTokenizeEnglish)->Unit(benchmark::kMillisecond)->UseRealTime();

static void BM_SpimiHttplogsTokenizeBasic(benchmark::State& state) {
    const auto& rows = spimi_bench_rows();
    for (auto _ : state) {
        spimi_bench_detail::tokenize_only("basic", rows);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
}
BENCHMARK(BM_SpimiHttplogsTokenizeBasic)->Unit(benchmark::kMillisecond)->UseRealTime();

} // namespace doris::segment_v2

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
//
// ---- Mapping these numbers to cluster E2E ingest (read this first) ----
// These benchmarks measure the INDEX-LAYER cost (tokenize + posting build +
// segment emit) of ONE segment writer. A cluster import differs in four ways
// that change the V4/V2 ratio — do not expect the single-thread ratio here to
// reproduce an E2E ingest ratio:
//   1. CONCURRENCY: imports run ~dozens of segment writers in parallel; the
//      historical cluster "V4 1.5x slower" was allocator contention invisible
//      single-thread. Use the BM_SpimiConcurrent* cases (aggregate
//      items_per_second at 16/32 threads) for the E2E-shaped comparison.
//   2. DILUTION: E2E ingest also pays parse/memtable/other columns/segment
//      data pages/upload, shared by V2 and V4 — an index-layer delta of X%
//      shows up in E2E as roughly X% × (index share of total ingest CPU).
//   3. PRODUCTION COSTS the bench must opt into: per-allocation memory
//      tracking (SPIMI_TRACK_MEM, default ON) and write-time searcher-cache
//      warmup (SPIMI_WARMUP_CACHE=1; CI/cluster confs enable it, default BE
//      config does not).
//   4. SCALE: row caps (see per-dataset *_ROWS envs) bound vocabulary growth;
//      full tablets have larger vocab/df tails.
// Every run logs its effective config ("[BENCH-CONFIG] ...") — quote it with
// any number you report.

#pragma once

#include <benchmark/benchmark.h>

#include <algorithm>
#include <atomic>
#include <array>
#include <cmath>
#include <thread>
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
#include "storage/index/inverted/spimi/posting_buffer.h"

namespace doris::segment_v2 {

namespace spimi_bench_detail {

inline constexpr const char* kBenchDir = "/tmp/spimi_gbench";

// Row-cap resolution: a per-dataset env (e.g. SPIMI_WIKIPEDIA_ROWS) always
// wins; the global SPIMI_BENCH_ROWS applies only when `allow_global` — large-
// doc corpora (wikipedia, ~27 KB/doc) opt OUT so a global cap meant for short-
// doc datasets (200K rows) can't silently inflate them into multi-GB segments.
inline size_t resolve_rows_cap(const char* rows_env_var, size_t default_cap, bool allow_global) {
    if (rows_env_var != nullptr) {
        if (const char* v = std::getenv(rows_env_var)) {
            const long n = std::atol(v);
            if (n > 0) {
                return static_cast<size_t>(n);
            }
        }
    }
    if (allow_global) {
        if (const char* v = std::getenv("SPIMI_BENCH_ROWS")) {
            const long n = std::atol(v);
            if (n > 0) {
                return static_cast<size_t>(n);
            }
        }
    }
    return default_cap;
}

// Rows per add_values() call. Production feeds the inverted-index builder one
// data-page batch at a time (thousands of rows for short strings), not tiny
// slivers; 4096 matches that order of magnitude. SPIMI_BENCH_BATCH overrides.
inline size_t bench_batch_size() {
    static const size_t cached = [] {
        if (const char* v = std::getenv("SPIMI_BENCH_BATCH")) {
            const long n = std::atol(v);
            if (n > 0) {
                return static_cast<size_t>(n);
            }
        }
        return static_cast<size_t>(4096);
    }();
    return cached;
}

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
        const size_t cap = resolve_rows_cap("SPIMI_HTTPLOGS_ROWS", max_lines, /*allow_global=*/true);
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
        const size_t cap = resolve_rows_cap("SPIMI_UNICODE_ROWS", max_lines, /*allow_global=*/true);
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

// Loads up to `max_lines` TextBench OTel-logs `Body` documents (one per
// physical line, no parse) from a newline-delimited corpus. This is the
// ClickHouse TextBench fulltext column (the same `Body` indexed in the 1B-row
// cluster comparison). Point SPIMI_TEXTBENCH_CORPUS at the extracted file
// (default /mnt/disk15/jiangkai/text_bench/textbench_body.txt, produced by the
// duckdb `Body`-export step). Falls back to a synthetic OTel-log-shaped fixture
// (short message lines + JSON gRPC error blobs) so the benchmark still runs
// anywhere. `max_lines` overridable via SPIMI_BENCH_ROWS.
inline const std::vector<std::string>& textbench_body_rows(size_t max_lines) {
    static const std::vector<std::string> cached = [max_lines]() -> std::vector<std::string> {
        const size_t cap =
                resolve_rows_cap("SPIMI_TEXTBENCH_ROWS", max_lines, /*allow_global=*/true);
        std::vector<std::string> out;
        out.reserve(cap);
        const char* env = std::getenv("SPIMI_TEXTBENCH_CORPUS");
        const std::string path =
                env != nullptr ? env : "/mnt/disk15/jiangkai/text_bench/textbench_body.txt";
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
        // Synthetic fallback: OTel-log-shaped — a Zipf mix of short status
        // messages and JSON gRPC-error blobs, so term frequencies are skewed
        // like the real Body column.
        static const std::array<const char*, 16> kMsg = {
                "Failed to place order",  "Checkout",
                "payment processed",      "shipping quote requested",
                "cart emptied",           "product not found",
                "recommendation served",  "currency converted",
                "ad request served",      "user session expired",
                "inventory reserved",     "order confirmed",
                "email dispatched",       "cache miss",
                "rpc deadline exceeded",  "connection reset by peer"};
        static const std::array<const char*, 8> kCode = {"13", "2", "5", "14",
                                                         "8",  "4", "7", "16"};
        std::mt19937 rng(0x7E47BE0CU);
        std::vector<double> cdf;
        cdf.reserve(kMsg.size());
        double sum = 0;
        for (size_t i = 1; i <= kMsg.size(); ++i) {
            sum += 1.0 / std::pow(static_cast<double>(i), 1.10);
            cdf.push_back(sum);
        }
        for (auto& v : cdf) {
            v /= sum;
        }
        std::uniform_real_distribution<double> u(0.0, 1.0);
        for (size_t row = 0; row < cap; ++row) {
            const auto pick = [&] {
                const auto it = std::lower_bound(cdf.begin(), cdf.end(), u(rng));
                return kMsg[static_cast<size_t>(it - cdf.begin())];
            };
            if ((rng() & 3U) == 0U) {
                // JSON gRPC-error blob (~1 in 4), like the real Body.
                std::string r = R"({"code":)";
                r += kCode[rng() % kCode.size()];
                r += R"(,"details":"failed to charge card: could not charge the card: rpc error: code = Unknown desc = )";
                r += pick();
                r += R"(.","metadata":{"content-type":["application/grpc"]}})";
                out.emplace_back(std::move(r));
            } else {
                out.emplace_back(pick());
            }
        }
        return out;
    }();
    return cached;
}

// Generic newline-delimited corpus loader: up to `cap` non-empty lines from
// the file named by `env_var` (default `default_path`), cap overridable by
// SPIMI_BENCH_ROWS. Returns empty if neither file exists (caller falls back to
// a synthetic fixture). Shared by the wikipedia/weibo datasets.
inline std::vector<std::string> load_line_corpus(const char* env_var,
                                                 const std::string& default_path, size_t cap) {
    std::vector<std::string> out;
    out.reserve(cap);
    const char* env = std::getenv(env_var);
    const std::string path = env != nullptr ? env : default_path;
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
    return out;
}

// English Wikipedia article text (one article's `content` per line). Point
// SPIMI_WIKIPEDIA_CORPUS at the extracted file (default
// /mnt/disk15/jiangkai/wiki_shards/wiki_corpus.txt); synthetic English-prose
// fallback otherwise. parser=english. Large docs (~27 KB), so the row cap is
// ONLY overridable via SPIMI_WIKIPEDIA_ROWS — the global SPIMI_BENCH_ROWS is
// deliberately ignored here (a 200K short-doc cap would make this a 5+ GB
// segment and a pathological measurement).
inline const std::vector<std::string>& wikipedia_rows(size_t max_lines) {
    static const std::vector<std::string> cached = [max_lines]() -> std::vector<std::string> {
        const size_t cap =
                resolve_rows_cap("SPIMI_WIKIPEDIA_ROWS", max_lines, /*allow_global=*/false);
        auto out = load_line_corpus("SPIMI_WIKIPEDIA_CORPUS",
                                    "/mnt/disk15/jiangkai/wiki_shards/wiki_corpus.txt", cap);
        if (!out.empty()) {
            return out;
        }
        static const std::array<const char*, 24> kVocab = {
                "the",     "history", "century", "government", "system",  "during",
                "national","world",   "州",     "war",        "language","people",
                "city",    "first",   "states",  "university", "company", "music",
                "river",   "north",   "south",   "church",     "island",  "party"};
        std::mt19937 rng(0xC0FFEE11U);
        out.reserve(cap);
        for (size_t row = 0; row < cap; ++row) {
            std::string r;
            const int words = 40 + static_cast<int>(rng() % 200);
            for (int wd = 0; wd < words; ++wd) {
                r += kVocab[rng() % kVocab.size()];
                r += ' ';
            }
            out.emplace_back(std::move(r));
        }
        return out;
    }();
    return cached;
}

// Chinese Weibo microblog text (one raw message per line). Point
// SPIMI_WEIBO_CORPUS at the extracted file (default
// /mnt/disk15/jiangkai/weibo/weibo_corpus.txt); synthetic CJK fallback
// otherwise. parser=unicode (CJK). Short docs; SPIMI_WEIBO_ROWS (or the global
// SPIMI_BENCH_ROWS) overrides the cap.
inline const std::vector<std::string>& weibo_rows(size_t max_lines) {
    static const std::vector<std::string> cached = [max_lines]() -> std::vector<std::string> {
        const size_t cap = resolve_rows_cap("SPIMI_WEIBO_ROWS", max_lines, /*allow_global=*/true);
        auto out = load_line_corpus("SPIMI_WEIBO_CORPUS",
                                    "/mnt/disk15/jiangkai/weibo/weibo_corpus.txt", cap);
        if (!out.empty()) {
            return out;
        }
        static const std::array<const char*, 16> kCjk = {
                "一分耕耘", "一分收获", "转发微博", "哈哈哈", "给力", "围观", "求关注",
                "今天天气", "好开心", "加油", "支持", "分享", "美图", "心情", "周末", "晚安"};
        std::mt19937 rng(0x5E1B0U);
        out.reserve(cap);
        for (size_t row = 0; row < cap; ++row) {
            std::string r;
            const int segs = 4 + static_cast<int>(rng() % 10);
            for (int s = 0; s < segs; ++s) {
                r += kCjk[rng() % kCjk.size()];
                r += (s % 3 == 2) ? "，" : " ";
            }
            r += "http://t.cn/abcDEF";
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
        //
        // CRITICAL: config::init's default-application loop ABORTS midway
        // (SET_FIELD prints "config field error" and returns false) on the
        // first field whose default needs an env substitution it can't
        // resolve — custom_config_dir = "${DORIS_HOME}/conf". Every field
        // registered after it (including ALL inverted_index_* knobs) then
        // stays at its zero-initialized global value: prx_zstd=0,
        // zstd_min=0, prx_window_docs=0, ram_dir=0 — nothing like
        // production defaults. The [BENCH-CONFIG] provenance line below
        // caught exactly that: this benchmark had been silently measuring
        // an all-zero config. So: provide DORIS_HOME (every "${...}"
        // default in config.cpp references only DORIS_HOME), use an empty
        // readable conf file, and HARD-FAIL if init still reports failure
        // instead of swallowing it.
        ::setenv("DORIS_HOME", kBenchDir, /*overwrite=*/0);
        if (!config::init("/dev/null", false, false, true)) {
            LOG(FATAL) << "spimi bench config::init failed — config would be all-zero "
                          "and every measurement meaningless; fix the env (DORIS_HOME?)";
        }
        config::inverted_index_ram_buffer_size = 512;        // MB, production default
        config::inverted_index_max_buffered_docs = -1;       // production default
        // frq/prx ZSTD defaults follow the compiled config (config::init above
        // applies the DEFINE defaults). Env overrides let one binary measure
        // either side of the split: SPIMI_FRQ_ZSTD=0/1, SPIMI_PRX_ZSTD=0/1
        // (prx is moot for support_phrase=false / no .prx).
        if (const char* z = std::getenv("SPIMI_FRQ_ZSTD")) {
            config::inverted_index_spimi_frq_zstd_enable = (std::string_view(z) != "0");
        }
        if (const char* z = std::getenv("SPIMI_PRX_ZSTD")) {
            config::inverted_index_spimi_prx_zstd_enable = (std::string_view(z) != "0");
        }
        // Write-time searcher-cache warmup: BE default is false, but CI and
        // E2E cluster confs enable it, adding a per-segment searcher build
        // (V2 = CLucene open, V4 = SPIMI open — asymmetric cost) inside
        // finish_close. SPIMI_WARMUP_CACHE=1 opts the benchmark into that
        // E2E-shaped cost.
        if (const char* w = std::getenv("SPIMI_WARMUP_CACHE")) {
            config::enable_write_index_searcher_cache = (std::string_view(w) != "0");
        }
        // Create the orphan mem-tracker so ThreadMemTrackerMgr::init() succeeds
        // for tracked allocators (faststring, used by the V4 FOR posting tail).
        ExecEnv::GetInstance()->init_mem_tracker();
        // Per-allocation Doris memory tracking. Production runs with the
        // thread-context alloc/free hook charging a tracker on EVERY heap op;
        // the formats allocate differently (V4 historically more, smaller
        // allocations), so disabling tracking hides an asymmetric production
        // cost. Default ON for production fidelity; SPIMI_TRACK_MEM=0 restores
        // the old untracked mode for comparison against historical numbers.
        const bool track_mem = !(std::getenv("SPIMI_TRACK_MEM") &&
                                 std::string_view(std::getenv("SPIMI_TRACK_MEM")) == "0");
        ExecEnv::GetInstance()->set_tracking_memory(track_mem);
        auto fs = io::global_local_filesystem();
        static_cast<void>(fs->delete_directory(kBenchDir));
        static_cast<void>(fs->create_directory(kBenchDir));
        std::vector<StorePath> paths;
        paths.emplace_back(kBenchDir, 1024);
        auto tmp_dirs = std::make_unique<TmpFileDirs>(paths);
        static_cast<void>(tmp_dirs->init());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_dirs));
        // Effective-config provenance: every measurement-affecting knob, one
        // greppable line. Historical incident: a whole result set was measured
        // with frq-ZSTD unknowingly off — quote this line with any number.
        LOG(INFO) << "[BENCH-CONFIG]"
                  << " frq_zstd=" << config::inverted_index_spimi_frq_zstd_enable
                  << " prx_zstd=" << config::inverted_index_spimi_prx_zstd_enable
                  << " zstd_min_window_bytes=" << config::inverted_index_spimi_zstd_min_window_bytes
                  << " prx_window_docs=" << config::inverted_index_spimi_prx_window_docs
                  << " buffer_budget_mb="
                  << (inverted_index::spimi::SpimiPostingBuffer::kDefaultMemoryBudget >> 20)
                  << " ram_dir_config=" << config::inverted_index_ram_dir_enable
                  << " ram_dir_bench=" << !(std::getenv("SPIMI_RAM_DIR") &&
                                            std::string_view(std::getenv("SPIMI_RAM_DIR")) == "0")
                  << " track_mem=" << track_mem
                  << " warmup_cache=" << config::enable_write_index_searcher_cache
                  << " batch=" << bench_batch_size();
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
//
// `support_phrase` defaults to true (positions written -> .prx stream).
// Pass false for a DOCS_ONLY (omit_term_freq_and_positions) index, which
// is the token-only shape the TextBench / ClickHouse / Elasticsearch
// comparison uses: no positions, no norms (CLucene V2 also omits norms
// when positions are omitted), so V2 and V4 idx sizes are apples-to-apples.
inline TabletIndex make_index_meta(const std::string& parser, bool support_phrase = true) {
    auto pb = std::make_unique<TabletIndexPB>();
    pb->set_index_type(IndexType::INVERTED);
    pb->set_index_id(1);
    pb->set_index_name("request_idx");
    pb->clear_col_unique_id();
    pb->add_col_unique_id(1);
    auto* props = pb->mutable_properties();
    (*props)["parser"] = parser;
    (*props)["support_phrase"] = support_phrase ? "true" : "false";
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
// Result of one timed segment write: exact peak heap high-water and the
// produced .idx file size (one combined file for both V2 and V4).
struct WriteResult {
    int64_t peak_bytes = 0;
    int64_t idx_bytes = 0;
};

inline WriteResult write_one_segment(InvertedIndexStorageFormatPB format, const std::string& parser,
                                     bool support_phrase, const std::vector<std::string>& rows,
                                     int64_t run_id) {
    ensure_env_inited();
    // Env override of support_phrase so ANY dataset can be benchmarked in either
    // DOCS_ONLY (SPIMI_SUPPORT_PHRASE=0) or phrase-on (=1) mode without adding a
    // separate case (e.g. run the DOCS_ONLY TextBench cases phrase-on via env).
    if (const char* sp = std::getenv("SPIMI_SUPPORT_PHRASE")) {
        support_phrase = (std::string_view(sp) != "0");
    }
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
    TabletIndex idx_meta = make_index_meta(parser, support_phrase);

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
    // Production-shaped batching: one add_values call per data-page-sized run
    // of rows (default 4096, SPIMI_BENCH_BATCH overrides) — not tiny slivers
    // that would amplify per-call overhead ~100x vs the real write path.
    const size_t batch = bench_batch_size();
    for (size_t i = 0; i < slices.size(); i += batch) {
        const size_t n = std::min(batch, slices.size() - i);
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
    // Capture the produced .idx size BEFORE deleting it. The IndexFileWriter
    // writes ONE combined .idx file for both V2 and V4 (same idx_path), so this
    // is a directly comparable cross-format size — reported as the idx_MB
    // benchmark counter (and still logged under SPIMI_IDX_SIZE).
    int64_t idx_bytes = 0;
    {
        bool ex = false;
        static_cast<void>(fs->exists(idx_path, &ex));
        if (ex) {
            static_cast<void>(fs->file_size(idx_path, &idx_bytes));
        }
    }
    if (std::getenv("SPIMI_IDX_SIZE")) {
        LOG(INFO) << "[IDX] " << fmt_tag << " idx_bytes=" << idx_bytes;
    }
    // Optionally keep a copy of the produced combined .idx for offline stream
    // inspection (e.g. confirm V2 omits norms = no .nrm sub-file, or the
    // frq-zstd effect on .frq). The live file is deleted each iteration to bound
    // directory growth, so copy to a stable per-format path first.
    if (std::getenv("SPIMI_KEEP_IDX")) {
        std::error_code cec;
        std::filesystem::copy_file(idx_path, std::string(kBenchDir) + "/keep_" + fmt_tag + ".idx",
                                   std::filesystem::copy_options::overwrite_existing, cec);
    }
    // Clean the produced file so the directory doesn't grow across
    // the auto-tuned iteration count Google Benchmark runs.
    static_cast<void>(fs->delete_file(idx_path));
    return WriteResult {peak_bytes > 0 ? peak_bytes : 0, idx_bytes};
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

// Tokenize + FNV-hash each token (no intern table, no probe, no encode). Used
// to put a hard upper bound on the CPU a "tokenizer emits term_id" scheme could
// ever remove from V4: SPIMI is the sole interner, so the most such a scheme can
// save is the per-token hash (measured here) + the term-equality memcmp
// (separately ~3% on the flame graph). hash_cost = HashOnly − TokenizeOnly.
inline void hash_only(const std::string& parser, const std::vector<std::string>& rows) {
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
    inverted_index::spimi::SpimiPostingBuffer buf(0xD0D0F00DULL,
                                                  /*omit_tfap=*/false); // fixed seed; HashTerm is const
    lucene::analysis::Token tok;
    volatile uint64_t sink = 0;
    for (const auto& r : rows) {
        if (r.empty()) {
            continue;
        }
        reader->init(r.data(), static_cast<int32_t>(r.size()), false);
        auto* stream = analyzer->reusableTokenStream(L"request", reader);
        stream->reset();
        while (stream->next(&tok) != nullptr) {
            const size_t len = tok.termLength<char>();
            if (len > 0) {
                sink ^= buf.HashTerm(std::string_view(tok.termBuffer<char>(), len));
            }
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
    int64_t idx_bytes = 0;
    for (auto _ : state) {
        auto res = spimi_bench_detail::write_one_segment(format, parser, /*support_phrase=*/true,
                                                         rows, run++);
        peak_mem = std::max(peak_mem, res.peak_bytes);
        idx_bytes = res.idx_bytes;
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
    state.counters["peak_mem_MB"] =
            benchmark::Counter(static_cast<double>(peak_mem) / (1024.0 * 1024.0));
    state.counters["idx_MB"] =
            benchmark::Counter(static_cast<double>(idx_bytes) / (1024.0 * 1024.0));
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
    int64_t idx_bytes = 0;
    for (auto _ : state) {
        auto res = spimi_bench_detail::write_one_segment(format, "unicode", /*support_phrase=*/true,
                                                         rows, run++);
        peak_mem = std::max(peak_mem, res.peak_bytes);
        idx_bytes = res.idx_bytes;
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
    state.counters["peak_mem_MB"] =
            benchmark::Counter(static_cast<double>(peak_mem) / (1024.0 * 1024.0));
    state.counters["idx_MB"] =
            benchmark::Counter(static_cast<double>(idx_bytes) / (1024.0 * 1024.0));
}

static void BM_SpimiAgentlogsWriteV2Unicode(benchmark::State& state) {
    run_write_bm_unicode(state, InvertedIndexStorageFormatPB::V2);
}
BENCHMARK(BM_SpimiAgentlogsWriteV2Unicode)->Unit(benchmark::kMillisecond)->UseRealTime();

static void BM_SpimiAgentlogsWriteV4Unicode(benchmark::State& state) {
    run_write_bm_unicode(state, InvertedIndexStorageFormatPB::V4);
}
BENCHMARK(BM_SpimiAgentlogsWriteV4Unicode)->Unit(benchmark::kMillisecond)->UseRealTime();

// ---- Concurrent write: N threads writing unicode segments simultaneously ----
// The cluster's original "V4 1.5x SLOWER than V2" was a CONCURRENCY effect:
// at ~36-way import the dominant cost was ALLOCATION CONTENTION, not per-thread
// CPU (single-thread was near-parity). Flat-mode V4 allocated an ~11M-record
// vector + an equal-size stable_sort scratch per segment, so 36 threads hammered
// the allocator. The compact-mode + deque fix removes those big allocations, so
// this benchmark checks whether V4 now wins at concurrency. Aggregate throughput
// (items_per_second over wall-clock with UseRealTime) is the V4-vs-V2 metric;
// thread count via ->Arg(N).
static void run_concurrent_write_unicode(benchmark::State& state,
                                         InvertedIndexStorageFormatPB format) {
    const int n_threads = static_cast<int>(state.range(0));
    const auto& rows = spimi_unicode_rows();
    spimi_bench_detail::ensure_env_inited(); // init once, OUTSIDE the timed region
    std::atomic<int64_t> run {0};
    for (auto _ : state) {
        const int64_t base = run.fetch_add(1) * n_threads;
        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(n_threads));
        for (int t = 0; t < n_threads; ++t) {
            threads.emplace_back([&rows, format, base, t] {
                // Unique run_id per (iteration, thread) keeps segment file paths
                // from colliding across the concurrent writers.
                spimi_bench_detail::write_one_segment(format, "unicode", /*support_phrase=*/true,
                                                      rows, base + t);
            });
        }
        for (auto& th : threads) {
            th.join();
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n_threads) *
                            static_cast<int64_t>(rows.size()));
}

static void BM_SpimiConcurrentWriteV2Unicode(benchmark::State& state) {
    run_concurrent_write_unicode(state, InvertedIndexStorageFormatPB::V2);
}
BENCHMARK(BM_SpimiConcurrentWriteV2Unicode)
        ->Arg(16)
        ->Arg(32)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime()
        ->Iterations(3);

static void BM_SpimiConcurrentWriteV4Unicode(benchmark::State& state) {
    run_concurrent_write_unicode(state, InvertedIndexStorageFormatPB::V4);
}
BENCHMARK(BM_SpimiConcurrentWriteV4Unicode)
        ->Arg(16)
        ->Arg(32)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime()
        ->Iterations(3);

static void BM_SpimiAgentlogsTokenizeUnicode(benchmark::State& state) {
    const auto& rows = spimi_unicode_rows();
    for (auto _ : state) {
        spimi_bench_detail::tokenize_only("unicode", rows);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
}
BENCHMARK(BM_SpimiAgentlogsTokenizeUnicode)->Unit(benchmark::kMillisecond)->UseRealTime();

// Upper bound on a "tokenizer emits term_id" scheme: HashOnly − TokenizeOnly is
// the per-token FNV hash cost (the largest piece such a scheme could remove,
// together with the ~3% term-equality memcmp). If this delta is small, the
// scheme cannot approach the 20%-faster target.
static void BM_SpimiAgentlogsHashOnlyUnicode(benchmark::State& state) {
    const auto& rows = spimi_unicode_rows();
    for (auto _ : state) {
        spimi_bench_detail::hash_only("unicode", rows);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
}
BENCHMARK(BM_SpimiAgentlogsHashOnlyUnicode)->Unit(benchmark::kMillisecond)->UseRealTime();

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

// ---- ClickHouse TextBench dataset (1B-row OTel-logs `Body` fulltext) ----
// The SAME column indexed in the cluster V2/V4/CK/ES size comparison. These
// cases use parser=english + support_phrase=FALSE (DOCS_ONLY: no positions, no
// norms — CLucene V2 omits norms too when positions are omitted), matching the
// cluster's token-only DDL so V2 and V4 .idx sizes are apples-to-apples with
// ClickHouse / Elasticsearch. Each timed iteration writes one segment of
// `SPIMI_BENCH_ROWS` (default 200K) Body docs; the table reports wall time
// (UseRealTime), peak_mem_MB (jemalloc thread.peak), and idx_MB (the produced
// combined .idx file). Point SPIMI_TEXTBENCH_CORPUS at the extracted Body file.
inline const std::vector<std::string>& spimi_textbench_rows() {
    return spimi_bench_detail::textbench_body_rows(200000);
}

static void run_write_bm_textbench(benchmark::State& state, InvertedIndexStorageFormatPB format) {
    const auto& rows = spimi_textbench_rows();
    int64_t run = 0;
    int64_t peak_mem = 0;
    int64_t idx_bytes = 0;
    for (auto _ : state) {
        // support_phrase=false -> DOCS_ONLY, matching the TextBench cluster config.
        auto res = spimi_bench_detail::write_one_segment(format, "english",
                                                         /*support_phrase=*/false, rows, run++);
        peak_mem = std::max(peak_mem, res.peak_bytes);
        idx_bytes = res.idx_bytes;
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
    state.counters["peak_mem_MB"] =
            benchmark::Counter(static_cast<double>(peak_mem) / (1024.0 * 1024.0));
    state.counters["idx_MB"] =
            benchmark::Counter(static_cast<double>(idx_bytes) / (1024.0 * 1024.0));
}

static void BM_SpimiTextbenchWriteV2English(benchmark::State& state) {
    run_write_bm_textbench(state, InvertedIndexStorageFormatPB::V2);
}
BENCHMARK(BM_SpimiTextbenchWriteV2English)->Unit(benchmark::kMillisecond)->UseRealTime();

static void BM_SpimiTextbenchWriteV4English(benchmark::State& state) {
    run_write_bm_textbench(state, InvertedIndexStorageFormatPB::V4);
}
BENCHMARK(BM_SpimiTextbenchWriteV4English)->Unit(benchmark::kMillisecond)->UseRealTime();

// Tokenize-only baseline for the TextBench Body corpus:
// storage_layer = WriteVx - TokenizeOnly isolates the index-write cost from the
// tokenize cost shared by V2 and V4.
static void BM_SpimiTextbenchTokenizeEnglish(benchmark::State& state) {
    const auto& rows = spimi_textbench_rows();
    for (auto _ : state) {
        spimi_bench_detail::tokenize_only("english", rows);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
}
BENCHMARK(BM_SpimiTextbenchTokenizeEnglish)->Unit(benchmark::kMillisecond)->UseRealTime();

// ---- Wikipedia (English article text) & Weibo (Chinese microblog) datasets ----
// Generic write benchmark: writes one segment of `rows` through `parser` in
// `format`, reporting wall (UseRealTime), peak_mem_MB, idx_MB. support_phrase
// defaults to TRUE (phrase-on, like a typical fulltext DDL) but is overridable
// per run via SPIMI_SUPPORT_PHRASE=0/1 (handled in write_one_segment).
static void run_write_bm_named(benchmark::State& state, InvertedIndexStorageFormatPB format,
                               const char* parser, const std::vector<std::string>& rows) {
    int64_t run = 0;
    int64_t peak_mem = 0;
    int64_t idx_bytes = 0;
    for (auto _ : state) {
        auto res = spimi_bench_detail::write_one_segment(format, parser, /*support_phrase=*/true,
                                                         rows, run++);
        peak_mem = std::max(peak_mem, res.peak_bytes);
        idx_bytes = res.idx_bytes;
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
    state.counters["peak_mem_MB"] =
            benchmark::Counter(static_cast<double>(peak_mem) / (1024.0 * 1024.0));
    state.counters["idx_MB"] =
            benchmark::Counter(static_cast<double>(idx_bytes) / (1024.0 * 1024.0));
}

// Wikipedia: english parser. Full articles average ~27 KB/doc, so the default
// row count is LOW (3000 -> ~81 MB/segment) to keep an iteration tractable;
// SPIMI_BENCH_ROWS overrides for a heavier run.
inline const std::vector<std::string>& spimi_wikipedia_rows() {
    return spimi_bench_detail::wikipedia_rows(3000);
}
static void BM_SpimiWikipediaWriteV2English(benchmark::State& state) {
    run_write_bm_named(state, InvertedIndexStorageFormatPB::V2, "english", spimi_wikipedia_rows());
}
BENCHMARK(BM_SpimiWikipediaWriteV2English)->Unit(benchmark::kMillisecond)->UseRealTime();
static void BM_SpimiWikipediaWriteV4English(benchmark::State& state) {
    run_write_bm_named(state, InvertedIndexStorageFormatPB::V4, "english", spimi_wikipedia_rows());
}
BENCHMARK(BM_SpimiWikipediaWriteV4English)->Unit(benchmark::kMillisecond)->UseRealTime();
static void BM_SpimiWikipediaTokenizeEnglish(benchmark::State& state) {
    const auto& rows = spimi_wikipedia_rows();
    for (auto _ : state) {
        spimi_bench_detail::tokenize_only("english", rows);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
}
BENCHMARK(BM_SpimiWikipediaTokenizeEnglish)->Unit(benchmark::kMillisecond)->UseRealTime();

// Weibo: unicode (CJK) parser, short docs -> default 200K rows (SPIMI_BENCH_ROWS overrides).
inline const std::vector<std::string>& spimi_weibo_rows() {
    return spimi_bench_detail::weibo_rows(200000);
}
static void BM_SpimiWeiboWriteV2Unicode(benchmark::State& state) {
    run_write_bm_named(state, InvertedIndexStorageFormatPB::V2, "unicode", spimi_weibo_rows());
}
BENCHMARK(BM_SpimiWeiboWriteV2Unicode)->Unit(benchmark::kMillisecond)->UseRealTime();
static void BM_SpimiWeiboWriteV4Unicode(benchmark::State& state) {
    run_write_bm_named(state, InvertedIndexStorageFormatPB::V4, "unicode", spimi_weibo_rows());
}
BENCHMARK(BM_SpimiWeiboWriteV4Unicode)->Unit(benchmark::kMillisecond)->UseRealTime();
static void BM_SpimiWeiboTokenizeUnicode(benchmark::State& state) {
    const auto& rows = spimi_weibo_rows();
    for (auto _ : state) {
        spimi_bench_detail::tokenize_only("unicode", rows);
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
}
BENCHMARK(BM_SpimiWeiboTokenizeUnicode)->Unit(benchmark::kMillisecond)->UseRealTime();

// ---- Concurrent write on the REAL corpora ----
// This is the E2E-shaped headline metric: a cluster import runs dozens of
// segment writers in parallel, and the historical "V4 1.5x slower in E2E"
// was an allocator-contention effect that single-thread benchmarks cannot
// see. Aggregate throughput = items_per_second (UseRealTime) at ->Arg(N)
// threads, each thread writing one full segment of the corpus. Compare the
// V2/V4 ratio HERE against cluster ingest ratios — not the single-thread
// cases above.
static void run_concurrent_write_named(benchmark::State& state, InvertedIndexStorageFormatPB format,
                                       const char* parser, bool support_phrase,
                                       const std::vector<std::string>& rows) {
    const int n_threads = static_cast<int>(state.range(0));
    spimi_bench_detail::ensure_env_inited(); // init once, OUTSIDE the timed region
    std::atomic<int64_t> run {0};
    for (auto _ : state) {
        const int64_t base = run.fetch_add(1) * n_threads;
        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(n_threads));
        for (int t = 0; t < n_threads; ++t) {
            threads.emplace_back([&rows, format, parser, support_phrase, base, t] {
                spimi_bench_detail::write_one_segment(format, parser, support_phrase, rows,
                                                      base + t);
            });
        }
        for (auto& th : threads) {
            th.join();
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n_threads) *
                            static_cast<int64_t>(rows.size()));
}

// TextBench Body, DOCS_ONLY (matches its single-thread case + the cluster DDL).
static void BM_SpimiConcurrentTextbenchV2English(benchmark::State& state) {
    run_concurrent_write_named(state, InvertedIndexStorageFormatPB::V2, "english",
                               /*support_phrase=*/false, spimi_textbench_rows());
}
BENCHMARK(BM_SpimiConcurrentTextbenchV2English)
        ->Arg(16)
        ->Arg(32)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime()
        ->Iterations(3);
static void BM_SpimiConcurrentTextbenchV4English(benchmark::State& state) {
    run_concurrent_write_named(state, InvertedIndexStorageFormatPB::V4, "english",
                               /*support_phrase=*/false, spimi_textbench_rows());
}
BENCHMARK(BM_SpimiConcurrentTextbenchV4English)
        ->Arg(16)
        ->Arg(32)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime()
        ->Iterations(3);

// Weibo CJK, phrase-on (matches its single-thread case).
static void BM_SpimiConcurrentWeiboV2Unicode(benchmark::State& state) {
    run_concurrent_write_named(state, InvertedIndexStorageFormatPB::V2, "unicode",
                               /*support_phrase=*/true, spimi_weibo_rows());
}
BENCHMARK(BM_SpimiConcurrentWeiboV2Unicode)
        ->Arg(16)
        ->Arg(32)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime()
        ->Iterations(3);
static void BM_SpimiConcurrentWeiboV4Unicode(benchmark::State& state) {
    run_concurrent_write_named(state, InvertedIndexStorageFormatPB::V4, "unicode",
                               /*support_phrase=*/true, spimi_weibo_rows());
}
BENCHMARK(BM_SpimiConcurrentWeiboV4Unicode)
        ->Arg(16)
        ->Arg(32)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime()
        ->Iterations(3);

} // namespace doris::segment_v2

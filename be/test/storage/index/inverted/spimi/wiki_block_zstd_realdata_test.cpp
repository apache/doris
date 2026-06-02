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

// REAL-CORPUS validation of the S3 block-granular ZSTD tradeoff across
// httplogs / wikipedia / agentlogs / weibo. Tokenizes a bounded prefix of each
// corpus (UTF-8 aware: ASCII alnum words + per-Han-char CJK tokens, like the
// unicode parser) into real per-term posting streams, then compares total
// .frq + .prx bytes under whole-term ZSTD (today), fixed-W per-block ZSTD, and
// an ADAPTIVE per-term policy (finest block within a compression budget). Also
// prints a selective-query S3 bytes proxy. BUILD IN RELEASE (this is a
// benchmark, not a correctness test) — e.g. BUILD_TYPE_UT=RELEASE. Skips any
// corpus whose file is absent.

#include <gtest/gtest.h>
#include <zstd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/index/inverted/spimi/pfor_encoder.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

void append_vint(std::vector<uint8_t>& b, uint32_t v) {
    while (v & ~0x7FU) {
        b.push_back(static_cast<uint8_t>((v & 0x7FU) | 0x80U));
        v >>= 7U;
    }
    b.push_back(static_cast<uint8_t>(v));
}

size_t framed_zstd(const uint8_t* p, size_t n) {
    if (n == 0) {
        return 0;
    }
    const size_t bound = ZSTD_compressBound(n);
    std::vector<uint8_t> out(bound);
    const size_t c = ZSTD_compress(out.data(), bound, p, n, 1);
    if (!ZSTD_isError(c) && c + 10 < n) {
        return 11 + c;
    }
    return 1 + n;
}

std::vector<uint8_t> pfor_bytes(const std::vector<uint32_t>& v, bool patch) {
    std::vector<uint8_t> out;
    for (size_t off = 0; off < v.size(); off += SpimiPforEncoder::kBlockSize) {
        const size_t n = std::min(SpimiPforEncoder::kBlockSize, v.size() - off);
        std::vector<uint32_t> blk(v.begin() + static_cast<long>(off),
                                  v.begin() + static_cast<long>(off + n));
        const auto b = SpimiPforEncoder::EncodeBlockToBytes(blk, patch);
        out.insert(out.end(), b.begin(), b.end());
    }
    return out;
}
std::vector<uint8_t> vint_bytes(const std::vector<uint32_t>& v) {
    std::vector<uint8_t> b;
    for (uint32_t x : v) {
        append_vint(b, x);
    }
    return b;
}

template <typename Enc>
size_t windowed(const std::vector<uint32_t>& v, size_t W, Enc enc) {
    if (W == 0) {
        const auto b = enc(v);
        return framed_zstd(b.data(), b.size());
    }
    size_t total = 0;
    for (size_t off = 0; off < v.size(); off += W) {
        const size_t n = std::min(W, v.size() - off);
        std::vector<uint32_t> w(v.begin() + static_cast<long>(off),
                                v.begin() + static_cast<long>(off + n));
        const auto b = enc(w);
        total += framed_zstd(b.data(), b.size());
    }
    return total;
}

// Extract a JSON string field value by key from one line (read_json_by_line).
bool extract_json_str(const std::string& line, const std::string& key, std::string* out) {
    const std::string pat = "\"" + key + "\":";
    const size_t k = line.find(pat);
    if (k == std::string::npos) {
        return false;
    }
    size_t i = line.find('"', k + pat.size());
    if (i == std::string::npos) {
        return false;
    }
    ++i;
    out->clear();
    while (i < line.size()) {
        const char c = line[i];
        if (c == '\\' && i + 1 < line.size()) {
            const char n = line[i + 1];
            out->push_back((n == 'n' || n == 't' || n == 'r') ? ' ' : n);
            i += 2;
            continue;
        }
        if (c == '"') {
            return true;
        }
        out->push_back(c);
        ++i;
    }
    return true;
}

// Extract the 6th quoted CSV field (index 5 = `text`) from a weibo line.
bool extract_weibo_text(const std::string& line, std::string* out) {
    int field = 0;
    size_t i = 0;
    while (i < line.size()) {
        if (line[i] != '"') {
            ++i;
            continue;
        }
        ++i; // opening quote
        const size_t start = i;
        while (i < line.size() && line[i] != '"') {
            ++i;
        }
        if (field == 5) {
            out->assign(line, start, i - start);
            return true;
        }
        ++field;
        ++i; // closing quote
        while (i < line.size() && line[i] != '"') {
            ++i; // skip the comma
        }
    }
    return false;
}

uint32_t decode_utf8(const std::string& s, size_t& i) {
    const auto c = static_cast<unsigned char>(s[i]);
    if (c < 0x80) {
        ++i;
        return c;
    }
    if ((c >> 5) == 0x6 && i + 1 < s.size()) {
        const uint32_t cp = ((c & 0x1FU) << 6) | (static_cast<unsigned char>(s[i + 1]) & 0x3FU);
        i += 2;
        return cp;
    }
    if ((c >> 4) == 0xE && i + 2 < s.size()) {
        const uint32_t cp = ((c & 0x0FU) << 12) |
                            ((static_cast<unsigned char>(s[i + 1]) & 0x3FU) << 6) |
                            (static_cast<unsigned char>(s[i + 2]) & 0x3FU);
        i += 3;
        return cp;
    }
    if ((c >> 3) == 0x1E && i + 3 < s.size()) {
        i += 4;
        return 0x10000; // some supplementary char; treated as CJK-ish below
    }
    ++i;
    return 0xFFFD;
}

bool is_cjk(uint32_t cp) {
    return (cp >= 0x4E00 && cp <= 0x9FFF) || (cp >= 0x3400 && cp <= 0x4DBF) ||
           (cp >= 0xF900 && cp <= 0xFAFF) || (cp >= 0x3040 && cp <= 0x30FF) /*kana*/ ||
           cp >= 0x10000;
}

using Postings = std::unordered_map<std::string, std::vector<std::pair<uint32_t, uint32_t>>>;

// UTF-8 aware tokenizer: lowercase ASCII alnum words + per-CJK-char tokens.
void tokenize(const std::string& content, uint32_t doc, Postings& post, size_t* ntok) {
    std::string tok;
    uint32_t pos = 0;
    auto flush_word = [&] {
        if (!tok.empty()) {
            post[tok].emplace_back(doc, pos++);
            ++*ntok;
            tok.clear();
        }
    };
    size_t i = 0;
    while (i < content.size()) {
        const size_t start = i;
        const uint32_t cp = decode_utf8(content, i);
        if (cp < 0x80) {
            const auto c = static_cast<char>(cp);
            if (c >= 'a' && c <= 'z') {
                tok.push_back(c);
            } else if (c >= 'A' && c <= 'Z') {
                tok.push_back(static_cast<char>(c | 0x20));
            } else if (c >= '0' && c <= '9') {
                tok.push_back(c);
            } else {
                flush_word();
            }
        } else if (is_cjk(cp)) {
            flush_word();
            post[content.substr(start, i - start)].emplace_back(doc, pos++); // single-char token
            ++*ntok;
        } else {
            flush_word(); // non-CJK non-ASCII (punctuation etc.) = boundary
        }
    }
    flush_word();
}

struct Corpus {
    std::string name;
    std::string path;
    std::string field; // json key, or "" for weibo csv
};

void run_corpus(const Corpus& cz) {
    std::ifstream in(cz.path, std::ios::binary);
    if (!in.good()) {
        std::printf("[corpus][%-10s] SKIP (not found: %s)\n", cz.name.c_str(), cz.path.c_str());
        return;
    }
    const char* mb_env = std::getenv("SPIMI_CORPUS_MB");
    const size_t max_bytes = (mb_env != nullptr ? std::strtoul(mb_env, nullptr, 10) : 150) << 20;

    Postings post;
    post.reserve(1 << 20);
    std::string line, content;
    uint32_t doc = 0;
    size_t total_tokens = 0, bytes_read = 0;
    while (bytes_read < max_bytes && std::getline(in, line)) {
        bytes_read += line.size() + 1;
        bool ok = cz.field.empty() ? extract_weibo_text(line, &content)
                                   : extract_json_str(line, cz.field, &content);
        if (!ok || content.empty()) {
            continue;
        }
        tokenize(content, doc, post, &total_tokens);
        ++doc;
    }

    constexpr int32_t kLargeDf = 512;
    const std::vector<size_t> Ws = {512, 1024, 2048};
    size_t n_large = 0;
    size_t frq_whole = 0, prx_whole = 0, frq_adaptive = 0, prx_adaptive = 0;
    std::unordered_map<size_t, size_t> frq_fixed, prx_fixed, adapt_hist;
    size_t s3_whole = 0, s3_adaptive = 0;

    auto frq_enc = [](const std::vector<uint32_t>& w) { return pfor_bytes(w, true); };
    auto prx_enc = [](const std::vector<uint32_t>& w) { return vint_bytes(w); };

    for (auto& [term, occ] : post) {
        std::vector<uint32_t> dd, fq, pd;
        uint32_t last_doc = 0;
        size_t i = 0;
        while (i < occ.size()) {
            const uint32_t d = occ[i].first;
            size_t j = i;
            uint32_t last_pos = 0;
            bool first = true;
            while (j < occ.size() && occ[j].first == d) {
                pd.push_back(first ? occ[j].second : occ[j].second - last_pos);
                last_pos = occ[j].second;
                first = false;
                ++j;
            }
            dd.push_back(d - last_doc);
            fq.push_back(static_cast<uint32_t>(j - i));
            last_doc = d;
            i = j;
        }
        const auto df = static_cast<int32_t>(dd.size());
        if (df < kLargeDf) {
            continue;
        }
        ++n_large;
        const size_t fw = windowed(dd, 0, frq_enc) + windowed(fq, 0, frq_enc);
        const size_t pw = windowed(pd, 0, prx_enc);
        frq_whole += fw;
        prx_whole += pw;

        size_t f_adapt = fw, chosen = 0;
        bool picked = false;
        for (size_t W : Ws) {
            const size_t fW = windowed(dd, W, frq_enc) + windowed(fq, W, frq_enc);
            frq_fixed[W] += fW;
            prx_fixed[W] += windowed(pd, W, prx_enc);
            if (!picked && fW <= fw + fw / 10) {
                f_adapt = fW;
                chosen = W;
                picked = true;
            }
        }
        size_t p_adapt = pw;
        for (size_t W : Ws) {
            const size_t pW = windowed(pd, W, prx_enc);
            if (pW <= pw + pw / 10) {
                p_adapt = pW;
                break;
            }
        }
        frq_adaptive += f_adapt;
        prx_adaptive += p_adapt;
        adapt_hist[chosen]++;
        s3_whole += fw + pw;
        if (chosen == 0) {
            s3_adaptive += f_adapt + p_adapt;
        } else {
            const size_t nb = (static_cast<size_t>(df) + chosen - 1) / chosen;
            const size_t want = std::max<size_t>(1, static_cast<size_t>(df) / 100);
            const size_t need = std::min(nb, (want + chosen - 1) / chosen + 1);
            s3_adaptive += (f_adapt + p_adapt) * need / std::max<size_t>(1, nb);
        }
    }

    auto pc = [](size_t a, size_t b) {
        return b ? 100.0 * (static_cast<double>(a) / static_cast<double>(b) - 1.0) : 0.0;
    };
    std::printf("\n[corpus][%-10s] docs=%u tokens=%zu terms=%zu large(df>=512)=%zu\n",
                cz.name.c_str(), doc, total_tokens, post.size(), n_large);
    std::printf(
            "  FRQ whole=%zuB | W512 %+.1f%% W1024 %+.1f%% W2048 %+.1f%% | ADAPTIVE %zuB %+.1f%%\n",
            frq_whole, pc(frq_fixed[512], frq_whole), pc(frq_fixed[1024], frq_whole),
            pc(frq_fixed[2048], frq_whole), frq_adaptive, pc(frq_adaptive, frq_whole));
    std::printf(
            "  PRX whole=%zuB | W512 %+.1f%% W1024 %+.1f%% W2048 %+.1f%% | ADAPTIVE %zuB %+.1f%%\n",
            prx_whole, pc(prx_fixed[512], prx_whole), pc(prx_fixed[1024], prx_whole),
            pc(prx_fixed[2048], prx_whole), prx_adaptive, pc(prx_adaptive, prx_whole));
    std::printf("  ADAPTIVE granularity (terms): kept-whole=%zu W512=%zu W1024=%zu W2048=%zu\n",
                adapt_hist[0], adapt_hist[512], adapt_hist[1024], adapt_hist[2048]);
    std::printf(
            "  S3 selective(~1%% docs of large terms): whole=%zuB adaptive=%zuB (%.1fx less)\n",
            s3_whole, s3_adaptive,
            s3_adaptive ? static_cast<double>(s3_whole) / static_cast<double>(s3_adaptive) : 0.0);
}

} // namespace

TEST(CorpusBlockZstdRealData, AdaptiveVsFixedVsWholeTerm) {
    std::puts("\n===== REAL corpora: block ZSTD granularity (whole vs fixed-W vs ADAPTIVE) =====");
    std::puts(
            "(release build recommended; +% = per-window vs whole-term; adaptive picks finest"
            " block within +10% of whole-term)");
    const std::vector<Corpus> corpora = {
            {"httplogs", "/mnt/disk1/jiangkai/httplogs/data/documents-171998.json", "request"},
            {"wikipedia", "/mnt/disk1/jiangkai/workspace/bin/test/wikipedia/data/part_1.json",
             "content"},
            {"agentlogs", "/mnt/disk15/jiangkai/agentlogs_raw/agent_observations_0001.ndjson",
             "input"},
            {"weibo", "/mnt/disk15/jiangkai/weibo/parsed_messages.txt", ""},
    };
    for (const auto& c : corpora) {
        run_corpus(c);
    }
    SUCCEED();
}

} // namespace doris::segment_v2::inverted_index::spimi

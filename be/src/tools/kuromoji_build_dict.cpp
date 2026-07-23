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

// Offline tool: compile a UTF-8 mecab-ipadic source directory into the four
// kuromoji .bin files consumed by KuromojiDictionary.
//   usage: kuromoji_build_dict <ipadic_src_dir> <out_dir>
// Built on demand via `ninja kuromoji_dict`; never linked into doris_be.

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary_builder.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_ipadic_parser.h"

namespace fs = std::filesystem;
using namespace doris::segment_v2::inverted_index::kuromoji;
using doris::Status;

namespace {

bool read_file(const std::string& path, std::string* out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        std::fprintf(stderr, "cannot open %s\n", path.c_str());
        return false;
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    *out = ss.str();
    return true;
}

void for_each_line(const std::string& content, const std::function<void(std::string_view)>& fn) {
    std::size_t i = 0;
    while (i < content.size()) {
        auto nl = content.find('\n', i);
        if (nl == std::string::npos) {
            nl = content.size();
        }
        std::string_view line(content.data() + i, nl - i);
        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }
        if (!line.empty()) {
            fn(line);
        }
        i = nl + 1;
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s <ipadic_src_dir> <out_dir>\n", argv[0]);
        return 2;
    }
    const std::string src = argv[1];
    const std::string out = argv[2];
    std::error_code ec;
    if (!fs::is_directory(src, ec)) {
        std::fprintf(stderr,
                     "kuromoji dict source directory not found: %s\n"
                     "stage the mecab-ipadic thirdparty package before building the dictionary.\n",
                     src.c_str());
        return 1;
    }
    fs::create_directories(out, ec);

    // --- system dictionary: group all *.csv lexicon rows by surface (homographs) ---
    std::vector<std::string> csv_paths;
    for (const auto& entry : fs::directory_iterator(src)) {
        if (entry.is_regular_file() && entry.path().extension() == ".csv") {
            csv_paths.push_back(entry.path().string());
        }
    }
    std::sort(csv_paths.begin(), csv_paths.end());

    std::unordered_map<std::string, std::vector<BuilderWord>> by_surface;
    std::size_t lexicon_rows = 0;
    for (const auto& path : csv_paths) {
        std::string content;
        if (!read_file(path, &content)) {
            return 1;
        }
        Status line_st = Status::OK();
        for_each_line(content, [&](std::string_view line) {
            if (!line_st.ok()) {
                return;
            }
            std::string surface;
            BuilderWord w;
            line_st = parse_lexicon_line(line, &surface, &w);
            if (!line_st.ok()) {
                return;
            }
            by_surface[surface].push_back(std::move(w));
            ++lexicon_rows;
        });
        if (!line_st.ok()) {
            std::fprintf(stderr, "lexicon parse failed in %s: %s\n", path.c_str(),
                         line_st.to_string().c_str());
            return 1;
        }
    }
    if (lexicon_rows == 0) {
        std::fprintf(stderr, "no lexicon rows parsed from %s; source missing or truncated?\n",
                     src.c_str());
        return 1;
    }
    SystemDictInput sys;
    sys.surfaces.reserve(by_surface.size());
    for (auto& kv : by_surface) {
        std::sort(kv.second.begin(), kv.second.end(),
                  [](const BuilderWord& a, const BuilderWord& b) {
                      if (a.word_cost != b.word_cost) {
                          return a.word_cost < b.word_cost;
                      }
                      if (a.left_id != b.left_id) {
                          return a.left_id < b.left_id;
                      }
                      if (a.right_id != b.right_id) {
                          return a.right_id < b.right_id;
                      }
                      return a.feature < b.feature;
                  });
        sys.surfaces.emplace_back(kv.first, std::move(kv.second));
    }
    if (Status st = KuromojiDictionaryBuilder::write_system(out + "/system.bin", sys); !st.ok()) {
        std::fprintf(stderr, "write_system failed: %s\n", st.to_string().c_str());
        return 1;
    }

    // --- connection cost matrix ---
    std::string matrix_txt;
    MatrixInput matrix;
    if (!read_file(src + "/matrix.def", &matrix_txt) ||
        !parse_matrix_def(matrix_txt, &matrix).ok() ||
        !KuromojiDictionaryBuilder::write_matrix(out + "/matrix.bin", matrix).ok()) {
        std::fprintf(stderr, "matrix.def build failed\n");
        return 1;
    }

    // --- character definitions ---
    std::string char_txt;
    CharDefInput chardef;
    if (!read_file(src + "/char.def", &char_txt) || !parse_char_def(char_txt, &chardef).ok() ||
        !KuromojiDictionaryBuilder::write_chardef(out + "/chardef.bin", chardef).ok()) {
        std::fprintf(stderr, "char.def build failed\n");
        return 1;
    }

    // --- unknown-word dictionary ---
    std::string unk_txt;
    UnkDictInput unk;
    if (!read_file(src + "/unk.def", &unk_txt) || !parse_unk_def(unk_txt, &unk).ok() ||
        !KuromojiDictionaryBuilder::write_unkdict(out + "/unkdict.bin", unk).ok()) {
        std::fprintf(stderr, "unk.def build failed\n");
        return 1;
    }

    std::fprintf(stderr,
                 "kuromoji dict built: %zu surfaces (%zu lexicon rows), matrix %ux%u -> %s\n",
                 sys.surfaces.size(), lexicon_rows, matrix.forward_size, matrix.backward_size,
                 out.c_str());
    return 0;
}

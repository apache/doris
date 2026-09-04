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

#include "storage/index/inverted/gram/gram_extractor.h"

#include <unordered_set>

namespace doris::segment_v2::gram {

// BE 的 Storage 目标开启了 CMake Unity Build（多个 .cpp 合并编译，见
// be/src/storage/CMakeLists.txt 的 UNITY_BUILD_BATCH_SIZE），同一批次内所有
// 文件的匿名命名空间会被合并进同一个翻译单元，裸的匿名命名空间一旦与批次内
// 其他文件重名（哪怕在不同 .cpp 里）就会重定义报错，且批次分组会随目录下
// 文件增删而变化，无法长期假设「这批次只有这几个文件」。因此这里额外套一层
// 本文件专属的具名命名空间，隔离本文件的匿名命名空间，不影响其中各符号本身
// 仍是内部链接（匿名命名空间语义不受具名外层嵌套影响）。
namespace gram_extractor_detail {

namespace {

// splitmix64 的 finalizer，用作字节对 (a,b) 的边界哈希混合函数；必须与原型
// tools/regex-ngram-model/ngram_model.cpp 的 mix64 逐位一致，golden 数据由该原型生成。
inline uint64_t mix64(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

// 返回前导字节 c 所属 UTF-8 序列的字节长度（1/2/3/4）；非法前导字节按 1 处理。
inline int utf8_len(unsigned char c) {
    if (c < 0x80) {
        return 1;
    }
    if ((c >> 5) == 0x6) {
        return 2;
    }
    if ((c >> 4) == 0xE) {
        return 3;
    }
    if ((c >> 3) == 0x1E) {
        return 4;
    }
    return 1;
}

// 返回从 p 开始的一个合法 UTF-8 码点的字节长度；序列被截断或延续字节不合法
// （非 10xxxxxx）时判定为非法，返回 1（按单字节处理，产出为 1-gram）。
inline size_t codepoint_len(const char* p, size_t remain) {
    int l = utf8_len((unsigned char)p[0]);
    if (l == 1 || (size_t)l > remain) {
        return 1;
    }
    for (int k = 1; k < l; k++) {
        if (((unsigned char)p[k] & 0xC0) != 0x80) {
            return 1;
        }
    }
    return l;
}

} // namespace

} // namespace gram_extractor_detail

GramExtractor::GramExtractor(const GramScheme& scheme) : _scheme(scheme) {
    _build_boundary_table();
}

void GramExtractor::_build_boundary_table() {
    _boundary_bits.assign(8192, 0);
    const uint64_t threshold = (uint64_t)_scheme.density_permille * 65536ULL / 1000ULL;
    for (unsigned idx = 0; idx < 65536; idx++) {
        uint64_t key = ((uint64_t)idx) ^ 0x5bd1e995ULL; // idx = (a<<8)|b
        if ((gram_extractor_detail::mix64(key) & 0xFFFF) < threshold) {
            _boundary_bits[idx >> 3] |= (uint8_t)(1U << (idx & 7));
        }
    }
}

void GramExtractor::_ascii_segment(std::string_view seg, std::vector<std::string_view>* out) {
    const size_t L = seg.size();
    const size_t n = _scheme.min_len;
    if (L < n) {
        return;
    }
    if (_scheme.mode == GramMode::DENSE) {
        for (size_t i = 0; i + n <= L; i++) {
            // Ruling R9：候选 gram 含 NUL 字节（0x00）就整体跳过，不产出、也不
            // 影响其余窗口——窗口边界计算本身与是否含 NUL 无关。
            std::string_view g = seg.substr(i, n);
            if (g.find('\0') == std::string_view::npos) {
                out->push_back(g);
            }
        }
        return;
    }
    // SPARSE：内容定义分块（CDC）。边界 k 起，延伸到首个使 j+2-k >= min_len 的
    // 后续边界 j，gram = [k, j+2)；max_len 内找不到这样的边界则取满窗
    // [k, k+max_len)；连满窗都凑不满（到串尾前不足 max_len）则该起点不产出。
    const size_t maxlen = _scheme.max_len;
    _is_boundary_at.assign(L, 0);
    for (size_t i = 0; i + 1 < L; i++) {
        _is_boundary_at[i] = is_boundary((uint8_t)seg[i], (uint8_t)seg[i + 1]);
    }
    for (size_t k = 0; k + 1 < L; k++) {
        if (!_is_boundary_at[k]) {
            continue;
        }
        size_t end = 0;
        for (size_t j = k + 1; j + 1 < L && j + 2 - k <= maxlen; j++) {
            if (_is_boundary_at[j] && j + 2 - k >= n) {
                end = j + 2;
                break;
            }
        }
        if (end == 0) {
            if (k + maxlen <= L) {
                end = k + maxlen;
            } else {
                continue;
            }
        }
        // Ruling R9：同上，候选 gram 含 NUL 就跳过，边界（k/end）计算不受影响。
        std::string_view g = seg.substr(k, end - k);
        if (g.find('\0') == std::string_view::npos) {
            out->push_back(g);
        }
    }
}

void GramExtractor::_dedupe(std::vector<std::string_view>* out) {
    std::unordered_set<std::string_view> seen;
    size_t w = 0;
    for (size_t r = 0; r < out->size(); r++) {
        if (seen.insert((*out)[r]).second) {
            (*out)[w++] = (*out)[r];
        }
    }
    out->resize(w);
}

void GramExtractor::extract(std::string_view value, std::vector<std::string_view>* out) {
    out->clear();
    if (_scheme.lower_case) {
        _folded.assign(value.data(), value.size());
        for (auto& ch : _folded) {
            if (ch >= 'A' && ch <= 'Z') {
                ch = (char)(ch - 'A' + 'a');
            }
        }
        value = _folded;
    }
    size_t i = 0;
    const size_t L = value.size();
    while (i < L) {
        if ((unsigned char)value[i] < 0x80) {
            // ASCII 段：向后扫到第一个非 ASCII 字节（或串尾），整段一起切 gram。
            size_t j = i;
            while (j < L && (unsigned char)value[j] < 0x80) {
                j++;
            }
            _ascii_segment(value.substr(i, j - i), out);
            i = j;
        } else {
            // 非 ASCII：一个合法码点（或一个非法字节）产出一个 1-gram。
            size_t l = gram_extractor_detail::codepoint_len(value.data() + i, L - i);
            out->push_back(value.substr(i, l));
            i += l;
        }
    }
    _dedupe(out);
}

void GramExtractor::grams_of_literal(std::string_view s, std::vector<std::string>* out) {
    std::vector<std::string_view> tmp;
    extract(s, &tmp);
    out->assign(tmp.begin(), tmp.end());
}

} // namespace doris::segment_v2::gram

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

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary_builder.h"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <fstream>

#include "storage/index/inverted/analyzer/kuromoji/dict/darts.h"

namespace doris::segment_v2::kuromoji {

namespace {

// Append-only byte buffer that tracks offset and 8-byte-aligns sections.
class ByteSink {
public:
    void align8() {
        while ((_buf.size() % 8) != 0) {
            _buf.push_back(0);
        }
    }
    uint64_t offset() const { return static_cast<uint64_t>(_buf.size()); }
    void put(const void* p, std::size_t n) {
        const auto* b = static_cast<const uint8_t*>(p);
        _buf.insert(_buf.end(), b, b + n);
    }
    template <typename T>
    void put_pod(const T& v) {
        put(&v, sizeof(T));
    }
    std::vector<uint8_t>& buf() { return _buf; }

private:
    std::vector<uint8_t> _buf;
};

KmjFileHeader make_header(KmjFileKind kind) {
    KmjFileHeader h {};
    std::memcpy(h.magic, KMJ_MAGIC, sizeof(h.magic));
    h.format_version = KMJ_FORMAT_VERSION;
    h.file_kind = kind;
    h.file_size = 0; // patched at flush
    return h;
}

// Appends one run's worth of entries (+ feature blob bytes) and returns the run.
WordIdRun append_words(const std::vector<BuilderWord>& words, std::vector<WordEntry>& entries,
                       std::vector<uint8_t>& features) {
    WordIdRun run {static_cast<uint32_t>(entries.size()), static_cast<uint32_t>(words.size())};
    for (const auto& w : words) {
        WordEntry e {};
        e.left_id = w.left_id;
        e.right_id = w.right_id;
        e.word_cost = w.word_cost;
        e.pad = 0;
        if (w.feature.empty()) {
            e.feature_offset = KMJ_NO_FEATURE;
        } else {
            e.feature_offset = static_cast<uint32_t>(features.size());
            const auto len =
                    static_cast<uint16_t>(std::min<std::size_t>(w.feature.size(), 0xFFFFU));
            features.push_back(static_cast<uint8_t>(len & 0xFFU));
            features.push_back(static_cast<uint8_t>((len >> 8) & 0xFFU));
            const auto* p = reinterpret_cast<const uint8_t*>(w.feature.data());
            features.insert(features.end(), p, p + len);
        }
        entries.push_back(e);
    }
    return run;
}

Status flush_file(const std::string& path, std::vector<uint8_t>& buf) {
    auto* hdr = reinterpret_cast<KmjFileHeader*>(buf.data());
    hdr->file_size = static_cast<uint64_t>(buf.size());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        return Status::IOError("kuromoji dict: cannot open {} for write", path);
    }
    out.write(reinterpret_cast<const char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
    if (!out) {
        return Status::IOError("kuromoji dict: short write to {}", path);
    }
    return Status::OK();
}

} // namespace

Status KuromojiDictionaryBuilder::write_system(const std::string& path, const SystemDictInput& in) {
    // Sort surfaces by raw bytes (Darts requirement); trie value = sorted index.
    auto surfaces = in.surfaces;
    std::sort(surfaces.begin(), surfaces.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    std::vector<WordIdRun> runs;
    std::vector<WordEntry> entries;
    std::vector<uint8_t> features;
    runs.reserve(surfaces.size());
    for (const auto& [surface, words] : surfaces) {
        runs.push_back(append_words(words, entries, features));
    }

    Darts::DoubleArray da;
    if (!surfaces.empty()) {
        std::vector<const char*> kptrs;
        std::vector<std::size_t> klens;
        std::vector<int> values;
        kptrs.reserve(surfaces.size());
        klens.reserve(surfaces.size());
        values.reserve(surfaces.size());
        for (std::size_t i = 0; i < surfaces.size(); ++i) {
            kptrs.push_back(surfaces[i].first.data());
            klens.push_back(surfaces[i].first.size());
            values.push_back(static_cast<int>(i));
        }
        try {
            if (da.build(surfaces.size(), kptrs.data(), klens.data(), values.data()) != 0) {
                return Status::InternalError("kuromoji dict: darts build failed");
            }
        } catch (const std::exception& e) {
            return Status::InternalError("kuromoji dict: darts build threw: {}", e.what());
        }
    }

    ByteSink sink;
    sink.put_pod(make_header(KMJ_KIND_SYSTEM));
    const uint64_t subhdr_at = sink.offset();
    KmjSystemHeader sub {};
    sink.put_pod(sub);

    sink.align8();
    sub.trie_offset = sink.offset();
    sub.trie_bytes = static_cast<uint64_t>(da.total_size());
    if (sub.trie_bytes > 0) {
        sink.put(da.array(), da.total_size());
    }

    sink.align8();
    sub.runs_offset = sink.offset();
    sub.runs_count = runs.size();
    if (!runs.empty()) {
        sink.put(runs.data(), runs.size() * sizeof(WordIdRun));
    }

    sink.align8();
    sub.entries_offset = sink.offset();
    sub.entries_count = entries.size();
    if (!entries.empty()) {
        sink.put(entries.data(), entries.size() * sizeof(WordEntry));
    }

    sink.align8();
    sub.features_offset = sink.offset();
    sub.features_bytes = features.size();
    if (!features.empty()) {
        sink.put(features.data(), features.size());
    }

    std::memcpy(sink.buf().data() + subhdr_at, &sub, sizeof(sub));
    return flush_file(path, sink.buf());
}

Status KuromojiDictionaryBuilder::write_matrix(const std::string& path, const MatrixInput& in) {
    if (in.cells.size() != static_cast<std::size_t>(in.forward_size) * in.backward_size) {
        return Status::InvalidArgument("kuromoji dict: matrix cell count mismatch");
    }
    ByteSink sink;
    sink.put_pod(make_header(KMJ_KIND_MATRIX));
    const uint64_t subhdr_at = sink.offset();
    KmjMatrixHeader sub {};
    sink.put_pod(sub);

    sink.align8();
    sub.forward_size = in.forward_size;
    sub.backward_size = in.backward_size;
    sub.cells_offset = sink.offset();
    if (!in.cells.empty()) {
        sink.put(in.cells.data(), in.cells.size() * sizeof(int16_t));
    }

    std::memcpy(sink.buf().data() + subhdr_at, &sub, sizeof(sub));
    return flush_file(path, sink.buf());
}

Status KuromojiDictionaryBuilder::write_chardef(const std::string& path, const CharDefInput& in) {
    ByteSink sink;
    sink.put_pod(make_header(KMJ_KIND_CHARDEF));
    const uint64_t subhdr_at = sink.offset();
    KmjCharDefHeader sub {};
    sink.put_pod(sub);

    sink.align8();
    sub.class_count = static_cast<uint32_t>(in.defs.size());
    sub.catmap_offset = sink.offset();
    sink.put(in.catmap.data(), in.catmap.size()); // exactly 0x10000 bytes

    sink.align8();
    sub.defs_offset = sink.offset();
    if (!in.defs.empty()) {
        sink.put(in.defs.data(), in.defs.size() * sizeof(CategoryDef));
    }

    std::memcpy(sink.buf().data() + subhdr_at, &sub, sizeof(sub));
    return flush_file(path, sink.buf());
}

Status KuromojiDictionaryBuilder::write_unkdict(const std::string& path, const UnkDictInput& in) {
    std::vector<WordIdRun> runs;
    std::vector<WordEntry> entries;
    std::vector<uint8_t> features;
    runs.reserve(in.per_category.size());
    for (const auto& words : in.per_category) {
        runs.push_back(append_words(words, entries, features));
    }

    ByteSink sink;
    sink.put_pod(make_header(KMJ_KIND_UNKDICT));
    const uint64_t subhdr_at = sink.offset();
    KmjUnkHeader sub {};
    sink.put_pod(sub);

    sink.align8();
    sub.class_count = static_cast<uint32_t>(runs.size());
    sub.runs_offset = sink.offset();
    if (!runs.empty()) {
        sink.put(runs.data(), runs.size() * sizeof(WordIdRun));
    }

    sink.align8();
    sub.entries_offset = sink.offset();
    sub.entries_count = entries.size();
    if (!entries.empty()) {
        sink.put(entries.data(), entries.size() * sizeof(WordEntry));
    }

    sink.align8();
    sub.features_offset = sink.offset();
    sub.features_bytes = features.size();
    if (!features.empty()) {
        sink.put(features.data(), features.size());
    }

    std::memcpy(sink.buf().data() + subhdr_at, &sub, sizeof(sub));
    return flush_file(path, sink.buf());
}

} // namespace doris::segment_v2::kuromoji

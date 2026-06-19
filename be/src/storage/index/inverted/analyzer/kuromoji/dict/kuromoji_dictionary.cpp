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

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <map>
#include <mutex>

#include "common/logging.h"

namespace doris::segment_v2::kuromoji {

MappedFile::~MappedFile() {
    if (_data != nullptr) {
        ::munmap(const_cast<uint8_t*>(_data), _size);
        _data = nullptr;
        _size = 0;
    }
}

Status MappedFile::open(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::IOError("kuromoji dict: cannot open {}", path);
    }
    struct stat st {};
    if (::fstat(fd, &st) != 0 || st.st_size <= 0) {
        ::close(fd);
        return Status::IOError("kuromoji dict: cannot stat {}", path);
    }
    auto bytes = static_cast<std::size_t>(st.st_size);
    void* m = ::mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    if (m == MAP_FAILED) {
        return Status::IOError("kuromoji dict: mmap failed for {}", path);
    }
    _data = static_cast<const uint8_t*>(m);
    _size = bytes;
    return Status::OK();
}

Status KuromojiDictionary::check_header(const uint8_t* p, std::size_t size, KmjFileKind kind) {
    if (size < sizeof(KmjFileHeader)) {
        return Status::Corruption("kuromoji dict: file too small");
    }
    KmjFileHeader h {};
    std::memcpy(&h, p, sizeof(h));
    if (std::memcmp(h.magic, KMJ_MAGIC, sizeof(h.magic)) != 0) {
        return Status::Corruption("kuromoji dict: bad magic");
    }
    if (h.format_version != KMJ_FORMAT_VERSION) {
        return Status::Corruption("kuromoji dict: version {} != {}", h.format_version,
                                  KMJ_FORMAT_VERSION);
    }
    if (h.file_kind != static_cast<uint32_t>(kind)) {
        return Status::Corruption("kuromoji dict: wrong file_kind {}", h.file_kind);
    }
    if (h.file_size != size) {
        return Status::Corruption("kuromoji dict: file_size {} != actual {}", h.file_size, size);
    }
    return Status::OK();
}

std::string_view KuromojiDictionary::feature_at(const uint8_t* blob, uint64_t blob_bytes,
                                                uint32_t off) {
    if (off == KMJ_NO_FEATURE || blob == nullptr || static_cast<uint64_t>(off) + 2 > blob_bytes) {
        return {};
    }
    auto len = static_cast<uint16_t>(static_cast<uint16_t>(blob[off]) |
                                     static_cast<uint16_t>(blob[off + 1] << 8));
    if (static_cast<uint64_t>(off) + 2 + len > blob_bytes) {
        return {};
    }
    return {reinterpret_cast<const char*>(blob + off + 2), len};
}

Status KuromojiDictionary::map_system(const std::string& path) {
    RETURN_IF_ERROR(_system_map.open(path));
    const uint8_t* p = _system_map.data();
    RETURN_IF_ERROR(check_header(p, _system_map.size(), KMJ_KIND_SYSTEM));
    KmjSystemHeader s {};
    std::memcpy(&s, p + sizeof(KmjFileHeader), sizeof(s));
    _runs = reinterpret_cast<const WordIdRun*>(p + s.runs_offset);
    _entries = reinterpret_cast<const WordEntry*>(p + s.entries_offset);
    _features = p + s.features_offset;
    _features_bytes = s.features_bytes;
    if (s.trie_bytes > 0) {
        // size is in 4-byte units; the mmap outlives _trie (both owned by this object).
        _trie.set_array(p + s.trie_offset, static_cast<std::size_t>(s.trie_bytes / 4));
    }
    return Status::OK();
}

Status KuromojiDictionary::map_matrix(const std::string& path) {
    RETURN_IF_ERROR(_matrix_map.open(path));
    const uint8_t* p = _matrix_map.data();
    RETURN_IF_ERROR(check_header(p, _matrix_map.size(), KMJ_KIND_MATRIX));
    KmjMatrixHeader m {};
    std::memcpy(&m, p + sizeof(KmjFileHeader), sizeof(m));
    _forward_size = m.forward_size;
    _cells = reinterpret_cast<const int16_t*>(p + m.cells_offset);
    return Status::OK();
}

Status KuromojiDictionary::map_chardef(const std::string& path) {
    RETURN_IF_ERROR(_chardef_map.open(path));
    const uint8_t* p = _chardef_map.data();
    RETURN_IF_ERROR(check_header(p, _chardef_map.size(), KMJ_KIND_CHARDEF));
    KmjCharDefHeader c {};
    std::memcpy(&c, p + sizeof(KmjFileHeader), sizeof(c));
    _catmap = p + c.catmap_offset;
    _defs = reinterpret_cast<const CategoryDef*>(p + c.defs_offset);
    return Status::OK();
}

Status KuromojiDictionary::map_unkdict(const std::string& path) {
    RETURN_IF_ERROR(_unk_map.open(path));
    const uint8_t* p = _unk_map.data();
    RETURN_IF_ERROR(check_header(p, _unk_map.size(), KMJ_KIND_UNKDICT));
    KmjUnkHeader u {};
    std::memcpy(&u, p + sizeof(KmjFileHeader), sizeof(u));
    _unk_runs = reinterpret_cast<const WordIdRun*>(p + u.runs_offset);
    _unk_entries = reinterpret_cast<const WordEntry*>(p + u.entries_offset);
    _unk_features = p + u.features_offset;
    _unk_features_bytes = u.features_bytes;
    return Status::OK();
}

Status KuromojiDictionary::load(const std::string& dir, std::unique_ptr<KuromojiDictionary>* out) {
    auto dict = std::make_unique<KuromojiDictionary>();
    RETURN_IF_ERROR(dict->map_system(dir + "/system.bin"));
    RETURN_IF_ERROR(dict->map_matrix(dir + "/matrix.bin"));
    RETURN_IF_ERROR(dict->map_chardef(dir + "/chardef.bin"));
    RETURN_IF_ERROR(dict->map_unkdict(dir + "/unkdict.bin"));
    *out = std::move(dict);
    return Status::OK();
}

const KuromojiDictionary* KuromojiDictionary::get_or_load(const std::string& dir) {
    static std::mutex mu;
    static std::map<std::string, std::unique_ptr<KuromojiDictionary>> cache;
    std::lock_guard<std::mutex> lock(mu);
    auto it = cache.find(dir);
    if (it != cache.end()) {
        return it->second.get(); // may be nullptr if a prior load failed
    }
    std::unique_ptr<KuromojiDictionary> dict;
    Status st = load(dir, &dict);
    if (!st.ok()) {
        LOG(WARNING) << "kuromoji: failed to load dictionary from " << dir << ": " << st;
        cache.emplace(dir, nullptr);
        return nullptr;
    }
    const KuromojiDictionary* ptr = dict.get();
    cache.emplace(dir, std::move(dict));
    return ptr;
}

void KuromojiDictionary::common_prefix_search(const char* text, std::size_t len,
                                              std::vector<PrefixMatch>* out) const {
    out->clear();
    constexpr std::size_t kBatch = 64;
    Darts::DoubleArray::result_pair_type results[kBatch];
    std::size_t n = _trie.commonPrefixSearch(text, results, kBatch, len);
    if (n > kBatch) {
        // Rare: more prefix matches than the stack buffer; re-query with an exact buffer.
        std::vector<Darts::DoubleArray::result_pair_type> big(n);
        std::size_t m = _trie.commonPrefixSearch(text, big.data(), n, len);
        std::size_t take = m < n ? m : n;
        for (std::size_t i = 0; i < take; ++i) {
            out->push_back({static_cast<uint32_t>(big[i].value),
                            static_cast<uint32_t>(big[i].length)});
        }
        return;
    }
    for (std::size_t i = 0; i < n; ++i) {
        out->push_back(
                {static_cast<uint32_t>(results[i].value), static_cast<uint32_t>(results[i].length)});
    }
}

} // namespace doris::segment_v2::kuromoji

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
#include <limits>
#include <map>
#include <mutex>

#include "common/logging.h"

namespace doris::segment_v2::inverted_index::kuromoji {

namespace {
Status check_region(const char* what, uint64_t offset, uint64_t count, uint64_t elem,
                    uint64_t min_offset, std::size_t size) {
    if (elem != 0 && count > std::numeric_limits<uint64_t>::max() / elem) {
        return Status::Corruption("kuromoji dict: {} count overflow ({} x {})", what, count, elem);
    }
    const uint64_t bytes = count * elem;
    if (offset < min_offset || offset > size || bytes > static_cast<uint64_t>(size) - offset) {
        return Status::Corruption("kuromoji dict: {} out of range (offset={}, bytes={}, file={})",
                                  what, offset, bytes, size);
    }
    return Status::OK();
}
} // namespace

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
    const std::size_t size = _system_map.size();
    RETURN_IF_ERROR(check_header(p, size, KMJ_KIND_SYSTEM));
    constexpr uint64_t kHdrEnd = sizeof(KmjFileHeader) + sizeof(KmjSystemHeader);
    if (size < kHdrEnd) {
        return Status::Corruption("kuromoji dict: system.bin truncated sub-header");
    }
    KmjSystemHeader s {};
    std::memcpy(&s, p + sizeof(KmjFileHeader), sizeof(s));
    // The trie is read as 4-byte Darts units, so both offset and length must be
    // 4-byte aligned/sized before set_array() walks them.
    if (s.trie_offset % 4 != 0 || s.trie_bytes % 4 != 0) {
        return Status::Corruption("kuromoji dict: trie not 4-byte aligned");
    }
    RETURN_IF_ERROR(check_region("system trie", s.trie_offset, s.trie_bytes, 1, kHdrEnd, size));
    RETURN_IF_ERROR(check_region("system runs", s.runs_offset, s.runs_count, sizeof(WordIdRun),
                                 kHdrEnd, size));
    RETURN_IF_ERROR(check_region("system entries", s.entries_offset, s.entries_count,
                                 sizeof(WordEntry), kHdrEnd, size));
    RETURN_IF_ERROR(
            check_region("system features", s.features_offset, s.features_bytes, 1, kHdrEnd, size));
    _runs = reinterpret_cast<const WordIdRun*>(p + s.runs_offset);
    _runs_count = s.runs_count;
    _entries = reinterpret_cast<const WordEntry*>(p + s.entries_offset);
    _entries_count = s.entries_count;
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
    const std::size_t size = _matrix_map.size();
    RETURN_IF_ERROR(check_header(p, size, KMJ_KIND_MATRIX));
    constexpr uint64_t kHdrEnd = sizeof(KmjFileHeader) + sizeof(KmjMatrixHeader);
    if (size < kHdrEnd) {
        return Status::Corruption("kuromoji dict: matrix.bin truncated sub-header");
    }
    KmjMatrixHeader m {};
    std::memcpy(&m, p + sizeof(KmjFileHeader), sizeof(m));
    if (m.forward_size == 0 || m.backward_size == 0) {
        return Status::Corruption("kuromoji dict: matrix has a zero dimension");
    }
    const uint64_t cells = static_cast<uint64_t>(m.forward_size) * m.backward_size;
    RETURN_IF_ERROR(
            check_region("matrix cells", m.cells_offset, cells, sizeof(int16_t), kHdrEnd, size));
    _forward_size = m.forward_size;
    _backward_size = m.backward_size;
    _cells = reinterpret_cast<const int16_t*>(p + m.cells_offset);
    return Status::OK();
}

Status KuromojiDictionary::map_chardef(const std::string& path) {
    RETURN_IF_ERROR(_chardef_map.open(path));
    const uint8_t* p = _chardef_map.data();
    const std::size_t size = _chardef_map.size();
    RETURN_IF_ERROR(check_header(p, size, KMJ_KIND_CHARDEF));
    constexpr uint64_t kHdrEnd = sizeof(KmjFileHeader) + sizeof(KmjCharDefHeader);
    if (size < kHdrEnd) {
        return Status::Corruption("kuromoji dict: chardef.bin truncated sub-header");
    }
    KmjCharDefHeader c {};
    std::memcpy(&c, p + sizeof(KmjFileHeader), sizeof(c));
    if (c.class_count != CAT_CLASS_COUNT) {
        return Status::Corruption("kuromoji dict: chardef class_count {} != {}", c.class_count,
                                  static_cast<uint32_t>(CAT_CLASS_COUNT));
    }
    // catmap is exactly one byte per BMP code point.
    RETURN_IF_ERROR(check_region("chardef catmap", c.catmap_offset, 0x10000, 1, kHdrEnd, size));
    RETURN_IF_ERROR(check_region("chardef defs", c.defs_offset, c.class_count, sizeof(CategoryDef),
                                 kHdrEnd, size));
    _catmap = p + c.catmap_offset;
    _defs = reinterpret_cast<const CategoryDef*>(p + c.defs_offset);
    return Status::OK();
}

Status KuromojiDictionary::map_unkdict(const std::string& path) {
    RETURN_IF_ERROR(_unk_map.open(path));
    const uint8_t* p = _unk_map.data();
    const std::size_t size = _unk_map.size();
    RETURN_IF_ERROR(check_header(p, size, KMJ_KIND_UNKDICT));
    constexpr uint64_t kHdrEnd = sizeof(KmjFileHeader) + sizeof(KmjUnkHeader);
    if (size < kHdrEnd) {
        return Status::Corruption("kuromoji dict: unkdict.bin truncated sub-header");
    }
    KmjUnkHeader u {};
    std::memcpy(&u, p + sizeof(KmjFileHeader), sizeof(u));
    if (u.class_count != CAT_CLASS_COUNT) {
        return Status::Corruption("kuromoji dict: unkdict class_count {} != {}", u.class_count,
                                  static_cast<uint32_t>(CAT_CLASS_COUNT));
    }
    RETURN_IF_ERROR(check_region("unk runs", u.runs_offset, u.class_count, sizeof(WordIdRun),
                                 kHdrEnd, size));
    RETURN_IF_ERROR(check_region("unk entries", u.entries_offset, u.entries_count,
                                 sizeof(WordEntry), kHdrEnd, size));
    RETURN_IF_ERROR(
            check_region("unk features", u.features_offset, u.features_bytes, 1, kHdrEnd, size));
    _unk_runs = reinterpret_cast<const WordIdRun*>(p + u.runs_offset);
    _unk_runs_count = u.class_count;
    _unk_entries = reinterpret_cast<const WordEntry*>(p + u.entries_offset);
    _unk_entries_count = u.entries_count;
    _unk_features = p + u.features_offset;
    _unk_features_bytes = u.features_bytes;
    return Status::OK();
}

Status KuromojiDictionary::validate_ranges() const {
    // Every run must reference a valid [entry_start, entry_start + count) slice.
    auto check_runs = [](const WordIdRun* runs, uint64_t run_count, uint64_t entries_count,
                         const char* what) -> Status {
        for (uint64_t i = 0; i < run_count; ++i) {
            if (static_cast<uint64_t>(runs[i].entry_start) + runs[i].count > entries_count) {
                return Status::Corruption(
                        "kuromoji dict: {} run {} references entries past the end", what, i);
            }
        }
        return Status::OK();
    };
    // Every entry's context ids must index the connection matrix (used directly
    // as offsets into _cells at query time).
    auto check_entries = [this](const WordEntry* entries, uint64_t count,
                                const char* what) -> Status {
        for (uint64_t i = 0; i < count; ++i) {
            const WordEntry& e = entries[i];
            if (e.left_id < 0 || static_cast<uint32_t>(e.left_id) >= _backward_size ||
                e.right_id < 0 || static_cast<uint32_t>(e.right_id) >= _forward_size) {
                return Status::Corruption("kuromoji dict: {} entry {} has out-of-range context id",
                                          what, i);
            }
        }
        return Status::OK();
    };
    RETURN_IF_ERROR(check_runs(_runs, _runs_count, _entries_count, "system"));
    RETURN_IF_ERROR(check_entries(_entries, _entries_count, "system"));
    RETURN_IF_ERROR(check_runs(_unk_runs, _unk_runs_count, _unk_entries_count, "unk"));
    RETURN_IF_ERROR(check_entries(_unk_entries, _unk_entries_count, "unk"));
    return Status::OK();
}

Status KuromojiDictionary::load(const std::string& dir, std::unique_ptr<KuromojiDictionary>* out) {
    auto dict = std::make_unique<KuromojiDictionary>();
    RETURN_IF_ERROR(dict->map_system(dir + "/system.bin"));
    RETURN_IF_ERROR(dict->map_matrix(dir + "/matrix.bin"));
    RETURN_IF_ERROR(dict->map_chardef(dir + "/chardef.bin"));
    RETURN_IF_ERROR(dict->map_unkdict(dir + "/unkdict.bin"));
    // Cross-file checks need every file mapped (entries vs. matrix bounds).
    RETURN_IF_ERROR(dict->validate_ranges());
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
            out->push_back(
                    {static_cast<uint32_t>(big[i].value), static_cast<uint32_t>(big[i].length)});
        }
        return;
    }
    for (std::size_t i = 0; i < n; ++i) {
        out->push_back({static_cast<uint32_t>(results[i].value),
                        static_cast<uint32_t>(results[i].length)});
    }
}

} // namespace doris::segment_v2::inverted_index::kuromoji

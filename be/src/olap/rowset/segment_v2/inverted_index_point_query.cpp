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

#include "olap/rowset/segment_v2/inverted_index_point_query.h"

#include <cstring>

#include "io/io_common.h"

namespace doris::segment_v2 {

// FNV-1a 64-bit hash
uint64_t TpqIndexReader::_hash_term(const std::string& term) {
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : term) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    // Ensure hash is never 0 (0 means empty slot)
    return hash == 0 ? 1 : hash;
}

// ---- Writer ----

void TpqIndexWriter::add_term(const std::string& term, const roaring::Roaring& docids) {
    Entry entry;
    entry.term = term;
    uint32_t size = static_cast<uint32_t>(docids.getSizeInBytes());
    entry.serialized_bitmap.resize(size);
    docids.write(entry.serialized_bitmap.data());

    if (_entries.empty()) {
        _min_term = term;
        _max_term = term;
    } else {
        if (term < _min_term) _min_term = term;
        if (term > _max_term) _max_term = term;
    }
    _entries.push_back(std::move(entry));
}

Status TpqIndexWriter::finalize(io::FileWriter* file_writer) {
    if (_entries.empty()) {
        return Status::OK();
    }

    // Buffer the data region in memory so we can compute offsets before writing.
    // Layout: [Header][Data Region][Hash Table]
    std::string data_region;
    std::vector<std::pair<uint64_t, uint64_t>> hash_and_offsets; // (hash, data_offset)

    for (const auto& entry : _entries) {
        uint64_t entry_offset = TPQ_HEADER_SIZE + data_region.size();
        uint64_t hash = TpqIndexReader::_hash_term(entry.term);
        hash_and_offsets.push_back({hash, entry_offset});

        uint32_t term_len = static_cast<uint32_t>(entry.term.size());
        uint32_t bitmap_len = static_cast<uint32_t>(entry.serialized_bitmap.size());

        data_region.append(reinterpret_cast<const char*>(&term_len), sizeof(term_len));
        data_region.append(entry.term);
        data_region.append(reinterpret_cast<const char*>(&bitmap_len), sizeof(bitmap_len));
        data_region.append(entry.serialized_bitmap);
    }

    // Build hash table
    size_t entry_count = _entries.size();
    if (entry_count > static_cast<size_t>(UINT32_MAX / 2)) {
        return Status::InternalError("TPQ too many entries to write: {}", entry_count);
    }
    uint32_t num_entries = static_cast<uint32_t>(entry_count);
    uint32_t num_slots = num_entries * 2;
    if (num_slots < 16) num_slots = 16;

    std::vector<TpqHashSlot> hash_table(num_slots, TpqHashSlot {0, 0});
    for (const auto& [hash, offset] : hash_and_offsets) {
        uint32_t slot = static_cast<uint32_t>(hash % num_slots);
        uint32_t probes = 0;
        while (hash_table[slot].hash != 0 && probes < num_slots) {
            slot = (slot + 1) % num_slots;
            ++probes;
        }
        if (probes >= num_slots) {
            return Status::InternalError("TPQ hash table full, cannot insert term");
        }
        hash_table[slot].hash = hash;
        hash_table[slot].data_offset = offset;
    }

    // Now we know all offsets — build the header with correct values
    TpqHeader header;
    header.num_entries = num_entries;
    header.data_region_offset = TPQ_HEADER_SIZE;
    header.hash_table_offset = TPQ_HEADER_SIZE + data_region.size();

    char header_buf[TPQ_HEADER_SIZE];
    memcpy(header_buf, &header.magic, 4);
    memcpy(header_buf + 4, &header.version, 4);
    memcpy(header_buf + 8, &header.num_entries, 4);
    memcpy(header_buf + 12, &header.hash_table_offset, 8);
    memcpy(header_buf + 20, &header.data_region_offset, 8);

    // Write everything sequentially: header, data region, hash table
    RETURN_IF_ERROR(file_writer->append({header_buf, TPQ_HEADER_SIZE}));
    RETURN_IF_ERROR(file_writer->append({data_region.data(), data_region.size()}));
    RETURN_IF_ERROR(file_writer->append(
            {reinterpret_cast<const char*>(hash_table.data()),
             hash_table.size() * sizeof(TpqHashSlot)}));

    return Status::OK();
}

// ---- Reader ----

Status TpqIndexReader::open(io::FileReaderSPtr file_reader) {
    _file_reader = file_reader;

    if (_file_reader->size() < TPQ_HEADER_SIZE) {
        return Status::Corruption("TPQ file too small: {}", _file_reader->size());
    }

    // Read header
    char header_buf[TPQ_HEADER_SIZE];
    size_t bytes_read = 0;
    io::IOContext io_ctx;
    RETURN_IF_ERROR(_file_reader->read_at(0, {header_buf, TPQ_HEADER_SIZE}, &bytes_read, &io_ctx));
    if (bytes_read != TPQ_HEADER_SIZE) {
        return Status::Corruption("TPQ header short read: expected {}, got {}",
                                  TPQ_HEADER_SIZE, bytes_read);
    }

    memcpy(&_header.magic, header_buf, 4);
    memcpy(&_header.version, header_buf + 4, 4);
    memcpy(&_header.num_entries, header_buf + 8, 4);
    memcpy(&_header.hash_table_offset, header_buf + 12, 8);
    memcpy(&_header.data_region_offset, header_buf + 20, 8);

    if (_header.magic != TPQ_MAGIC) {
        return Status::Corruption("Invalid TPQ magic: {}", _header.magic);
    }
    if (_header.version != TPQ_VERSION) {
        return Status::Corruption("Unsupported TPQ version: {}", _header.version);
    }

    static constexpr uint32_t MAX_NUM_ENTRIES = 1u << 30; // ~1 billion entries
    if (_header.num_entries > MAX_NUM_ENTRIES) {
        return Status::Corruption("TPQ num_entries too large: {}", _header.num_entries);
    }

    // Use uint64_t for the multiplication to avoid uint32_t overflow
    uint64_t num_slots_64 = static_cast<uint64_t>(_header.num_entries) * 2;
    if (num_slots_64 < 16) num_slots_64 = 16;
    uint32_t num_slots = static_cast<uint32_t>(num_slots_64);

    size_t hash_table_size = static_cast<size_t>(num_slots) * sizeof(TpqHashSlot);
    size_t file_size = _file_reader->size();
    if (_header.hash_table_offset > file_size ||
        hash_table_size > file_size - _header.hash_table_offset) {
        return Status::Corruption(
                "TPQ hash table region [{}, +{}) exceeds file size {}",
                _header.hash_table_offset, hash_table_size, file_size);
    }

    _hash_table.resize(num_slots);
    RETURN_IF_ERROR(_file_reader->read_at(
            _header.hash_table_offset,
            {reinterpret_cast<char*>(_hash_table.data()), hash_table_size}, &bytes_read, &io_ctx));
    if (bytes_read != hash_table_size) {
        return Status::Corruption("TPQ hash table short read: expected {}, got {}",
                                  hash_table_size, bytes_read);
    }

    _is_open = true;
    return Status::OK();
}

Status TpqIndexReader::lookup(const std::string& term,
                              std::shared_ptr<roaring::Roaring>& result) {
    if (!_is_open || _header.num_entries == 0) {
        result = std::make_shared<roaring::Roaring>();
        return Status::OK();
    }

    uint64_t hash = _hash_term(term);
    uint32_t num_slots = static_cast<uint32_t>(_hash_table.size());
    uint32_t slot = static_cast<uint32_t>(hash % num_slots);
    uint32_t probes = 0;
    io::IOContext io_ctx;

    static constexpr uint32_t MAX_TERM_LEN = 1u << 20;    // 1 MB
    static constexpr uint32_t MAX_BITMAP_LEN = 1u << 28;  // 256 MB
    size_t file_size = _file_reader->size();

    while (_hash_table[slot].hash != 0 && probes < num_slots) {
        if (_hash_table[slot].hash == hash) {
            uint64_t offset = _hash_table[slot].data_offset;

            char len_buf[4];
            size_t bytes_read = 0;
            RETURN_IF_ERROR(_file_reader->read_at(offset, {len_buf, 4}, &bytes_read, &io_ctx));
            if (bytes_read != 4) {
                return Status::Corruption("TPQ short read for term_len at offset {}", offset);
            }
            uint32_t term_len;
            memcpy(&term_len, len_buf, 4);
            if (term_len > MAX_TERM_LEN || offset > file_size ||
                file_size - offset < static_cast<uint64_t>(4) + term_len) {
                return Status::Corruption("TPQ invalid term_len {} at offset {}", term_len, offset);
            }

            std::string stored_term(term_len, '\0');
            RETURN_IF_ERROR(_file_reader->read_at(offset + 4, {stored_term.data(), term_len},
                                                  &bytes_read, &io_ctx));
            if (bytes_read != term_len) {
                return Status::Corruption("TPQ short read for term data at offset {}", offset + 4);
            }

            if (stored_term == term) {
                uint64_t bitmap_len_offset = offset + 4 + term_len;
                RETURN_IF_ERROR(_file_reader->read_at(bitmap_len_offset, {len_buf, 4}, &bytes_read,
                                                      &io_ctx));
                if (bytes_read != 4) {
                    return Status::Corruption("TPQ short read for bitmap_len at offset {}",
                                              bitmap_len_offset);
                }
                uint32_t bitmap_len;
                memcpy(&bitmap_len, len_buf, 4);
                if (bitmap_len > MAX_BITMAP_LEN || bitmap_len_offset > file_size ||
                    file_size - bitmap_len_offset < static_cast<uint64_t>(4) + bitmap_len) {
                    return Status::Corruption("TPQ invalid bitmap_len {} at offset {}", bitmap_len,
                                              bitmap_len_offset);
                }

                std::string bitmap_data(bitmap_len, '\0');
                RETURN_IF_ERROR(_file_reader->read_at(bitmap_len_offset + 4,
                                                      {bitmap_data.data(), bitmap_len},
                                                      &bytes_read, &io_ctx));
                if (bytes_read != bitmap_len) {
                    return Status::Corruption("TPQ short read for bitmap data at offset {}",
                                              bitmap_len_offset + 4);
                }

                result = std::make_shared<roaring::Roaring>(
                        roaring::Roaring::readSafe(bitmap_data.data(), bitmap_len));
                return Status::OK();
            }
        }
        slot = (slot + 1) % num_slots;
        ++probes;
    }

    result = std::make_shared<roaring::Roaring>();
    return Status::OK();
}

} // namespace doris::segment_v2

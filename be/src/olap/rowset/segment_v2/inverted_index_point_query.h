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

#pragma once

#include <roaring/roaring.hh>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"

namespace doris::segment_v2 {

// .tpq (Term Point Query) file format:
//
// [Header]
//   magic: 4 bytes ("TPQ\0")
//   version: 4 bytes (1)
//   num_entries: 4 bytes
//   hash_table_offset: 8 bytes
//   data_region_offset: 8 bytes
//
// [Data Region] - variable length entries
//   For each term:
//     term_length: 4 bytes
//     term_data: term_length bytes
//     bitmap_size: 4 bytes
//     bitmap_data: bitmap_size bytes (serialized roaring bitmap)
//
// [Hash Table] - fixed size array
//   For each slot (num_slots = num_entries * 2 for ~50% load factor):
//     hash: 8 bytes (0 = empty)
//     data_offset: 8 bytes (offset into data region)

static constexpr uint32_t TPQ_MAGIC = 0x00515054; // "TPQ\0"
static constexpr uint32_t TPQ_VERSION = 1;
static constexpr size_t TPQ_HEADER_SIZE = 28;

struct __attribute__((packed)) TpqHeader {
    uint32_t magic = TPQ_MAGIC;
    uint32_t version = TPQ_VERSION;
    uint32_t num_entries = 0;
    uint64_t hash_table_offset = 0;
    uint64_t data_region_offset = 0;
};

static_assert(sizeof(TpqHeader) == TPQ_HEADER_SIZE,
              "TpqHeader must match the on-disk TPQ_HEADER_SIZE");

struct TpqHashSlot {
    uint64_t hash = 0;
    uint64_t data_offset = 0;
};

// Writer: builds a .tpq file from a set of term -> roaring bitmap mappings.
class TpqIndexWriter {
public:
    void add_term(const std::string& term, const roaring::Roaring& docids);
    Status finalize(io::FileWriter* file_writer);

    size_t term_count() const { return _entries.size(); }
    const std::string& min_term() const { return _min_term; }
    const std::string& max_term() const { return _max_term; }

private:
    struct Entry {
        std::string term;
        std::string serialized_bitmap;
    };
    std::vector<Entry> _entries;
    std::string _min_term;
    std::string _max_term;
};

// Reader: loads and queries a .tpq file.
// The hash table is loaded into memory; bitmap data is read on demand.
class TpqIndexReader {
public:
    Status open(io::FileReaderSPtr file_reader);

    // Lookup a term and return its docid bitmap. Returns empty bitmap if not found.
    Status lookup(const std::string& term, std::shared_ptr<roaring::Roaring>& result);

    bool is_open() const { return _is_open; }

private:
    static uint64_t _hash_term(const std::string& term);

    io::FileReaderSPtr _file_reader;
    TpqHeader _header;
    std::vector<TpqHashSlot> _hash_table;
    bool _is_open = false;
};

} // namespace doris::segment_v2

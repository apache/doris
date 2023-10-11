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

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
class TypeInfo;

namespace io {
class FileWriter;
} // namespace io
namespace segment_v2 {
class PrimaryKeyIndexMetaPB;
} // namespace segment_v2

// Build index for primary key.
// The primary key index is designed in a similar way like RocksDB
// Partitioned Index, which is created in the segment file when MemTable flushes.
// Index is stored in multiple pages to leverage the IndexedColumnWriter.
//
// NOTE: for now, it's only used when unique key merge-on-write property enabled.
class PrimaryKeyIndexBuilder {
public:
    PrimaryKeyIndexBuilder(io::FileWriter* file_writer, size_t seq_col_length)
            : _file_writer(file_writer),
              _num_rows(0),
              _size(0),
              _disk_size(0),
              _seq_col_length(seq_col_length) {}

    ~PrimaryKeyIndexBuilder();

    Status init();

    Status add_item(const Slice& key);

    uint32_t num_rows() const { return _num_rows; }

    uint64_t size() const { return _size; }

    uint64_t disk_size() const { return _disk_size; }

    Slice min_key() { return Slice(_min_key.data(), _min_key.size() - _seq_col_length); }
    Slice max_key() { return Slice(_max_key.data(), _max_key.size() - _seq_col_length); }

    Status finalize(segment_v2::PrimaryKeyIndexMetaPB* meta);

    void update_mem_tracker();

private:
    io::FileWriter* _file_writer = nullptr;
    uint32_t _num_rows;
    uint64_t _size;
    uint64_t _disk_size;
    size_t _seq_col_length;

    faststring _min_key;
    faststring _max_key;
    std::unique_ptr<segment_v2::IndexedColumnWriter> _primary_key_index_builder;
    std::unique_ptr<segment_v2::BloomFilterIndexWriter> _bloom_filter_index_builder;
    // bloom filter index mem tracker
    std::unique_ptr<MemTracker> _bloom_filter_index_mem_tracker;
};

class PrimaryKeyIndexReader {
public:
    PrimaryKeyIndexReader() : _index_parsed(false), _bf_parsed(false) {}

    Status parse_index(io::FileReaderSPtr file_reader,
                       const segment_v2::PrimaryKeyIndexMetaPB& meta);

    Status parse_bf(io::FileReaderSPtr file_reader, const segment_v2::PrimaryKeyIndexMetaPB& meta);

    Status new_iterator(std::unique_ptr<segment_v2::IndexedColumnIterator>* index_iterator) const {
        DCHECK(_index_parsed);
        index_iterator->reset(new segment_v2::IndexedColumnIterator(_index_reader.get()));
        return Status::OK();
    }

    const TypeInfo* type_info() const {
        DCHECK(_index_parsed);
        return _index_reader->type_info();
    }

    // verify whether exist in BloomFilter
    bool check_present(const Slice& key) {
        DCHECK(_bf_parsed);
        return _bf->test_bytes(key.data, key.size);
    }

    uint32_t num_rows() const {
        DCHECK(_index_parsed);
        return _index_reader->num_values();
    }

    uint64_t get_bf_memory_size() {
        DCHECK(_bf_parsed);
        return _bf->size();
    }

    uint64_t get_memory_size() {
        DCHECK(_index_parsed);
        return _index_reader->get_memory_size();
    }

private:
    bool _index_parsed;
    bool _bf_parsed;
    std::unique_ptr<segment_v2::IndexedColumnReader> _index_reader;
    std::unique_ptr<segment_v2::BloomFilter> _bf;
};

} // namespace doris

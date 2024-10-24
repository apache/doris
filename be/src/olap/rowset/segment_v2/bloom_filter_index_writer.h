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

#include <butil/macros.h>
#include <stdint.h>

#include <cstddef>
#include <memory>
#include <vector>

#include "common/status.h"
#include "olap/itoken_extractor.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "util/slice.h"
#include "vec/common/arena.h"

namespace doris {

class TypeInfo;

namespace io {
class FileWriter;
}

namespace segment_v2 {

class ColumnIndexMetaPB;

class BloomFilterIndexWriter {
public:
    static Status create(const BloomFilterOptions& bf_options, const TypeInfo* typeinfo,
                         std::unique_ptr<BloomFilterIndexWriter>* res);

    BloomFilterIndexWriter() = default;
    virtual ~BloomFilterIndexWriter() = default;

    virtual Status add_values(const void* values, size_t count) = 0;

    virtual void add_nulls(uint32_t count) = 0;

    virtual Status flush() = 0;

    virtual Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* index_meta) = 0;

    virtual uint64_t size() = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(BloomFilterIndexWriter);
};

// For unique key with merge on write, the data for each segment is deduplicated.
// Bloom filter doesn't need to use `set` for deduplication like
// `BloomFilterIndexWriterImpl`, so vector can be used to accelerate.
class PrimaryKeyBloomFilterIndexWriterImpl : public BloomFilterIndexWriter {
public:
    explicit PrimaryKeyBloomFilterIndexWriterImpl(const BloomFilterOptions& bf_options,
                                                  const TypeInfo* type_info)
            : _bf_options(bf_options),
              _type_info(type_info),
              _has_null(false),
              _bf_buffer_size(0) {}

    ~PrimaryKeyBloomFilterIndexWriterImpl() override {
        for (auto& bf : _bfs) {
            g_pk_total_bloom_filter_num << -1;
            g_pk_total_bloom_filter_total_bytes << -static_cast<int64_t>(bf->size());
            g_pk_write_bloom_filter_decrease_num << 1;
            g_pk_write_bloom_filter_decrease_bytes << bf->size();
        }
    };

    // This method may allocate large memory for bf, will return error
    // when memory is exhaused to prevent oom.
    Status add_values(const void* values, size_t count) override;

    void add_nulls(uint32_t count) override { _has_null = true; }

    Status flush() override;

    Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* index_meta) override;

    uint64_t size() override;

private:
    BloomFilterOptions _bf_options;
    const TypeInfo* _type_info = nullptr;
    vectorized::Arena _arena;
    bool _has_null;
    uint64_t _bf_buffer_size;
    // distinct values
    std::vector<Slice> _values;
    std::vector<std::unique_ptr<BloomFilter>> _bfs;
};

class NGramBloomFilterIndexWriterImpl : public BloomFilterIndexWriter {
public:
    static Status create(const BloomFilterOptions& bf_options, const TypeInfo* typeinfo,
                         uint8_t gram_size, uint16_t gram_bf_size,
                         std::unique_ptr<BloomFilterIndexWriter>* res);

    NGramBloomFilterIndexWriterImpl(const BloomFilterOptions& bf_options, uint8_t gram_size,
                                    uint16_t bf_size);
    Status add_values(const void* values, size_t count) override;
    void add_nulls(uint32_t) override {}
    Status flush() override;
    Status finish(io::FileWriter* file_writer, ColumnIndexMetaPB* index_meta) override;
    uint64_t size() override;

private:
    BloomFilterOptions _bf_options;
    uint8_t _gram_size;
    uint16_t _bf_size;
    vectorized::Arena _arena;
    uint64_t _bf_buffer_size;
    NgramTokenExtractor _token_extractor;
    std::unique_ptr<BloomFilter> _bf;
    std::vector<std::unique_ptr<BloomFilter>> _bfs;
};

} // namespace segment_v2
} // namespace doris

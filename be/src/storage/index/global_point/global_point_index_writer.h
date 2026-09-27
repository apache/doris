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

#include <gen_cpp/olap_file.pb.h>
#include <stdint.h>

#include <memory>
#include <mutex>
#include <optional>

#include "common/status.h"
#include "storage/field_type.h"
#include "storage/index/bloom_filter/bloom_filter.h"
#include "storage/index/global_point/global_point_index_format.h"
#include "storage/types.h"
#include "util/slice.h"

namespace doris {

namespace io {
class FileWriter;
}

namespace segment_v2 {

// Sizing of one GLOBAL_POINT bloom filter.
//
// The fpp in the index DDL is a budget for a whole tablet: a query probes every bloom of the
// tablet, and the tablet is kept if any of them answers "maybe". With B blooms of fpp p each, the
// tablet's false-positive rate is about B * p. So each bloom gets
// p = tablet_fpp / config::global_point_index_expected_blooms_per_tablet.
//
// Every writer of the same rowset and column must derive its bloom from this one function: a
// memtable-on-sink-node load builds partial blooms on the sender and ORs them on the receiver,
// which only works when both sides picked the same size.
struct GlobalPointIndexSizing {
    // Bloom body size in bytes, always a power of two. BloomFilter::init() adds one more byte for
    // the has-null flag.
    uint64_t bloom_bytes = 0;
    double per_bloom_fpp = 0;
};

// `exact_row_count` is known on the compaction and index-build paths, where the rowset is sized
// exactly. On the load path it is empty: the bloom is sized from
// config::global_point_index_write_path_estimated_rows and capped by
// config::global_point_index_max_write_path_bloom_bytes. An underestimate only raises the fpp; it
// can never cause a false negative.
GlobalPointIndexSizing compute_global_point_index_sizing(double per_tablet_fpp,
                                                         std::optional<int64_t> exact_row_count);

// Result of the sizing self-check on a written bloom, judged from its descriptor alone.
enum class GlobalPointIndexHealth {
    OK,
    // Far fewer bits per key than the bloom's own fpp needs. The bloom saturates and answers
    // "maybe" for everything, so pruning silently stops working while results stay correct.
    UNDERSIZED,
    // The descriptor records zero inserted values. Either the bloom was never fed (a write-path
    // bug), or the indexed column is all NULL in this rowset, which is legitimate: nulls are not
    // inserted or counted. The descriptor cannot tell these apart.
    EMPTY,
};

// `bloom_bytes`, `inserted_rows` and `fpp` are ColumnPointIndexPB::size, ::total_rows and ::fpp
// (the per-bloom fpp). The expected bits per key are derived from `fpp`, since fpp is a DDL
// property in [1e-6, 0.5] and a correct bloom at fpp 0.3 carries far fewer bits than one at 0.01.
// UNDERSIZED is reported only when the bloom is more than `slack_percent` below that; a
// non-positive `slack_percent` disables it.
GlobalPointIndexHealth check_global_point_index_health(int64_t bloom_bytes, int64_t inserted_rows,
                                                       double fpp, int32_t slack_percent);

// Bits per key of a classic bloom filter at `fpp`: -ln(fpp) / (ln 2)^2.
double expected_bits_per_key(double fpp);

// Writes a .gpidx file (header + body) and fills `index_meta`, except index_file_suffix, which is
// owned by the caller's path convention.
Status write_global_point_index_file(io::FileWriter* file_writer, const char* body,
                                     size_t body_size, int32_t column_unique_id, int64_t index_id,
                                     double bloom_fpp, int64_t total_rows,
                                     ColumnPointIndexPB* index_meta);

// Builds the bloom filter of one GLOBAL_POINT-indexed column for one rowset.
//
// Unlike the per-segment bloom filter index, it is owned by the rowset writer and covers all
// segments of the rowset, so it outlives every ColumnWriter. Segments of one rowset may flush
// concurrently, so adding values is thread-safe.
class GlobalPointIndexBuilder {
public:
    GlobalPointIndexBuilder(int32_t column_unique_id, int64_t index_id)
            : _column_unique_id(column_unique_id), _index_id(index_id) {}

    // Must be called once, before adding any value. `bloom_bytes` must come from
    // compute_global_point_index_sizing().
    Status init(uint64_t bloom_bytes, double bloom_fpp) {
        _bloom_fpp = bloom_fpp;
        // Block bloom filter: testing one value touches a single 32-byte block.
        RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &_bloom));
        return _bloom->init(bloom_bytes, HASH_MURMUR3_X64_64);
    }

    void add_bytes(const char* buf, size_t size) {
        std::lock_guard<std::mutex> lock(_mutex);
        _bloom->add_bytes(buf, size);
        if (buf != nullptr) {
            // Only non-null values are inserted and counted: IS NULL does not use the index.
            ++_total_rows;
        }
    }

    // Adds `count` non-null values of a column of `type`, in the column writer's in-memory
    // layout: an array of Slice for string types, a packed array of fixed-width values otherwise.
    // FE encodes probe values the same way.
    void add_values(FieldType type, const void* data, size_t count) {
        if (type == FieldType::OLAP_FIELD_TYPE_VARCHAR || type == FieldType::OLAP_FIELD_TYPE_CHAR ||
            type == FieldType::OLAP_FIELD_TYPE_STRING) {
            const auto* slices = reinterpret_cast<const Slice*>(data);
            for (size_t i = 0; i < count; ++i) {
                add_bytes(slices[i].data, slices[i].size);
            }
        } else {
            size_t stride = field_type_size(type);
            const char* base = reinterpret_cast<const char*>(data);
            for (size_t i = 0; i < count; ++i) {
                add_bytes(base + i * stride, stride);
            }
        }
    }

    void add_nulls(size_t count) {
        if (count == 0) {
            return;
        }
        std::lock_guard<std::mutex> lock(_mutex);
        _bloom->set_has_null(true);
    }

    int32_t column_unique_id() const { return _column_unique_id; }
    int64_t index_id() const { return _index_id; }
    double bloom_fpp() const { return _bloom_fpp; }
    int64_t total_rows() const {
        std::lock_guard<std::mutex> lock(_mutex);
        return _total_rows;
    }
    // Bloom body: num_bytes() of bitmap plus the trailing has-null byte.
    const char* body() const { return _bloom->data(); }
    size_t body_size() const { return _bloom->size(); }
    uint64_t num_bits() const { return static_cast<uint64_t>(_bloom->num_bytes()) * 8; }

    Status finalize(io::FileWriter* file_writer, ColumnPointIndexPB* index_meta);

private:
    int32_t _column_unique_id;
    int64_t _index_id;
    double _bloom_fpp = 0;
    std::unique_ptr<BloomFilter> _bloom;
    mutable std::mutex _mutex;
    int64_t _total_rows = 0;
};

} // namespace segment_v2
} // namespace doris

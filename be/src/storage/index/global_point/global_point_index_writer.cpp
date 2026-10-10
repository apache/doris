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

#include "storage/index/global_point/global_point_index_writer.h"

#include <string.h>

#include <algorithm>
#include <cmath>

#include "common/config.h"
#include "io/fs/file_writer.h"
#include "util/hash_util.hpp"
#include "util/slice.h"

namespace doris::segment_v2 {

GlobalPointIndexSizing compute_global_point_index_sizing(double per_tablet_fpp,
                                                         std::optional<int64_t> exact_row_count) {
    GlobalPointIndexSizing sizing;
    sizing.per_bloom_fpp =
            per_tablet_fpp / std::max(1, config::global_point_index_expected_blooms_per_tablet);

    if (exact_row_count.has_value()) {
        // No cap here: compaction output tends to become the single base bloom of the tablet,
        // and pruning depends on that bloom being correctly sized.
        uint64_t rows = static_cast<uint64_t>(std::max<int64_t>(1, exact_row_count.value()));
        sizing.bloom_bytes = BloomFilter::optimal_bit_num(rows, sizing.per_bloom_fpp) / 8;
        return sizing;
    }

    uint64_t rows = static_cast<uint64_t>(
            std::max<int64_t>(1, config::global_point_index_write_path_estimated_rows));
    uint64_t bytes = BloomFilter::optimal_bit_num(rows, sizing.per_bloom_fpp) / 8;

    // A load holds one bloom per indexed column for every tablet it writes to, and a load can
    // touch every tablet of the table, so the load path caps the bloom size. BloomFilter::init()
    // needs a power of two, so the cap is rounded down to one.
    uint64_t cap = static_cast<uint64_t>(std::max<int64_t>(
            BloomFilter::MINIMUM_BYTES, config::global_point_index_max_write_path_bloom_bytes));
    uint64_t cap_pow2 = BloomFilter::MINIMUM_BYTES;
    while (cap_pow2 * 2 <= cap) {
        cap_pow2 *= 2;
    }
    sizing.bloom_bytes = std::min(bytes, cap_pow2);
    return sizing;
}

double expected_bits_per_key(double fpp) {
    // This is the classic sizing law, not the block-split formula of BloomFilter::optimal_bit_num.
    // The block-split result is always larger (1.17x to 2.91x over fpp in [1e-6, 0.5]), and
    // optimal_bit_num then rounds up to a power of two. Using the smaller value keeps the health
    // check below from ever flagging a correctly sized bloom.
    if (!(fpp > 0) || fpp >= 1) {
        return 0;
    }
    return -std::log(fpp) / (std::log(2) * std::log(2));
}

GlobalPointIndexHealth check_global_point_index_health(int64_t bloom_bytes, int64_t inserted_rows,
                                                       double fpp, int32_t slack_percent) {
    if (inserted_rows <= 0) {
        return GlobalPointIndexHealth::EMPTY;
    }
    if (slack_percent <= 0) {
        return GlobalPointIndexHealth::OK;
    }
    double expected = expected_bits_per_key(fpp);
    if (expected <= 0) {
        // No usable fpp on the descriptor: do not judge against a guessed value.
        return GlobalPointIndexHealth::OK;
    }
    double actual = static_cast<double>(bloom_bytes) * 8 / static_cast<double>(inserted_rows);
    return actual * 100 < expected * slack_percent ? GlobalPointIndexHealth::UNDERSIZED
                                                   : GlobalPointIndexHealth::OK;
}

Status write_global_point_index_file(io::FileWriter* file_writer, const char* body,
                                     size_t body_size, int32_t column_unique_id, int64_t index_id,
                                     double bloom_fpp, int64_t total_rows,
                                     ColumnPointIndexPB* index_meta) {
    uint32_t body_crc32 = HashUtil::zlib_crc_hash(body, static_cast<uint32_t>(body_size), 0);

    // Value-initialized, so the struct padding is zero and equal input gives an equal file.
    GlobalPointIndexHeader header {};
    memcpy(header.magic, GlobalPointIndexHeader::kMagic, sizeof(GlobalPointIndexHeader::kMagic));
    header.format_version = GlobalPointIndexHeader::kFormatVersion;
    header.hash_strategy = static_cast<int32_t>(HASH_MURMUR3_X64_64);
    // body_size includes the trailing has-null byte, which is not part of the bitmap.
    header.num_bits = static_cast<uint64_t>(body_size - 1) * 8;
    header.total_rows = total_rows;
    header.body_crc32 = body_crc32;

    RETURN_IF_ERROR(
            file_writer->append(Slice(reinterpret_cast<const char*>(&header), sizeof(header))));
    RETURN_IF_ERROR(file_writer->append(Slice(body, body_size)));

    index_meta->set_column_unique_id(column_unique_id);
    index_meta->set_offset(static_cast<int64_t>(kGlobalPointIndexHeaderSize));
    index_meta->set_size(static_cast<int64_t>(body_size));
    index_meta->set_fpp(bloom_fpp);
    index_meta->set_hash_strategy(static_cast<int32_t>(HASH_MURMUR3_X64_64));
    index_meta->set_total_rows(total_rows);
    index_meta->set_index_id(index_id);
    index_meta->set_body_crc32(body_crc32);
    return Status::OK();
}

Status GlobalPointIndexBuilder::finalize(io::FileWriter* file_writer,
                                         ColumnPointIndexPB* index_meta) {
    return write_global_point_index_file(file_writer, _bloom->data(), _bloom->size(),
                                         _column_unique_id, _index_id, _bloom_fpp, _total_rows,
                                         index_meta);
}

} // namespace doris::segment_v2

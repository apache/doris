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

#include "parquet_bloom.h"

#include <util/thrift_util.h>

#include "parquet/xxhasher.h"

namespace doris::vectorized {

BlockSplitBloomFilter::BlockSplitBloomFilter()
        : data_(nullptr),
          num_bytes_(0),
          hash_strategy_(HashStrategy::XXHASH),
          algorithm_(Algorithm::BLOCK),
          compression_strategy_(CompressionStrategy::UNCOMPRESSED) {}

void BlockSplitBloomFilter::init(uint32_t num_bytes) {
    if (num_bytes < K_MIN_BLOOM_FILTER_BYTES) {
        num_bytes = K_MIN_BLOOM_FILTER_BYTES;
    }

    if ((num_bytes & (num_bytes - 1)) != 0) {
        num_bytes = static_cast<uint32_t>(next_power2(num_bytes));
    }

    num_bytes_ = num_bytes;
    data_ = new (std::nothrow) uint8_t[num_bytes_];
    memcpy(data_, nullptr, num_bytes_);

    this->hasher_ = std::make_unique<parquet::XxHasher>();
}

void BlockSplitBloomFilter::init(const uint8_t* bitset, uint32_t num_bytes) {
    DCHECK(bitset != nullptr);

    if (num_bytes < K_MIN_BLOOM_FILTER_BYTES || num_bytes > K_MAX_BLOOM_FILTER_BYTES ||
        (num_bytes & (num_bytes - 1)) != 0) {
        //throw ParquetException("Given length of bitset is illegal");
    }

    num_bytes_ = num_bytes;
    data_ = new (std::nothrow) uint8_t[num_bytes_];
    memcpy(data_, nullptr, num_bytes_);

    this->hasher_ = std::make_unique<parquet::XxHasher>();
    hash_strategy_ = HashStrategy::XXHASH;
    algorithm_ = Algorithm::BLOCK;
    compression_strategy_ = CompressionStrategy::UNCOMPRESSED;
}

static constexpr uint32_t kBloomFilterHeaderSizeGuess = 256;

static Status validate_bloom_filter_header(const tparquet::BloomFilterHeader& header) {
    if (!header.algorithm.__isset.BLOCK) {
        std::stringstream ss;
        ss << "Unsupported Bloom filter algorithm: " << header.algorithm << ".";
        return Status::InternalError(ss.str());
    }

    if (!header.hash.__isset.XXHASH) {
        std::stringstream ss;
        ss << "Unsupported Bloom filter hash: " << header.hash << ".";
        return Status::InternalError(ss.str());
    }

    if (!header.compression.__isset.UNCOMPRESSED) {
        std::stringstream ss;
        ss << "Unsupported Bloom filter compression: " << header.compression << ".";
        return Status::InternalError(ss.str());
    }

    if (header.numBytes <= 0 ||
        static_cast<uint32_t>(header.numBytes) > BloomFilter::K_MAX_BLOOM_FILTER_BYTES) {
        std::stringstream ss;
        ss << "Bloom filter size is incorrect: " << header.numBytes << ". Must be in range (" << 0
           << ", " << BloomFilter::K_MAX_BLOOM_FILTER_BYTES << "].";
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

BlockSplitBloomFilter BlockSplitBloomFilter::deserialize(io::FileReaderSPtr file_reader_s,
                                                         int64_t bloom_offset,
                                                         const io::IOContext* io_context) {
    // NOTE: we don't know the bloom filter header size upfront, and we can't rely on
    // InputStream::Peek() which isn't always implemented. Therefore, we must first
    // Read() with an upper bound estimate of the header size, then once we know
    // the bloom filter data size, we can Read() the exact number of remaining data bytes.
    tparquet::BloomFilterHeader header;

    // Read and deserialize bloom filter header
    size_t bytes_read = kBloomFilterHeaderSizeGuess;
    if (file_reader_s->size() < kBloomFilterHeaderSizeGuess) {
        bytes_read = file_reader_s->size();
    }

    uint8_t hdr[bytes_read];
    auto st = file_reader_s->read_at(bloom_offset, Slice(hdr, bytes_read), &bytes_read, io_context);
    if (!st.ok()) {
        return BlockSplitBloomFilter();
    }

    // This gets used, then set by DeserializeThriftMsg
    auto header_size = static_cast<uint32_t>(bytes_read);
    st = deserialize_thrift_msg(hdr, &header_size, true, &header);
    if (!st.ok()) {
        return BlockSplitBloomFilter();
    }

    st = validate_bloom_filter_header(header);
    if (!st.ok()) {
        return BlockSplitBloomFilter();
    }

    const int32_t bloom_filter_size = header.numBytes;
    if (bloom_filter_size + header_size <= bytes_read) {
        // The bloom filter data is entirely contained in the buffer we just read
        // => just return it.
        BlockSplitBloomFilter bloom_filter;
        bloom_filter.init(hdr + header_size, bloom_filter_size);
        return bloom_filter;
    }
    // We have read a part of the bloom filter already, copy it to the target buffer
    // and read the remaining part from the InputStream.
    auto* buffer = new (std::nothrow) uint8_t[bloom_filter_size];

    const auto bloom_filter_bytes_in_header = bytes_read - header_size;
    if (bloom_filter_bytes_in_header > 0) {
        std::memcpy(buffer, hdr + header_size, bloom_filter_bytes_in_header);
    }

    const auto required_read_size = bloom_filter_size - bloom_filter_bytes_in_header;
    auto read_size = required_read_size;
    st = file_reader_s->read_at(bloom_offset + bytes_read,
                                Slice(buffer + bloom_filter_bytes_in_header, required_read_size),
                                &read_size, io_context);
    if (!st.ok()) {
        return BlockSplitBloomFilter();
    }
    BlockSplitBloomFilter bloom_filter;
    bloom_filter.init(buffer, bloom_filter_size);
    return bloom_filter;
}

bool BlockSplitBloomFilter::find_hash(uint64_t hash) const {
    const auto bucket_index =
            static_cast<uint32_t>(((hash >> 32) * (num_bytes_ / K_BYTES_PER_FILTER_BLOCK)) >> 32);
    const auto key = static_cast<uint32_t>(hash);
    const auto* bitset32 = reinterpret_cast<const uint32_t*>(data_);

    for (int i = 0; i < K_BITS_SET_PER_BLOCK; ++i) {
        // Calculate mask for key in the given bitset.
        const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
        if (0 == (bitset32[K_BITS_SET_PER_BLOCK * bucket_index + i] & mask)) {
            return false;
        }
    }
    return true;
}

void BlockSplitBloomFilter::insert_hash_impl(uint64_t hash) const {
    const auto bucket_index =
            static_cast<uint32_t>(((hash >> 32) * (num_bytes_ / K_BYTES_PER_FILTER_BLOCK)) >> 32);
    const auto key = static_cast<uint32_t>(hash);
    auto* bitset32 = reinterpret_cast<uint32_t*>(data_);

    for (int i = 0; i < K_BITS_SET_PER_BLOCK; i++) {
        // Calculate mask for key in the given bitset.
        const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
        bitset32[bucket_index * K_BITS_SET_PER_BLOCK + i] |= mask;
    }
}

void BlockSplitBloomFilter::insert_hash(const uint64_t hash) {
    insert_hash_impl(hash);
}

void BlockSplitBloomFilter::insert_hashes(const uint64_t* hashes, const int num_values) {
    for (int i = 0; i < num_values; ++i) {
        insert_hash_impl(hashes[i]);
    }
}

} // namespace doris::vectorized

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

#include "storage/index/global_point/global_point_index_reader.h"

#include <string.h>

#include "common/config.h"
#include "common/logging.h"
#include "io/fs/file_reader.h"
#include "storage/index/global_point/global_point_index_format.h"
#include "util/hash_util.hpp"
#include "util/slice.h"

namespace doris::segment_v2 {

namespace {

void degrade(const std::string& path, const std::string& reason) {
    LOG(WARNING) << "global point index is not usable, treating it as may-contain, path=" << path
                 << ", reason=" << reason;
}

} // namespace

io::FileReaderOptions global_point_index_reader_options(int64_t tablet_id, int64_t file_size,
                                                        int64_t path_version) {
    io::FileReaderOptions opts;
    // See the header comment for why path version 1 is not cached.
    opts.cache_type = (config::enable_file_cache && path_version == 0)
                              ? io::FileCachePolicy::FILE_BLOCK_CACHE
                              : io::FileCachePolicy::NO_CACHE;
    opts.is_doris_table = true;
    opts.file_size = file_size;
    opts.tablet_id = tablet_id;
    return opts;
}

Status try_load_global_point_index(const io::FileSystemSPtr& fs, const std::string& path,
                                   const ColumnPointIndexPB& desc, const io::IOContext* io_ctx,
                                   std::unique_ptr<BloomFilter>* out_bloom, int64_t* bytes_read,
                                   const io::FileReaderOptions* reader_opts) {
    *out_bloom = nullptr;
    *bytes_read = 0;

    io::FileReaderSPtr reader;
    Status open_st = fs->open_file(path, &reader, reader_opts);
    if (!open_st.ok()) {
        degrade(path, "open failed: " + open_st.to_string());
        return Status::OK();
    }

    if (reader->size() < kGlobalPointIndexHeaderSize) {
        degrade(path, "file smaller than header");
        return Status::OK();
    }

    GlobalPointIndexHeader header;
    size_t header_read = 0;
    Status read_st =
            reader->read_at(0, Slice(reinterpret_cast<char*>(&header), kGlobalPointIndexHeaderSize),
                            &header_read, io_ctx);
    if (!read_st.ok() || header_read != kGlobalPointIndexHeaderSize) {
        degrade(path, "header read failed or short read");
        return Status::OK();
    }
    *bytes_read += static_cast<int64_t>(header_read);

    if (memcmp(header.magic, GlobalPointIndexHeader::kMagic,
               sizeof(GlobalPointIndexHeader::kMagic)) != 0) {
        degrade(path, "magic mismatch");
        return Status::OK();
    }
    // A newer format than this BE understands must not be parsed.
    if (header.format_version > GlobalPointIndexHeader::kFormatVersion) {
        degrade(path, "format_version too new");
        return Status::OK();
    }
    if (header.hash_strategy != static_cast<int32_t>(HASH_MURMUR3_X64_64) ||
        header.hash_strategy != desc.hash_strategy()) {
        degrade(path, "unsupported or inconsistent hash_strategy");
        return Status::OK();
    }
    if (header.num_bits == 0 || (header.num_bits & (header.num_bits - 1)) != 0) {
        degrade(path, "num_bits not a power of two");
        return Status::OK();
    }

    // The body is the bitmap plus BloomFilter's trailing has-null byte.
    size_t body_size = static_cast<size_t>(header.num_bits / 8) + 1;
    if (reader->size() != kGlobalPointIndexHeaderSize + body_size) {
        degrade(path, "file length does not match header (truncated or corrupt)");
        return Status::OK();
    }
    if (static_cast<int64_t>(body_size) != desc.size()) {
        degrade(path, "body size does not match descriptor");
        return Status::OK();
    }

    std::unique_ptr<char[]> body(new char[body_size]);
    size_t body_read = 0;
    read_st = reader->read_at(kGlobalPointIndexHeaderSize, Slice(body.get(), body_size), &body_read,
                              io_ctx);
    if (!read_st.ok() || body_read != body_size) {
        degrade(path, "body read failed or short read");
        return Status::OK();
    }
    *bytes_read += static_cast<int64_t>(body_read);

    uint32_t actual_crc32 =
            HashUtil::zlib_crc_hash(body.get(), static_cast<uint32_t>(body_size), 0);
    if (actual_crc32 != header.body_crc32 ||
        static_cast<uint32_t>(actual_crc32) != static_cast<uint32_t>(desc.body_crc32())) {
        degrade(path, "body crc32 mismatch (corruption)");
        return Status::OK();
    }

    std::unique_ptr<BloomFilter> bloom;
    Status create_st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bloom);
    if (!create_st.ok()) {
        degrade(path, "BloomFilter::create failed: " + create_st.to_string());
        return Status::OK();
    }
    Status init_st = bloom->init(body.get(), body_size, HASH_MURMUR3_X64_64);
    if (!init_st.ok()) {
        degrade(path, "BloomFilter::init failed: " + init_st.to_string());
        return Status::OK();
    }

    *out_bloom = std::move(bloom);
    return Status::OK();
}

} // namespace doris::segment_v2

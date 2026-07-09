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

#include "io/fs/file_meta_disk_cache.h"

#include <crc32c/crc32c.h>
#include <gen_cpp/Types_types.h>

#include <algorithm>
#include <cstring>
#include <limits>

#include "common/config.h"
#include "common/logging.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris {
namespace {

constexpr std::string_view FILE_META_DISK_CACHE_MAGIC = "DFMC";
constexpr uint8_t FILE_META_DISK_CACHE_VERSION = 1;
constexpr size_t FILE_META_DISK_CACHE_HEADER_SIZE = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 4;

std::string_view format_name(FileMetaCacheFormat format) {
    switch (format) {
    case FileMetaCacheFormat::PARQUET:
        return "parquet";
    case FileMetaCacheFormat::ORC:
        return "orc";
    case FileMetaCacheFormat::PARQUET_V2:
        return "parquet_v2";
    }
    DCHECK(false) << "unknown file meta cache format";
    return "unknown";
}

Status parse_format(uint8_t format, FileMetaCacheFormat* parsed) {
    switch (static_cast<FileMetaCacheFormat>(format)) {
    case FileMetaCacheFormat::PARQUET:
    case FileMetaCacheFormat::ORC:
    case FileMetaCacheFormat::PARQUET_V2:
        *parsed = static_cast<FileMetaCacheFormat>(format);
        return Status::OK();
    }
    return Status::NotFound("file meta disk cache format mismatch");
}

struct FileMetaDiskCacheHeader {
    FileMetaCacheFormat format;
    int64_t modification_time = 0;
    int64_t file_size = 0;
    uint64_t payload_size = 0;
    uint32_t checksum = 0;
};

Status parse_header(std::string_view header, FileMetaDiskCacheHeader* parsed) {
    DCHECK(header.size() == FILE_META_DISK_CACHE_HEADER_SIZE);
    if (std::memcmp(header.data(), FILE_META_DISK_CACHE_MAGIC.data(),
                    FILE_META_DISK_CACHE_MAGIC.size()) != 0) {
        return Status::NotFound("file meta disk cache magic mismatch");
    }

    const auto* ptr =
            reinterpret_cast<const uint8_t*>(header.data() + FILE_META_DISK_CACHE_MAGIC.size());
    const uint8_t version = *ptr++;
    if (version != FILE_META_DISK_CACHE_VERSION) {
        return Status::NotFound("file meta disk cache version mismatch");
    }

    RETURN_IF_ERROR(parse_format(*ptr++, &parsed->format));
    ptr += 2;
    parsed->file_size = static_cast<int64_t>(decode_fixed64_le(ptr));
    ptr += sizeof(uint64_t);
    parsed->modification_time = static_cast<int64_t>(decode_fixed64_le(ptr));
    ptr += sizeof(uint64_t);
    parsed->payload_size = decode_fixed64_le(ptr);
    ptr += sizeof(uint64_t);
    parsed->checksum = decode_fixed32_le(ptr);
    return Status::OK();
}

std::string build_cache_value(FileMetaCacheFormat format, int64_t modification_time,
                              int64_t file_size, std::string_view payload) {
    std::string value;
    value.reserve(FILE_META_DISK_CACHE_HEADER_SIZE + payload.size());
    value.append(FILE_META_DISK_CACHE_MAGIC.data(), FILE_META_DISK_CACHE_MAGIC.size());
    value.push_back(static_cast<char>(FILE_META_DISK_CACHE_VERSION));
    value.push_back(static_cast<char>(format));
    value.push_back(0);
    value.push_back(0);
    put_fixed64_le(&value, static_cast<uint64_t>(file_size));
    put_fixed64_le(&value, static_cast<uint64_t>(modification_time));
    put_fixed64_le(&value, payload.size());
    put_fixed32_le(&value, crc32c::Crc32c(payload.data(), payload.size()));
    value.append(payload.data(), payload.size());
    return value;
}

io::CacheContext build_meta_cache_context() {
    io::CacheContext context;
    context.cache_type = io::FileCacheType::INDEX;
    context.query_id = TUniqueId();
    context.expiration_time = 0;
    context.is_cold_data = false;
    context.is_warmup = false;
    return context;
}

Status read_cached_range(io::BlockFileCache* cache, const io::UInt128Wrapper& hash, size_t offset,
                         size_t size, const io::CacheContext& context, std::string* output,
                         bool* missing_cached_range = nullptr) {
    DCHECK(cache != nullptr);
    DCHECK(output != nullptr);
    output->clear();
    if (missing_cached_range != nullptr) {
        *missing_cached_range = false;
    }
    if (size == 0) {
        return Status::OK();
    }

    io::FileBlocks blocks;
    bool fully_covered = false;
    RETURN_IF_ERROR(cache->get_downloaded_blocks_if_fully_covered(hash, offset, size, context,
                                                                  &blocks, &fully_covered));
    if (!fully_covered) {
        if (missing_cached_range != nullptr) {
            *missing_cached_range = true;
        }
        return Status::NotFound("file meta disk cache range is not cached");
    }

    output->resize(size);
    size_t copied_size = 0;
    const size_t requested_right = offset + size - 1;
    for (const auto& block : blocks) {
        const auto& range = block->range();
        const size_t read_left = std::max(range.left, offset);
        const size_t read_right = std::min(range.right, requested_right);
        DCHECK_LE(read_left, read_right);
        const size_t read_size = read_right - read_left + 1;
        RETURN_IF_ERROR(block->read(Slice(output->data() + copied_size, read_size),
                                    read_left - range.left));
        copied_size += read_size;
    }
    DCHECK_EQ(copied_size, size);
    return Status::OK();
}

} // namespace

std::string FileMetaDiskCache::get_key(FileMetaCacheFormat format,
                                       std::string_view file_meta_cache_key) {
    std::string key;
    key.reserve(32 + file_meta_cache_key.size());
    key.append("file_meta_cache:v1:");
    key.append(format_name(format));
    key.push_back(':');
    key.append(file_meta_cache_key.data(), file_meta_cache_key.size());
    return key;
}

Status FileMetaDiskCache::read(FileMetaCacheFormat format, const std::string& file_meta_cache_key,
                               int64_t modification_time, int64_t file_size, std::string* payload) {
    payload->clear();
    const std::string cache_key = get_key(format, file_meta_cache_key);
    const auto hash = io::BlockFileCache::hash(cache_key);
    io::BlockFileCache* cache = get_cache(hash);
    if (cache == nullptr) {
        return Status::NotFound("file meta disk cache is not available");
    }

    io::ReadStatistics stats;
    io::CacheContext context = build_meta_cache_context();
    context.stats = &stats;

    auto invalidate_entry = [&](const Status& status) {
        payload->clear();
        cache->remove_if_cached(hash);
        return status;
    };

    std::string header;
    bool missing_header_range = false;
    Status status = read_cached_range(cache, hash, 0, FILE_META_DISK_CACHE_HEADER_SIZE, context,
                                      &header, &missing_header_range);
    if (!status.ok()) {
        if (missing_header_range) {
            return status;
        }
        return invalidate_entry(status);
    }

    FileMetaDiskCacheHeader parsed;
    status = parse_header(header, &parsed);
    if (!status.ok()) {
        return invalidate_entry(status);
    }
    if (parsed.format != format || parsed.modification_time != modification_time ||
        parsed.file_size != file_size ||
        !FileMetaCache::is_persistent_cache_payload_size_allowed(parsed.payload_size)) {
        return invalidate_entry(Status::NotFound("file meta disk cache header mismatch"));
    }

    bool missing_payload_range = false;
    status = read_cached_range(cache, hash, FILE_META_DISK_CACHE_HEADER_SIZE, parsed.payload_size,
                               context, payload, &missing_payload_range);
    if (!status.ok()) {
        if (missing_payload_range) {
            payload->clear();
            return status;
        }
        return invalidate_entry(status);
    }
    const uint32_t checksum = crc32c::Crc32c(payload->data(), payload->size());
    if (checksum != parsed.checksum) {
        return invalidate_entry(Status::NotFound("file meta disk cache checksum mismatch"));
    }
    return Status::OK();
}

Status FileMetaDiskCache::write(FileMetaCacheFormat format, const std::string& file_meta_cache_key,
                                int64_t modification_time, int64_t file_size,
                                std::string_view payload) {
    if (!FileMetaCache::is_persistent_cache_payload_size_allowed(
                static_cast<uint64_t>(payload.size()))) {
        return Status::NotFound("file meta disk cache payload size is not allowed");
    }

    const std::string cache_key = get_key(format, file_meta_cache_key);
    const auto hash = io::BlockFileCache::hash(cache_key);
    io::BlockFileCache* cache = get_cache(hash);
    if (cache == nullptr) {
        return Status::NotFound("file meta disk cache is not available");
    }

    const std::string value = build_cache_value(format, modification_time, file_size, payload);
    io::ReadStatistics stats;
    io::CacheContext context = build_meta_cache_context();
    context.stats = &stats;
    auto holder = cache->get_or_set(hash, 0, value.size(), context);
    auto write_block = [&](const io::FileBlockSPtr& block) -> Status {
        auto state = block->state();
        if (state == io::FileBlock::State::DOWNLOADING && !block->is_downloader()) {
            state = block->wait();
        }
        if (state == io::FileBlock::State::DOWNLOADED) {
            return Status::OK();
        }
        if (state != io::FileBlock::State::EMPTY) {
            return Status::NotFound("file meta disk cache block is not writable");
        }

        if (block->get_or_set_downloader() != io::FileBlock::get_caller_id()) {
            return Status::NotFound("file meta disk cache block has another downloader");
        }
        const auto& range = block->range();
        DCHECK_LT(range.right, value.size());
        Status status = block->append(Slice(value.data() + range.left, range.size()));
        if (!status.ok()) {
            cache->remove_if_cached(hash);
            return status;
        }
        status = block->finalize();
        if (!status.ok()) {
            cache->remove_if_cached(hash);
            return status;
        }
        return Status::OK();
    };

    for (const auto& block : holder.file_blocks) {
        if (block->range().left >= FILE_META_DISK_CACHE_HEADER_SIZE) {
            RETURN_IF_ERROR(write_block(block));
        }
    }
    for (const auto& block : holder.file_blocks) {
        if (block->range().left < FILE_META_DISK_CACHE_HEADER_SIZE) {
            RETURN_IF_ERROR(write_block(block));
        }
    }
    return Status::OK();
}

void FileMetaDiskCache::remove(FileMetaCacheFormat format, const std::string& file_meta_cache_key) {
    const std::string cache_key = get_key(format, file_meta_cache_key);
    const auto hash = io::BlockFileCache::hash(cache_key);
    io::BlockFileCache* cache = get_cache(hash);
    if (cache != nullptr) {
        cache->remove_if_cached(hash);
    }
}

io::BlockFileCache* FileMetaDiskCache::get_cache(const io::UInt128Wrapper& hash) const {
    if (_cache != nullptr) {
        return _cache;
    }
    io::FileCacheFactory* factory = io::FileCacheFactory::instance();
    if (factory == nullptr || factory->get_cache_instance_size() == 0) {
        return nullptr;
    }
    return factory->get_by_path(hash);
}

} // namespace doris

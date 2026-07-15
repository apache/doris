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
#include <gen_cpp/file_cache.pb.h>

#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "common/logging.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris {
namespace {

constexpr uint8_t FILE_META_DISK_CACHE_VERSION = 1;
constexpr size_t FILE_META_DISK_CACHE_HEADER_SIZE_PREFIX = sizeof(uint32_t);
constexpr uint32_t FILE_META_DISK_CACHE_MAX_HEADER_SIZE = 1024;

std::string_view format_name(FileMetaCacheFormat format) {
    switch (format) {
    case FileMetaCacheFormat::PARQUET:
        return "parquet";
    case FileMetaCacheFormat::ORC:
        return "orc";
    case FileMetaCacheFormat::PARQUET_V2:
        return "parquet_v2";
    case FileMetaCacheFormat::ORC_V2:
        return "orc_v2";
    }
    DCHECK(false) << "unknown file meta cache format";
    return "unknown";
}

Status validate_format(uint32_t format) {
    switch (static_cast<FileMetaCacheFormat>(format)) {
    case FileMetaCacheFormat::PARQUET:
    case FileMetaCacheFormat::ORC:
    case FileMetaCacheFormat::PARQUET_V2:
    case FileMetaCacheFormat::ORC_V2:
        return Status::OK();
    }
    return Status::NotFound("file meta disk cache format mismatch");
}

Status parse_header(std::string_view serialized_header,
                    io::cache::FileMetaCacheEntryHeaderPB* parsed) {
    DCHECK(parsed != nullptr);
    DCHECK_LE(serialized_header.size(), FILE_META_DISK_CACHE_MAX_HEADER_SIZE);
    if (!parsed->ParseFromArray(serialized_header.data(),
                                static_cast<int>(serialized_header.size()))) {
        return Status::NotFound("failed to parse file meta disk cache header");
    }
    if (!parsed->has_version() || !parsed->has_format() || !parsed->has_file_size() ||
        !parsed->has_modification_time() || !parsed->has_payload_size() ||
        !parsed->has_checksum()) {
        return Status::NotFound("file meta disk cache header is incomplete");
    }
    if (parsed->version() != FILE_META_DISK_CACHE_VERSION) {
        return Status::NotFound("file meta disk cache version mismatch");
    }
    return validate_format(parsed->format());
}

Status build_cache_value(FileMetaCacheFormat format, int64_t modification_time, int64_t file_size,
                         std::string_view payload, std::string* value, size_t* header_end) {
    DCHECK(value != nullptr);
    DCHECK(header_end != nullptr);
    io::cache::FileMetaCacheEntryHeaderPB header;
    header.set_version(FILE_META_DISK_CACHE_VERSION);
    header.set_format(static_cast<uint32_t>(format));
    header.set_file_size(file_size);
    header.set_modification_time(modification_time);
    header.set_payload_size(payload.size());
    header.set_checksum(crc32c::Crc32c(payload.data(), payload.size()));

    std::string serialized_header;
    if (!header.SerializeToString(&serialized_header)) {
        return Status::InternalError("failed to serialize file meta disk cache header");
    }
    DCHECK_LE(serialized_header.size(), FILE_META_DISK_CACHE_MAX_HEADER_SIZE);
    value->clear();
    value->reserve(FILE_META_DISK_CACHE_HEADER_SIZE_PREFIX + serialized_header.size() +
                   payload.size());
    put_fixed32_le(value, static_cast<uint32_t>(serialized_header.size()));
    value->append(serialized_header);
    *header_end = value->size();
    value->append(payload.data(), payload.size());
    return Status::OK();
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
                         std::vector<io::FileBlocksHolder>* const holders) {
    DCHECK(cache != nullptr);
    DCHECK(output != nullptr);
    DCHECK(holders != nullptr);
    output->clear();
    if (size == 0) {
        return Status::OK();
    }

    io::FileBlocks blocks;
    bool fully_covered = false;
    RETURN_IF_ERROR(cache->get_downloaded_blocks_if_fully_covered(hash, offset, size, context,
                                                                  &blocks, &fully_covered));
    if (!fully_covered) {
        return Status::NotFound("file meta disk cache range is not cached");
    }
    holders->emplace_back(std::move(blocks));
    const auto& holder = holders->back();

    output->resize(size);
    size_t copied_size = 0;
    const size_t requested_right = offset + size - 1;
    for (const auto& block : holder.file_blocks) {
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

Status read_cache_entry(io::BlockFileCache* cache, const io::UInt128Wrapper& hash,
                        FileMetaCacheFormat format, int64_t modification_time, int64_t file_size,
                        std::string* payload) {
    DCHECK(cache != nullptr);
    DCHECK(payload != nullptr);
    payload->clear();

    io::ReadStatistics stats;
    io::CacheContext context = build_meta_cache_context();
    context.stats = &stats;
    std::vector<io::FileBlocksHolder> holders;

    std::string header_size_prefix;
    RETURN_IF_ERROR(read_cached_range(cache, hash, 0, FILE_META_DISK_CACHE_HEADER_SIZE_PREFIX,
                                      context, &header_size_prefix, &holders));

    const uint32_t header_size =
            decode_fixed32_le(reinterpret_cast<const uint8_t*>(header_size_prefix.data()));
    if (header_size == 0 || header_size > FILE_META_DISK_CACHE_MAX_HEADER_SIZE) {
        return Status::NotFound("file meta disk cache header size is invalid");
    }

    std::string serialized_header;
    RETURN_IF_ERROR(read_cached_range(cache, hash, FILE_META_DISK_CACHE_HEADER_SIZE_PREFIX,
                                      header_size, context, &serialized_header, &holders));

    io::cache::FileMetaCacheEntryHeaderPB parsed;
    RETURN_IF_ERROR(parse_header(serialized_header, &parsed));
    if (parsed.format() != static_cast<uint32_t>(format) ||
        parsed.modification_time() != modification_time || parsed.file_size() != file_size ||
        !FileMetaCache::is_persistent_cache_payload_size_allowed(parsed.payload_size())) {
        return Status::NotFound("file meta disk cache header mismatch");
    }

    const size_t payload_offset = FILE_META_DISK_CACHE_HEADER_SIZE_PREFIX + header_size;
    RETURN_IF_ERROR(read_cached_range(cache, hash, payload_offset, parsed.payload_size(), context,
                                      payload, &holders));
    const uint32_t checksum = crc32c::Crc32c(payload->data(), payload->size());
    if (checksum != parsed.checksum()) {
        payload->clear();
        return Status::NotFound("file meta disk cache checksum mismatch");
    }

    return Status::OK();
}

} // namespace

std::array<std::shared_mutex, FileMetaDiskCache::ENTRY_LOCK_SHARDS> FileMetaDiskCache::_entry_locks;

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
    if (!cache->get_async_open_success()) {
        return Status::NotFound("file meta disk cache is still restoring");
    }

    Status status;
    {
        std::shared_lock entry_lock(get_entry_lock(hash));
        status = read_cache_entry(cache, hash, format, modification_time, file_size, payload);
        if (status.ok()) {
            return status;
        }
        payload->clear();
    }

    // Revalidate after upgrading the entry lock so a writer that completed in between is preserved.
    std::unique_lock entry_lock(get_entry_lock(hash));
    status = read_cache_entry(cache, hash, format, modification_time, file_size, payload);
    if (status.ok()) {
        return status;
    }
    payload->clear();
    cache->remove_if_cached(hash);
    return status;
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
    std::string value;
    size_t header_end = 0;
    RETURN_IF_ERROR(
            build_cache_value(format, modification_time, file_size, payload, &value, &header_end));
    io::BlockFileCache* cache = get_cache(hash);
    if (cache == nullptr) {
        return Status::NotFound("file meta disk cache is not available");
    }
    if (!cache->get_async_open_success()) {
        return Status::NotFound("file meta disk cache is still restoring");
    }
    std::unique_lock entry_lock(get_entry_lock(hash));

    std::string existing_payload;
    Status existing_status =
            read_cache_entry(cache, hash, format, modification_time, file_size, &existing_payload);
    if (existing_status.ok() && existing_payload == payload) {
        return Status::OK();
    }
    cache->remove_if_cached(hash);

    io::ReadStatistics stats;
    io::CacheContext context = build_meta_cache_context();
    context.stats = &stats;
    auto holder = cache->get_or_set(hash, 0, value.size(), context);

    // Claim the complete entry before appending so a rejected block cannot leave partial data.
    io::FileBlocks blocks_to_write;
    for (const auto& block : holder.file_blocks) {
        auto state = block->state();
        if (state == io::FileBlock::State::DOWNLOADING && !block->is_downloader()) {
            state = block->wait();
        }
        if (state == io::FileBlock::State::DOWNLOADED) {
            continue;
        }
        if (state != io::FileBlock::State::EMPTY) {
            cache->remove_if_cached(hash);
            return Status::NotFound("file meta disk cache block is not writable");
        }
        if (block->get_or_set_downloader() != io::FileBlock::get_caller_id()) {
            cache->remove_if_cached(hash);
            return Status::NotFound("file meta disk cache block has another downloader");
        }
        blocks_to_write.emplace_back(block);
    }

    auto write_block = [&](const io::FileBlockSPtr& block) -> Status {
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

    for (const auto& block : blocks_to_write) {
        if (block->range().left >= header_end) {
            RETURN_IF_ERROR(write_block(block));
        }
    }
    // Publish the header in reverse block order so the length prefix at offset zero is last.
    for (auto it = blocks_to_write.rbegin(); it != blocks_to_write.rend(); ++it) {
        if ((*it)->range().left < header_end) {
            RETURN_IF_ERROR(write_block(*it));
        }
    }
    return Status::OK();
}

void FileMetaDiskCache::remove(FileMetaCacheFormat format, const std::string& file_meta_cache_key) {
    const std::string cache_key = get_key(format, file_meta_cache_key);
    const auto hash = io::BlockFileCache::hash(cache_key);
    io::BlockFileCache* cache = get_cache(hash);
    if (cache != nullptr && cache->get_async_open_success()) {
        std::unique_lock entry_lock(get_entry_lock(hash));
        cache->remove_if_cached(hash);
    }
}

std::shared_mutex& FileMetaDiskCache::get_entry_lock(const io::UInt128Wrapper& hash) {
    return _entry_locks[io::KeyHash()(hash) % ENTRY_LOCK_SHARDS];
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

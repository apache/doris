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

#include "io/cache/cache_lru_dumper.h"

#include "io/cache/block_file_cache.h"
#include "io/cache/cache_lru_dumper.h"
#include "io/cache/lru_queue_recorder.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace doris::io {

std::string CacheLRUDumper::Footer::serialize_as_string() const {
    std::string result;
    result.reserve(sizeof(Footer));

    // Serialize meta_offset (convert to little-endian)
    uint64_t meta_offset_le;
    encode_fixed64_le(reinterpret_cast<uint8_t*>(&meta_offset_le), meta_offset);
    result.append(reinterpret_cast<const char*>(&meta_offset_le), sizeof(meta_offset_le));

    // Serialize checksum (convert to little-endian)
    uint32_t checksum_le;
    encode_fixed32_le(reinterpret_cast<uint8_t*>(&checksum_le), checksum);

    result.append(reinterpret_cast<const char*>(&checksum_le), sizeof(checksum_le));

    result.append(reinterpret_cast<const char*>(&version), sizeof(version));

    // Serialize magic
    result.append(magic, sizeof(magic));

    return result;
}

bool CacheLRUDumper::Footer::deserialize_from_string(const std::string& data) {
    DCHECK(data.size() == sizeof(Footer));

    const char* ptr = data.data();

    // Deserialize meta_offset (convert from little-endian)
    uint64_t meta_offset_le;
    std::memcpy(&meta_offset_le, ptr, sizeof(meta_offset_le));
    meta_offset = decode_fixed64_le(reinterpret_cast<uint8_t*>(&meta_offset_le));
    ptr += sizeof(meta_offset_le);

    // Deserialize checksum (convert from little-endian)
    uint32_t checksum_le;
    std::memcpy(&checksum_le, ptr, sizeof(checksum_le));
    checksum = decode_fixed32_le(reinterpret_cast<uint8_t*>(&checksum_le));
    ptr += sizeof(checksum_le);

    version = *((uint8_t*)ptr);
    ptr += sizeof(version);

    // Deserialize magic
    std::memcpy(magic, ptr, sizeof(magic));

    return true;
}

Status CacheLRUDumper::check_ofstream_status(std::ofstream& out, std::string& filename) {
    if (!out.good()) {
        std::ios::iostate state = out.rdstate();
        std::stringstream err_msg;
        if (state & std::ios::eofbit) {
            err_msg << "End of file reached.";
        }
        if (state & std::ios::failbit) {
            err_msg << "Input/output operation failed, err_code: " << strerror(errno);
        }
        if (state & std::ios::badbit) {
            err_msg << "Serious I/O error occurred, err_code: " << strerror(errno);
        }
        out.close();
        std::string warn_msg = fmt::format("dump lru writing failed, file={}, {}", filename,
                                           err_msg.str().c_str());
        LOG(WARNING) << warn_msg;
        return Status::InternalError<false>(warn_msg);
    }

    return Status::OK();
}

Status CacheLRUDumper::check_ifstream_status(std::ifstream& in, std::string& filename) {
    if (!in.good()) {
        std::ios::iostate state = in.rdstate();
        std::stringstream err_msg;
        if (state & std::ios::eofbit) {
            err_msg << "End of file reached.";
        }
        if (state & std::ios::failbit) {
            err_msg << "Input/output operation failed, err_code: " << strerror(errno);
        }
        if (state & std::ios::badbit) {
            err_msg << "Serious I/O error occurred, err_code: " << strerror(errno);
        }
        in.close();
        std::string warn_msg = std::string(
                fmt::format("dump lru reading failed, file={}, {}", filename, err_msg.str()));
        LOG(WARNING) << warn_msg;
        return Status::InternalError<false>(warn_msg);
    }

    return Status::OK();
}

Status CacheLRUDumper::dump_one_lru_entry(std::ofstream& out, std::string& filename,
                                          const UInt128Wrapper& hash, size_t offset, size_t size) {
    // Dump file format description:
    // +-----------------------------------------------+
    // | LRUDumpEntryGroupPb_1                         |
    // +-----------------------------------------------+
    // | LRUDumpEntryGroupPb_2                         |
    // +-----------------------------------------------+
    // | LRUDumpEntryGroupPb_3                         |
    // +-----------------------------------------------+
    // | ...                                           |
    // +-----------------------------------------------+
    // | LRUDumpEntryGroupPb_n                         |
    // +-----------------------------------------------+
    // | LRUDumpMetaPb (List<offset,size,crc>)         |
    // +-----------------------------------------------+
    // | FOOTER_OFFSET (8Bytes)                        |
    // +-----------------------------------------------+
    // | CHECKSUM (4Bytes)｜VERSION (1Byte)｜MAGIC (3B)|
    // +-----------------------------------------------+
    //
    // why we are not using protobuf as a whole?
    // AFAIK, current protobuf version dose not support streaming mode,
    // so that we need to store all the message in memory which will
    // consume loads of RAMs.
    // Instead, we use protobuf serialize each of the single entry
    // and provide the version field in the footer for upgrade

    ::doris::io::cache::LRUDumpEntryPb* entry = _current_dump_group.add_entries();
    ::doris::io::cache::UInt128WrapperPb* hash_pb = entry->mutable_hash();
    hash_pb->set_high(hash.high());
    hash_pb->set_low(hash.low());
    entry->set_offset(offset);
    entry->set_size(size);

    _current_dump_group_count++;
    if (_current_dump_group_count >= 10000) {
        RETURN_IF_ERROR(flush_current_group(out, filename));
    }
    return Status::OK();
}

Status CacheLRUDumper::flush_current_group(std::ofstream& out, std::string& filename) {
    if (_current_dump_group_count == 0) {
        return Status::OK();
    }

    // Record current position as group start offset
    size_t group_start = out.tellp();

    // Serialize and write the group
    std::string serialized;
    VLOG_DEBUG << "Serialized size: " << serialized.size()
               << " Before serialization: " << _current_dump_group.DebugString();
    if (!_current_dump_group.SerializeToString(&serialized)) {
        std::string warn_msg = fmt::format("Failed to serialize LRUDumpEntryGroupPb");
        LOG(WARNING) << warn_msg;
        return Status::InternalError<false>(warn_msg);
    }

    out.write(serialized.data(), serialized.size());
    RETURN_IF_ERROR(check_ofstream_status(out, filename));

    // Record group metadata
    ::doris::io::cache::EntryGroupOffsetSizePb* group_info = _dump_meta.add_group_offset_size();
    group_info->set_offset(group_start);
    group_info->set_size(serialized.size());
    uint32_t checksum = crc32c::Value(serialized.data(), serialized.size());
    group_info->set_checksum(checksum);

    // Reset for next group
    _current_dump_group.Clear();
    _current_dump_group_count = 0;
    return Status::OK();
}

Status CacheLRUDumper::finalize_dump(std::ofstream& out, size_t entry_num,
                                     std::string& tmp_filename, std::string& final_filename,
                                     size_t& file_size) {
    // Flush any remaining entries
    if (_current_dump_group_count > 0) {
        RETURN_IF_ERROR(flush_current_group(out, tmp_filename));
    }

    // Write meta information
    _dump_meta.set_entry_num(entry_num);
    size_t meta_offset = out.tellp();
    LOG(INFO) << "dump meta: " << _dump_meta.DebugString();
    std::string meta_serialized;
    if (!_dump_meta.SerializeToString(&meta_serialized)) {
        std::string warn_msg =
                fmt::format("Failed to serialize LRUDumpMetaPb, file={}", tmp_filename);
        LOG(WARNING) << warn_msg;
        return Status::InternalError<false>(warn_msg);
    }
    out.write(meta_serialized.data(), meta_serialized.size());
    RETURN_IF_ERROR(check_ofstream_status(out, tmp_filename));

    // Write footer
    Footer footer;
    footer.meta_offset = meta_offset;
    footer.checksum = 0;
    footer.version = 1;
    std::memcpy(footer.magic, "DOR", 3);

    std::string footer_str = footer.serialize_as_string();
    out.write(footer_str.data(), footer_str.size());
    RETURN_IF_ERROR(check_ofstream_status(out, tmp_filename));

    out.close();

    // Rename tmp to formal file
    try {
        std::rename(tmp_filename.c_str(), final_filename.c_str());
        std::remove(tmp_filename.c_str());
        file_size = std::filesystem::file_size(final_filename);
    } catch (const std::filesystem::filesystem_error& e) {
        LOG(WARNING) << "failed to rename " << tmp_filename << " to " << final_filename
                     << " err: " << e.what();
    }

    _dump_meta.Clear();
    _current_dump_group.Clear();
    _current_dump_group_count = 0;

    return Status::OK();
}

void CacheLRUDumper::dump_queue(const std::string& queue_name) {
    FileCacheType type = string_to_cache_type(queue_name);
    if (_recorder->get_lru_queue_update_cnt_from_last_dump(type) >
        config::file_cache_background_lru_dump_update_cnt_threshold) {
        LRUQueue& queue = _recorder->get_shadow_queue(type);
        do_dump_queue(queue, queue_name);
        _recorder->reset_lru_queue_update_cnt_from_last_dump(type);
    }
}

void CacheLRUDumper::do_dump_queue(LRUQueue& queue, const std::string& queue_name) {
    Status st;
    std::vector<std::tuple<UInt128Wrapper, size_t, size_t>> elements;
    elements.reserve(config::file_cache_background_lru_dump_tail_record_num);

    {
        std::lock_guard<std::mutex> lru_log_lock(_recorder->_mutex_lru_log);
        size_t count = 0;
        for (const auto& [hash, offset, size] : queue) {
            if (count++ >= config::file_cache_background_lru_dump_tail_record_num) break;
            elements.emplace_back(hash, offset, size);
        }
    }

    // Write to disk
    int64_t duration_ns = 0;
    std::uintmax_t file_size = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        std::string tmp_filename =
                fmt::format("{}/lru_dump_{}.tail.tmp", _mgr->_cache_base_path, queue_name);
        std::string final_filename =
                fmt::format("{}/lru_dump_{}.tail", _mgr->_cache_base_path, queue_name);
        std::ofstream out(tmp_filename, std::ios::binary);
        if (out) {
            LOG(INFO) << "begin dump " << queue_name << " with " << elements.size() << " elements";
            for (const auto& [hash, offset, size] : elements) {
                RETURN_IF_STATUS_ERROR(st,
                                       dump_one_lru_entry(out, tmp_filename, hash, offset, size));
            }
            RETURN_IF_STATUS_ERROR(st, finalize_dump(out, elements.size(), tmp_filename,
                                                     final_filename, file_size));
        } else {
            LOG(WARNING) << "open lru dump file failed, reason: " << tmp_filename
                         << " failed to create";
        }
    }
    *(_mgr->_lru_dump_latency_us) << (duration_ns / 1000);
    LOG(INFO) << fmt::format("lru dump for {} size={} element={} time={}us", queue_name, file_size,
                             elements.size(), duration_ns / 1000);
};

Status CacheLRUDumper::parse_dump_footer(std::ifstream& in, std::string& filename,
                                         size_t& entry_num) {
    size_t file_size = std::filesystem::file_size(filename);

    // Read footer
    Footer footer;
    size_t footer_size = sizeof(footer);
    if (file_size < footer_size) {
        std::string warn_msg = std::string(fmt::format(
                "LRU dump file too small to contain footer, file={}, skip restore", filename));
        LOG(WARNING) << warn_msg;
        return Status::InternalError<false>(warn_msg);
    }

    in.seekg(-footer_size, std::ios::end);
    std::string footer_str(footer_size, '\0');
    in.read(&footer_str[0], footer_size);
    RETURN_IF_ERROR(check_ifstream_status(in, filename));

    if (!footer.deserialize_from_string(footer_str)) {
        std::string warn_msg = std::string(
                fmt::format("Failed to deserialize footer, file={}, skip restore", filename));
        LOG(WARNING) << warn_msg;
        return Status::InternalError<false>(warn_msg);
    }

    // Validate footer
    if (footer.version != 1 || std::string(footer.magic, 3) != "DOR") {
        std::string warn_msg = std::string(fmt::format(
                "LRU dump file invalid footer format, file={}, skip restore", filename));
        LOG(WARNING) << warn_msg;
        return Status::InternalError<false>(warn_msg);
    }

    // Read meta
    in.seekg(footer.meta_offset, std::ios::beg);
    size_t meta_size = file_size - footer.meta_offset - footer_size;
    if (meta_size <= 0) {
        std::string warn_msg = std::string(
                fmt::format("LRU dump file invalid meta size, file={}, skip restore", filename));
        LOG(WARNING) << warn_msg;
        return Status::InternalError<false>(warn_msg);
    }
    std::string meta_serialized(meta_size, '\0');
    in.read(&meta_serialized[0], meta_serialized.size());
    RETURN_IF_ERROR(check_ifstream_status(in, filename));
    _parse_meta.Clear();
    _current_parse_group.Clear();
    if (!_parse_meta.ParseFromString(meta_serialized)) {
        std::string warn_msg = std::string(
                fmt::format("LRU dump file meta parse failed, file={}, skip restore", filename));
        LOG(WARNING) << warn_msg;
        return Status::InternalError<false>(warn_msg);
    }
    VLOG_DEBUG << "parse meta: " << _parse_meta.DebugString();

    entry_num = _parse_meta.entry_num();
    return Status::OK();
}

Status CacheLRUDumper::parse_one_lru_entry(std::ifstream& in, std::string& filename,
                                           UInt128Wrapper& hash, size_t& offset, size_t& size) {
    // Read next group if current is empty
    if (_current_parse_group.entries_size() == 0) {
        if (_parse_meta.group_offset_size_size() == 0) {
            return Status::EndOfFile("No more entries");
        }

        auto group_info = _parse_meta.group_offset_size(0);
        in.seekg(group_info.offset(), std::ios::beg);
        std::string group_serialized(group_info.size(), '\0');
        in.read(&group_serialized[0], group_serialized.size());
        RETURN_IF_ERROR(check_ifstream_status(in, filename));
        uint32_t checksum = crc32c::Value(group_serialized.data(), group_serialized.size());
        if (checksum != group_info.checksum()) {
            std::string warn_msg =
                    fmt::format("restore lru failed as checksum not match, file={}", filename);
            LOG(WARNING) << warn_msg;
            return Status::InternalError(warn_msg);
        }
        if (!_current_parse_group.ParseFromString(group_serialized)) {
            std::string warn_msg =
                    fmt::format("restore lru failed to parse group, file={}", filename);
            LOG(WARNING) << warn_msg;
            return Status::InternalError(warn_msg);
        }

        // Remove processed group info
        _parse_meta.mutable_group_offset_size()->erase(_parse_meta.group_offset_size().begin());
    }

    // Get next entry from current group
    VLOG_DEBUG << "After deserialization: " << _current_parse_group.DebugString();
    auto entry = _current_parse_group.entries(0);
    hash = UInt128Wrapper((static_cast<uint128_t>(entry.hash().high()) << 64) | entry.hash().low());
    offset = entry.offset();
    size = entry.size();

    // Remove processed entry
    _current_parse_group.mutable_entries()->erase(_current_parse_group.entries().begin());
    return Status::OK();
}

void CacheLRUDumper::restore_queue(LRUQueue& queue, const std::string& queue_name,
                                   std::lock_guard<std::mutex>& cache_lock) {
    Status st;
    std::string filename = fmt::format("{}/lru_dump_{}.tail", _mgr->_cache_base_path, queue_name);
    std::ifstream in(filename, std::ios::binary);
    int64_t duration_ns = 0;
    if (in) {
        LOG(INFO) << "lru dump file is founded for " << queue_name << ". starting lru restore.";

        SCOPED_RAW_TIMER(&duration_ns);
        size_t entry_num = 0;
        RETURN_IF_STATUS_ERROR(st, parse_dump_footer(in, filename, entry_num));
        LOG(INFO) << "lru dump file for " << queue_name << " has " << entry_num << " entries.";
        in.seekg(0, std::ios::beg);
        UInt128Wrapper hash;
        size_t offset, size;
        for (int i = 0; i < entry_num; ++i) {
            RETURN_IF_STATUS_ERROR(st, parse_one_lru_entry(in, filename, hash, offset, size));
            CacheContext ctx;
            if (queue_name == "ttl") {
                ctx.cache_type = FileCacheType::TTL;
                // TODO(zhengyu): we haven't persist expiration time yet, use 3h default
                // There are mulitiple places we can correct this fake 3h ttl, e.g.:
                // 1. during load_cache_info_into_memory (this will cause overwriting the ttl of async load)
                // 2. after restoring, use sync_meta to modify the ttl
                // However, I plan not to do this in this commit but to figure a more elegant way
                // after ttl expiration time being changed from file name encoding to rocksdb persistency.
                ctx.expiration_time = 10800;
            } else if (queue_name == "index") {
                ctx.cache_type = FileCacheType::INDEX;
            } else if (queue_name == "normal") {
                ctx.cache_type = FileCacheType::NORMAL;
            } else if (queue_name == "disposable") {
                ctx.cache_type = FileCacheType::DISPOSABLE;
            } else {
                LOG_WARNING("unknown queue type for lru restore, skip");
                DCHECK(false);
                return;
            }
            // TODO(zhengyu): we don't use stats yet, see if this will cause any problem
            _mgr->add_cell(hash, ctx, offset, size, FileBlock::State::DOWNLOADED, cache_lock);
        }
        in.close();
    } else {
        LOG(INFO) << "no lru dump file is founded for " << queue_name;
    }
    LOG(INFO) << "lru restore time costs: " << (duration_ns / 1000) << "us.";
};

void CacheLRUDumper::remove_lru_dump_files() {
    std::vector<std::string> queue_names = {"disposable", "index", "normal", "ttl"};
    for (const auto& queue_name : queue_names) {
        std::string filename =
                fmt::format("{}/lru_dump_{}.tail", _mgr->_cache_base_path, queue_name);
        if (std::filesystem::exists(filename)) {
            std::filesystem::remove(filename);
        }
    }
}

} // end of namespace doris::io

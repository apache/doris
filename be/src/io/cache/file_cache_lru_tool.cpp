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

#include <gflags/gflags.h>

#include <fstream>
#include <iostream>
#include <sstream>

#include "common/status.h"
#include "gen_cpp/file_cache.pb.h"
#include "io/cache/cache_lru_dumper.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/lru_queue_recorder.h"
#include "util/coding.h"
#include "util/crc32c.h"

using namespace doris;

DEFINE_string(filename, "", "dump file name");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE file cache lru tool for examing dumped content.\n";

    ss << "Usage:\n";
    ss << progname << " --filename [filename]\n";
    ss << "\nExample:\n";
    ss << progname << " --filename ./lru_dump_ttl.tail\n";
    return ss.str();
}

Status check_ifstream_status(std::ifstream& in, std::string& filename) {
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
        std::cerr << warn_msg << std::endl;
        return Status::InternalError<false>(warn_msg);
    }

    return Status::OK();
}

struct Footer {
    size_t meta_offset;
    uint32_t checksum;
    uint8_t version;
    char magic[3];

    std::string serialize_as_string() const;
    bool deserialize_from_string(const std::string& data) {
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
} __attribute__((packed));

Status parse_dump_footer(std::ifstream& in, std::string& filename, size_t& entry_num,
                         doris::io::cache::LRUDumpMetaPb& parse_meta,
                         doris::io::cache::LRUDumpEntryGroupPb& current_parse_group) {
    size_t file_size = std::filesystem::file_size(filename);

    // Read footer
    Footer footer;
    size_t footer_size = sizeof(footer);
    if (file_size < footer_size) {
        std::string warn_msg = std::string(fmt::format(
                "LRU dump file too small to contain footer, file={}, skip restore", filename));
        std::cerr << warn_msg << std::endl;
        return Status::InternalError<false>(warn_msg);
    }

    in.seekg(-footer_size, std::ios::end);
    std::string footer_str(footer_size, '\0');
    in.read(&footer_str[0], footer_size);
    RETURN_IF_ERROR(check_ifstream_status(in, filename));

    if (!footer.deserialize_from_string(footer_str)) {
        std::string warn_msg = std::string(
                fmt::format("Failed to deserialize footer, file={}, skip restore", filename));
        std::cerr << warn_msg << std::endl;
        return Status::InternalError<false>(warn_msg);
    }

    // Validate footer
    if (footer.version != 1 || std::string(footer.magic, 3) != "DOR") {
        std::string warn_msg = std::string(fmt::format(
                "LRU dump file invalid footer format, file={}, skip restore", filename));
        std::cerr << warn_msg << std::endl;
        return Status::InternalError<false>(warn_msg);
    }

    // Read meta
    in.seekg(footer.meta_offset, std::ios::beg);
    size_t meta_size = file_size - footer.meta_offset - footer_size;
    if (meta_size <= 0) {
        std::string warn_msg = std::string(
                fmt::format("LRU dump file invalid meta size, file={}, skip restore", filename));
        std::cerr << warn_msg << std::endl;
        return Status::InternalError<false>(warn_msg);
    }
    std::string meta_serialized(meta_size, '\0');
    in.read(&meta_serialized[0], meta_serialized.size());
    RETURN_IF_ERROR(check_ifstream_status(in, filename));
    parse_meta.Clear();
    current_parse_group.Clear();
    if (!parse_meta.ParseFromString(meta_serialized)) {
        std::string warn_msg = std::string(
                fmt::format("LRU dump file meta parse failed, file={}, skip restore", filename));
        std::cerr << warn_msg << std::endl;
        return Status::InternalError<false>(warn_msg);
    }
    std::cout << "parse meta: " << parse_meta.DebugString() << std::endl;

    entry_num = parse_meta.entry_num();
    return Status::OK();
}

Status parse_one_lru_entry(std::ifstream& in, std::string& filename, io::UInt128Wrapper& hash,
                           size_t& offset, size_t& size,
                           doris::io::cache::LRUDumpMetaPb& parse_meta,
                           doris::io::cache::LRUDumpEntryGroupPb& current_parse_group) {
    // Read next group if current is empty
    if (current_parse_group.entries_size() == 0) {
        if (parse_meta.group_offset_size_size() == 0) {
            return Status::EndOfFile("No more entries");
        }

        auto group_info = parse_meta.group_offset_size(0);
        in.seekg(group_info.offset(), std::ios::beg);
        std::string group_serialized(group_info.size(), '\0');
        in.read(&group_serialized[0], group_serialized.size());
        RETURN_IF_ERROR(check_ifstream_status(in, filename));
        uint32_t checksum = crc32c::Value(group_serialized.data(), group_serialized.size());
        if (checksum != group_info.checksum()) {
            std::string warn_msg =
                    fmt::format("restore lru failed as checksum not match, file={}", filename);
            std::cerr << warn_msg << std::endl;
            return Status::InternalError(warn_msg);
        }
        if (!current_parse_group.ParseFromString(group_serialized)) {
            std::string warn_msg =
                    fmt::format("restore lru failed to parse group, file={}", filename);
            std::cerr << warn_msg << std::endl;
            return Status::InternalError(warn_msg);
        }

        // Remove processed group info
        parse_meta.mutable_group_offset_size()->erase(parse_meta.group_offset_size().begin());
    }

    // Get next entry from current group
    std::cout << "After deserialization: " << current_parse_group.DebugString() << std::endl;
    auto entry = current_parse_group.entries(0);
    hash = io::UInt128Wrapper((static_cast<uint128_t>(entry.hash().high()) << 64) |
                              entry.hash().low());
    offset = entry.offset();
    size = entry.size();

    std::cout << hash.to_string() << " " << offset << " " << size << std::endl;

    // Remove processed entry
    current_parse_group.mutable_entries()->erase(current_parse_group.entries().begin());
    return Status::OK();
}

int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    std::ifstream in(FLAGS_filename, std::ios::binary);
    size_t entry_num;
    doris::io::cache::LRUDumpMetaPb parse_meta;
    doris::io::cache::LRUDumpEntryGroupPb current_parse_group;
    auto s = parse_dump_footer(in, FLAGS_filename, entry_num, parse_meta, current_parse_group);

    in.seekg(0, std::ios::beg);
    io::UInt128Wrapper hash;
    size_t offset, size;
    for (int i = 0; i < entry_num; ++i) {
        EXIT_IF_ERROR(parse_one_lru_entry(in, FLAGS_filename, hash, offset, size, parse_meta,
                                          current_parse_group));
    }

    return 0;
}

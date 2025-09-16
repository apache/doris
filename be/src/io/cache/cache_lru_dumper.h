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

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#include "gen_cpp/file_cache.pb.h"
#include "io/cache/file_cache_common.h"

namespace doris::io {
class LRUQueue;
class LRUQueueRecorder;

class CacheLRUDumper {
public:
    CacheLRUDumper(BlockFileCache* mgr, LRUQueueRecorder* recorder)
            : _mgr(mgr), _recorder(recorder) {};
    void dump_queue(const std::string& queue_name);
    void restore_queue(LRUQueue& queue, const std::string& queue_name,
                       std::lock_guard<std::mutex>& cache_lock);
    void remove_lru_dump_files();

private:
    void do_dump_queue(LRUQueue& queue, const std::string& queue_name);
    Status check_ofstream_status(std::ofstream& out, std::string& filename);
    Status check_ifstream_status(std::ifstream& in, std::string& filename);
    Status dump_one_lru_entry(std::ofstream& out, std::string& filename, const UInt128Wrapper& hash,
                              size_t offset, size_t size);
    Status finalize_dump(std::ofstream& out, size_t entry_num, std::string& tmp_filename,
                         std::string& final_filename, size_t& file_size);
    Status parse_dump_footer(std::ifstream& in, std::string& filename, size_t& entry_num);
    Status parse_one_lru_entry(std::ifstream& in, std::string& filename, UInt128Wrapper& hash,
                               size_t& offset, size_t& size);
    Status flush_current_group(std::ofstream& out, std::string& filename);

    struct Footer {
        size_t meta_offset;
        uint32_t checksum;
        uint8_t version;
        char magic[3];

        std::string serialize_as_string() const;
        bool deserialize_from_string(const std::string& data);
    } __attribute__((packed));

private:
    // For dumping
    doris::io::cache::LRUDumpEntryGroupPb _current_dump_group;
    doris::io::cache::LRUDumpMetaPb _dump_meta;
    size_t _current_dump_group_count = 0;

    // For parsing
    doris::io::cache::LRUDumpEntryGroupPb _current_parse_group;
    doris::io::cache::LRUDumpMetaPb _parse_meta;

    BlockFileCache* _mgr;
    LRUQueueRecorder* _recorder;
};
} // namespace doris::io
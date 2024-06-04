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

#include "common/status.h"
#include "gen_cpp/internal_service.pb.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace doris {

using PBlockArray = std::vector<PBlock*>;
extern const char* k_wal_magic;
extern const uint32_t k_wal_magic_length;

class WalWriter {
public:
    explicit WalWriter(const std::string& file_name);
    ~WalWriter();

    Status init();
    Status finalize();

    Status append_blocks(const PBlockArray& blocks);
    Status append_header(std::string col_ids);

    std::string file_name() { return _file_name; };

public:
    static const int64_t LENGTH_SIZE = 8;
    static const int64_t CHECKSUM_SIZE = 4;
    static const int64_t VERSION_SIZE = 4;

private:
    std::string _file_name;
    io::FileWriterPtr _file_writer;
};

} // namespace doris
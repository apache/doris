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

class WalReader {
public:
    explicit WalReader(const std::string& file_name);
    ~WalReader();

    Status init();
    Status finalize();

    Status read_block(PBlock& block);

private:
    Status _deserialize(PBlock& block, std::string& buf);
    Status _check_checksum(const char* binary, size_t size, uint32_t checksum);

    std::string _file_name;
    size_t _offset;
    io::FileReaderSPtr file_reader;
};

} // namespace doris
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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_WAL_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_WAL_READER_H

#include "gen_cpp/internal_service.pb.h"
#include "olap/file_helper.h"

namespace doris {

class WalReader {
public:
    explicit WalReader(const std::string& file_name);
    ~WalReader();

    Status init();
    Status finalize();

    Status read_row(PDataRow& row);

    uint64_t file_length() { return _file_handler.length(); };

private:
    Status _deserialize(PDataRow& row_pb, const void* row_binary, size_t size);

    std::string _file_name;
    size_t _offset;
    FileHandler _file_handler;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_WAL_READER_H
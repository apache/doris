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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_WAL_WRITER_H
#define DORIS_BE_SRC_OLAP_ROWSET_WAL_WRITER_H

#include "gen_cpp/internal_service.pb.h"
#include "olap/file_helper.h"

namespace doris {

using PDataRowArray = google::protobuf::RepeatedPtrField<PDataRow>;

class WalWriter {
public:
    explicit WalWriter(const std::string& file_name);
    ~WalWriter();

    Status init();
    Status finalize();

    Status append_rows(const PDataRowArray& rows);

    std::string file_name() { return _file_name; };
    int64_t row_count() { return _row_count; };
    uint64_t file_length() { return _file_handler.length(); };
    uint64_t elapsed_time() { return timer.elapsed_time(); };

    static const int64_t ROW_LENGTH_SIZE = 8;

private:
    std::string _file_name;
    uint64_t _row_count;
    MonotonicStopWatch timer;
    FileHandler _file_handler;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_WAL_WRITER_H
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

#include "olap/wal_reader.h"

#include "common/status.h"
#include "olap/storage_engine.h"
#include "olap/wal_writer.h"

namespace doris {

WalReader::WalReader(const std::string& file_name) : _file_name(file_name), _offset(0) {}

WalReader::~WalReader() {}

Status WalReader::init() {
    Status st = _file_handler.open(_file_name, O_RDONLY);
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "fail to open wal file: '" << _file_name << "', error: " << st.to_string();
    }
    return st;
}

Status WalReader::finalize() {
    Status st = _file_handler.close();
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "fail to close wal file: '" << _file_name << "', error: " << st.to_string();
    }
    return st;
}

Status WalReader::read_row(PDataRow& row) {
    if (_offset >= _file_handler.length()) {
        return Status::EndOfFile("end of wal file");
    }
    // read row length
    uint8_t row_len_buf[WalWriter::ROW_LENGTH_SIZE];
    RETURN_IF_ERROR(_file_handler.pread(row_len_buf, WalWriter::ROW_LENGTH_SIZE, _offset));
    _offset += WalWriter::ROW_LENGTH_SIZE;
    size_t row_len;
    memcpy(&row_len, row_len_buf, WalWriter::ROW_LENGTH_SIZE);
    // read row
    uint8_t row_buf[row_len];
    RETURN_IF_ERROR(_file_handler.pread(row_buf, row_len, _offset));
    _offset += row_len;
    RETURN_IF_ERROR(_deserialize(row, &row_buf, row_len));
    return Status::OK();
}

Status WalReader::_deserialize(PDataRow& row_pb, const void* row_binary, size_t size) {
    if (UNLIKELY(!row_pb.ParseFromArray(row_binary, size))) {
        return Status::InternalError("failed to deserialize row");
    }
    return Status::OK();
}

} // namespace doris

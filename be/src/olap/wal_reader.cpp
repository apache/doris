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
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "util/crc32c.h"
#include "wal_writer.h"

namespace doris {

WalReader::WalReader(const std::string& file_name) : _file_name(file_name), _offset(0) {}

WalReader::~WalReader() {}

Status WalReader::init() {
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(_file_name, &file_reader));
    return Status::OK();
}

Status WalReader::finalize() {
    auto st = file_reader->close();
    if (!st.ok()) {
        LOG(WARNING) << "fail to close file " << _file_name;
    }
    return Status::OK();
}

Status WalReader::read_block(PBlock& block) {
    if (_offset >= file_reader->size()) {
        return Status::EndOfFile("end of wal file");
    }
    size_t bytes_read = 0;
    uint8_t row_len_buf[WalWriter::LENGTH_SIZE];
    RETURN_IF_ERROR(
            file_reader->read_at(_offset, {row_len_buf, WalWriter::LENGTH_SIZE}, &bytes_read));
    _offset += WalWriter::LENGTH_SIZE;
    size_t block_len;
    memcpy(&block_len, row_len_buf, WalWriter::LENGTH_SIZE);
    // read block
    std::string block_buf;
    block_buf.resize(block_len);
    RETURN_IF_ERROR(file_reader->read_at(_offset, {block_buf.c_str(), block_len}, &bytes_read));
    _offset += block_len;
    RETURN_IF_ERROR(_deserialize(block, block_buf));
    // checksum
    uint8_t checksum_len_buf[WalWriter::CHECKSUM_SIZE];
    RETURN_IF_ERROR(file_reader->read_at(_offset, {checksum_len_buf, WalWriter::CHECKSUM_SIZE},
                                         &bytes_read));
    _offset += WalWriter::CHECKSUM_SIZE;
    uint32_t checksum;
    memcpy(&checksum, checksum_len_buf, WalWriter::CHECKSUM_SIZE);
    RETURN_IF_ERROR(_check_checksum(block_buf.data(), block_len, checksum));
    return Status::OK();
}

Status WalReader::_deserialize(PBlock& block, std::string& buf) {
    if (UNLIKELY(!block.ParseFromString(buf))) {
        return Status::InternalError("failed to deserialize row");
    }
    return Status::OK();
}

Status WalReader::_check_checksum(const char* binary, size_t size, uint32_t checksum) {
    uint32_t computed_checksum = crc32c::Value(binary, size);
    if (LIKELY(computed_checksum == checksum)) {
        return Status::OK();
    }
    return Status::InternalError("checksum failed for wal=" + _file_name +
                                 ", computed checksum=" + std::to_string(computed_checksum) +
                                 ", expected=" + std::to_string(checksum));
}

} // namespace doris

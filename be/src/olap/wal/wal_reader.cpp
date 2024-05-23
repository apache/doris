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

#include "olap/wal/wal_reader.h"

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "wal_writer.h"

namespace doris {

WalReader::WalReader(const std::string& file_name) : _file_name(file_name), _offset(0) {}

WalReader::~WalReader() = default;

static Status _deserialize(PBlock& block, const std::string& buf) {
    if (UNLIKELY(!block.ParseFromString(buf))) {
        return Status::InternalError("failed to deserialize row");
    }
    return Status::OK();
}

Status WalReader::init() {
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(_file_name, &exists));
    if (!exists) {
        LOG(WARNING) << "not exist wal= " << _file_name;
        return Status::NotFound("wal {} doesn't exist", _file_name);
    }
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(_file_name, &file_reader));
    return Status::OK();
}

Status WalReader::finalize() {
    if (file_reader) {
        return file_reader->close();
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
    size_t block_len = decode_fixed64_le(row_len_buf);
    if (block_len == 0) {
        return Status::DataQualityError("fail to read wal {} ,block is empty", _file_name);
    }
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
    uint32_t checksum = decode_fixed32_le(checksum_len_buf);
    RETURN_IF_ERROR(_check_checksum(block_buf.data(), block_len, checksum));
    return Status::OK();
}

Status WalReader::read_header(uint32_t& version, std::string& col_ids) {
    if (file_reader->size() == 0) {
        return Status::DataQualityError("empty file");
    }
    size_t bytes_read = 0;
    std::string magic_str;
    magic_str.resize(k_wal_magic_length);
    RETURN_IF_ERROR(file_reader->read_at(_offset, magic_str, &bytes_read));
    if (strcmp(magic_str.c_str(), k_wal_magic) != 0) {
        return Status::Corruption("Bad wal file {}: magic number not match", _file_name);
    }
    _offset += k_wal_magic_length;
    uint8_t version_buf[WalWriter::VERSION_SIZE];
    RETURN_IF_ERROR(
            file_reader->read_at(_offset, {version_buf, WalWriter::VERSION_SIZE}, &bytes_read));
    _offset += WalWriter::VERSION_SIZE;
    version = decode_fixed32_le(version_buf);
    uint8_t len_buf[WalWriter::LENGTH_SIZE];
    RETURN_IF_ERROR(file_reader->read_at(_offset, {len_buf, WalWriter::LENGTH_SIZE}, &bytes_read));
    _offset += WalWriter::LENGTH_SIZE;
    size_t len = decode_fixed64_le(len_buf);
    col_ids.resize(len);
    RETURN_IF_ERROR(file_reader->read_at(_offset, col_ids, &bytes_read));
    _offset += len;
    if (len != bytes_read) {
        return Status::InternalError("failed to read header expected= " + std::to_string(len) +
                                     ",actually=" + std::to_string(bytes_read));
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

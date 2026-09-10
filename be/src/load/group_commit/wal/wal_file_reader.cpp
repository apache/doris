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

#include "load/group_commit/wal/wal_file_reader.h"

#include <crc32c/crc32c.h>

#include <string_view>
#include <utility>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "load/group_commit/wal/wal_writer.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace doris {

WalFileReader::WalFileReader(const std::string& file_name) : _file_name(file_name), _offset(0) {}

WalFileReader::~WalFileReader() = default;
Status WalFileReader::_deserialize(PBlock& block, const std::string& buf, size_t block_len,
                                   size_t bytes_read) {
    if (UNLIKELY(!block.ParseFromString(buf))) {
        return Status::InternalError(
                "failed to deserialize row, file_size=" + std::to_string(file_reader->size()) +
                ", read_offset=" + std::to_string(_offset) + +", block_bytes=" +
                std::to_string(block_len) + ", read_block_bytes=" + std::to_string(bytes_read));
    }
    return Status::OK();
}

std::pair<int64_t, int64_t> parse_db_tb_from_wal_path(const std::string& wal_path) {
    auto ret = split(wal_path, "/");
    DCHECK_GT(ret.size(), 3);
    auto db_id_pos = ret.size() - 1 - 2;
    auto tb_id_pos = ret.size() - 1 - 1;
    auto db_id = std::stoll(ret[db_id_pos]);
    auto tb_id = std::stoll(ret[tb_id_pos]);

    return {db_id, tb_id};
}

Status WalFileReader::init() {
    auto [db_id, tb_id] = parse_db_tb_from_wal_path(_file_name);
    io::FileSystemSPtr fs;
    RETURN_IF_ERROR(determine_wal_fs(db_id, tb_id, fs));
    bool exists = false;
    RETURN_IF_ERROR(fs->exists(_file_name, &exists));
    if (!exists) {
        LOG(WARNING) << "not exist wal= " << _file_name;
        return Status::NotFound("wal {} doesn't exist", _file_name);
    }
    RETURN_IF_ERROR(fs->open_file(_file_name, &file_reader));

    return Status::OK();
}

Status WalFileReader::finalize() {
    if (file_reader) {
        return file_reader->close();
    }
    return Status::OK();
}

Status WalFileReader::read_block(PBlock& block) {
    if (_offset >= file_reader->size()) {
        return Status::EndOfFile("end of wal file");
    }
    const size_t file_size = file_reader->size();
    if (file_size - _offset < WalWriter::LENGTH_SIZE) {
        LOG(WARNING) << "ignore incomplete wal tail, path=" << _file_name
                     << ", file_size=" << file_size << ", read_offset=" << _offset
                     << ", expected_length_bytes=" << WalWriter::LENGTH_SIZE;
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
    const size_t remaining_bytes = file_size - _offset;
    if (block_len > remaining_bytes) {
        LOG(WARNING) << "ignore incomplete wal tail, path=" << _file_name
                     << ", file_size=" << file_size << ", read_offset=" << _offset
                     << ", block_bytes=" << block_len << ", available_bytes=" << remaining_bytes;
        return Status::EndOfFile("end of wal file");
    }
    // read block
    std::string block_buf;
    block_buf.resize(block_len);
    RETURN_IF_ERROR(file_reader->read_at(_offset, {block_buf.c_str(), block_len}, &bytes_read));
    if (bytes_read != block_len) {
        LOG(WARNING) << "ignore incomplete wal tail, path=" << _file_name
                     << ", file_size=" << file_size << ", read_offset=" << _offset
                     << ", block_bytes=" << block_len << ", read_block_bytes=" << bytes_read;
        return Status::EndOfFile("end of wal file");
    }
    RETURN_IF_ERROR(_deserialize(block, block_buf, block_len, bytes_read));
    _offset += block_len;
    const size_t checksum_bytes = file_size - _offset;
    if (checksum_bytes < WalWriter::CHECKSUM_SIZE) {
        LOG(WARNING) << "replay wal block without complete checksum, path=" << _file_name
                     << ", file_size=" << file_size << ", read_offset=" << _offset
                     << ", block_bytes=" << block_len << ", checksum_bytes=" << checksum_bytes
                     << ", expected_checksum_bytes=" << WalWriter::CHECKSUM_SIZE;
        _offset = file_size;
        return Status::OK();
    }
    // checksum
    uint8_t checksum_len_buf[WalWriter::CHECKSUM_SIZE];
    RETURN_IF_ERROR(file_reader->read_at(_offset, {checksum_len_buf, WalWriter::CHECKSUM_SIZE},
                                         &bytes_read));
    _offset += WalWriter::CHECKSUM_SIZE;
    uint32_t checksum = decode_fixed32_le(checksum_len_buf);
    RETURN_IF_ERROR(_check_checksum(block_buf.data(), block_len, checksum));
    return Status::OK();
}

Status WalFileReader::read_header(uint32_t& version, std::string& col_ids) {
    const size_t file_size = file_reader->size();
    auto incomplete_header = [&](const char* field, size_t expected_bytes, size_t actual_bytes) {
        LOG(WARNING) << "ignore incomplete wal header, path=" << _file_name
                     << ", file_size=" << file_size << ", read_offset=" << _offset
                     << ", field=" << field << ", expected_bytes=" << expected_bytes
                     << ", actual_bytes=" << actual_bytes;
        return Status::DataQualityError(
                "incomplete wal header {}, field={}, expected_bytes={}, actual_bytes={}",
                _file_name, field, expected_bytes, actual_bytes);
    };
    size_t bytes_read = 0;
    if (file_size - _offset < k_wal_magic_length) {
        return incomplete_header("magic", k_wal_magic_length, file_size - _offset);
    }
    std::string magic_str;
    magic_str.resize(k_wal_magic_length);
    RETURN_IF_ERROR(file_reader->read_at(_offset, magic_str, &bytes_read));
    if (bytes_read != k_wal_magic_length) {
        return incomplete_header("magic", k_wal_magic_length, bytes_read);
    }
    if (strcmp(magic_str.c_str(), k_wal_magic) != 0) {
        return Status::Corruption("Bad wal file {}: magic number not match", _file_name);
    }
    _offset += k_wal_magic_length;
    if (file_size - _offset < WalWriter::VERSION_SIZE) {
        return incomplete_header("version", WalWriter::VERSION_SIZE, file_size - _offset);
    }
    uint8_t version_buf[WalWriter::VERSION_SIZE];
    RETURN_IF_ERROR(
            file_reader->read_at(_offset, {version_buf, WalWriter::VERSION_SIZE}, &bytes_read));
    if (bytes_read != WalWriter::VERSION_SIZE) {
        return incomplete_header("version", WalWriter::VERSION_SIZE, bytes_read);
    }
    _offset += WalWriter::VERSION_SIZE;
    version = decode_fixed32_le(version_buf);
    if (file_size - _offset < WalWriter::LENGTH_SIZE) {
        return incomplete_header("column_ids_length", WalWriter::LENGTH_SIZE, file_size - _offset);
    }
    uint8_t len_buf[WalWriter::LENGTH_SIZE];
    RETURN_IF_ERROR(file_reader->read_at(_offset, {len_buf, WalWriter::LENGTH_SIZE}, &bytes_read));
    if (bytes_read != WalWriter::LENGTH_SIZE) {
        return incomplete_header("column_ids_length", WalWriter::LENGTH_SIZE, bytes_read);
    }
    _offset += WalWriter::LENGTH_SIZE;
    size_t len = decode_fixed64_le(len_buf);
    if (len > file_size - _offset) {
        return incomplete_header("column_ids", len, file_size - _offset);
    }
    col_ids.resize(len);
    RETURN_IF_ERROR(file_reader->read_at(_offset, col_ids, &bytes_read));
    if (len != bytes_read) {
        return incomplete_header("column_ids", len, bytes_read);
    }
    _offset += len;
    return Status::OK();
}

Status WalFileReader::_check_checksum(const char* binary, size_t size, uint32_t checksum) {
    uint32_t computed_checksum = crc32c::Crc32c(binary, size);
    if (LIKELY(computed_checksum == checksum)) {
        return Status::OK();
    }
    return Status::InternalError("checksum failed for wal=" + _file_name +
                                 ", computed checksum=" + std::to_string(computed_checksum) +
                                 ", expected=" + std::to_string(checksum));
}

} // namespace doris

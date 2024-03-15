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

#include <stdio.h>
#include <sys/stat.h>

#include <memory>
#include <string>
#include <vector>

#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/lru_cache.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/utils.h"
#include "util/debug_util.h"

namespace doris {

using FixedFileHeader = struct _FixedFileHeader {
    // the length of the entire file
    uint32_t file_length;
    // Checksum of the file's contents except the FileHeader
    uint32_t checksum;
    // Protobuf length of section
    uint32_t protobuf_length;
    // Checksum of Protobuf part
    uint32_t protobuf_checksum;
} __attribute__((packed));

using FixedFileHeaderV2 = struct _FixedFileHeaderV2 {
    uint64_t magic_number;
    uint32_t version;
    // the length of the entire file
    uint64_t file_length;
    // Checksum of the file's contents except the FileHeader
    uint32_t checksum;
    // Protobuf length of section
    uint64_t protobuf_length;
    // Checksum of Protobuf part
    uint32_t protobuf_checksum;
} __attribute__((packed));

template <typename MessageType, typename ExtraType = uint32_t>
class FileHeader {
public:
    FileHeader(const std::string& file_path) : _file_path(file_path) {
        memset(&_fixed_file_header, 0, sizeof(_fixed_file_header));
        memset(&_extra_fixed_header, 0, sizeof(_extra_fixed_header));
        _fixed_file_header_size = sizeof(_fixed_file_header);
    }
    ~FileHeader() = default;

    // To calculate the length of the proto part, it needs to be called after the proto is operated,
    // and prepare must be called before calling serialize
    Status prepare();

    // call prepare() first, serialize() will write fixed header and protobuffer.
    // Write the header to the starting position of the incoming file handle
    Status serialize();

    // read from file, validate file length, signature and alder32 of protobuffer.
    // Read the header from the beginning of the incoming file handle
    Status deserialize();

    // Check the validity of Header
    // it is actually call deserialize().
    Status validate();

    uint64_t file_length() const { return _fixed_file_header.file_length; }
    uint32_t checksum() const { return _fixed_file_header.checksum; }
    const ExtraType& extra() const { return _extra_fixed_header; }
    ExtraType* mutable_extra() { return &_extra_fixed_header; }
    const MessageType& message() const { return _proto; }
    MessageType* mutable_message() { return &_proto; }
    uint64_t size() const {
        return _fixed_file_header_size + sizeof(_extra_fixed_header) +
               _fixed_file_header.protobuf_length;
    }

    void set_file_length(uint64_t file_length) { _fixed_file_header.file_length = file_length; }
    void set_checksum(uint32_t checksum) { _fixed_file_header.checksum = checksum; }

private:
    std::string _file_path;
    FixedFileHeaderV2 _fixed_file_header;
    uint32_t _fixed_file_header_size;

    std::string _proto_string;
    ExtraType _extra_fixed_header;
    MessageType _proto;
};

// FileHeader implementation
template <typename MessageType, typename ExtraType>
Status FileHeader<MessageType, ExtraType>::prepare() {
    try {
        if (!_proto.SerializeToString(&_proto_string)) {
            return Status::Error<ErrorCode::SERIALIZE_PROTOBUF_ERROR>(
                    "serialize file header to string error. [path={}]", _file_path);
        }
    } catch (...) {
        return Status::Error<ErrorCode::SERIALIZE_PROTOBUF_ERROR>(
                "serialize file header to string error. [path={}]", _file_path);
    }

    _fixed_file_header.protobuf_checksum =
            olap_adler32(olap_adler32_init(), _proto_string.c_str(), _proto_string.size());

    _fixed_file_header.checksum = 0;
    _fixed_file_header.protobuf_length = _proto_string.size();
    _fixed_file_header.file_length = size();
    _fixed_file_header.version = OLAP_DATA_VERSION_APPLIED;
    _fixed_file_header.magic_number = OLAP_FIX_HEADER_MAGIC_NUMBER;

    return Status::OK();
}

template <typename MessageType, typename ExtraType>
Status FileHeader<MessageType, ExtraType>::serialize() {
    // write to file
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(_file_path, &file_writer));
    RETURN_IF_ERROR(
            file_writer->append({(const uint8_t*)&_fixed_file_header, _fixed_file_header_size}));
    RETURN_IF_ERROR(file_writer->append(
            {(const uint8_t*)&_extra_fixed_header, sizeof(_extra_fixed_header)}));
    RETURN_IF_ERROR(file_writer->append({_proto_string}));
    return file_writer->close();
}

template <typename MessageType, typename ExtraType>
Status FileHeader<MessageType, ExtraType>::deserialize() {
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(_file_path, &file_reader));
    off_t real_file_length = 0;
    uint32_t real_protobuf_checksum = 0;
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader->read_at(
            0, {(const uint8_t*)&_fixed_file_header, _fixed_file_header_size}, &bytes_read));
    DCHECK(_fixed_file_header_size == bytes_read)
            << " deserialize read bytes dismatch, request bytes " << _fixed_file_header_size
            << " actual read " << bytes_read;

    //Status read_at(size_t offset, Slice result, size_t* bytes_read,
    //             const IOContext* io_ctx = nullptr);

    if (_fixed_file_header.magic_number != OLAP_FIX_HEADER_MAGIC_NUMBER) {
        VLOG_TRACE << "old fix header found, magic num=" << _fixed_file_header.magic_number;
        FixedFileHeader tmp_header;
        RETURN_IF_ERROR(file_reader->read_at(0, {(const uint8_t*)&tmp_header, sizeof(tmp_header)},
                                             &bytes_read));
        DCHECK(sizeof(tmp_header) == bytes_read)
                << " deserialize read bytes dismatch, request bytes " << sizeof(tmp_header)
                << " actual read " << bytes_read;
        _fixed_file_header.file_length = tmp_header.file_length;
        _fixed_file_header.checksum = tmp_header.checksum;
        _fixed_file_header.protobuf_length = tmp_header.protobuf_length;
        _fixed_file_header.protobuf_checksum = tmp_header.protobuf_checksum;
        _fixed_file_header.magic_number = OLAP_FIX_HEADER_MAGIC_NUMBER;
        _fixed_file_header.version = OLAP_DATA_VERSION_APPLIED;
        _fixed_file_header_size = sizeof(tmp_header);
    }

    VLOG_NOTICE << "fix head loaded. file_length=" << _fixed_file_header.file_length
                << ", checksum=" << _fixed_file_header.checksum
                << ", protobuf_length=" << _fixed_file_header.protobuf_length
                << ", magic_number=" << _fixed_file_header.magic_number
                << ", version=" << _fixed_file_header.version;

    RETURN_IF_ERROR(file_reader->read_at(
            _fixed_file_header_size,
            {(const uint8_t*)&_extra_fixed_header, sizeof(_extra_fixed_header)}, &bytes_read));

    std::unique_ptr<char[]> buf(new (std::nothrow) char[_fixed_file_header.protobuf_length]);
    if (nullptr == buf) {
        char errmsg[64];
        return Status::Error<ErrorCode::MEM_ALLOC_FAILED>(
                "malloc protobuf buf error. file={}, error={}", file_reader->path().native(),
                strerror_r(errno, errmsg, 64));
    }
    RETURN_IF_ERROR(file_reader->read_at(_fixed_file_header_size + sizeof(_extra_fixed_header),
                                         {buf.get(), _fixed_file_header.protobuf_length},
                                         &bytes_read));
    real_file_length = file_reader->size();

    if (file_length() != static_cast<uint64_t>(real_file_length)) {
        return Status::InternalError(
                "file length is not match. file={}, file_length={}, real_file_length={}",
                file_reader->path().native(), file_length(), real_file_length);
    }

    // check proto checksum
    real_protobuf_checksum =
            olap_adler32(olap_adler32_init(), buf.get(), _fixed_file_header.protobuf_length);

    if (real_protobuf_checksum != _fixed_file_header.protobuf_checksum) {
        // When compiling using gcc there woule be error like:
        // Cannot bind packed field '_FixedFileHeaderV2::protobuf_checksum' to 'unsigned int&'
        // so we need to using unary operator+ to evaluate one value to pass
        // to status to successfully compile.
        return Status::InternalError("checksum is not match. file={}, expect={}, actual={}",
                                     file_reader->path().native(),
                                     +_fixed_file_header.protobuf_checksum, real_protobuf_checksum);
    }

    try {
        std::string protobuf_str(buf.get(), _fixed_file_header.protobuf_length);

        if (!_proto.ParseFromString(protobuf_str)) {
            return Status::Error<ErrorCode::PARSE_PROTOBUF_ERROR>(
                    "fail to parse file content to protobuf object. file={}",
                    file_reader->path().native());
        }
    } catch (...) {
        LOG(WARNING) << "fail to load protobuf. file='" << file_reader->path().native();
        return Status::Error<ErrorCode::PARSE_PROTOBUF_ERROR>("fail to load protobuf. file={}",
                                                              file_reader->path().native());
    }

    return Status::OK();
}

template <typename MessageType, typename ExtraType>
Status FileHeader<MessageType, ExtraType>::validate() {
    return deserialize();
}

} // namespace doris

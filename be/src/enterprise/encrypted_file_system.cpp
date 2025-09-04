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

#include "enterprise/encrypted_file_system.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"
#include "enterprise/encrypted_file_reader.h"
#include "enterprise/encrypted_file_writer.h"
#include "enterprise/encryption_common.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/io_common.h"
#include "util/coding.h"

namespace doris::io {

// info_pb_len(uint32_t) + version(uint8_t) + magic_code(uint64_t)
constexpr uint64_t footer_info_len = sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint64_t);

Status EncryptedFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                             const FileWriterOptions* opts) {
    auto maybe_encryption_info = io::EncryptionInfo::create(_algorithm);
    if (!maybe_encryption_info) {
        return Status::InternalError("create encryption info error: {}",
                                     maybe_encryption_info.error());
    }
    auto encryption_info = std::move(maybe_encryption_info.value());

    switch (encryption_info->data_key->algorithm()) {
    case EncryptionAlgorithmPB::AES_256_CTR:
    case EncryptionAlgorithmPB::SM4_128_CTR:
        break;
    default:
        return Status::InvalidArgument(
                "Invalid encryption mode {}, only support AES_256_CTR or SM4_128_CTR for now",
                encryption_info->data_key->algorithm());
    }

    RETURN_IF_ERROR(_fs_inner->create_file(file, writer, opts));
    *writer = std::make_unique<EncryptedFileWriter>(std::move(*writer), std::move(encryption_info));
    return Status::OK();
}

Result<std::unique_ptr<EncryptionInfo>> parse_footer(std::span<uint8_t> footer) {
    auto magic_code_span = footer.subspan(footer.size() - sizeof(uint64_t));
    auto magic_code = decode_fixed64_le(magic_code_span.data());
    if (magic_code != MAGIC_CODE) {
        return ResultError(Status::Corruption("Magic code={} is unexpected", magic_code));
    }

    auto version_span = footer.subspan(0, sizeof(uint8_t));
    auto version = decode_fixed8(version_span.data());
    if (version != 0) {
        return ResultError(
                Status::Corruption("Version={} is unexpected, expected={}", version, VERSION));
    }

    auto info_pb_size_span = footer.subspan(sizeof(uint8_t), sizeof(uint64_t));
    auto info_pb_size = decode_fixed64_le(info_pb_size_span.data());
    if (info_pb_size > ENCRYPT_FOOTER_LENGTH) {
        return ResultError(Status::Corruption(
                "Encryption info pb size should be less than encrypt footer length, but real={}",
                info_pb_size));
    }

    auto info_pb_span = footer.subspan(sizeof(uint8_t) + sizeof(uint64_t), info_pb_size);
    FileEncryptionInfoPB info_pb;
    if (!info_pb.ParseFromArray(info_pb_span.data(), info_pb_span.size())) {
        return ResultError(Status::Corruption("parse encryption info failed"));
    }

    return EncryptionInfo::load(info_pb);
}

Status EncryptedFileSystem::open_file_impl(const Path& file, FileReaderSPtr* reader,
                                           const FileReaderOptions* opts) {
    size_t file_size;
    size_t footer_start;
    FileReaderSPtr reader_inner;
    if (opts != nullptr && opts->file_size != -1) {
        file_size = opts->file_size + ENCRYPT_FOOTER_LENGTH;
        footer_start = opts->file_size;
        FileReaderOptions tmp_opts = *opts;
        tmp_opts.file_size = file_size;
        RETURN_IF_ERROR(_fs_inner->open_file(file, reader, &tmp_opts));
        reader_inner = *reader;
    } else {
        RETURN_IF_ERROR(_fs_inner->open_file(file, reader, opts));
        reader_inner = *reader;
        file_size = reader_inner->size();
        footer_start = file_size - ENCRYPT_FOOTER_LENGTH;
    }

    size_t bytes_read;
    std::vector<uint8_t> footer_buf;
    footer_buf.reserve(ENCRYPT_FOOTER_LENGTH);
    Slice footer(footer_buf.data(), ENCRYPT_FOOTER_LENGTH);

    IOContext dummy_io_ctx;
    RETURN_IF_ERROR(reader_inner->read_at(footer_start, footer, &bytes_read, &dummy_io_ctx));
    if (bytes_read != ENCRYPT_FOOTER_LENGTH) {
        return Status::Corruption(
                "Insufficient bytes for footer of encrypted file, expected={}, real={}",
                ENCRYPT_FOOTER_LENGTH, bytes_read);
    }
    auto encryption_info =
            DORIS_TRY(parse_footer({reinterpret_cast<uint8_t*>(footer.data), footer.size}));

    *reader = std::make_shared<EncryptedFileReader>(
            std::move(reader_inner), std::move(encryption_info), file_size - ENCRYPT_FOOTER_LENGTH);
    return Status::OK();
}

Status EncryptedFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    return _fs_inner->create_directory(dir, failed_if_exists);
}

Status EncryptedFileSystem::delete_file_impl(const Path& file) {
    return _fs_inner->delete_file(file);
}

Status EncryptedFileSystem::delete_directory_impl(const Path& dir) {
    return _fs_inner->delete_directory(dir);
}

Status EncryptedFileSystem::batch_delete_impl(const std::vector<Path>& files) {
    return _fs_inner->batch_delete(files);
}

Status EncryptedFileSystem::exists_impl(const Path& path, bool* res) const {
    return _fs_inner->exists(path, res);
}

Status EncryptedFileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    return _fs_inner->file_size(file, file_size);
}

Status EncryptedFileSystem::list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                                      bool* exists) {
    return _fs_inner->list(dir, only_file, files, exists);
}

Status EncryptedFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    return _fs_inner->rename(orig_name, new_name);
}

Status EncryptedFileSystem::absolute_path(const Path& path, Path& abs_path) const {
    abs_path = path;
    return Status::OK();
}

} // namespace doris::io

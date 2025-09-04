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

#include "enterprise/encrypted_file_reader.h"

#include <bvar/latency_recorder.h>
#include <bvar/window.h>
#include <glog/logging.h>
#include <openssl/evp.h>

#include <cstddef>
#include <cstdint>

#include "common/status.h"
#include "encryption_common.h"

namespace doris::io {

bvar::Adder<uint64_t> decrypted_bytes_read("encrypted_file_writer", "bytes_read");
bvar::PerSecond<bvar::Adder<uint64_t>> decryption_throughput("encrypted_file_writer",
                                                             "decryption_throughput",
                                                             &decrypted_bytes_read);

Status decrypt_ctr(const EncryptionInfo* info, size_t offset, Slice result) {
    const auto* cipher = info->evp_cipher();
    if (cipher == nullptr) {
        return Status::InternalError("failed to get cipher");
    }
    auto* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        return Status::InternalError("failed to create cipher context");
    }
    Defer defer {[ctx]() { EVP_CIPHER_CTX_free(ctx); }};

    auto iv_gen = generate_iv(info->iv_base, offset);
    auto* iv = iv_gen.data();

    if (EVP_DecryptInit_ex(ctx, cipher, nullptr, info->plain_key().data(),
                           reinterpret_cast<const unsigned char*>(iv)) != 1) {
        return Status::InternalError("decryption init failed");
    }
    EVP_CIPHER_CTX_set_padding(ctx, info->padding);

    auto mod = offset % ENCRYPT_BLOCK_SIZE;
    if (mod != 0) {
        std::vector<unsigned char> dummy(mod);
        int out_len;
        if (EVP_DecryptUpdate(ctx, dummy.data(), &out_len, dummy.data(), mod) != 1) {
            return Status::InternalError("decrypt dummy part error");
        }
        DCHECK_LE(out_len, ENCRYPT_BLOCK_SIZE);
    }

    int out_len;
    if (EVP_DecryptUpdate(ctx, reinterpret_cast<uint8_t*>(result.mutable_data()), &out_len,
                          reinterpret_cast<uint8_t*>(result.data), result.size) != 1) {
        return Status::InternalError("decrypt error, offset={}", offset);
    }

    DCHECK_EQ(out_len, result.size);
    return Status::OK();
}

Status EncryptedFileReader::close() {
    return _reader_inner->close();
}

const Path& EncryptedFileReader::path() const {
    return _reader_inner->path();
}

size_t EncryptedFileReader::size() const {
    return _file_size;
}

bool EncryptedFileReader::closed() const {
    return _reader_inner->closed();
}

Status EncryptedFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                         const IOContext* io_ctx) {
    const auto decrypt_fsize = size();
    if (offset > decrypt_fsize) {
        return Status::InvalidArgument("offset={} exceeded file size, file_size={}", offset,
                                       decrypt_fsize);
    }

    Slice encrypted_result(result.data, std::min(result.size, decrypt_fsize - offset));
    RETURN_IF_ERROR(_reader_inner->read_at(offset, encrypted_result, bytes_read, io_ctx));
    if (*bytes_read == 0) {
        return Status::OK();
    }

    RETURN_IF_ERROR(decrypt_ctr(_encryption_info.get(), offset, encrypted_result));

    decrypted_bytes_read << *bytes_read;
    return Status::OK();
}

} // namespace doris::io

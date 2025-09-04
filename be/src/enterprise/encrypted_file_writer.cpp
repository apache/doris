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

#include "enterprise/encrypted_file_writer.h"

#include <bvar/latency_recorder.h>
#include <bvar/window.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <openssl/rand.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <numeric>
#include <ranges>
#include <span>

#include "common/status.h"
#include "enterprise/encryption_common.h"
#include "util/coding.h"
#include "util/defer_op.h"

namespace doris::io {

bvar::Adder<uint64_t> encrypted_bytes_written("encrypted_file_writer", "bytes_wriiten");
bvar::PerSecond<bvar::Adder<uint64_t>> encryption_throughput("encrypted_file_writer",
                                                             "encryption_throughput",
                                                             &encrypted_bytes_written);

Status encrypt_ctr(const std::shared_ptr<const EncryptionInfo>& info, size_t offset,
                   std::span<const Slice> input, std::span<Slice> output) {
    DCHECK_EQ(input.size(), output.size());
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

    if (EVP_EncryptInit_ex(ctx, cipher, nullptr, info->plain_key().data(),
                           reinterpret_cast<const unsigned char*>(iv)) != 1) {
        return Status::InternalError("encryption init failed");
    }
    EVP_CIPHER_CTX_set_padding(ctx, info->padding);
    auto mod = offset % ENCRYPT_BLOCK_SIZE;
    if (mod != 0) {
        std::vector<unsigned char> dummy(mod);
        int out_len;
        if (EVP_EncryptUpdate(ctx, dummy.data(), &out_len, dummy.data(), mod) != 1) {
            return Status::InternalError("encrypt dummy part error");
        }
        DCHECK_LE(out_len, ENCRYPT_BLOCK_SIZE);
    }

    size_t bytes_encrypted = 0;
    for (size_t i = 0; i < input.size(); ++i) {
        int out_len;
        auto in = input[i];
        if (EVP_EncryptUpdate(ctx, reinterpret_cast<uint8_t*>(output[i].data), &out_len,
                              reinterpret_cast<uint8_t*>(in.data), in.size) != 1) {
            return Status::InternalError("encrypt error, offset={}", offset + bytes_encrypted);
        }
        DCHECK_EQ(out_len, in.size);
        bytes_encrypted += out_len;
    }
    return Status::OK();
}

EncryptedFileWriter::~EncryptedFileWriter() {
    if (!_is_footer_written) [[unlikely]] {
        LOG(WARNING) << "encrypted file writer leaked, try to close when destructed";
        auto st = close();
        if (!st) [[unlikely]] {
            LOG(WARNING) << "failed to close encrypted file writer, st=" << st;
        }
    }
}

Status EncryptedFileWriter::close(bool non_block) {
    auto write_footer = [this]() -> Status {
        uint8_t version_buf[sizeof(uint8_t)];
        encode_fixed8(version_buf, VERSION);

        std::string info_pb_buf = _encryption_info->serialize();

        uint32_t info_pb_len = info_pb_buf.length();
        uint8_t info_len_buf[sizeof(uint64_t)];
        encode_fixed64_le(info_len_buf, info_pb_len);

        uint8_t magic_code_buf[sizeof(uint64_t)];
        encode_fixed64_le(magic_code_buf, MAGIC_CODE);

        auto footer_size = sizeof(uint8_t) + sizeof(uint64_t) + info_pb_len + sizeof(uint64_t);
        if (footer_size > ENCRYPT_FOOTER_LENGTH) {
            return Status::InternalError("Footer size is larger than assumed max size={}",
                                         ENCRYPT_FOOTER_LENGTH);
        }
        auto padding_size = ENCRYPT_FOOTER_LENGTH - footer_size;
        std::vector<uint8_t> padding_buf;
        padding_buf.assign(padding_size, 0);

        std::vector<Slice> footer;
        // footer structure:
        // ┌────────────┬───────────────────┬───────────────────┬─────────────────┬──────────────┐
        // │  version   │     info_pb_len   │    info_pb_buf    │  padding zeros  │  magic_code  │
        // │ (uint8_t)  │     (uint64_t)    │  (serialized PB)  │      (...)      │  (uint64_t)  │
        // └────────────┴───────────────────┴───────────────────┴─────────────────┴──────────────┘
        footer.reserve(5);
        footer.emplace_back(version_buf, sizeof(uint8_t));
        footer.emplace_back(info_len_buf, sizeof(uint64_t));
        footer.emplace_back(info_pb_buf);
        footer.emplace_back(padding_buf.data(), padding_size);
        footer.emplace_back(magic_code_buf, sizeof(uint64_t));

        RETURN_IF_ERROR(_writer_inner->appendv(footer.data(), footer.size()));
        _is_footer_written = true;
        return Status::OK();
    };

    RETURN_IF_ERROR(_write_footer_once.call(std::move(write_footer)));

    return _writer_inner->close(false);
}

const Path& EncryptedFileWriter::path() const {
    return _writer_inner->path();
}

size_t EncryptedFileWriter::bytes_appended() const {
    return _bytes_appended;
}

EncryptedFileWriter::State EncryptedFileWriter::state() const {
    return _writer_inner->state();
}

//FileCacheAllocatorBuilder* EncryptedFileWriter::cache_builder() const {
//    return _writer_inner->cache_builder();
//}

Status EncryptedFileWriter::appendv(const Slice* data, size_t data_cnt) {
    std::span input_slices {data, data_cnt};

    auto size_view = input_slices | std::views::transform([](const auto& s) { return s.size; });
    size_t total_size = std::accumulate(size_view.begin(), size_view.end(), 0ULL);
    std::vector<uint8_t> encrypted_data(total_size);
    std::vector<Slice> encrypted_slices;

    size_t offset = 0;
    for (size_t size : size_view) {
        encrypted_slices.emplace_back(encrypted_data.data() + offset, size);
        offset += size;
    }
    DCHECK_EQ(offset, total_size);
    DCHECK_EQ(encrypted_slices.size(), data_cnt);

    RETURN_IF_ERROR(
            encrypt_ctr(_encryption_info, bytes_appended(), input_slices, encrypted_slices));
    RETURN_IF_ERROR(_writer_inner->appendv(encrypted_slices.data(), data_cnt));
    _bytes_appended = _writer_inner->bytes_appended();
    encrypted_bytes_written << total_size;

    return Status::OK();
}

} // namespace doris::io

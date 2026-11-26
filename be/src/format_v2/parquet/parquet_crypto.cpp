// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/parquet/parquet_crypto.h"

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <algorithm>
#include <limits>
#include <string>
#include <utility>

#include "gen_cpp/PlanNodes_types.h"
#include "util/coding.h"

#if __has_include(<parquet/encryption/encryption_internal.h>)
#include <arrow/util/secure_string.h>
#include <parquet/encryption/encryption_internal.h>
#endif

namespace doris::format::parquet {

#if __has_include(<parquet/encryption/encryption_internal.h>)
namespace crypto_detail {
// The externally linked Impl types share this key. An anonymous-namespace key makes GCC
// reject their members with -Wsubobject-linkage when this file is included in a unity build.
struct FileKey {
    ::arrow::util::SecureString key;
    std::string file_aad;
};
} // namespace crypto_detail
#endif

namespace {

using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::protocol::TList;
using apache::thrift::protocol::TMap;
using apache::thrift::protocol::TProtocolException;
using apache::thrift::protocol::TSet;
using apache::thrift::transport::TMemoryBuffer;

class BoundedCryptoProtocol final : public TCompactProtocol {
public:
    explicit BoundedCryptoProtocol(const std::shared_ptr<TMemoryBuffer>& transport, int limit)
            : TCompactProtocol(transport, limit, 65536) {}

protected:
    void checkReadBytesAvailable(TList& list) override {
        TCompactProtocol::checkReadBytesAvailable(list);
        charge(list.size_);
    }
    void checkReadBytesAvailable(TSet& set) override {
        TCompactProtocol::checkReadBytesAvailable(set);
        charge(set.size_);
    }
    void checkReadBytesAvailable(TMap& map) override {
        TCompactProtocol::checkReadBytesAvailable(map);
        // Both the key and value containers contribute to the native allocation budget.
        charge(map.size_);
        charge(map.size_);
    }

private:
    void charge(uint32_t count) {
        // Generated readers resize their statically typed vectors without checking the wire element
        // type: a list advertised as BYTE can still allocate ColumnChunks. Charge every element at
        // the largest native size; trusting the wire type would undercount those allocations.
        constexpr size_t unit =
                std::max({sizeof(tparquet::ColumnChunk), sizeof(tparquet::SchemaElement),
                          sizeof(tparquet::RowGroup)});
        if (count > _budget / unit) {
            throw TProtocolException(TProtocolException::SIZE_LIMIT);
        }
        _budget -= count * unit;
    }
    size_t _budget = MAX_ENCRYPTED_PARQUET_MODULE_BYTES;
};

template <typename T>
Status deserialize_bounded(std::span<const uint8_t> bytes, uint32_t* consumed, T* metadata) {
    if (bytes.empty() || bytes.size() > MAX_ENCRYPTED_PARQUET_MODULE_BYTES) {
        return Status::Corruption("File encryption [INVALID_METADATA_SIZE]");
    }
    try {
        auto configuration = std::make_shared<apache::thrift::TConfiguration>();
        configuration->setMaxMessageSize(static_cast<int>(bytes.size()));
        configuration->setRecursionLimit(64);
        auto transport = std::make_shared<TMemoryBuffer>(const_cast<uint8_t*>(bytes.data()),
                                                         static_cast<uint32_t>(bytes.size()),
                                                         TMemoryBuffer::OBSERVE, configuration);
        BoundedCryptoProtocol protocol(transport, static_cast<int>(bytes.size()));
        metadata->read(&protocol);
        *consumed = static_cast<uint32_t>(bytes.size()) - transport->available_read();
        return Status::OK();
    } catch (const Exception& e) {
        return e.to_status();
    } catch (const std::exception&) {
        // Thrift exceptions may quote binary fields, including footer key-retrieval metadata.
        return Status::Corruption("File encryption [INVALID_METADATA]");
    }
}

#if __has_include(<parquet/encryption/encryption_internal.h>)
Status decrypt_module(::parquet::encryption::AesDecryptor* decryptor,
                      const crypto_detail::FileKey& key, const std::string& aad,
                      std::span<const uint8_t> bytes, DorisVector<uint8_t>* plaintext) {
    if (bytes.size() < 4 || bytes.size() > MAX_ENCRYPTED_PARQUET_MODULE_BYTES ||
        decode_fixed32_le(bytes.data()) != bytes.size() - 4) {
        return Status::Corruption("File encryption [INVALID_MODULE_SIZE]");
    }
    try {
        const auto length = decryptor->PlaintextLength(static_cast<int32_t>(bytes.size()));
        plaintext->resize(length);
        auto aad_bytes = std::span(reinterpret_cast<const uint8_t*>(aad.data()), aad.size());
        const int32_t actual = decryptor->Decrypt(bytes, key.key.as_span(), aad_bytes,
                                                  std::span(plaintext->data(), plaintext->size()));
        if (actual != length) {
            return Status::Corruption("File encryption [INVALID_PLAINTEXT_SIZE]");
        }
        return Status::OK();
    } catch (const Exception& e) {
        return e.to_status();
    } catch (const std::exception&) {
        plaintext->clear();
        return Status::Corruption("File encryption [DECRYPTION_FAILED]");
    }
}
#endif
} // namespace

Status validate_crypto_metadata_padding(std::span<const uint8_t> bytes, uint32_t consumed) {
    DORIS_CHECK(consumed <= bytes.size());
    // parquet-mr Util.write encrypts the entire TMemoryBuffer backing array. A 28-byte header
    // can therefore contain 72 authenticated zero bytes from its initial 100-byte capacity.
    // The module framing/tag was already verified; only this writer's zero padding is accepted.
    const auto padding = bytes.subspan(consumed);
    if (!std::ranges::all_of(padding, [](uint8_t value) { return value == 0; })) {
        return Status::Corruption("File encryption [TRAILING_METADATA]");
    }
    return Status::OK();
}

Status deserialize_crypto_metadata(std::span<const uint8_t> bytes, uint32_t* consumed,
                                   tparquet::FileCryptoMetaData* metadata) {
    return deserialize_bounded(bytes, consumed, metadata);
}
Status deserialize_crypto_metadata(std::span<const uint8_t> bytes, uint32_t* consumed,
                                   tparquet::FileMetaData* metadata) {
    return deserialize_bounded(bytes, consumed, metadata);
}
Status deserialize_crypto_metadata(std::span<const uint8_t> bytes, uint32_t* consumed,
                                   tparquet::PageHeader* metadata) {
    return deserialize_bounded(bytes, consumed, metadata);
}

struct ParquetFileCrypto::Impl {
#if __has_include(<parquet/encryption/encryption_internal.h>)
    std::shared_ptr<const crypto_detail::FileKey> key;
#endif
};
struct ParquetColumnCrypto::Impl {
#if __has_include(<parquet/encryption/encryption_internal.h>)
    std::shared_ptr<const crypto_detail::FileKey> key;
    std::unique_ptr<::parquet::encryption::AesDecryptor> header;
    std::unique_ptr<::parquet::encryption::AesDecryptor> data;
    int16_t row_group;
    int16_t column;
#endif
};

ParquetFileCrypto::ParquetFileCrypto(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}
ParquetFileCrypto::~ParquetFileCrypto() = default;
ParquetColumnCrypto::ParquetColumnCrypto(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}
ParquetColumnCrypto::~ParquetColumnCrypto() = default;

Status ParquetFileCrypto::create(const TFileEncryptionInfo& info,
                                 const tparquet::FileCryptoMetaData& metadata,
                                 std::shared_ptr<ParquetFileCrypto>* crypto) {
    if (!info.__isset.version || info.version != 1 || !info.__isset.key ||
        (info.key.size() != 16 && info.key.size() != 24 && info.key.size() != 32) ||
        !info.__isset.aad_prefix || info.aad_prefix.size() > 65536) {
        return Status::InvalidArgument("File encryption [INVALID_DESCRIPTOR]");
    }
    const auto& algorithm = metadata.encryption_algorithm;
    if (!algorithm.__isset.AES_GCM_V1 || algorithm.__isset.AES_GCM_CTR_V1) {
        return Status::NotSupported("File encryption [UNSUPPORTED_CIPHER]");
    }
    const auto& aes = algorithm.AES_GCM_V1;
    if (!aes.__isset.aad_file_unique || aes.aad_file_unique.size() != 8) {
        return Status::Corruption("File encryption [INVALID_FILE_AAD]");
    }
    if (aes.supply_aad_prefix && info.aad_prefix.empty()) {
        return Status::InvalidArgument("File encryption [MISSING_AAD]");
    }
    if ((!aes.aad_prefix.empty() && aes.aad_prefix != info.aad_prefix) ||
        (aes.aad_prefix.empty() && !aes.supply_aad_prefix && !info.aad_prefix.empty())) {
        return Status::InvalidArgument("File encryption [AAD_MISMATCH]");
    }
#if __has_include(<parquet/encryption/encryption_internal.h>)
    auto impl = std::make_unique<Impl>();
    auto key = std::make_shared<crypto_detail::FileKey>();
    key->key = ::arrow::util::SecureString(std::string(info.key));
    key->file_aad = info.aad_prefix + aes.aad_file_unique;
    impl->key = std::move(key);
    crypto->reset(new ParquetFileCrypto(std::move(impl)));
    return Status::OK();
#else
    // Older prebuilt dependency bundles do not install PME internals. They must continue to
    // reject encrypted files explicitly until rebuilt with PARQUET_REQUIRE_ENCRYPTION=ON.
    return Status::NotSupported("File encryption [PME_DEPENDENCY_UNAVAILABLE]");
#endif
}

Status ParquetFileCrypto::decrypt_footer(std::span<const uint8_t> bytes,
                                         DorisVector<uint8_t>* plaintext) const {
#if __has_include(<parquet/encryption/encryption_internal.h>)
    try {
        auto decryptor = ::parquet::encryption::AesDecryptor::Make(
                ::parquet::ParquetCipher::AES_GCM_V1, static_cast<int32_t>(_impl->key->key.size()),
                true);
        return decrypt_module(decryptor.get(), *_impl->key,
                              ::parquet::encryption::CreateFooterAad(_impl->key->file_aad), bytes,
                              plaintext);
    } catch (const std::exception&) {
        return Status::NotSupported("File encryption [PME_DEPENDENCY_UNAVAILABLE]");
    }
#else
    return Status::NotSupported("File encryption [PME_DEPENDENCY_UNAVAILABLE]");
#endif
}

Status ParquetFileCrypto::validate_column(const tparquet::ColumnChunk& column, int row_group,
                                          int column_ordinal) {
    if (column.__isset.encrypted_column_metadata ||
        (column.__isset.crypto_metadata &&
         (!column.crypto_metadata.__isset.ENCRYPTION_WITH_FOOTER_KEY ||
          column.crypto_metadata.__isset.ENCRYPTION_WITH_COLUMN_KEY))) {
        return Status::NotSupported("File encryption [COLUMN_KEY_UNSUPPORTED]");
    }
    if (!column.__isset.crypto_metadata) {
        return Status::OK();
    }
    if (row_group < 0 || row_group > std::numeric_limits<int16_t>::max() || column_ordinal < 0 ||
        column_ordinal > std::numeric_limits<int16_t>::max()) {
        return Status::NotSupported("File encryption [ORDINAL_OUT_OF_RANGE]");
    }
    return Status::OK();
}

Status ParquetFileCrypto::column_crypto(const tparquet::ColumnChunk& column, int row_group,
                                        int column_ordinal,
                                        std::shared_ptr<ParquetColumnCrypto>* crypto) const {
    crypto->reset();
    RETURN_IF_ERROR(validate_column(column, row_group, column_ordinal));
    if (!column.__isset.crypto_metadata) {
        return Status::OK();
    }
#if __has_include(<parquet/encryption/encryption_internal.h>)
    try {
        auto impl = std::make_unique<ParquetColumnCrypto::Impl>();
        impl->key = _impl->key;
        impl->row_group = static_cast<int16_t>(row_group);
        impl->column = static_cast<int16_t>(column_ordinal);
        impl->header = ::parquet::encryption::AesDecryptor::Make(
                ::parquet::ParquetCipher::AES_GCM_V1, static_cast<int32_t>(impl->key->key.size()),
                true);
        impl->data = ::parquet::encryption::AesDecryptor::Make(
                ::parquet::ParquetCipher::AES_GCM_V1, static_cast<int32_t>(impl->key->key.size()),
                false);
        crypto->reset(new ParquetColumnCrypto(std::move(impl)));
        return Status::OK();
    } catch (const std::exception&) {
        return Status::NotSupported("File encryption [PME_DEPENDENCY_UNAVAILABLE]");
    }
#else
    return Status::NotSupported("File encryption [PME_DEPENDENCY_UNAVAILABLE]");
#endif
}

Status ParquetColumnCrypto::plaintext_size(int32_t ciphertext_size, int32_t* size) const {
#if __has_include(<parquet/encryption/encryption_internal.h>)
    if (ciphertext_size < 0 ||
        std::cmp_greater(ciphertext_size, MAX_ENCRYPTED_PARQUET_MODULE_BYTES)) {
        return Status::Corruption("File encryption [INVALID_MODULE_SIZE]");
    }
    try {
        *size = _impl->data->PlaintextLength(ciphertext_size);
        return Status::OK();
    } catch (const std::exception&) {
        return Status::Corruption("File encryption [INVALID_MODULE_SIZE]");
    }
#else
    return Status::NotSupported("File encryption [PME_DEPENDENCY_UNAVAILABLE]");
#endif
}

Status ParquetColumnCrypto::decrypt_header(std::span<const uint8_t> bytes, bool dictionary,
                                           int32_t page, DorisVector<uint8_t>* plaintext) {
#if __has_include(<parquet/encryption/encryption_internal.h>)
    if (page < 0 || page > std::numeric_limits<int16_t>::max()) {
        return Status::NotSupported("File encryption [ORDINAL_OUT_OF_RANGE]");
    }
    const auto module = dictionary ? ::parquet::encryption::kDictionaryPageHeader
                                   : ::parquet::encryption::kDataPageHeader;
    return decrypt_module(
            _impl->header.get(), *_impl->key,
            ::parquet::encryption::CreateModuleAad(_impl->key->file_aad, module, _impl->row_group,
                                                   _impl->column, page),
            bytes, plaintext);
#else
    return Status::NotSupported("File encryption [PME_DEPENDENCY_UNAVAILABLE]");
#endif
}

Status ParquetColumnCrypto::decrypt_page(std::span<const uint8_t> bytes, bool dictionary,
                                         int32_t page, DorisVector<uint8_t>* plaintext) {
#if __has_include(<parquet/encryption/encryption_internal.h>)
    const auto module =
            dictionary ? ::parquet::encryption::kDictionaryPage : ::parquet::encryption::kDataPage;
    return decrypt_module(
            _impl->data.get(), *_impl->key,
            ::parquet::encryption::CreateModuleAad(_impl->key->file_aad, module, _impl->row_group,
                                                   _impl->column, page),
            bytes, plaintext);
#else
    return Status::NotSupported("File encryption [PME_DEPENDENCY_UNAVAILABLE]");
#endif
}

} // namespace doris::format::parquet

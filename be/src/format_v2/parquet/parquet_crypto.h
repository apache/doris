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

#pragma once

#include <gen_cpp/parquet_types.h>

#include <cstdint>
#include <memory>
#include <span>

#include "common/status.h"
#include "core/custom_allocator.h"

namespace doris {
class TFileEncryptionInfo;
}

namespace doris::format::parquet {

inline constexpr size_t MAX_ENCRYPTED_PARQUET_MODULE_BYTES = 64UL << 20;

Status validate_crypto_metadata_padding(std::span<const uint8_t> bytes, uint32_t consumed);

// Encrypted module parsing also bounds decoded containers and recursion, not just wire bytes.
Status deserialize_crypto_metadata(std::span<const uint8_t> bytes, uint32_t* consumed,
                                   tparquet::FileCryptoMetaData* metadata);
Status deserialize_crypto_metadata(std::span<const uint8_t> bytes, uint32_t* consumed,
                                   tparquet::FileMetaData* metadata);
Status deserialize_crypto_metadata(std::span<const uint8_t> bytes, uint32_t* consumed,
                                   tparquet::PageHeader* metadata);

class ParquetColumnCrypto;

// Immutable file key/AAD owner. Encrypted metadata is never admitted to shared caches.
class ParquetFileCrypto {
public:
    static Status create(const TFileEncryptionInfo& info,
                         const tparquet::FileCryptoMetaData& metadata,
                         std::shared_ptr<ParquetFileCrypto>* crypto);
    ~ParquetFileCrypto();

    Status decrypt_footer(std::span<const uint8_t> bytes, DorisVector<uint8_t>* plaintext) const;
    static Status validate_column(const tparquet::ColumnChunk& column, int row_group,
                                  int column_ordinal);
    Status column_crypto(const tparquet::ColumnChunk& column, int row_group, int column_ordinal,
                         std::shared_ptr<ParquetColumnCrypto>* crypto) const;

private:
    struct Impl;
    explicit ParquetFileCrypto(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> _impl;
};

// Each physical column reader owns its mutable parquet-cpp decryptors; they are not thread-safe.
// References extend one column's reader stack only; never reuse an instance between readers.
class ParquetColumnCrypto {
public:
    ~ParquetColumnCrypto();
    Status plaintext_size(int32_t ciphertext_size, int32_t* size) const;
    Status decrypt_header(std::span<const uint8_t> bytes, bool dictionary, int32_t page,
                          DorisVector<uint8_t>* plaintext);
    Status decrypt_page(std::span<const uint8_t> bytes, bool dictionary, int32_t page,
                        DorisVector<uint8_t>* plaintext);

private:
    friend class ParquetFileCrypto;
    struct Impl;
    explicit ParquetColumnCrypto(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> _impl;
};

} // namespace doris::format::parquet

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

#include "util/variant/variant_encoded_block.h"

#include <utility>

#include "common/exception.h"
#include "util/variant/variant_tracked_storage.h"

namespace doris {

size_t VariantEncodedBlockView::num_rows() const noexcept {
    return _offsets.empty() ? 0 : _offsets.size() - 1;
}

VariantValueRef VariantEncodedBlockView::value_at(size_t row) const {
    if (row >= num_rows()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant encoded block row {} is outside [0, {})", row, num_rows());
    }
    const uint32_t begin = _offsets[row];
    const uint32_t end = _offsets[row + 1];
    return {.metadata = _metadata, .data = _values.data + begin, .size = end - begin};
}

VariantEncodedBlock::VariantEncodedBlock(
        std::unique_ptr<VariantEncodedBlockStorage> storage) noexcept
        : _storage(std::move(storage)) {}

VariantEncodedBlock::VariantEncodedBlock(VariantEncodedBlock&&) noexcept = default;

VariantEncodedBlock& VariantEncodedBlock::operator=(VariantEncodedBlock&&) noexcept = default;

VariantEncodedBlock::~VariantEncodedBlock() = default;

VariantEncodedBlockView VariantEncodedBlock::view() const noexcept {
    return VariantEncodedBlockView(
            {.data = _storage->metadata.data(), .size = _storage->metadata.size()},
            StringRef(_storage->values.data(), _storage->values.size()), _storage->offsets);
}

} // namespace doris

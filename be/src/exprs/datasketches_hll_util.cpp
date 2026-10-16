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

#include "exprs/datasketches_hll_util.h"

#include <cstring>
#include <exception>
#include <stdexcept>

#include "common/exception.h"
#include "common/status.h"

namespace doris::datasketches_hll {

Sketch deserialize(const StringRef& value, std::string_view action) {
    // The library reads the preamble byte before it checks the length.
    if (value.size == 0) {
        throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when {}: empty input.",
                        action);
    }
    try {
        // All modes have an eight-byte preamble. An invalid lgK would otherwise reach shifts used
        // to size the register/coupon arrays before the library validates it.
        datasketches::HllUtil<Alloc>::checkMemSize(
                datasketches::hll_constants::EMPTY_SKETCH_SIZE_BYTES, value.size);
        datasketches::HllUtil<Alloc>::checkLgK(
                static_cast<uint8_t>(value.data[datasketches::hll_constants::LG_K_BYTE]));
        // HLL_4 uses nibble 0xf to reference its auxiliary map. The library accepts an image
        // with auxCount=0 and such a nibble, then dereferences a null map during union/conversion.
        // This occurs, for example, when UTF-8 replacement bytes shift a binary sketch's fields.
        // Validate this structural invariant before exposing the sketch, including on a no-op.
        using namespace datasketches::hll_constants;
        const auto* bytes = reinterpret_cast<const uint8_t*>(value.data);
        const bool is_hll4 = bytes[PREAMBLE_INTS_BYTE] == HLL_PREINTS &&
                             (bytes[MODE_BYTE] & loNibbleMask) == datasketches::hll_mode::HLL;
        if (is_hll4) {
            const size_t register_bytes = size_t {1} << (bytes[LG_K_BYTE] - 1);
            datasketches::HllUtil<Alloc>::checkMemSize(HLL_BYTE_ARR_START + register_bytes,
                                                       value.size);
            uint32_t aux_count = 0;
            std::memcpy(&aux_count, bytes + AUX_COUNT_INT, sizeof(aux_count));
            if (aux_count == 0) {
                for (size_t i = HLL_BYTE_ARR_START; i < HLL_BYTE_ARR_START + register_bytes; ++i) {
                    if ((bytes[i] & loNibbleMask) == AUX_TOKEN ||
                        ((bytes[i] >> 4) & loNibbleMask) == AUX_TOKEN) {
                        throw std::invalid_argument(
                                "HLL_4 contains AUX_TOKEN without an auxiliary map");
                    }
                }
            }
        }
        return Sketch::deserialize(value.data, value.size, Alloc());
    } catch (const doris::Exception& e) {
        throw Exception(e.code(), "Failed to deserialize HLL sketch when {}: {}", action,
                        e.to_string());
    } catch (const std::exception& e) {
        throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when {}: {}", action,
                        e.what());
    } catch (...) {
        throw Exception(ErrorCode::CORRUPTION,
                        "HLL sketch data corrupted when {}: unknown exception.", action);
    }
}

} // namespace doris::datasketches_hll

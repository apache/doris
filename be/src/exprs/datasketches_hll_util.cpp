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

namespace {

using namespace datasketches::hll_constants;

// Reuse the library's protected static header readers without constructing a sketch or
// duplicating the mode/type bit decoding. This abstract adapter is never instantiated.
struct HeaderReader : datasketches::HllSketchImpl<Alloc> {
    using datasketches::HllSketchImpl<Alloc>::extractCurMode;
    using datasketches::HllSketchImpl<Alloc>::extractTgtHllType;
};

uint32_t read_uint32(const uint8_t* bytes) {
    uint32_t value;
    std::memcpy(&value, bytes, sizeof(value));
    return value;
}

void check_set_size(const uint8_t* bytes, size_t size, uint8_t lg_k, bool compact) {
    datasketches::HllUtil<Alloc>::checkMemSize(HASH_SET_INT_ARR_START, size);
    const auto count = read_uint32(bytes + HASH_SET_COUNT_INT);
    // SET promotes to HLL above 3/4 of its largest table (K/8 entries). Bound the count before
    // computeLgArrInts(), whose power-of-two and load-factor arithmetic uses uint32_t.
    if (lg_k <= 7 || count > (RESIZE_NUMER * (uint32_t {1} << (lg_k - 3))) / RESIZE_DENOM) {
        throw std::invalid_argument("Invalid HLL SET coupon count or lgK");
    }
    size_t items = count;
    if (!compact) {
        // The library evaluates 1 << lgArr before checking the payload. For example, lgArr=32
        // is an undefined shift. Preserve its support for older images with an unset lgArr.
        const uint8_t lg_arr = bytes[LG_ARR_BYTE] < LG_INIT_SET_SIZE
                                       ? datasketches::HllUtil<Alloc>::computeLgArrInts(
                                                 datasketches::SET, count, lg_k)
                                       : bytes[LG_ARR_BYTE];
        if (lg_arr > lg_k - 3) {
            throw std::invalid_argument("Invalid HLL SET array exponent");
        }
        items = size_t {1} << lg_arr;
    }
    datasketches::HllUtil<Alloc>::checkMemSize(HASH_SET_INT_ARR_START + items * sizeof(uint32_t),
                                               size);
}

void check_aux_map(const uint8_t* bytes, size_t size, size_t offset, uint8_t lg_k, uint32_t count,
                   bool compact) {
    const uint32_t registers = uint32_t {1} << lg_k;
    // There can be at most one auxiliary entry per register. This also bounds the uint32_t
    // capacity arithmetic used by the library to rebuild a compact map.
    if (count > registers) {
        throw std::invalid_argument("HLL_4 auxiliary count exceeds register count");
    }
    size_t items = count;
    if (!compact) {
        const auto lg_arr = bytes[LG_ARR_BYTE];
        // An updatable map starts at LG_AUX_ARR_INTS[lgK] and grows at 3/4 load, so at most
        // K entries need 2*K slots. Reject bad exponents before the library's 1 << lgArr.
        if (lg_arr < LG_AUX_ARR_INTS[lg_k] || lg_arr > lg_k + 1) {
            throw std::invalid_argument("Invalid HLL_4 auxiliary array exponent");
        }
        items = size_t {1} << lg_arr;
    }
    datasketches::HllUtil<Alloc>::checkMemSize(offset + items * sizeof(uint32_t), size);

    // AuxHashMap::deserialize owns a raw map until it returns. mustAdd() throws for duplicate
    // slots before that ownership transfers, leaking the map. Check the same masked slot keys
    // before allocating it. This bitmap is needed only for sketches with an auxiliary map.
    DorisVector<uint8_t> seen(registers / 8, 0);
    for (size_t i = 0; i < items; ++i) {
        const auto pair = read_uint32(bytes + offset + i * sizeof(uint32_t));
        if (!compact && pair == EMPTY) {
            continue;
        }
        const auto slot = datasketches::HllUtil<Alloc>::getLow26(pair) & (registers - 1);
        const auto bit = static_cast<uint8_t>(1U << (slot % 8));
        if (seen[slot / 8] & bit) {
            throw std::invalid_argument("HLL_4 auxiliary map contains duplicate slots");
        }
        seen[slot / 8] |= bit;
    }
}

void check_hll_size(const uint8_t* bytes, size_t size, uint8_t lg_k, bool compact) {
    datasketches::HllUtil<Alloc>::checkMemSize(HLL_BYTE_ARR_START, size);
    const auto type = HeaderReader::extractTgtHllType(bytes[MODE_BYTE]);
    const size_t offset =
            HLL_BYTE_ARR_START + datasketches::HllArray<Alloc>::hllArrBytes(type, lg_k);
    datasketches::HllUtil<Alloc>::checkMemSize(offset, size);
    const auto aux_count = read_uint32(bytes + AUX_COUNT_INT);
    if (type != datasketches::HLL_4) {
        // newHll() installs any nonempty auxiliary map through an unchecked Hll4Array cast.
        // HLL_6/HLL_8 have no map member, so accepting such an image writes outside the object.
        if (aux_count != 0) {
            throw std::invalid_argument("HLL_6/HLL_8 cannot contain an auxiliary map");
        }
        return;
    }
    if (aux_count != 0) {
        check_aux_map(bytes, size, offset, lg_k, aux_count, compact);
        return;
    }
    // HLL_4 uses nibble 0xf to reference its auxiliary map. The library accepts auxCount=0
    // with such a nibble, then dereferences a null map during union/conversion, even on a no-op.
    for (size_t i = HLL_BYTE_ARR_START; i < offset; ++i) {
        if ((bytes[i] & loNibbleMask) == AUX_TOKEN ||
            ((bytes[i] >> 4) & loNibbleMask) == AUX_TOKEN) {
            throw std::invalid_argument("HLL_4 contains AUX_TOKEN without an auxiliary map");
        }
    }
}

} // namespace

Sketch deserialize(const StringRef& value, std::string_view action) {
    // The library reads the preamble byte before it checks the length.
    if (value.size == 0) {
        throw Exception(ErrorCode::CORRUPTION, "HLL sketch data corrupted when {}: empty input.",
                        action);
    }
    try {
        // All modes have an eight-byte preamble. An invalid lgK would otherwise reach shifts used
        // to size the register/coupon arrays before the library validates it.
        datasketches::HllUtil<Alloc>::checkMemSize(EMPTY_SKETCH_SIZE_BYTES, value.size);
        const auto* bytes = reinterpret_cast<const uint8_t*>(value.data);
        const auto lg_k = datasketches::HllUtil<Alloc>::checkLgK(bytes[LG_K_BYTE]);
        const auto mode = HeaderReader::extractCurMode(bytes[MODE_BYTE]);
        const bool compact = (bytes[FLAGS_BYTE] & COMPACT_FLAG_MASK) != 0;
        if (mode == datasketches::SET) {
            check_set_size(bytes, value.size, lg_k, compact);
        } else if (mode == datasketches::HLL) {
            check_hll_size(bytes, value.size, lg_k, compact);
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

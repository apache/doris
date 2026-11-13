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

#include <bthread/bthread.h>

#include <cstring>
#include <exception>
#include <mutex>
#include <new>
#include <stdexcept>

#include "common/check.h"
#include "common/exception.h"
#include "common/status.h"
#include "util/debug_points.h"

namespace doris::datasketches_hll {

struct alignas(std::max_align_t) AllocationHeader {
    AllocationHeader* previous;
    AllocationHeader* next;
    detail::AllocationOwner* owner;
    size_t count;
};

namespace {

using namespace datasketches::hll_constants;

thread_local detail::AllocationOwner* pthread_scope = nullptr;

bthread_key_t scope_key() {
    static bthread_key_t key;
    static std::once_flag once;
    std::call_once(once, [] { DORIS_CHECK(bthread_key_create(&key, nullptr) == 0); });
    return key;
}

detail::AllocationOwner* current_scope() {
    return bthread_self() == 0
                   ? pthread_scope
                   : static_cast<detail::AllocationOwner*>(bthread_getspecific(scope_key()));
}

void set_scope(detail::AllocationOwner* scope) {
    if (bthread_self() == 0) {
        pthread_scope = scope;
    } else {
        DORIS_CHECK(bthread_setspecific(scope_key(), scope) == 0);
    }
}

class ActiveAllocationOwner {
public:
    explicit ActiveAllocationOwner(detail::AllocationOwner* owner) : _previous(current_scope()) {
        set_scope(owner);
    }
    ~ActiveAllocationOwner() { set_scope(_previous); }

private:
    detail::AllocationOwner* _previous;
};

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
    if (!compact) {
        // newSet() copies the whole table but trusts the declared count. With ten occupied
        // slots and count=1, a later compact serializer allocates 16 bytes and writes 52.
        uint32_t occupied = 0;
        for (size_t i = 0; i < items; ++i) {
            occupied += read_uint32(bytes + HASH_SET_INT_ARR_START + i * sizeof(uint32_t)) != EMPTY;
        }
        if (occupied != count) {
            throw std::invalid_argument("HLL SET coupon count does not match occupied slots");
        }
    }
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
        if (pair == EMPTY) {
            // Compact maps contain only real entries. Treating EMPTY as slot 0 would let
            // an AUX_TOKEN at register 0 pass this check, but mustFindValueFor(0) still fails.
            if (compact) {
                throw std::invalid_argument("HLL_4 compact auxiliary map contains an EMPTY entry");
            }
            continue;
        }
        const auto slot = datasketches::HllUtil<Alloc>::getLow26(pair) & (registers - 1);
        const auto bit = static_cast<uint8_t>(1U << (slot % 8));
        if (seen[slot / 8] & bit) {
            throw std::invalid_argument("HLL_4 auxiliary map contains duplicate slots");
        }
        seen[slot / 8] |= bit;
    }
    // A nonempty map is insufficient: every AUX_TOKEN must refer to a slot in that map.
    // For example, token at register 0 with only slot 3 in the map makes mergeHll() throw
    // after allocating its target array. Reject that image even on the unchanged-byte path.
    for (uint32_t slot = 0; slot < registers; ++slot) {
        const auto byte = bytes[HLL_BYTE_ARR_START + slot / 2];
        const auto value = static_cast<uint8_t>((byte >> ((slot % 2) * 4)) & loNibbleMask);
        if (value == AUX_TOKEN && !(seen[slot / 8] & (1U << (slot % 8)))) {
            throw std::invalid_argument("HLL_4 AUX_TOKEN has no matching auxiliary slot");
        }
    }
}

bool check_hll_size(const uint8_t* bytes, size_t size, uint8_t lg_k, bool compact) {
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
        return false;
    }
    if (aux_count != 0) {
        check_aux_map(bytes, size, offset, lg_k, aux_count, compact);
        return true;
    }
    // HLL_4 uses nibble 0xf to reference its auxiliary map. The library accepts auxCount=0
    // with such a nibble, then dereferences a null map during union/conversion, even on a no-op.
    for (size_t i = HLL_BYTE_ARR_START; i < offset; ++i) {
        if ((bytes[i] & loNibbleMask) == AUX_TOKEN ||
            ((bytes[i] >> 4) & loNibbleMask) == AUX_TOKEN) {
            throw std::invalid_argument("HLL_4 contains AUX_TOKEN without an auxiliary map");
        }
    }
    return false;
}

} // namespace

AllocationScope::AllocationScope()
        : _previous(current_scope()), _exceptions(std::uncaught_exceptions()) {
    set_scope(&_owner);
}

AllocationScope::~AllocationScope() {
    set_scope(_previous);
    if (std::uncaught_exceptions() == _exceptions) {
        _owner.release();
    }
}

detail::AllocationOwner::~AllocationOwner() {
    clear();
}

void detail::AllocationOwner::clear() noexcept {
    while (_head != nullptr) {
        auto* header = _head;
        _head = header->next;
        CustomStdAllocator<AllocationHeader>().deallocate(header, header->count);
    }
}

void detail::AllocationOwner::release() noexcept {
    while (_head != nullptr) {
        auto* header = _head;
        _head = header->next;
        // Returned sketches keep normal library ownership after a successful call.
        header->owner = nullptr;
    }
}

void* allocate(size_t bytes) {
    DBUG_EXECUTE_IF("datasketches_hll.before_allocate", { DBUG_RUN_CALLBACK(); });
    auto* scope = current_scope();
    const size_t count =
            1 + bytes / sizeof(AllocationHeader) + (bytes % sizeof(AllocationHeader) != 0);
    auto* header = CustomStdAllocator<AllocationHeader>().allocate(count);
    *header = {.previous = nullptr,
               .next = scope == nullptr ? nullptr : scope->_head,
               .owner = scope,
               .count = count};
    if (scope != nullptr) {
        if (header->next != nullptr) {
            header->next->previous = header;
        }
        scope->_head = header;
    }
    return header + 1;
}

void deallocate(void* ptr) noexcept {
    auto* header = static_cast<AllocationHeader*>(ptr) - 1;
    if (header->owner != nullptr) {
        if (header->previous != nullptr) {
            header->previous->next = header->next;
        } else {
            header->owner->_head = header->next;
        }
        if (header->next != nullptr) {
            header->next->previous = header->previous;
        }
    }
    CustomStdAllocator<AllocationHeader>().deallocate(header, header->count);
}

struct Union::Impl {
    using LibraryUnion = datasketches::hll_union_alloc<Alloc>;

    detail::AllocationOwner allocations;
    alignas(LibraryUnion) std::byte storage[sizeof(LibraryUnion)];
    bool valid = false;

    LibraryUnion* get() { return std::launder(reinterpret_cast<LibraryUnion*>(storage)); }
    const LibraryUnion* get() const {
        return std::launder(reinterpret_cast<const LibraryUnion*>(storage));
    }

    ~Impl() {
        if (valid) {
            get()->~LibraryUnion();
        }
        // Failed library updates can leave a dangling gadget pointer. AllocationOwner frees
        // the remaining blocks directly, instead of dereferencing that invalid gadget.
    }
};

void Union::Deleter::operator()(Impl* ptr) const noexcept {
    ptr->~Impl();
    datasketches_hll::deallocate(ptr);
}

Union::Union(uint8_t lg_k, const Alloc& allocator)
        : _impl(new(datasketches_hll::allocate(sizeof(Impl))) Impl()) {
    ActiveAllocationOwner scope(&_impl->allocations);
    new (_impl->storage) Impl::LibraryUnion(lg_k, allocator);
    _impl->valid = true;
}

Union::~Union() = default;
Union::Union(Union&&) noexcept = default;
Union& Union::operator=(Union&&) noexcept = default;

Union::Union(const Union& other) : _impl(new(datasketches_hll::allocate(sizeof(Impl))) Impl()) {
    DORIS_CHECK(other._impl->valid);
    ActiveAllocationOwner scope(&_impl->allocations);
    new (_impl->storage) Impl::LibraryUnion(*other._impl->get());
    _impl->valid = true;
}

Union& Union::operator=(const Union& other) {
    Union copy(other);
    *this = std::move(copy);
    return *this;
}

void Union::update(const Sketch& sketch) {
    DORIS_CHECK(_impl->valid);
    ActiveAllocationOwner scope(&_impl->allocations);
    try {
        _impl->get()->update(sketch);
    } catch (...) {
        // The library can free its gadget during promotion and then throw before replacing
        // the pointer. This state must be discarded; its allocation owner remains intact.
        _impl->valid = false;
        throw;
    }
}

Sketch Union::get_result(datasketches::target_hll_type type) const {
    DORIS_CHECK(_impl->valid);
    AllocationScope scope;
    return _impl->get()->get_result(type);
}

double Union::get_estimate() const {
    DORIS_CHECK(_impl->valid);
    return _impl->get()->get_estimate();
}

double Union::get_lower_bound(uint8_t std_devs) const {
    DORIS_CHECK(_impl->valid);
    return _impl->get()->get_lower_bound(std_devs);
}

double Union::get_upper_bound(uint8_t std_devs) const {
    DORIS_CHECK(_impl->valid);
    return _impl->get()->get_upper_bound(std_devs);
}

uint8_t Union::get_lg_config_k() const {
    DORIS_CHECK(_impl->valid);
    return _impl->get()->get_lg_config_k();
}

double Union::get_rel_err(bool upper, bool unioned, uint8_t lg_k, uint8_t std_devs) {
    return Impl::LibraryUnion::get_rel_err(upper, unioned, lg_k, std_devs);
}

Sketch deserialize(const StringRef& value, std::string_view action) {
    AllocationScope allocations;
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
        bool has_aux_map = false;
        if (mode == datasketches::SET) {
            check_set_size(bytes, value.size, lg_k, compact);
        } else if (mode == datasketches::HLL) {
            has_aux_map = check_hll_size(bytes, value.size, lg_k, compact);
        }
        // AuxHashMap::deserialize reads uint32_t directly. STRING rows have no alignment
        // guarantee: a 233-byte HLL_6 before this HLL_4 leaves its auxiliary entries unaligned.
        // Only this library reader needs an aligned copy; the other payloads use memcpy.
        if (has_aux_map && reinterpret_cast<uintptr_t>(value.data) % alignof(uint32_t) != 0) {
            DorisVector<uint32_t> aligned(value.size / sizeof(uint32_t) +
                                          (value.size % sizeof(uint32_t) != 0));
            std::memcpy(aligned.data(), value.data, value.size);
            return Sketch::deserialize(aligned.data(), value.size, Alloc());
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

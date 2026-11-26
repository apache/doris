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

#include "storage/index/inverted/query/exec/roaring_docid_sink.h"

#include <algorithm>
#include <bit>
#include <cstddef>

// clang-format off
// CRoaring's public header defines ROARING_CONTAINER_T; its internal headers undefine it.
#include "roaring/roaring.hh"
#include "roaring/containers/containers.h"
// clang-format on

namespace doris::index_query {
namespace {

using roaring::api::roaring_bitmap_add_bulk;
using roaring::api::roaring_bitmap_add_many;
using roaring::api::roaring_bitmap_t;
using roaring::api::roaring_bulk_context_t;
using roaring::internal::array_container_t;
using roaring::internal::bitset_container_t;

// A sink only promises sorted spans. Equal neighbours, or an unsorted span from a damaged posting,
// would break a sorted array container and could put a document in the wrong chunk.
bool all_strictly_ascending(std::span<const uint32_t> docids) {
    uint32_t out_of_order = 0;
    for (size_t i = 1; i < docids.size(); ++i) {
        out_of_order |= static_cast<uint32_t>(docids[i - 1] >= docids[i]);
    }
    return out_of_order == 0;
}

// Gathers the bits of one 64-bit word in a register and writes it once: consecutive documents
// mostly share a word, and a store per document would wait on the one before it.
void set_sorted_bits(bitset_container_t* bitset, std::span<const uint32_t> docids) {
    uint64_t* const words = bitset->words;
    int32_t added = 0;
    size_t i = 0;
    while (i < docids.size()) {
        const uint32_t word = (docids[i] & 0xFFFF) >> 6;
        uint64_t bits = 0;
        do {
            bits |= uint64_t {1} << (docids[i] & 63);
            ++i;
        } while (i < docids.size() && ((docids[i] & 0xFFFF) >> 6) == word);
        added += std::popcount(bits & ~words[word]);
        words[word] |= bits;
    }
    bitset->cardinality += added;
}

// Appends to the tail of an array container, which must stay sorted and within its size limit.
void append_sorted_to_array(array_container_t* array, std::span<const uint32_t> docids) {
    const int32_t cardinality = array->cardinality;
    const auto total = cardinality + static_cast<int32_t>(docids.size());
    if (total > array->capacity) {
        roaring::internal::array_container_grow(array, total, /*preserve=*/true);
    }
    uint16_t* const out = array->array + cardinality;
    for (size_t i = 0; i < docids.size(); ++i) {
        out[i] = static_cast<uint16_t>(docids[i]);
    }
    array->cardinality = total;
}

// Adds strictly ascending documents of one 64K chunk. roaring_bitmap_add_bulk finds or makes the
// container, and changes its type when the document needs that; the rest go straight into it.
void add_sorted_chunk(roaring_bitmap_t* bitmap, std::span<const uint32_t> docids) {
    while (!docids.empty()) {
        roaring_bulk_context_t context {};
        roaring_bitmap_add_bulk(bitmap, &context, docids.front());
        docids = docids.subspan(1);
        if (context.typecode == BITSET_CONTAINER_TYPE) {
            set_sorted_bits(CAST_bitset(context.container), docids);
            return;
        }
        if (docids.empty()) {
            return;
        }
        auto* array = CAST_array(context.container);
        if (context.typecode != ARRAY_CONTAINER_TYPE ||
            array->array[array->cardinality - 1] >= static_cast<uint16_t>(docids.front())) {
            roaring_bitmap_add_many(bitmap, docids.size(), docids.data());
            return;
        }
        // Up to the limit of an array: the next document, if any, turns the container to a bitset.
        const size_t room = roaring::internal::DEFAULT_MAX_SIZE - array->cardinality;
        const size_t count = std::min(docids.size(), room);
        append_sorted_to_array(array, docids.first(count));
        docids = docids.subspan(count);
    }
}

} // namespace

// addMany goes through the container's type for every document, while a posting list is decoded in
// windows of documents that are mostly in one container.
Status RoaringDocIdSink::append_sorted(std::span<const uint32_t> docids) {
    if (!all_strictly_ascending(docids)) {
        bitmap_.addMany(docids.size(), docids.data());
        return Status::OK();
    }
    while (!docids.empty()) {
        const uint32_t chunk = docids.front() >> 16;
        const auto chunk_end = std::ranges::partition_point(
                docids, [chunk](uint32_t docid) { return docid >> 16 == chunk; });
        const auto count = static_cast<size_t>(chunk_end - docids.begin());
        add_sorted_chunk(&bitmap_.roaring, docids.first(count));
        docids = docids.subspan(count);
    }
    return Status::OK();
}

} // namespace doris::index_query

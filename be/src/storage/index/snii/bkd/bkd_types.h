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

#pragma once

#include <cstdint>

#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/olap_common.h"

// Plain data definitions shared by the SNII-native BKD write and read sides.
// DEFINITIONS ONLY -- no behaviour lives here, so neither side has to include
// the other's headers to name a parameter or a result. Every type is trivially
// copyable and owns nothing: a reader can hold them by value and share them
// across concurrent queries without locking or per-query copies.
namespace doris::snii::bkd {

// One point as handed to / decoded from the builder. The sort key is the pair
// (value, doc_id): a doc contributing several points (array column) is a
// first-class case, not something a "single value per doc" flag has to promise
// away.
struct PointRef {
    // Unsigned big-endian sortable bytes from KeyCoder::full_encode_ascending,
    // exactly bytes_per_dim long (INV-1 / INV-2). A VIEW -- the referenced bytes
    // are owned elsewhere and must outlive this struct.
    Slice value;
    // Segment-local row id (INV-3).
    uint32_t doc_id = 0;
};

// One decoded leaf-directory row. The directory is stored column-wise in
// bkd_index as delta-varint64 offsets followed by varint32 counts (design 5.1);
// this is the row view a reader works with.
struct LeafRef {
    // Byte offset of the leaf block within bkd_data. Directory offsets are
    // strictly increasing and the last one is bounded by the bkd_data length --
    // both established once at open, so query-time access needs no re-checking.
    uint64_t offset = 0;
    // Points in this leaf; <= points_per_leaf, and only the last leaf may be
    // short.
    uint32_t count = 0;
};

// Decoded bkd_index header (design 5.1). A default-constructed value already
// reads as the EMPTY index: leaf_count == 0 states emptiness explicitly,
// unlike the old implementation's implicit indexFP == 0 sentinel over an
// unchecked bkd_meta.
struct BkdIndexHeader {
    uint32_t format_version = kFormatVersion;
    // index_flags bits; diagnostic only, never branched on while reading.
    uint32_t flags = 0;
    // == sizeof(CppType) for field_type (INV-2). 0 only for a header that has
    // not been decoded yet.
    uint32_t bytes_per_dim = 0;
    // The type the index was BUILT with. The KeyCoder used at query time is
    // resolved from this, not from the query's own type, or the comparison
    // would silently run against a different byte order (INV-1).
    FieldType field_type {};
    uint64_t point_count = 0;
    // Distinct doc ids owning at least one point. Counted by the builder, not
    // pushed in from outside before finish() as the old docs_seen_ was.
    uint32_t doc_count = 0;
    // 0 == empty index: no bounds, no split values, no leaf directory, and a
    // zero-length bkd_data.
    uint32_t leaf_count = 0;
    uint32_t points_per_leaf = 0;
};

// Builder construction parameters (design 6.1). Validated once by
// BkdBuilder::create, so a constructed builder is always fully valid -- there
// is no half-initialized state to defend against later.
struct BkdBuilderOptions {
    // REQUIRED, == sizeof(CppType). 0 is the unset sentinel.
    uint32_t bytes_per_dim = 0;
    // REQUIRED. FieldType has no enumerator 0, so the value-initialized state is
    // an unambiguous unset sentinel.
    FieldType field_type {};
    uint32_t points_per_leaf = kDefaultPointsPerLeaf;
    // Resident point-buffer ceiling; crossing it sorts and spills a run.
    uint64_t build_buffer_bytes = kDefaultBuildBufferBytes;
    // Build-time RAM accounting. Legitimately null off-Doris (unit tests,
    // benchmarks), where only the local buffer bound applies.
    writer::MemoryReporter* reporter = nullptr;
};

// What a completed build reports back to its caller.
struct BkdStats {
    uint64_t point_count = 0;
    uint32_t doc_count = 0;
    uint32_t leaf_count = 0;
    // Encoded sizes of the two sub-files, for container bookkeeping and for
    // reporting real resident/on-disk cost instead of an estimate.
    uint64_t index_bytes = 0;
    uint64_t data_bytes = 0;
    // Mirrors index_flags::kBuiltWithSpill; diagnostic only, the emitted bytes
    // are identical either way.
    bool built_with_spill = false;
    // How many merge passes the spill path ran. 0 = no spill; 1 = every run was
    // folded in one k-way merge; >1 = the run count exceeded the fan-in the
    // memory bound allows, so runs were folded in groups first.
    uint32_t merge_passes = 0;
    // Largest resident cursor-window footprint any single merge held, in bytes.
    // This is the quantity the build_buffer_bytes ceiling is supposed to bound,
    // and it is reported rather than assumed so a test can hold the bound to
    // account instead of trusting the arithmetic that produced it.
    uint64_t peak_merge_buffer_bytes = 0;
};

// Where the two sub-files live inside the SNII container, as resolved from the
// blob logical index's named-file table. length == 0 is LEGAL and means the
// empty index (design 5.3) -- it must never be treated as corruption.
struct BkdSections {
    uint64_t index_offset = 0;
    uint64_t index_length = 0;
    uint64_t data_offset = 0;
    uint64_t data_length = 0;
};

} // namespace doris::snii::bkd

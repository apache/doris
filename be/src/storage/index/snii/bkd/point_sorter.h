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

#include <cstddef>
#include <cstdint>

// Ordering for the builder's fixed-width point records (design 6.3).
//
// A build-time record is [value: bytes_per_dim][doc_id: kPointDocIdBytes
// BIG-endian] and the order is the memcmp of the WHOLE record. Because the doc id
// tail is big-endian, that memcmp IS lexicographic (value, doc_id) order, which is
// why this function takes a record WIDTH and never learns where the value ends:
// the field boundary is not needed to compare, and (value, doc_id) becomes the
// sort key by construction rather than by a separate tie-break rule. leaf_codec
// then depends on the consequence -- doc ids ascend inside every run of equal
// values -- so this file and that one agree through the record layout alone.
//
// A FREE FUNCTION WITH NO STATE, deliberately. The old MSBRadixSorter was an
// abstract class whose inner IntroSorter held a shared_ptr<MSBRadixSorter>; that
// forced bkd_writer to derive from enable_shared_from_this, and the requirement
// propagated all the way out to InvertedIndexColumnWriter::_bkd_writer having to
// be a shared_ptr. Nothing here can impose an ownership model on a caller: there
// is no object to own.
//
// Everything the sorter sees is data the builder produced in this same run, never
// disk bytes, so its preconditions are internal invariants (DORIS_CHECK) and not
// Status returns -- the corruption contract of design 8 applies to the decode
// side, which this file is not part of.
namespace doris::snii::bkd::point_sorter {

// Sorts `count` records of `record_size` bytes each, ascending by the unsigned
// byte-wise comparison of the whole record, IN PLACE.
//
// In place is a requirement, not an implementation note: a run buffer is sized by
// BkdBuilderOptions::build_buffer_bytes (256 MB by default), and design 6.2 bounds
// build RSS by that figure, so sorting must not need a second buffer of the same
// size. The implementation is an MSB radix sort permuting the array bucket by
// bucket, falling back to a comparison sort once a bucket is small; peak auxiliary
// memory is one histogram per byte position still being distinguished.
//
// NOT stable, and stability is unobservable here: the whole record is the key, so
// two records that compare equal are byte-identical and no permutation of them can
// be told apart. Exact duplicates are legal input (an array column may repeat one
// value inside one row) and come back with the same multiplicity.
//
// `records` must hold count * record_size bytes and `record_size` must be
// non-zero; both are guaranteed by the builder that owns the buffer.
void sort(uint8_t* records, size_t count, uint32_t record_size);

} // namespace doris::snii::bkd::point_sorter

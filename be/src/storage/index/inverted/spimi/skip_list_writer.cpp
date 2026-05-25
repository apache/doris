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

#include "storage/index/inverted/spimi/skip_list_writer.h"

#include <cmath>
#include <limits>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Mirrors CLucene's MultiLevelSkipListWriter constructor: how many levels of
// skip data are useful for this document frequency, given the per-level fan-
// out factor `skip_interval`. df = 0 means no skip data at all.
//
// CLucene's Java original computes `floor(log(df) / log(skip_interval))` in
// IEEE-754 double precision. We MUST match that precision exactly: a float
// computation diverges around df ≈ 1.6e7 (single-precision log loses bits)
// and produces a level count that is off by one for some inputs, which makes
// the SPIMI-emitted skip list unreadable to CLucene's `SegmentTermDocs::
// skipTo` (the reader walks one level fewer/more than the writer wrote).
int32_t ComputeNumberOfSkipLevels(int32_t df, int32_t skip_interval, int32_t max_skip_levels) {
    if (df <= 0) {
        return 0;
    }
    const auto levels = static_cast<int32_t>(std::floor(
            std::log(static_cast<double>(df)) / std::log(static_cast<double>(skip_interval))));
    return std::min(levels, max_skip_levels);
}

} // namespace

SkipListWriter::SkipListWriter(int32_t skip_interval, int32_t max_skip_levels)
        : _skip_interval(skip_interval), _max_skip_levels(max_skip_levels) {
    DCHECK_GT(_skip_interval, 0);
    DCHECK_GT(_max_skip_levels, 0);
}

void SkipListWriter::EnsureLevels(int32_t levels) {
    if (static_cast<int32_t>(_skip_buffers.size()) < levels) {
        _skip_buffers.reserve(levels);
        while (static_cast<int32_t>(_skip_buffers.size()) < levels) {
            _skip_buffers.emplace_back(std::make_unique<MemoryLuceneOutput>());
        }
    }
    if (static_cast<int32_t>(_last_skip_doc.size()) < levels) {
        _last_skip_doc.resize(levels, 0);
        _last_skip_freq_pointer.resize(levels, 0);
        _last_skip_prox_pointer.resize(levels, 0);
    }
}

void SkipListWriter::Reset(int32_t df, int64_t freq_pointer_start, int64_t prox_pointer_start) {
    _number_of_skip_levels = ComputeNumberOfSkipLevels(df, _skip_interval, _max_skip_levels);
    EnsureLevels(_number_of_skip_levels);

    for (int32_t i = 0; i < _number_of_skip_levels; ++i) {
        _skip_buffers[i]->Clear();
        _last_skip_doc[i] = 0;
        _last_skip_freq_pointer[i] = freq_pointer_start;
        _last_skip_prox_pointer[i] = prox_pointer_start;
    }

    _cur_doc = 0;
    _cur_freq_pointer = freq_pointer_start;
    _cur_prox_pointer = prox_pointer_start;
}

void SkipListWriter::SetSkipData(int32_t doc, int64_t freq_pointer, int64_t prox_pointer) {
    _cur_doc = doc;
    _cur_freq_pointer = freq_pointer;
    _cur_prox_pointer = prox_pointer;
}

void SkipListWriter::WriteSkipEntry(int32_t level, MemoryLuceneOutput* level_buf) {
    // CLucene's skip-list format uses VInt for freq/prox pointer deltas (the
    // reader's `DefaultSkipListReader::readSkipData` calls `readVInt` and
    // implicitly assumes the delta fits in a signed 32-bit value). A delta
    // larger than INT32_MAX (≈ 2 GiB between two adjacent skip entries on
    // the .frq or .prx stream) would silently wrap negative under the cast,
    // and CLucene would decode it as a backwards skip, corrupting the
    // reader cursor. Practical Doris segments stay well under this. The
    // monotonicity invariant (the writer is append-only, so pointer deltas
    // are non-negative by construction) stays a debug-only DCHECK because a
    // violation indicates a writer bug rather than a runtime input boundary;
    // the magnitude check is promoted to DORIS_CHECK (always-on) because it
    // guards a real file-format failure mode that, while unreachable on real
    // workloads, would silently corrupt the on-disk segment if it fired.
    const int64_t freq_delta = _cur_freq_pointer - _last_skip_freq_pointer[level];
    const int64_t prox_delta = _cur_prox_pointer - _last_skip_prox_pointer[level];
    DCHECK_GE(freq_delta, 0);
    DCHECK_GE(prox_delta, 0);
    DORIS_CHECK(freq_delta <= static_cast<int64_t>(std::numeric_limits<int32_t>::max()));
    DORIS_CHECK(prox_delta <= static_cast<int64_t>(std::numeric_limits<int32_t>::max()));
    level_buf->WriteVInt(_cur_doc - _last_skip_doc[level]);
    level_buf->WriteVInt(static_cast<int32_t>(freq_delta));
    level_buf->WriteVInt(static_cast<int32_t>(prox_delta));

    _last_skip_doc[level] = _cur_doc;
    _last_skip_freq_pointer[level] = _cur_freq_pointer;
    _last_skip_prox_pointer[level] = _cur_prox_pointer;
}

void SkipListWriter::BufferSkip(int32_t df) {
    // Determine how many levels this skip propagates to. CLucene's logic:
    //   for (num_levels = 0; df % skip_interval == 0 && num_levels < max; df /= skip_interval)
    //     num_levels++;
    int32_t num_levels = 0;
    int32_t remaining = df;
    while ((remaining % _skip_interval) == 0 && num_levels < _number_of_skip_levels) {
        ++num_levels;
        remaining /= _skip_interval;
    }

    int64_t child_pointer = 0;
    for (int32_t level = 0; level < num_levels; ++level) {
        WriteSkipEntry(level, _skip_buffers[level].get());

        const int64_t new_child_pointer = _skip_buffers[level]->FilePointer();
        if (level != 0) {
            _skip_buffers[level]->WriteVLong(child_pointer);
        }
        child_pointer = new_child_pointer;
    }
}

int64_t SkipListWriter::WriteSkip(LuceneOutput* out) {
    const int64_t skip_pointer = out->FilePointer();
    if (_number_of_skip_levels == 0) {
        return skip_pointer;
    }

    for (int32_t level = _number_of_skip_levels - 1; level > 0; --level) {
        const auto& buf = _skip_buffers[level];
        const int64_t length = buf->FilePointer();
        if (length > 0) {
            out->WriteVLong(length);
            out->WriteBytes(buf->bytes().data(), buf->bytes().size());
        }
    }
    const auto& level0 = _skip_buffers[0];
    if (level0->FilePointer() > 0) {
        out->WriteBytes(level0->bytes().data(), level0->bytes().size());
    }

    return skip_pointer;
}

} // namespace doris::segment_v2::inverted_index::spimi

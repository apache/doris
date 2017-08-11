// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/mem_tracker.h"
#include "util/palo_metrics.h"

#include <algorithm>
#include <stdio.h>
#include <sstream>

namespace palo {

#define MEM_POOL_POISON (0x66aa77bb)

const int MemPool::DEFAULT_INITIAL_CHUNK_SIZE;
const int64_t MemPool::MAX_CHUNK_SIZE;

const char* MemPool::_s_llvm_class_name = "class.palo::MemPool";

// uint32_t MemPool::_s_zero_length_region alignas(max_align_t) = MEM_POOL_POISON;
uint32_t MemPool::_s_zero_length_region = MEM_POOL_POISON;

MemPool::MemPool(MemTracker* mem_tracker, int chunk_size) :
        _current_chunk_idx(-1),
        _last_offset_conversion_chunk_idx(-1),
        // round up chunk size to nearest 8 bytes
        _chunk_size(chunk_size == 0 ? 0 : ((chunk_size + 7) / 8) * 8),
        _total_allocated_bytes(0),
        // _total_chunk_bytes(0),
        _peak_allocated_bytes(0),
        _total_reserved_bytes(0),
        _mem_tracker(mem_tracker) {
    DCHECK_GE(_chunk_size, 0);
    DCHECK(mem_tracker != NULL);
}

MemPool::ChunkInfo::ChunkInfo(int64_t size, uint8_t* buf) :
        owns_data(true),
        data(buf),
        size(size),
        cumulative_allocated_bytes(0),
        allocated_bytes(0) {
    if (PaloMetrics::mem_pool_total_bytes() != NULL) {
        PaloMetrics::mem_pool_total_bytes()->increment(size);
    }
}

MemPool::~MemPool() {
    int64_t total_bytes_released = 0;
    for (size_t i = 0; i < _chunks.size(); ++i) {
        if (!_chunks[i].owns_data) {
            continue;
        }

        total_bytes_released += _chunks[i].size;
        free(_chunks[i].data);
    }
    _chunks.clear();

    _mem_tracker->release(total_bytes_released);
    // DCHECK(_chunks.empty()) << "Must call FreeAll() or AcquireData() for this pool";

    if (PaloMetrics::mem_pool_total_bytes() != NULL) {
        PaloMetrics::mem_pool_total_bytes()->increment(-total_bytes_released);
    }
}

void MemPool::free_all() {
    int64_t total_bytes_released = 0;
    for (size_t i = 0; i < _chunks.size(); ++i) {
        if (!_chunks[i].owns_data) {
            continue;
        }
        total_bytes_released += _chunks[i].size;
        free(_chunks[i].data);
    }
    _chunks.clear();
    _current_chunk_idx = -1;
    _last_offset_conversion_chunk_idx = -1;
    _total_allocated_bytes = 0;
    _total_reserved_bytes = 0;

    _mem_tracker->release(total_bytes_released);
    if (PaloMetrics::mem_pool_total_bytes() != NULL) {
        PaloMetrics::mem_pool_total_bytes()->increment(-total_bytes_released);
    }
}

bool MemPool::find_chunk(int64_t min_size, bool check_limits) {
    // Try to allocate from a free chunk. The first free chunk, if any, will be immediately
    // after the current chunk.
    int first_free_idx = _current_chunk_idx + 1;

    // (cast size() to signed int in order to avoid everything else being cast to
    // unsigned long, in particular -1)
    while (++_current_chunk_idx  < static_cast<int>(_chunks.size())) {
        // we found a free chunk
        DCHECK_EQ(_chunks[_current_chunk_idx].allocated_bytes, 0);
        if (_chunks[_current_chunk_idx].size >= min_size) {
            // This chunk is big enough.  Move it before the other free chunks.
            if (_current_chunk_idx != first_free_idx) {
                std::swap(_chunks[_current_chunk_idx], _chunks[first_free_idx]);
                _current_chunk_idx = first_free_idx;
            }
            break;
        }
    }

    if (_current_chunk_idx == static_cast<int>(_chunks.size())) {
        // need to allocate new chunk.
        int64_t chunk_size = _chunk_size;
        if (chunk_size == 0) {
            if (_current_chunk_idx == 0) {
                chunk_size = DEFAULT_INITIAL_CHUNK_SIZE;
            } else {
                // double the size of the last chunk in the list, up to a maximum
                // TODO: stick with constant sizes throughout?
                chunk_size = std::min(_chunks[_current_chunk_idx - 1].size * 2, MAX_CHUNK_SIZE);
            }
        }
        chunk_size = std::max(min_size, chunk_size);

        if (check_limits) {
            if (!_mem_tracker->try_consume(chunk_size)) {
                // We couldn't allocate a new chunk so _current_chunk_idx is now be past the
                // end of _chunks.
                DCHECK_EQ(_current_chunk_idx, static_cast<int>(_chunks.size()));
                _current_chunk_idx = static_cast<int>(_chunks.size()) - 1;
                return false;
            }
        } else {
            _mem_tracker->consume(chunk_size);
        }

        // Allocate a new chunk. Return early if malloc fails.
        uint8_t* buf = reinterpret_cast<uint8_t*>(malloc(chunk_size));
        if (UNLIKELY(buf == NULL)) {
            _mem_tracker->release(chunk_size);
            DCHECK_EQ(_current_chunk_idx, static_cast<int>(_chunks.size()));
            _current_chunk_idx = static_cast<int>(_chunks.size()) - 1;
            return false;
        }

        // If there are no free chunks put it at the end, otherwise before the first free.
        if (first_free_idx == static_cast<int>(_chunks.size())) {
            _chunks.push_back(ChunkInfo(chunk_size, buf));
        } else {
            _current_chunk_idx = first_free_idx;
            std::vector<ChunkInfo>::iterator insert_chunk = _chunks.begin() + _current_chunk_idx;
            _chunks.insert(insert_chunk, ChunkInfo(chunk_size, buf));
        }
        _total_reserved_bytes += chunk_size;
    }

    if (_current_chunk_idx > 0) {
        ChunkInfo& prev_chunk = _chunks[_current_chunk_idx - 1];
        _chunks[_current_chunk_idx].cumulative_allocated_bytes =
            prev_chunk.cumulative_allocated_bytes + prev_chunk.allocated_bytes;
    }

    DCHECK_LT(_current_chunk_idx, static_cast<int>(_chunks.size()));
    DCHECK(check_integrity(true));
    return true;
}

void MemPool::acquire_data(MemPool* src, bool keep_current) {
    DCHECK(src->check_integrity(false));
    int num_acquired_chunks = 0;

    if (keep_current) {
        num_acquired_chunks = src->_current_chunk_idx;
    } else if (src->get_free_offset() == 0) {
        // nothing in the last chunk
        num_acquired_chunks = src->_current_chunk_idx;
    } else {
        num_acquired_chunks = src->_current_chunk_idx + 1;
    }

    if (num_acquired_chunks <= 0) {
        if (!keep_current) {
            src->free_all();
        }
        return;
    }

    std::vector<ChunkInfo>::iterator end_chunk = src->_chunks.begin() + num_acquired_chunks;
    int64_t total_transfered_bytes = 0;
    for (std::vector<ChunkInfo>::iterator i = src->_chunks.begin(); i != end_chunk; ++i) {
        total_transfered_bytes += i->size;
    }
    src->_total_reserved_bytes -= total_transfered_bytes;
    _total_reserved_bytes += total_transfered_bytes;

    src->_mem_tracker->release(total_transfered_bytes);
    _mem_tracker->consume(total_transfered_bytes);

    // insert new chunks after _current_chunk_idx
    std::vector<ChunkInfo>::iterator insert_chunk = _chunks.begin() + _current_chunk_idx + 1;
    _chunks.insert(insert_chunk, src->_chunks.begin(), end_chunk);
    src->_chunks.erase(src->_chunks.begin(), end_chunk);
    _current_chunk_idx += num_acquired_chunks;

    if (keep_current) {
        src->_current_chunk_idx = 0;
        DCHECK(src->_chunks.size() == 1 || src->_chunks[1].allocated_bytes == 0);
        _total_allocated_bytes += src->_total_allocated_bytes - src->get_free_offset();
        src->_chunks[0].cumulative_allocated_bytes = 0;
        src->_total_allocated_bytes = src->get_free_offset();
    } else {
        src->_current_chunk_idx = -1;
        _total_allocated_bytes += src->_total_allocated_bytes;
        src->_total_allocated_bytes = 0;
    }
    _peak_allocated_bytes = std::max(_total_allocated_bytes, _peak_allocated_bytes);

    // recompute cumulative_allocated_bytes
    int start_idx = _chunks.size() - num_acquired_chunks;
    int64_t cumulative_bytes = (start_idx == 0
                                ? 0
                                : _chunks[start_idx - 1].cumulative_allocated_bytes
                                + _chunks[start_idx - 1].allocated_bytes);
    for (int i = start_idx; i <= _current_chunk_idx; ++i) {
        _chunks[i].cumulative_allocated_bytes = cumulative_bytes;
        cumulative_bytes += _chunks[i].allocated_bytes;
    }

    if (!keep_current) {
        src->free_all();
    }
    DCHECK(check_integrity(false));
}

bool MemPool::contains(uint8_t* ptr, int size) {
    for (int i = 0; i < _chunks.size(); ++i) {
        const ChunkInfo& info = _chunks[i];
        if (ptr >= info.data && ptr < info.data + info.allocated_bytes) {
            if (ptr + size > info.data + info.allocated_bytes) {
                DCHECK_LE(reinterpret_cast<size_t>(ptr + size),
                          reinterpret_cast<size_t>(info.data + info.allocated_bytes));
                return false;
            }
            return true;
        }
    }
    return false;
}

std::string MemPool::debug_string() {
    std::stringstream out;
    char str[16];
    out << "MemPool(#chunks=" << _chunks.size() << " [";
    for (int i = 0; i < _chunks.size(); ++i) {
        snprintf(str, 16, "0x%lx=", reinterpret_cast<size_t>(_chunks[i].data));
        out << (i > 0 ? " " : "")
            << str
            << _chunks[i].size
            << "/" << _chunks[i].cumulative_allocated_bytes
            << "/" << _chunks[i].allocated_bytes;
    }

    out << "] current_chunk=" << _current_chunk_idx
        << " total_sizes=" << get_total_chunk_sizes()
        << " total_alloc=" << _total_allocated_bytes
        << ")";
    return out.str();
}

int64_t MemPool::get_total_chunk_sizes() const {
    int64_t result = 0;
    for (int i = 0; i < _chunks.size(); ++i) {
        result += _chunks[i].size;
    }
    return result;
}

bool MemPool::check_integrity(bool current_chunk_empty) {
    // check that _current_chunk_idx points to the last chunk with allocated data
    DCHECK_LT(_current_chunk_idx, static_cast<int>(_chunks.size()));
    int64_t total_allocated = 0;
    for (int i = 0; i < _chunks.size(); ++i) {
        DCHECK_GT(_chunks[i].size, 0);
        if (i < _current_chunk_idx) {
            DCHECK_GT(_chunks[i].allocated_bytes, 0);
        } else if (i == _current_chunk_idx) {
            if (current_chunk_empty) {
                DCHECK_EQ(_chunks[i].allocated_bytes, 0);
            } else {
                DCHECK_GT(_chunks[i].allocated_bytes, 0);
            }
        } else {
            DCHECK_EQ(_chunks[i].allocated_bytes, 0);
        }

        if (i > 0 && i <= _current_chunk_idx) {
            DCHECK_EQ(_chunks[i - 1].cumulative_allocated_bytes + _chunks[i - 1].allocated_bytes,
                      _chunks[i].cumulative_allocated_bytes);
        }

        if (_chunk_size != 0) {
            DCHECK_GE(_chunks[i].size, _chunk_size);
        }
        total_allocated += _chunks[i].allocated_bytes;
    }

    DCHECK_EQ(total_allocated, _total_allocated_bytes);
    return true;
}

void MemPool::get_chunk_info(std::vector<std::pair<uint8_t*, int> >* chunk_info) {
    chunk_info->clear();
    for (std::vector<ChunkInfo>::iterator info = _chunks.begin(); info != _chunks.end(); ++info) {
        chunk_info->push_back(std::make_pair(info->data, info->allocated_bytes));
    }
}

std::string MemPool::debug_print() {
    char str[3];
    std::stringstream out;
    for (int i = 0; i < _chunks.size(); ++i) {
        ChunkInfo& info = _chunks[i];

        if (info.allocated_bytes == 0) {
            return out.str();
        }

        for (int j = 0; j < info.allocated_bytes; ++j) {
            snprintf(str, 3, "%x ", info.data[j]);
            out << str;
        }
    }
    return out.str();
}

} // end namespace palo

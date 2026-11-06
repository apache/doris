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

#include "storage/rowid_conversion.h"

#include <algorithm>
#include <variant>

#include "common/cast_set.h"
#include "common/exception.h"
#include "runtime/thread_context.h"
#include "storage/utils.h"

namespace doris {
namespace {
using Pair = RowIdConversion::RowIdPair;
constexpr Pair MISSING_ROW {UINT32_MAX, UINT32_MAX};

Status check_rowid_memory_limit(size_t reserved_memory) {
    if (!GlobalMemoryArbitrator::is_exceed_hard_mem_limit(reserved_memory)) {
        return Status::OK();
    }
    const auto* tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker();
    return Status::MemoryLimitExceeded(fmt::format(
            "RowIdConversion allocation failed, process memory exceed limit or sys available "
            "memory less than low water mark, {}, consuming tracker:<{}>, peak used {}, "
            "current used {}.",
            GlobalMemoryArbitrator::process_mem_log_str(), tracker->label(),
            tracker->peak_consumption(), tracker->consumption()));
}

// Both uncompressed layouts store segment/row pairs as the writer advances.
struct RowIdDestination {
    Pair next(const std::vector<uint32_t>& segment_rows) {
        if (segment < segment_rows.size() && row >= segment_rows[segment]) {
            ++segment;
            row = 0;
        }
        return {segment, row++};
    }
    uint32_t segment = 0;
    uint32_t row = 0;
};

struct DenseRowIdStorage {
    DenseRowIdStorage() = default;
    DenseRowIdStorage(const DenseRowIdStorage&) = delete;
    ~DenseRowIdStorage() { RELEASE_THREAD_MEM_TRACKER(tracked_bytes); }

    Status init_source(uint32_t rows) {
        RETURN_IF_ERROR(check_rowid_memory_limit(10 * 1024 * 1024));
        std::vector<Pair> values(rows, MISSING_ROW);
        size_t capacity = values.capacity();
        segments.emplace_back(std::move(values));
        row_capacity += capacity;
        // CLucene requires the ordinary STL matrix, so account its capacities manually.
        size_t bytes =
                row_capacity * sizeof(Pair) + segments.capacity() * sizeof(std::vector<Pair>);
        CONSUME_THREAD_MEM_TRACKER(bytes - tracked_bytes);
        tracked_bytes = bytes;
        return Status::OK();
    }
    Status append(uint32_t source, uint32_t row, const std::vector<uint32_t>& segment_rows) {
        segments[source][row] = destination.next(segment_rows);
        return Status::OK();
    }
    Status get(uint32_t source, uint32_t row, CompressedRowIdMap::Cursor&,
               CompressedRowIdMap::DecodeWorkspace&, Pair* value) const {
        *value = segments[source][row];
        return Status::OK();
    }
    Status finalize(const std::vector<uint32_t>&) { return Status::OK(); }
    size_t memory_usage() const { return tracked_bytes; }

    RowIdConversion::DenseMap segments;
    RowIdDestination destination;
    size_t row_capacity = 0;
    size_t tracked_bytes = 0;
};

struct LazyRowIdStorage {
    // A 4096-row chunk uses 32 KiB, preserving the existing sparse layout.
    static constexpr uint32_t ROWS_PER_CHUNK = 4096;
    using Chunks = DorisVector<std::unique_ptr<Pair[]>>;
    LazyRowIdStorage() = default;
    LazyRowIdStorage(const LazyRowIdStorage&) = delete;
    ~LazyRowIdStorage() { RELEASE_THREAD_MEM_TRACKER(chunk_bytes); }

    Status init_source(uint32_t rows) {
        RETURN_IF_ERROR(check_rowid_memory_limit(10 * 1024 * 1024));
        segments.emplace_back((cast_set<size_t>(rows) + ROWS_PER_CHUNK - 1) / ROWS_PER_CHUNK);
        return Status::OK();
    }
    Status append(uint32_t source, uint32_t row, const std::vector<uint32_t>& segment_rows) {
        auto& chunk = segments[source][row / ROWS_PER_CHUNK];
        if (chunk == nullptr) {
            constexpr size_t CHUNK_BYTES = ROWS_PER_CHUNK * sizeof(Pair);
            RETURN_IF_ERROR(check_rowid_memory_limit(CHUNK_BYTES));
            chunk = std::make_unique<Pair[]>(ROWS_PER_CHUNK);
            std::fill_n(chunk.get(), ROWS_PER_CHUNK, MISSING_ROW);
            CONSUME_THREAD_MEM_TRACKER(CHUNK_BYTES);
            chunk_bytes += CHUNK_BYTES;
        }
        chunk[row % ROWS_PER_CHUNK] = destination.next(segment_rows);
        return Status::OK();
    }
    Status get(uint32_t source, uint32_t row, CompressedRowIdMap::Cursor&,
               CompressedRowIdMap::DecodeWorkspace&, Pair* value) const {
        const auto& chunk = segments[source][row / ROWS_PER_CHUNK];
        *value = chunk == nullptr ? MISSING_ROW : chunk[row % ROWS_PER_CHUNK];
        return Status::OK();
    }
    Status finalize(const std::vector<uint32_t>&) { return Status::OK(); }
    size_t memory_usage() const {
        size_t bytes = chunk_bytes + segments.capacity() * sizeof(Chunks);
        for (const auto& chunks : segments)
            bytes += chunks.capacity() * sizeof(std::unique_ptr<Pair[]>);
        return bytes;
    }

    DorisVector<Chunks> segments;
    RowIdDestination destination;
    size_t chunk_bytes = 0;
};

struct CompressedRowIdStorage {
    Status init_source(uint32_t rows) { return map.init_source(rows); }
    Status append(uint32_t source, uint32_t row, const std::vector<uint32_t>&) {
        RETURN_IF_ERROR(map.append(source, row, next_global_row));
        ++next_global_row;
        return Status::OK();
    }
    Status finalize(const std::vector<uint32_t>& destination_rows) {
        if (finalized) return Status::OK();
        DorisVector<uint64_t> prefix;
        prefix.reserve(destination_rows.size() + 1);
        prefix.push_back(0);
        for (uint32_t rows : destination_rows) {
            if (rows > UINT64_MAX - prefix.back()) {
                return Status::InvalidArgument("Rowid destination prefix overflow");
            }
            prefix.push_back(prefix.back() + rows);
        }
        if (prefix.back() != next_global_row) {
            return Status::InvalidArgument("Rowid output row count mismatch: mapping {}, output {}",
                                           next_global_row, prefix.back());
        }
        RETURN_IF_ERROR(map.finish());
        destination_prefix = std::move(prefix);
        finalized = true;
        return Status::OK();
    }
    Status get(uint32_t source, uint32_t row, CompressedRowIdMap::Cursor& cursor,
               CompressedRowIdMap::DecodeWorkspace& workspace, Pair* value) const {
        if (!finalized) {
            return Status::InvalidArgument("Rowid conversion must be finalized before lookup");
        }
        uint64_t global = 0;
        RETURN_IF_ERROR(map.get(source, row, cursor, workspace, &global));
        if (global == CompressedRowIdMap::MISSING) {
            *value = MISSING_ROW;
            return Status::OK();
        }
        if (global >= destination_prefix.back()) {
            return Status::InternalError("Rowid conversion target is outside output rowset");
        }
        auto end = std::upper_bound(destination_prefix.begin(), destination_prefix.end(), global);
        size_t position = static_cast<size_t>(end - destination_prefix.begin() - 1);
        *value = {cast_set<uint32_t>(position),
                  cast_set<uint32_t>(global - destination_prefix[position])};
        return Status::OK();
    }
    size_t memory_usage() const {
        return map.memory_usage().allocated_bytes() +
               destination_prefix.capacity() * sizeof(uint64_t);
    }

    CompressedRowIdMap map;
    DorisVector<uint64_t> destination_prefix;
    uint64_t next_global_row = 0;
    bool finalized = false;
};
} // namespace

struct RowIdConversion::Storage {
    explicit Storage(Mode mode) {
        switch (mode) {
        case Mode::DENSE:
            break;
        case Mode::LAZY_CHUNKED:
            map.emplace<LazyRowIdStorage>();
            break;
        case Mode::COMPRESSED:
            map.emplace<CompressedRowIdStorage>();
            break;
        }
    }
    static void* operator new(size_t size) { return Allocator<false>().alloc(size); }
    static void operator delete(void* pointer, size_t size) {
        Allocator<false>().free(pointer, size);
    }
    std::variant<DenseRowIdStorage, LazyRowIdStorage, CompressedRowIdStorage> map;
};

RowIdConversion::RowIdConversion(Mode mode)
        : _storage(std::make_unique<Storage>(mode)), _mode(mode) {}
RowIdConversion::~RowIdConversion() = default;

void RowIdConversion::set_mode(Mode mode) {
    DORIS_CHECK(_segment_num_rows.empty());
    _storage = std::make_unique<Storage>(mode);
    _mode = mode;
}

Status RowIdConversion::init_segment_map(const RowsetId& src_rowset_id,
                                         const std::vector<uint32_t>& segment_ids,
                                         const std::vector<uint32_t>& num_rows) {
    RETURN_IF_CATCH_EXCEPTION(return std::visit(
            [&](auto& storage) -> Status {
                DCHECK_EQ(segment_ids.size(), num_rows.size());
                for (size_t i = 0; i < num_rows.size(); ++i) {
                    auto src_segment =
                            std::pair<RowsetId, uint32_t> {src_rowset_id, segment_ids[i]};
                    auto iter = _segment_to_id_map.find(src_segment);
                    // A segment-group reader can be reopened, so reuse existing source-segment maps.
                    if (iter != _segment_to_id_map.end()) {
                        DORIS_CHECK_EQ(_segment_num_rows[iter->second], num_rows[i]);
                        continue;
                    }
                    uint32_t id = cast_set<uint32_t>(_segment_num_rows.size());
                    RETURN_IF_ERROR(storage.init_source(num_rows[i]));
                    auto inserted = _segment_to_id_map.emplace(src_segment, id);
                    DORIS_CHECK(inserted.second);
                    _id_to_segment_map.push_back(src_segment);
                    _segment_num_rows.push_back(num_rows[i]);
                }
                return Status::OK();
            },
            _storage->map));
    return Status::OK();
}

Status RowIdConversion::add(const std::vector<RowLocation>& rss_row_ids,
                            const std::vector<uint32_t>& dst_segments_num_row) {
    RETURN_IF_CATCH_EXCEPTION(return std::visit(
            [&](auto& storage) -> Status {
                for (const auto& item : rss_row_ids) {
                    if (item.row_id == -1) continue;
                    uint32_t id = _segment_to_id_map.at({item.rowset_id, item.segment_id});
                    DCHECK_LT(item.row_id, _segment_num_rows[id]);
                    RETURN_IF_ERROR(storage.append(id, item.row_id, dst_segments_num_row));
                }
                return Status::OK();
            },
            _storage->map));
    return Status::OK();
}

Status RowIdConversion::finalize(const std::vector<uint32_t>& destination_rows) {
    RETURN_IF_CATCH_EXCEPTION(return std::visit(
            [&](auto& storage) { return storage.finalize(destination_rows); }, _storage->map));
    return Status::OK();
}

RowIdConversionSource RowIdConversion::source(uint32_t source_id) const {
    DORIS_CHECK_LT(source_id, _segment_num_rows.size());
    if (const auto* dense = std::get_if<DenseRowIdStorage>(&_storage->map)) {
        return RowIdConversionSource(dense->segments[source_id]);
    }
    return RowIdConversionSource(this, source_id, _segment_num_rows[source_id]);
}

RowIdConversionSource RowIdConversion::find_source(const RowsetId& rowset, uint32_t segment) const {
    auto it = _segment_to_id_map.find({rowset, segment});
    return it == _segment_to_id_map.end() ? RowIdConversionSource() : source(it->second);
}

Status RowIdConversion::lookup_source(uint32_t source_id, uint32_t row,
                                      CompressedRowIdMap::Cursor& cursor,
                                      CompressedRowIdMap::DecodeWorkspace& workspace,
                                      RowIdConversionSource::Pair* destination) const {
    *destination = MISSING_ROW;
    if (row >= _segment_num_rows[source_id]) return Status::OK();
    return std::visit(
            [&](const auto& storage) {
                return storage.get(source_id, row, cursor, workspace, destination);
            },
            _storage->map);
}

int RowIdConversion::get(const RowLocation& src, DestinationRowId* dst) const {
    DORIS_CHECK(_mode != Mode::COMPRESSED);
    CompressedRowIdMap::Cursor cursor;
    CompressedRowIdMap::DecodeWorkspace workspace;
    Pair value;
    auto status =
            find_source(src.rowset_id, src.segment_id).get(src.row_id, cursor, workspace, &value);
    DORIS_CHECK(status.ok()); // The two uncompressed layouts cannot fail during lookup.
    if (value == MISSING_ROW) return -1;
    dst->segment_pos = value.first;
    dst->row_id = value.second;
    return 0;
}

const RowIdConversion::DenseMap& RowIdConversion::get_rowid_conversion_map() const {
    DORIS_CHECK(_mode == Mode::DENSE);
    return std::get<DenseRowIdStorage>(_storage->map).segments;
}

size_t RowIdConversion::memory_usage() const {
    return sizeof(Storage) +
           std::visit([](const auto& storage) { return storage.memory_usage(); }, _storage->map);
}

CompressedRowIdMap::MemoryUsage RowIdConversion::compressed_memory_usage() const {
    const auto* compressed = std::get_if<CompressedRowIdStorage>(&_storage->map);
    return compressed == nullptr ? CompressedRowIdMap::MemoryUsage {}
                                 : compressed->map.memory_usage();
}

Status RowIdConversionSource::get(uint32_t row, CompressedRowIdMap::Cursor& cursor,
                                  CompressedRowIdMap::DecodeWorkspace& workspace,
                                  Pair* destination) const {
    if (_owner == nullptr) {
        *destination = row < _dense.size() ? _dense[row] : MISSING_ROW;
        return Status::OK();
    }
    return _owner->lookup_source(_source, row, cursor, workspace, destination);
}

} // namespace doris

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

#include "common/exception.h"

namespace doris {

Status RowIdConversion::init_segment_map(const RowsetId& src_rowset_id,
                                         const std::vector<uint32_t>& segment_ids,
                                         const std::vector<uint32_t>& num_rows) {
    RETURN_IF_CATCH_EXCEPTION(return ([&]() -> Status {
        DCHECK_EQ(segment_ids.size(), num_rows.size());
        for (size_t i = 0; i < num_rows.size(); i++) {
            auto src_segment = std::pair<RowsetId, uint32_t> {src_rowset_id, segment_ids[i]};
            auto iter = _segment_to_id_map.find(src_segment);
            // A segment-group reader can be reopened, so reuse existing source-segment maps.
            if (iter != _segment_to_id_map.end()) {
                DORIS_CHECK_LT(iter->second, _segment_num_rows.size());
                DORIS_CHECK_EQ(_segment_num_rows[iter->second], num_rows[i]);
                continue;
            }

            if (_mode != Mode::COMPRESSED) {
                constexpr size_t RESERVED_MEMORY = 10 * 1024 * 1024; // 10M
                RETURN_IF_ERROR(check_memory_limit(RESERVED_MEMORY));
            }
            // Compressed allocations use Doris allocators, which check the actual
            // directory/builder/page size and charge the current task tracker.

            uint32_t id = cast_set<uint32_t>(_segment_num_rows.size());
            auto insert_result = _segment_to_id_map.emplace(src_segment, id);
            DORIS_CHECK(insert_result.second);
            _id_to_segment_map.push_back(src_segment);
            _segment_num_rows.push_back(num_rows[i]);
            if (_mode == Mode::COMPRESSED) {
                RETURN_IF_ERROR(_compressed->init_source(num_rows[i]));
                continue;
            }
            if (_mode == Mode::LAZY_CHUNKED) {
                _lazy_segments_rowid_map.emplace_back();
                auto& chunks = _lazy_segments_rowid_map.back();
                chunks.resize((cast_set<size_t>(num_rows[i]) + ROWS_PER_CHUNK - 1) /
                              ROWS_PER_CHUNK);
                track_lazy_mem_usage(chunks.capacity() * sizeof(std::unique_ptr<RowIdPair[]>));
                continue;
            }

            std::vector<std::pair<uint32_t, uint32_t>> vec(
                    num_rows[i], std::pair<uint32_t, uint32_t>(UINT32_MAX, UINT32_MAX));

            //NOTE: manually count _segments_rowid_map's memory here, because _segments_rowid_map could be used by indexCompaction.
            // indexCompaction is a thridparty code, it's too complex to modify it.
            // refer compact_column.
            track_mem_usage(vec.capacity());
            _segments_rowid_map.emplace_back(std::move(vec));
        }
        return Status::OK();
    })());
    return Status::OK();
}

Status RowIdConversion::finalize(const std::vector<uint32_t>& destination_rows) {
    if (_mode != Mode::COMPRESSED || _finalized) {
        return Status::OK();
    }
    RETURN_IF_CATCH_EXCEPTION({
        DorisVector<uint64_t> prefix;
        prefix.reserve(destination_rows.size() + 1);
        prefix.push_back(0);
        for (uint32_t rows : destination_rows) {
            if (rows > UINT64_MAX - prefix.back()) {
                return Status::InvalidArgument("Rowid destination prefix overflow");
            }
            prefix.push_back(prefix.back() + rows);
        }
        if (prefix.back() != _next_global_row) {
            return Status::InvalidArgument("Rowid output row count mismatch: mapping {}, output {}",
                                           _next_global_row, prefix.back());
        }
        RETURN_IF_ERROR(_compressed->finish());
        _destination_prefix = std::move(prefix);
        _finalized = true;
    });
    return Status::OK();
}

RowIdConversionSource RowIdConversion::source(uint32_t source_id) const {
    DORIS_CHECK_LT(source_id, _segment_num_rows.size());
    if (_mode == Mode::DENSE) {
        return RowIdConversionSource(_segments_rowid_map[source_id]);
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
    *destination = {UINT32_MAX, UINT32_MAX};
    if (row >= _segment_num_rows[source_id]) {
        return Status::OK();
    }
    if (_mode == Mode::LAZY_CHUNKED) {
        if (auto* value = get_lazy_destination(source_id, row); value != nullptr) {
            *destination = *value;
        }
        return Status::OK();
    }
    DORIS_CHECK(_mode == Mode::COMPRESSED);
    if (!_finalized) {
        return Status::InvalidArgument("Rowid conversion must be finalized before lookup");
    }
    uint64_t global = 0;
    RETURN_IF_ERROR(_compressed->get(source_id, row, cursor, workspace, &global));
    if (global == CompressedRowIdMap::MISSING) {
        return Status::OK();
    }
    if (global >= _destination_prefix.back()) {
        return Status::InternalError("Rowid conversion target is outside output rowset");
    }
    auto end = std::upper_bound(_destination_prefix.begin(), _destination_prefix.end(), global);
    size_t position = static_cast<size_t>(end - _destination_prefix.begin() - 1);
    *destination = {cast_set<uint32_t>(position),
                    cast_set<uint32_t>(global - _destination_prefix[position])};
    return Status::OK();
}

Status RowIdConversionSource::get(uint32_t row, CompressedRowIdMap::Cursor& cursor,
                                  CompressedRowIdMap::DecodeWorkspace& workspace,
                                  Pair* destination) const {
    if (_owner == nullptr) {
        *destination = row < _dense.size() ? _dense[row] : Pair {UINT32_MAX, UINT32_MAX};
        return Status::OK();
    }
    return _owner->lookup_source(_source, row, cursor, workspace, destination);
}

} // namespace doris

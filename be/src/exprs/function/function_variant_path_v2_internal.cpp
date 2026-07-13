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

#include "exprs/function/function_variant_path_v2_internal.h"

#include <limits>

#include "common/exception.h"

namespace doris::variant_native_v2_internal {

bool is_outer_null(std::span<const uint8_t> outer_nulls, size_t row) noexcept {
    return !outer_nulls.empty() && outer_nulls[row] != 0;
}

VariantPathV2BatchReader::VariantPathV2BatchReader(const ColumnVariantV2& source,
                                                   const ResolvedVariantElementV2Path& path)
        : _source(source.read_view()),
          _path(path),
          _resolved_metadata(_source.metadata_count(), 0) {
    DORIS_CHECK_GT(_path.size(), 0);
    if (_source.metadata_count() != 0 &&
        _path.size() > std::numeric_limits<size_t>::max() / _source.metadata_count()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant path cache size overflows size_t");
    }
    _object_ids.assign(_source.metadata_count() * _path.size(), -1);
}

void VariantPathV2BatchReader::_resolve_metadata(uint32_t metadata_id) {
    if (_resolved_metadata[metadata_id] != 0) {
        return;
    }
    const VariantMetadataRef metadata = _source.metadata_at(metadata_id);
    const size_t base = static_cast<size_t>(metadata_id) * _path.size();
    for (size_t position = 0; position < _path.size(); ++position) {
        if (_path.kind_at(position) == VariantElementV2PathSegment::Kind::OBJECT_KEY) {
            _object_ids[base + position] = metadata.find_key(_path.object_key_at(position));
        }
    }
    _resolved_metadata[metadata_id] = 1;
}

bool VariantPathV2BatchReader::find_at(size_t row, VariantValueRef* const output) {
    DORIS_CHECK(output != nullptr);
    const uint32_t metadata_id = _source.metadata_id_at(row);
    _resolve_metadata(metadata_id);
    VariantValueRef current = _source.value_at(row);
    const size_t cache_base = static_cast<size_t>(metadata_id) * _path.size();
    for (size_t position = 0; position < _path.size(); ++position) {
        if (_path.kind_at(position) == VariantElementV2PathSegment::Kind::OBJECT_KEY) {
            const int64_t field_id = _object_ids[cache_base + position];
            if (current.basic_type() != VariantBasicType::OBJECT || field_id < 0 ||
                !current.object_find_by_id(static_cast<uint32_t>(field_id), &current)) {
                return false;
            }
        } else {
            if (current.basic_type() != VariantBasicType::ARRAY) {
                return false;
            }
            const int64_t requested_index = _path.array_index_at(position);
            const int64_t element_count = current.num_elements();
            const int64_t resolved_index =
                    requested_index < 0 ? element_count + requested_index : requested_index;
            if (resolved_index < 0 || resolved_index >= element_count) {
                return false;
            }
            current = current.array_at(static_cast<uint32_t>(resolved_index));
        }
    }
    *output = current;
    return true;
}

size_t VariantPathV2BatchReader::metadata_count() const noexcept {
    return _source.metadata_count();
}

uint32_t VariantPathV2BatchReader::metadata_id_at(size_t row) const {
    return _source.metadata_id_at(row);
}

VariantMetadataRef VariantPathV2BatchReader::metadata_at(uint32_t id) {
    return _source.metadata_at(id);
}

} // namespace doris::variant_native_v2_internal

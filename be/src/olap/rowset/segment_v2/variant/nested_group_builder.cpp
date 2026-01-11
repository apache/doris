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

#include "olap/rowset/segment_v2/variant/nested_group_builder.h"

#include <algorithm>
#include <glog/logging.h>
#include <string>

#include "common/exception.h"
#include "util/jsonb_document.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"

namespace doris::segment_v2 {

void NestedGroup::ensure_offsets() {
    if (!offsets) {
        offsets = vectorized::ColumnOffset64::create();
    }
}

Status NestedGroupBuilder::build_from_jsonb(const vectorized::ColumnPtr& jsonb_column,
                                           const vectorized::PathInData& base_path,
                                           NestedGroupsMap& nested_groups, size_t num_rows) {
    if (!jsonb_column) {
        return Status::OK();
    }

    const auto* col_nullable =
            vectorized::check_and_get_column<vectorized::ColumnNullable>(jsonb_column.get());
    const vectorized::IColumn* data_col = jsonb_column.get();
    const vectorized::ColumnUInt8* null_map = nullptr;
    if (col_nullable) {
        data_col = &col_nullable->get_nested_column();
        null_map = &col_nullable->get_null_map_column();
    }

    const auto* str_col = vectorized::check_and_get_column<vectorized::ColumnString>(data_col);
    if (!str_col) {
        return Status::InvalidArgument("NestedGroupBuilder expects JSONB as ColumnString, got {}",
                                       data_col->get_name());
    }

    // helper lambda to pad all existing NestedGroups so they have offsets
    // for rows [0, target_row]. This ensures each row has a corresponding offset entry.
    auto pad_all_groups_to_row = [&nested_groups](size_t target_row) {
        for (auto& [path, group] : nested_groups) {
            if (!group || group->is_disabled) {
                continue;
            }
            group->ensure_offsets();
            auto* offsets_col =
                    assert_cast<vectorized::ColumnOffset64*>(group->offsets.get());
            // if offsets.size() <= target_row, this group is missing entries
            // for some rows. Pad with the same offset (indicating empty arrays).
            while (offsets_col->size() <= target_row) {
                offsets_col->get_data().push_back(static_cast<uint64_t>(group->current_flat_size));
            }
        }
    };

    const size_t rows = std::min(num_rows, str_col->size());
    for (size_t r = 0; r < rows; ++r) {
        if (null_map && (*null_map).get_data()[r]) {
            // for null rows, pad all existing groups with empty arrays
            pad_all_groups_to_row(r);
            continue;
        }
        const auto val = str_col->get_data_at(r);
        if (val.size == 0) {
            // empty JSONB, pad all existing groups with empty arrays
            pad_all_groups_to_row(r);
            continue;
        }

        const doris::JsonbValue* root = doris::JsonbDocument::createValue(val.data, val.size);
        if (!root) {
            pad_all_groups_to_row(r);
            continue;
        }

        // base_path is the JSON path of this JSONB column in ColumnVariant.
        // For root JSONB, base_path is empty and we only traverse into objects to discover
        // nested arrays under named fields.
        RETURN_IF_ERROR(_process_jsonb_value(root, base_path, nested_groups, r, 0));

        // after processing this row, pad any groups that weren't updated.
        // If a row doesn't contain a field that corresponds to a NestedGroup, we need to
        // add an empty array entry for that row.
        pad_all_groups_to_row(r);
    }

    return Status::OK();
}

Status NestedGroupBuilder::_process_jsonb_value(const doris::JsonbValue* value,
                                               const vectorized::PathInData& current_path,
                                               NestedGroupsMap& nested_groups, size_t row_idx,
                                               size_t depth) {
    if (!value) {
        return Status::OK();
    }
    if (_max_depth > 0 && depth > _max_depth) {
        return Status::InvalidArgument(
                "NestedGroupBuilder: nested depth {} exceeds max_depth {} at path '{}'. "
                "Consider increasing 'variant_nested_group_max_depth' configuration.",
                depth, _max_depth, current_path.get_path());
    }

    if (value->isObject()) {
        const auto* obj = value->unpack<doris::ObjectVal>();
        for (auto it = obj->begin(); it != obj->end(); ++it) {
            std::string key(it->getKeyStr(), it->klen());
            vectorized::PathInData next =
                    current_path.empty() ? vectorized::PathInData(key)
                                         : vectorized::PathInData(current_path.get_path() + "." +
                                                                  key);
            RETURN_IF_ERROR(_process_jsonb_value(it->value(), next, nested_groups, row_idx,
                                                depth + 1));
        }
        return Status::OK();
    }

    if (value->isArray()) {
        if (!_is_array_of_objects(value)) {
            return Status::OK();
        }

        // For top-level arrays (current_path is empty), use special "$root" path marker.
        // For nested arrays, use the actual current_path.
        vectorized::PathInData array_path =
                current_path.empty() ? vectorized::PathInData(std::string(kRootNestedGroupPath))
                                     : current_path;

        // Get or create group keyed by array path.
        std::shared_ptr<NestedGroup>& gptr = nested_groups[array_path];
        if (!gptr) {
            gptr = std::make_shared<NestedGroup>();
            gptr->path = array_path;
        }

        if (_handle_conflict(*gptr, /*is_array_object=*/true)) {
            return Status::OK();
        }

        return _process_array_of_objects(value, *gptr, row_idx, depth + 1);
    }

    return Status::OK();
}

bool NestedGroupBuilder::_is_array_of_objects(const doris::JsonbValue* arr_value) const {
    if (!arr_value || !arr_value->isArray()) {
        return false;
    }
    const auto* arr = arr_value->unpack<doris::ArrayVal>();
    const int n = arr->numElem();
    for (int i = 0; i < n; ++i) {
        const auto* elem = arr->get(i);
        if (!elem || elem->isNull()) {
            continue;
        }
        if (!elem->isObject()) {
            return false;
        }
    }
    return true;
}

Status NestedGroupBuilder::_process_array_of_objects(const doris::JsonbValue* arr_value,
                                                    NestedGroup& group, size_t parent_row_idx,
                                                    size_t depth) {
    DCHECK(arr_value && arr_value->isArray());
    group.ensure_offsets();
    auto* offsets_col = assert_cast<vectorized::ColumnOffset64*>(group.offsets.get());

    // Back-fill missing rows with empty arrays before processing current row.
    // This handles the case when a NestedGroup is created mid-batch (e.g., when
    // mixing top-level arrays and objects), ensuring earlier rows have proper offsets.
    while (offsets_col->size() < parent_row_idx) {
        offsets_col->get_data().push_back(static_cast<uint64_t>(group.current_flat_size));
    }

    const auto* arr = arr_value->unpack<doris::ArrayVal>();
    const int n = arr->numElem();

    const size_t prev_total = group.current_flat_size;
    const size_t new_total = prev_total + static_cast<size_t>(std::max(0, n));
    offsets_col->get_data().push_back(static_cast<uint64_t>(new_total));
    group.current_flat_size = new_total;

    // Process each element (flat index in [prev_total, new_total)).
    size_t flat_idx = prev_total;
    for (int i = 0; i < n; ++i, ++flat_idx) {
        const auto* elem = arr->get(i);

        std::unordered_set<std::string> seen_child;
        std::unordered_set<std::string> seen_nested;

        if (elem && !elem->isNull()) {
            if (!elem->isObject()) {
                // array<object> validation already checked, skip defensively.
            } else {
                RETURN_IF_ERROR(_process_object_as_paths(elem, vectorized::PathInData {}, group,
                                                        flat_idx, seen_child, seen_nested,
                                                        depth + 1));
            }
        }

        // Fill defaults for missing scalar children.
        for (auto& [p, sub] : group.children) {
            if (!seen_child.contains(p.get_path())) {
                sub.insert_default();
            }
        }
        // Fill empty offsets for missing nested groups.
        for (auto& [p, ng] : group.nested_groups) {
            if (!seen_nested.contains(p.get_path())) {
                ng->ensure_offsets();
                auto* off = assert_cast<vectorized::ColumnOffset64*>(ng->offsets.get());
                off->get_data().push_back(static_cast<uint64_t>(ng->current_flat_size));
            }
        }
    }

    return Status::OK();
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status NestedGroupBuilder::_process_object_as_paths(
        const doris::JsonbValue* obj_value, const vectorized::PathInData& current_prefix,
        NestedGroup& group, size_t element_flat_idx,
        std::unordered_set<std::string>& seen_child_paths,
        std::unordered_set<std::string>& seen_nested_paths, size_t depth) {
    DCHECK(obj_value && obj_value->isObject());
    if (_max_depth > 0 && depth > _max_depth) {
        return Status::InvalidArgument(
                "NestedGroupBuilder: nested depth {} exceeds max_depth {} at path prefix '{}'. "
                "Consider increasing 'variant_nested_group_max_depth' configuration.",
                depth, _max_depth, current_prefix.get_path());
    }

    const auto* obj = obj_value->unpack<doris::ObjectVal>();
    for (const auto& kv : *obj) {
        std::string key(kv.getKeyStr(), kv.klen());
        vectorized::PathInData next_prefix =
                current_prefix.empty() ? vectorized::PathInData(key)
                                       : vectorized::PathInData(current_prefix.get_path() + "." +
                                                                key);
        const auto* v = kv.value();
        if (!v) {
            continue;
        }

        if (v->isObject()) {
            // flatten object fields into dotted paths.
            RETURN_IF_ERROR(_process_object_as_paths(v, next_prefix, group, element_flat_idx,
                                                    seen_child_paths, seen_nested_paths,
                                                    depth + 1));
            continue;
        }

        if (v->isArray() && _is_array_of_objects(v)) {
            // Nested array<object> inside this group.
            // array<object> has the highest priority. If the same path was
            // previously treated as a scalar child, discard it.
            if (auto it_child = group.children.find(next_prefix); it_child != group.children.end()) {
                group.children.erase(it_child);
            }
            std::shared_ptr<NestedGroup>& ng = group.nested_groups[next_prefix];
            if (!ng) {
                ng = std::make_shared<NestedGroup>();
                ng->path = next_prefix;
            }

            if (_handle_conflict(*ng, /*is_array_object=*/true)) {
                continue;
            }

            // Ensure offsets size up to current parent element.
            ng->ensure_offsets();
            auto* off = assert_cast<vectorized::ColumnOffset64*>(ng->offsets.get());
            if (off->size() < element_flat_idx) {
                // fill missing parent elements with empty arrays.
                const size_t gap = element_flat_idx - off->size();
                for (size_t i = 0; i < gap; ++i) {
                    off->get_data().push_back(static_cast<uint64_t>(ng->current_flat_size));
                }
            }

            // Process nested group for this parent element (one offsets entry appended inside).
            RETURN_IF_ERROR(_process_array_of_objects(v, *ng, element_flat_idx, depth + 1));
            seen_nested_paths.insert(ng->path.get_path());
            continue;
        }

        // Scalar / non-array value becomes a child subcolumn.
        // if this path is already a nested array<object>, discard scalars.
        if (group.nested_groups.contains(next_prefix)) {
            continue;
        }
        vectorized::Field f;
        RETURN_IF_ERROR(_jsonb_to_field(v, f));

        // Ensure subcolumn exists and is nullable (for NestedGroup children, we need nullable
        // to support NULL values when a field is missing in some rows)
        if (group.children.find(next_prefix) == group.children.end()) {
            // Create a new nullable subcolumn for this path
            group.children[next_prefix] = vectorized::ColumnVariant::Subcolumn(0, true, false);
        }

        auto& sub = group.children[next_prefix];
        if (sub.size() < element_flat_idx) {
            sub.insert_many_defaults(element_flat_idx - sub.size());
        }
        try {
            sub.insert(f);
        } catch (const doris::Exception& e) {
            return Status::InternalError("NestedGroupBuilder insert failed at {}: {}",
                                         next_prefix.get_path(), e.to_string());
        }
        seen_child_paths.insert(next_prefix.get_path());
    }

    return Status::OK();
}

Status NestedGroupBuilder::_jsonb_to_field(const doris::JsonbValue* value,
                                          vectorized::Field& out) const {
    if (!value || value->isNull()) {
        out = vectorized::Field();
        return Status::OK();
    }
    if (value->isTrue()) {
        out = vectorized::Field::create_field<PrimitiveType::TYPE_BOOLEAN>(true);
        return Status::OK();
    }
    if (value->isFalse()) {
        out = vectorized::Field::create_field<PrimitiveType::TYPE_BOOLEAN>(false);
        return Status::OK();
    }
    if (value->isInt()) {
        out = vectorized::Field::create_field<PrimitiveType::TYPE_BIGINT>(static_cast<int64_t>(value->int_val()));
        return Status::OK();
    }
    if (value->isDouble()) {
        out = vectorized::Field::create_field<PrimitiveType::TYPE_DOUBLE>(value->unpack<doris::JsonbDoubleVal>()->val());
        return Status::OK();
    }
    if (value->isFloat()) {
        out = vectorized::Field::create_field<PrimitiveType::TYPE_DOUBLE>(static_cast<double>(
                value->unpack<doris::JsonbFloatVal>()->val()));
        return Status::OK();
    }
    if (value->isString()) {
        const auto* s = value->unpack<doris::JsonbStringVal>();
        out = vectorized::Field::create_field<PrimitiveType::TYPE_STRING>(
                vectorized::String(s->getBlob(), s->getBlobLen()));
        return Status::OK();
    }
    if (value->isBinary()) {
        // keep binary as JSONB blob to avoid data loss.
        const auto* b = value->unpack<doris::JsonbBinaryVal>();
        out = vectorized::Field::create_field<PrimitiveType::TYPE_JSONB>(
                vectorized::JsonbField(b->getBlob(), b->getBlobLen()));
        return Status::OK();
    }

    return Status::InvalidArgument("NestedGroupBuilder cannot convert container type {} to field",
                                   value->typeName());
}

bool NestedGroupBuilder::_handle_conflict(NestedGroup& group, bool is_array_object) const {
    // conflict handling with logging.
    // Priority: array<object > scalar. Prefer nested data over flat data.
    if (group.is_disabled) {
        return true;
    }
    if (group.expected_type == NestedGroup::StructureType::UNKNOWN) {
        group.expected_type =
                is_array_object ? NestedGroup::StructureType::ARRAY : NestedGroup::StructureType::SCALAR;
        return false;
    }
    const bool expected_array = (group.expected_type == NestedGroup::StructureType::ARRAY);
    if (expected_array != is_array_object) {
        // Conflict detected: same path has both array<object> and scalar data
        LOG(WARNING) << "NestedGroup conflict at path '" << group.path.get_path()
                     << "': expected_type=" << (expected_array ? "ARRAY" : "SCALAR")
                     << ", current=" << (is_array_object ? "ARRAY" : "SCALAR")
                     << ". Priority: array<object> > scalar, "
                     << (is_array_object ? "discarding existing scalar data"
                                        : "discarding current scalar data");
        // Prefer array<object> (keep nested) by default: discard scalars.
        if (!is_array_object) {
            // Current is scalar, expected is array - discard current scalar
            return true;
        }
        // Current is array, expected is scalar - discard existing scalar children
        group.children.clear();
        group.expected_type = NestedGroup::StructureType::ARRAY;
        return false;
    }
    return false;
}

} // namespace doris::segment_v2


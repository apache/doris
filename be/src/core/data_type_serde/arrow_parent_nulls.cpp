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

#include "core/data_type_serde/arrow_parent_nulls.h"

#include <arrow/array/array_base.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>

#include "common/check.h"
#include "common/logging.h"
#include "runtime/exec_env.h"

namespace doris {
namespace {

// The pool that LanceRecordBatchConverter::_query_arrow_memory_pool() picks as well: ExecEnv's
// doris::ArrowMemoryPool charges the current query's memory tracker and checks its limit, while
// Arrow's default pool is invisible to both. Only unit tests that never initialize ExecEnv leave
// it null.
arrow::MemoryPool* doris_arrow_memory_pool() {
    auto* memory_pool = ExecEnv::GetInstance()->arrow_memory_pool();
    return memory_pool != nullptr ? memory_pool : arrow::default_memory_pool();
}

// Allocates a validity bitmap of `bit_length` bits from `pool`, with every slot NULL.
Status allocate_null_bitmap(int64_t bit_length, arrow::MemoryPool* pool,
                            std::shared_ptr<arrow::Buffer>* bitmap) {
    auto allocated = arrow::AllocateEmptyBitmap(bit_length, pool);
    if (!allocated.ok()) {
        return Status::InternalError("allocate Arrow child validity bitmap failed: {}",
                                     allocated.status().message());
    }
    *bitmap = std::move(allocated).ValueUnsafe();
    return Status::OK();
}

// Copies the validity of the slots [begin, end) of `data` to the same bits of `bitmap`. Validity
// bits are addressed from the start of the buffer, before the array offset.
void copy_validity(const arrow::ArrayData& data, int64_t begin, int64_t end, uint8_t* bitmap) {
    if (data.buffers[0] != nullptr) {
        arrow::internal::CopyBitmap(data.buffers[0]->data(), data.offset + begin, end - begin,
                                    bitmap, data.offset + begin);
    } else {
        arrow::bit_util::SetBitsTo(bitmap, data.offset + begin, end - begin, true);
    }
}

// Returns a copy of `data` whose validity bitmap is `validity`.
std::shared_ptr<arrow::ArrayData> with_validity(const arrow::ArrayData& data,
                                                std::shared_ptr<arrow::Buffer> validity) {
    auto masked = data.Copy();
    masked->buffers[0] = std::move(validity);
    masked->SetNullCount(arrow::kUnknownNullCount);
    return masked;
}

// Keeps the validity of the slots of `target` that a non-NULL row of `parent` references and
// marks every other slot NULL. Row `row` references the slots [slot_begin(row),
// slot_begin(row + 1)) counted from slot `base` of `target`, so the rows of `parent` reference
// [base + slot_begin(0), base + slot_begin(parent.length())). `*masked` is `target` itself when
// every slot to hide is NULL already.
template <typename SlotBegin>
Status mask_unreferenced_slots(const arrow::Array& parent, const SlotBegin& slot_begin,
                               int64_t base, const std::shared_ptr<arrow::ArrayData>& target,
                               arrow::MemoryPool* pool, std::shared_ptr<arrow::ArrayData>* masked) {
    *masked = target;
    // An Arrow Null child has no validity buffer and is NULL in every slot already.
    if (target->type->id() == arrow::Type::NA) {
        return Status::OK();
    }
    const int64_t first = base + slot_begin(0);
    const int64_t last = base + slot_begin(parent.length());
    // Nothing validated the producer's offsets and lengths yet, and a range outside the child
    // would be written outside the new bitmap.
    if (first < 0 || last < first || last > target->length) {
        return Status::InvalidArgument(
                "Arrow {} references child slots [{}, {}) of a child with length {}",
                parent.type()->ToString(), first, last, target->length);
    }
    // Only a non-NULL slot needs hiding. A child that an earlier pass masked already, like the
    // narrowed Decimal256 leaf that a compaction validates again, therefore costs no bitmap.
    const auto has_valid_slot = [&target](int64_t begin, int64_t end) {
        if (begin == end) {
            return false;
        }
        if (target->buffers[0] == nullptr) {
            return true;
        }
        return arrow::internal::CountSetBits(target->buffers[0]->data(), target->offset + begin,
                                             end - begin) != 0;
    };
    std::shared_ptr<arrow::Buffer> validity;
    const auto keep_referenced_slots = [&]() -> Status {
        RETURN_IF_ERROR(allocate_null_bitmap(target->offset + target->length, pool, &validity));
        copy_validity(*target, first, last, validity->mutable_data());
        return Status::OK();
    };
    // A sliced parent leaves the slots of the rows outside the slice unreferenced.
    if (has_valid_slot(0, first) || has_valid_slot(last, target->length)) {
        RETURN_IF_ERROR(keep_referenced_slots());
    }
    for (int64_t row = 0; row < parent.length(); ++row) {
        if (!parent.IsNull(row)) {
            continue;
        }
        const int64_t begin = base + slot_begin(row);
        const int64_t end = base + slot_begin(row + 1);
        if (begin < first || end < begin || end > last) {
            return Status::InvalidArgument(
                    "Arrow {} row {} references child slots [{}, {}) outside [{}, {})",
                    parent.type()->ToString(), row, begin, end, first, last);
        }
        // A NULL list or map row may own no slots, and the slots of a NULL row may be NULL
        // already. It hides nothing new then, so such NULL rows allocate nothing.
        if (!has_valid_slot(begin, end)) {
            continue;
        }
        if (validity == nullptr) {
            RETURN_IF_ERROR(keep_referenced_slots());
        }
        arrow::bit_util::SetBitsTo(validity->mutable_data(), target->offset + begin, end - begin,
                                   false);
    }
    if (validity != nullptr) {
        *masked = with_validity(*target, std::move(validity));
    }
    return Status::OK();
}

} // namespace

Status mask_arrow_children_hidden_by_null_parents(
        const arrow::Array& parent, int64_t start, int64_t end,
        const std::shared_ptr<arrow::Array>& child,
        const std::function<std::pair<int64_t, int64_t>(int64_t)>& child_slots,
        std::shared_ptr<arrow::Array>* masked) {
    DORIS_CHECK(child != nullptr);
    DORIS_CHECK(masked != nullptr);
    *masked = child;
    // An Arrow Null child has no validity buffer and is NULL in every slot already.
    if (parent.null_count() == 0 || child->type_id() == arrow::Type::NA) {
        return Status::OK();
    }
    const auto& data = *child->data();
    std::shared_ptr<arrow::Buffer> validity;
    for (int64_t row = start; row < end; ++row) {
        if (!parent.IsNull(row)) {
            continue;
        }
        if (validity == nullptr) {
            RETURN_IF_ERROR(allocate_null_bitmap(data.offset + data.length,
                                                 doris_arrow_memory_pool(), &validity));
            copy_validity(data, 0, data.length, validity->mutable_data());
        }
        const auto [begin, slot_end] = child_slots(row);
        DCHECK(begin >= 0 && begin <= slot_end && slot_end <= data.length);
        arrow::bit_util::SetBitsTo(validity->mutable_data(), data.offset + begin, slot_end - begin,
                                   false);
    }
    if (validity == nullptr) {
        return Status::OK();
    }
    *masked = arrow::MakeArray(with_validity(data, std::move(validity)));
    return Status::OK();
}

Status mask_arrow_child_slots_not_visible(const arrow::Array& parent, int child_index,
                                          arrow::MemoryPool* pool,
                                          std::shared_ptr<arrow::Array>* masked) {
    DORIS_CHECK(pool != nullptr);
    DORIS_CHECK(masked != nullptr);
    DORIS_CHECK(child_index >= 0 && child_index < parent.num_fields());
    const auto& child = parent.data()->child_data[child_index];
    std::shared_ptr<arrow::ArrayData> masked_child;
    switch (parent.type_id()) {
    case arrow::Type::LIST: {
        const auto& list = static_cast<const arrow::ListArray&>(parent);
        RETURN_IF_ERROR(mask_unreferenced_slots(
                parent, [&list](int64_t row) { return list.value_offset(row); }, 0, child, pool,
                &masked_child));
        break;
    }
    case arrow::Type::LARGE_LIST: {
        const auto& list = static_cast<const arrow::LargeListArray&>(parent);
        RETURN_IF_ERROR(mask_unreferenced_slots(
                parent, [&list](int64_t row) { return list.value_offset(row); }, 0, child, pool,
                &masked_child));
        break;
    }
    case arrow::Type::FIXED_SIZE_LIST: {
        const auto& list = static_cast<const arrow::FixedSizeListArray&>(parent);
        RETURN_IF_ERROR(mask_unreferenced_slots(
                parent, [&list](int64_t row) { return list.value_offset(row); }, 0, child, pool,
                &masked_child));
        break;
    }
    case arrow::Type::STRUCT:
        RETURN_IF_ERROR(mask_unreferenced_slots(
                parent, [offset = parent.offset()](int64_t row) { return offset + row; }, 0, child,
                pool, &masked_child));
        break;
    case arrow::Type::MAP: {
        // MapArray::SetData() aborts through ARROW_CHECK_OK(MapArray::ValidateChildData()) once
        // the entries struct or the keys may hold a NULL, so the entries that a map row hides
        // are marked NULL in the items alone. Entry j of the map is item slot entries.offset + j.
        const auto& map = static_cast<const arrow::MapArray&>(parent);
        const auto& items = child->child_data[1];
        std::shared_ptr<arrow::ArrayData> masked_items;
        RETURN_IF_ERROR(mask_unreferenced_slots(
                parent, [&map](int64_t row) { return map.value_offset(row); }, child->offset, items,
                pool, &masked_items));
        masked_child = child;
        if (masked_items != items) {
            masked_child = child->Copy();
            masked_child->child_data[1] = std::move(masked_items);
        }
        break;
    }
    default:
        return Status::InvalidArgument("cannot mask the child slots of Arrow {}",
                                       parent.type()->ToString());
    }
    *masked = arrow::MakeArray(masked_child);
    return Status::OK();
}

Status mask_arrow_descendants_not_visible(const std::shared_ptr<arrow::Array>& array,
                                          arrow::MemoryPool* pool,
                                          std::shared_ptr<arrow::Array>* masked) {
    DORIS_CHECK(array != nullptr);
    DORIS_CHECK(pool != nullptr);
    DORIS_CHECK(masked != nullptr);
    *masked = array;
    const auto type_id = array->type_id();
    // A leaf has no descendants to mask. Other nested layouts, such as list views, unions and
    // run-end encoded arrays, are left as they are: the slot mask does not know them, and no
    // reader that masks nested arrays maps them to a Doris type.
    if (type_id != arrow::Type::LIST && type_id != arrow::Type::LARGE_LIST &&
        type_id != arrow::Type::FIXED_SIZE_LIST && type_id != arrow::Type::MAP &&
        type_id != arrow::Type::STRUCT) {
        return Status::OK();
    }
    const auto& children = array->data()->child_data;
    std::shared_ptr<arrow::ArrayData> masked_data;
    for (int child_index = 0; child_index < array->num_fields(); ++child_index) {
        std::shared_ptr<arrow::Array> child;
        RETURN_IF_ERROR(mask_arrow_child_slots_not_visible(*array, child_index, pool, &child));
        std::shared_ptr<arrow::Array> masked_child;
        if (type_id == arrow::Type::MAP) {
            // The child is the entries struct, whose keys stay unmasked, so only its items are
            // masked further down.
            const auto& entries = child->data();
            std::shared_ptr<arrow::Array> items;
            RETURN_IF_ERROR(mask_arrow_descendants_not_visible(
                    arrow::MakeArray(entries->child_data[1]), pool, &items));
            masked_child = child;
            if (items->data() != entries->child_data[1]) {
                auto masked_entries = entries->Copy();
                masked_entries->child_data[1] = items->data();
                masked_child = arrow::MakeArray(masked_entries);
            }
        } else {
            RETURN_IF_ERROR(mask_arrow_descendants_not_visible(child, pool, &masked_child));
        }
        if (masked_child->data() == children[child_index]) {
            continue;
        }
        if (masked_data == nullptr) {
            masked_data = array->data()->Copy();
        }
        masked_data->child_data[child_index] = masked_child->data();
    }
    if (masked_data != nullptr) {
        *masked = arrow::MakeArray(masked_data);
    }
    return Status::OK();
}

} // namespace doris

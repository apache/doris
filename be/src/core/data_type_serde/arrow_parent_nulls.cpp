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
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>

#include "common/check.h"
#include "common/logging.h"

namespace doris {

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
    // Validity bits are addressed from the start of the buffer, before the array offset.
    const int64_t bit_length = data.offset + data.length;
    std::shared_ptr<arrow::Buffer> validity;
    for (int64_t row = start; row < end; ++row) {
        if (!parent.IsNull(row)) {
            continue;
        }
        if (validity == nullptr) {
            auto allocated = arrow::AllocateBitmap(bit_length);
            if (!allocated.ok()) {
                return Status::InternalError("allocate Arrow child validity bitmap failed: {}",
                                             allocated.status().message());
            }
            validity = std::move(allocated).ValueUnsafe();
            if (data.buffers[0] != nullptr) {
                arrow::internal::CopyBitmap(data.buffers[0]->data(), 0, bit_length,
                                            validity->mutable_data(), 0);
            } else {
                arrow::bit_util::SetBitsTo(validity->mutable_data(), 0, bit_length, true);
            }
        }
        const auto [begin, slot_end] = child_slots(row);
        DCHECK(begin >= 0 && begin <= slot_end && slot_end <= data.length);
        arrow::bit_util::SetBitsTo(validity->mutable_data(), data.offset + begin, slot_end - begin,
                                   false);
    }
    if (validity == nullptr) {
        return Status::OK();
    }
    auto masked_data = data.Copy();
    masked_data->buffers[0] = std::move(validity);
    masked_data->SetNullCount(arrow::kUnknownNullCount);
    *masked = arrow::MakeArray(masked_data);
    return Status::OK();
}

} // namespace doris

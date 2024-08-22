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

#include "sparse_inverted_index_builder.h"

#include "common/config.h"
#include "olap/tablet_schema.h"

namespace doris::segment_v2::sparse {

struct SparseItem {
    int32_t id;
    float val;

    SparseItem() = default;
    SparseItem(int32_t id, float val) : id(id), val(val) {}
};

// sort by id asc
bool compare_by_sparse_item_id(const SparseItem &s1, const SparseItem &s2) {
    return s1.id < s2.id;
}

Status SparseInvertedIndexBuilder::init() {
    // index_type=sparse_wand, use_wand=true;
    // index_type=sparse_inverted_index, use_wand=false
    _index_builder = std::make_shared<sparse::InvertedIndex<true>>();
    RETURN_IF_ERROR(_fs->create_file(_index_path, &_file_writer));

    return Status::OK();
}

Status SparseInvertedIndexBuilder::add_map_int_to_float_values(const uint8_t* raw_key_data_ptr,
                                                               const uint8_t* raw_value_data_ptr,
                                                               const size_t count,
                                                               const uint8_t* offsets_ptr) {
    if (count == 0) {
        // no values to add vector index
        return Status::OK();
    }

    const int32_t* key_data = reinterpret_cast<const int32_t*>(raw_key_data_ptr);
    const float* value_data = reinterpret_cast<const float*>(raw_value_data_ptr);
    const auto* offsets = reinterpret_cast<const uint64_t*>(offsets_ptr);

    std::vector<SparseRow> sparse_rows;
    std::vector<rowid_t> row_ids;

    int32_t max_id = 0;

    for (int i = 0; i < count; i++) {
        auto elem_size = offsets[i + 1] - offsets[i];

        // not null
        if (elem_size > 0) {
            std::vector<SparseItem> row;
            for (int j = 0; j < elem_size; j++) {
                row.emplace_back(*key_data++, *value_data++);
            }
            std::sort(row.begin(), row.end(), compare_by_sparse_item_id);

            max_id = std::max(max_id, row.back().id);

            // covert to SparseRow
            sparse_rows.emplace_back(elem_size);
            for (size_t j = 0; j < elem_size; j++) {
                sparse_rows.back().set_at(j, row[j].id, row[j].val);
            }

            row_ids.push_back(_rid);
        }

        _rid++;
    }

    if (!sparse_rows.empty()) {
        RETURN_IF_ERROR(_index_builder->Add(sparse_rows.data(), sparse_rows.size(), max_id + 1, row_ids.data()));
    }

    return Status::OK();
}

Status SparseInvertedIndexBuilder::flush() {
    RETURN_IF_ERROR(_index_builder->Save(_file_writer));
    return Status::OK();
}

Status SparseInvertedIndexBuilder::close() {
    if (_closed || !_file_writer) {
        return Status::OK();
    }
    _closed = true;

    RETURN_IF_ERROR(_file_writer->close());
    _file_writer.reset();
    return Status::OK();
}

} // namespace doris::segment_v2::sparse
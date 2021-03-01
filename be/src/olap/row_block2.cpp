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

#include "olap/row_block2.h"

#include <sstream>

#include "gutil/strings/substitute.h"
#include "olap/row_cursor.h"
#include "util/bitmap.h"

using strings::Substitute;
namespace doris {

RowBlockV2::RowBlockV2(const Schema& schema, uint16_t capacity)
        : _schema(schema),
          _capacity(capacity),
          _column_vector_batches(_schema.num_columns()),
          _tracker(new MemTracker(-1, "RowBlockV2")),
          _pool(new MemPool(_tracker.get())),
          _selection_vector(nullptr) {
    for (auto cid : _schema.column_ids()) {
        Status status = ColumnVectorBatch::create(
                _capacity, _schema.column(cid)->is_nullable(), _schema.column(cid)->type_info(),
                const_cast<Field*>(_schema.column(cid)), &_column_vector_batches[cid]);
        if (!status.ok()) {
            LOG(ERROR) << "failed to create ColumnVectorBatch for type: "
                       << _schema.column(cid)->type();
            return;
        }
    }
    _selection_vector = new uint16_t[_capacity];
    clear();
}

RowBlockV2::~RowBlockV2() {
    delete[] _selection_vector;
}

Status RowBlockV2::convert_to_row_block(RowCursor* helper, RowBlock* dst) {
    for (auto cid : _schema.column_ids()) {
        bool is_nullable = _schema.column(cid)->is_nullable();
        if (is_nullable) {
            for (uint16_t i = 0; i < _selected_size; ++i) {
                uint16_t row_idx = _selection_vector[i];
                dst->get_row(i, helper);
                bool is_null = _column_vector_batches[cid]->is_null_at(row_idx);
                if (is_null) {
                    helper->set_null(cid);
                } else {
                    helper->set_not_null(cid);
                    helper->set_field_content_shallow(
                            cid,
                            reinterpret_cast<const char*>(column_block(cid).cell_ptr(row_idx)));
                }
            }
        } else {
            for (uint16_t i = 0; i < _selected_size; ++i) {
                uint16_t row_idx = _selection_vector[i];
                dst->get_row(i, helper);
                helper->set_not_null(cid);
                helper->set_field_content_shallow(
                        cid, reinterpret_cast<const char*>(column_block(cid).cell_ptr(row_idx)));
            }
        }
    }
    // swap MemPool to copy string content
    dst->mem_pool()->exchange_data(_pool.get());
    dst->set_pos(0);
    dst->set_limit(_selected_size);
    dst->finalize(_selected_size);
    return Status::OK();
}

std::string RowBlockRow::debug_string() const {
    std::stringstream ss;
    ss << "[";
    for (int i = 0; i < _block->schema()->num_column_ids(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ColumnId cid = _block->schema()->column_ids()[i];
        auto col_schema = _block->schema()->column(cid);
        if (col_schema->is_nullable() && is_null(cid)) {
            ss << "NULL";
        } else {
            ss << col_schema->type_info()->to_string(cell_ptr(cid));
        }
    }
    ss << "]";
    return ss.str();
}

} // namespace doris

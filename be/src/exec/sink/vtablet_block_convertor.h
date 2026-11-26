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

#pragma once
#include <stddef.h>
#include <stdint.h>

// IWYU pragma: no_include <bits/chrono.h>

#include <chrono> // IWYU pragma: keep

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/data_type/data_type_factory.hpp"
#include "exec/sink/autoinc_buffer.h"
#include "exec/sink/tablet_column_validator.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/descriptors.h"
#include "util/bitmap.h"

namespace doris {

class OlapTableBlockConvertor {
public:
    OlapTableBlockConvertor(TupleDescriptor* output_tuple_desc)
            : _output_tuple_desc(output_tuple_desc), _tablet_validate_context(_filter_map) {}

    Status validate_and_convert_block(RuntimeState* state, Block* input_block,
                                      std::shared_ptr<Block>& block,
                                      VExprContextSPtrs output_vexpr_ctxs, size_t rows,
                                      bool& has_filtered_rows);

    const char* filter_map() const { return _filter_map.data(); }

    int64_t validate_data_ns() const { return _validate_data_ns; }

    int64_t num_filtered_rows() const { return _num_filtered_rows; }

    void init_autoinc_info(int64_t db_id, int64_t table_id, int batch_size,
                           bool is_partial_update_and_auto_inc = false,
                           int32_t auto_increment_column_unique_id = -1);

    AutoIncIDAllocator& auto_inc_id_allocator() { return _auto_inc_id_allocator; }

private:
    // make input data valid for OLAP table
    // return number of invalid/filtered rows.
    // invalid row number is set in Bitmap
    Status _validate_data(Block* block, const size_t rows, int& filtered_rows);

    // some output column of output expr may have different nullable property with dest slot desc
    // so here need to do the convert operation
    void _convert_to_dest_desc_block(Block* block);

    Status _fill_auto_inc_cols(Block* block, size_t rows);

    Status _partial_update_fill_auto_inc_cols(Block* block, size_t rows);

    TupleDescriptor* _output_tuple_desc = nullptr;

    std::vector<char> _filter_map;

    TabletValidateContext _tablet_validate_context;

    int64_t _validate_data_ns = 0;
    int64_t _num_filtered_rows = 0;

    size_t _batch_size;
    std::optional<size_t> _auto_inc_col_idx;
    std::shared_ptr<AutoIncIDBuffer> _auto_inc_id_buffer = nullptr;
    AutoIncIDAllocator _auto_inc_id_allocator;
    bool _is_partial_update_and_auto_inc = false;
};

} // namespace doris

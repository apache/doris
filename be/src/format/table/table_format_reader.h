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

#include "format/generic_reader.h"

namespace doris {
#include "common/compile_check_begin.h"

/// Intermediate base class for "table readers" used by FileScanner.
/// Provides default on_after_read_block that fills partition/missing/synthesized columns.
/// Parquet/ORC override to no-op (they fill per-batch internally).
class TableFormatReader : public GenericReader {
protected:
    Status on_after_read_block(Block* block, size_t* read_rows) override {
        if (*read_rows > 0 && _push_down_agg_type != TPushAggOp::type::COUNT) {
            RETURN_IF_ERROR(fill_remaining_columns(block, *read_rows));
        }
        return Status::OK();
    }
};

#include "common/compile_check_end.h"
} // namespace doris

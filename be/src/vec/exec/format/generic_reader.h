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

#include <gen_cpp/PlanNodes_types.h>

#include "common/factory_creator.h"
#include "common/status.h"
#include "runtime/types.h"
#include "util/profile_collector.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

class Block;
// This a reader interface for all file readers.
// A GenericReader is responsible for reading a file and return
// a set of blocks with specified schema,
class GenericReader : public ProfileCollector {
public:
    GenericReader() : _push_down_agg_type(TPushAggOp::type::NONE) {}
    void set_push_down_agg_type(TPushAggOp::type push_down_agg_type) {
        _push_down_agg_type = push_down_agg_type;
    }

    virtual Status get_next_block(Block* block, size_t* read_rows, bool* eof) = 0;

    virtual Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                               std::unordered_set<std::string>* missing_cols) {
        return Status::NotSupported("get_columns is not implemented");
    }

    virtual Status get_parsed_schema(std::vector<std::string>* col_names,
                                     std::vector<TypeDescriptor>* col_types) {
        return Status::NotSupported("get_parsed_schema is not implemented for this reader.");
    }
    virtual ~GenericReader() = default;

    /// If the underlying FileReader has filled the partition&missing columns,
    /// The FileScanner does not need to fill
    virtual bool fill_all_columns() const { return _fill_all_columns; }

    /// Tell the underlying FileReader the partition&missing columns,
    /// and the FileReader determine to fill columns or not.
    /// Should set _fill_all_columns = true, if fill the columns.
    virtual Status set_fill_columns(
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
        return Status::OK();
    }

    virtual Status close() { return Status::OK(); }

protected:
    const size_t _MIN_BATCH_SIZE = 4064; // 4094 - 32(padding)

    /// Whether the underlying FileReader has filled the partition&missing columns
    bool _fill_all_columns = false;
    TPushAggOp::type _push_down_agg_type;
};

} // namespace doris::vectorized

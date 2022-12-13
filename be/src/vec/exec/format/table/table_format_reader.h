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

#include <vec/exec/format/parquet/parquet_common.h>

#include <string>

#include "runtime/runtime_state.h"
#include "vec/exec/format/generic_reader.h"

namespace doris::vectorized {

class TableFormatReader : public GenericReader {
public:
    TableFormatReader(GenericReader* file_format_reader);
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override {
        return _file_format_reader->get_next_block(block, read_rows, eof);
    }
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override {
        return _file_format_reader->get_columns(name_to_type, missing_cols);
    }

    virtual void filter_rows() = 0;

protected:
    std::string _table_format;                          // hudi, iceberg
    std::unique_ptr<GenericReader> _file_format_reader; // parquet, orc
};

} // namespace doris::vectorized

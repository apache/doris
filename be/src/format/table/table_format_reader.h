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

#include <algorithm>
#include <cstddef>
#include <string>

#include "common/status.h"
#include "core/block/block.h"
#include "format/generic_reader.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {
class TFileRangeDesc;
class Block;
} // namespace doris

namespace doris {
#include "common/compile_check_begin.h"
class TableFormatReader : public GenericReader {
public:
    TableFormatReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeState* state,
                      RuntimeProfile* profile, const TFileScanRangeParams& params,
                      const TFileRangeDesc& range, io::IOContext* io_ctx, FileMetaCache* meta_cache)
            : _file_format_reader(std::move(file_format_reader)),
              _state(state),
              _profile(profile),
              _params(params),
              _range(range),
              _io_ctx(io_ctx) {
        _meta_cache = meta_cache;
    }
    ~TableFormatReader() override = default;
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) final {
        return get_next_block_inner(block, read_rows, eof);
    }

    virtual Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) = 0;

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<DataTypePtr>* col_types) override {
        return _file_format_reader->get_parsed_schema(col_names, col_types);
    }

    virtual Status init_row_filters() = 0;

    bool count_read_rows() override { return _file_format_reader->count_read_rows(); }

    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) override {
        _file_format_reader->set_condition_cache_context(std::move(ctx));
    }

    bool has_delete_operations() const override {
        return _file_format_reader->has_delete_operations();
    }

    bool supports_count_pushdown() const override {
        return _file_format_reader->supports_count_pushdown();
    }

    int64_t get_total_rows() const override { return _file_format_reader->get_total_rows(); }

protected:
    std::string _table_format;                          // hudi, iceberg, paimon
    std::unique_ptr<GenericReader> _file_format_reader; // parquet, orc
    RuntimeState* _state = nullptr;                     // for query options
    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    io::IOContext* _io_ctx = nullptr;

    void _collect_profile_before_close() override {
        if (_file_format_reader != nullptr) {
            _file_format_reader->collect_profile_before_close();
        }
    }
};

#include "common/compile_check_end.h"
} // namespace doris

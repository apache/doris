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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"
#include "core/block/block.h"
#include "format/generic_reader.h"

namespace doris {

/// A lightweight reader that emits row counts without reading any actual data.
/// Used as a decorator to replace the real reader when COUNT(*) push down is active.
///
/// Instead of duplicating the COUNT short-circuit logic in every format reader
/// (ORC, Parquet, etc.), FileScanner creates a CountReader after the real reader
/// is initialized and the total row count is known. The CountReader then serves
/// all subsequent get_next_block calls by simply resizing columns.
///
/// This cleanly separates the "how many rows" concern from the actual data reading,
/// eliminating duplicated COUNT blocks across format readers.
class CountReader : public GenericReader {
public:
    /// @param total_rows   Total number of rows to emit (post-filter).
    /// @param batch_size   Maximum rows per batch.
    /// @param inner_reader The original reader, kept alive for profile collection
    ///                     and lifecycle management. Ownership is transferred.
    CountReader(int64_t total_rows, size_t batch_size,
                std::unique_ptr<GenericReader> inner_reader = nullptr)
            : _total_rows(total_rows),
              _remaining_rows(total_rows),
              _batch_size(batch_size),
              _inner_reader(std::move(inner_reader)) {
        set_push_down_agg_type(TPushAggOp::type::COUNT);
    }

    ~CountReader() override = default;

    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override {
        auto rows = std::min(_remaining_rows, static_cast<int64_t>(_batch_size));
        _remaining_rows -= rows;

        auto mutate_columns = block->mutate_columns();
        for (auto& col : mutate_columns) {
            col->resize(rows);
        }
        block->set_columns(std::move(mutate_columns));

        *read_rows = rows;
        *eof = (_remaining_rows == 0);
        return Status::OK();
    }

    /// CountReader counts rows by definition.
    bool count_read_rows() override { return true; }

    /// Delegate to inner reader if available, otherwise return our total.
    int64_t get_total_rows() const override {
        return _inner_reader ? _inner_reader->get_total_rows() : _initial_total_rows();
    }

    Status close() override {
        if (_inner_reader) {
            return _inner_reader->close();
        }
        return Status::OK();
    }

    /// Access the inner reader for profile collection or other lifecycle needs.
    GenericReader* inner_reader() const { return _inner_reader.get(); }

protected:
    void _collect_profile_before_close() override {
        if (_inner_reader) {
            _inner_reader->collect_profile_before_close();
        }
    }

private:
    int64_t _initial_total_rows() const { return _total_rows; }

    const int64_t _total_rows;
    int64_t _remaining_rows;
    size_t _batch_size;
    std::unique_ptr<GenericReader> _inner_reader;
};

} // namespace doris

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

#include "storage/iterator/vgeneric_iterators.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
class RuntimeProfile;

using namespace ErrorCode;

Status VStatisticsIterator::init(const StorageReadOptions& opts) {
    if (!_init) {
        _push_down_agg_type_opt = opts.push_down_agg_type_opt;

        if (_push_down_agg_type_opt != TPushAggOp::COUNT) {
            _column_iterators.resize(_schema.num_block_columns());
            for (size_t ordinal = 0; ordinal < _schema.num_block_columns(); ++ordinal) {
                RETURN_IF_ERROR(_segment->new_column_iterator(*_schema.column(ordinal),
                                                              &_column_iterators[ordinal], &opts));
            }
        }

        _target_rows = _push_down_agg_type_opt == TPushAggOp::MINMAX ? 2 : _segment->num_rows();
        _init = true;
    }

    return Status::OK();
}

Status VStatisticsIterator::next_batch(Block* block) {
    DCHECK_EQ(block->columns(), _schema.num_block_columns());
    if (_output_rows < _target_rows) {
        block->clear_column_data();
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();

        size_t size = _push_down_agg_type_opt == TPushAggOp::MINMAX
                              ? 2
                              : std::min(_target_rows - _output_rows, MAX_ROW_SIZE_IN_COUNT);
        if (_push_down_agg_type_opt == TPushAggOp::COUNT) {
            for (auto& column : columns) {
                column->insert_many_defaults(size);
            }
        } else {
            for (size_t i = 0; i < columns.size(); ++i) {
                RETURN_IF_ERROR(_column_iterators[i]->next_batch_of_zone_map(&size, columns[i]));
            }
        }
        _output_rows += size;
        return Status::OK();
    }
    return Status::EndOfFile("End of VStatisticsIterator");
}

// VUnionIterator reads input iterators one by one. Every input writes the same
// visible slot prefix; a SegmentIterator may retain delete-predicate columns
// privately after that prefix.
class VUnionIterator : public RowwiseIterator {
public:
    // Iterators' ownership it transferred to this class.
    // This class will delete all iterators when destructs
    // Client should not use iterators anymore.
    VUnionIterator(std::vector<RowwiseIteratorUPtr>&& v, ReadSchemaSPtr read_schema)
            : _read_schema(std::move(read_schema)), _origin_iters(std::move(v)) {}

    ~VUnionIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override {
        if (_cur_iter != nullptr) {
            _cur_iter->update_profile(profile);
        }
    }

private:
    const ReadSchemaSPtr _read_schema;
    RowwiseIteratorUPtr _cur_iter = nullptr;
    std::optional<StorageReadOptions> _read_options;
    std::vector<RowwiseIteratorUPtr> _origin_iters;
};

Status VUnionIterator::init(const StorageReadOptions& opts) {
    _read_options.emplace(opts);
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    // we use back() and pop_back() of std::vector to handle each iterator,
    // so reverse the vector here to keep result block of next_batch to be
    // in the same order as the original segments.
    std::reverse(_origin_iters.begin(), _origin_iters.end());

    _cur_iter = std::move(_origin_iters.back());
    RETURN_IF_ERROR(_cur_iter->init(*_read_options));
    return Status::OK();
}

Status VUnionIterator::next_batch(Block* block) {
    while (_cur_iter != nullptr) {
        auto st = _cur_iter->next_batch(block);
        if (st.is<END_OF_FILE>()) {
            _origin_iters.pop_back();
            if (!_origin_iters.empty()) {
                _cur_iter = std::move(_origin_iters.back());
                RETURN_IF_ERROR(_cur_iter->init(*_read_options));
            } else {
                _cur_iter = nullptr;
            }
        } else {
            return st;
        }
    }
    return Status::EndOfFile("End of VUnionIterator");
}

RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       ReadSchemaSPtr read_schema) {
    if (inputs.size() == 1) {
        return std::move(inputs[0]);
    }
    return std::make_unique<VUnionIterator>(std::move(inputs), std::move(read_schema));
}

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment,
                                          const ReadSchema& schema) {
    return new VStatisticsIterator(segment, schema);
}

} // namespace doris

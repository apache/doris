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

#include "storage/iterator/statistics_iterator.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

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

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment,
                                          const ReadSchema& schema) {
    return new VStatisticsIterator(segment, schema);
}

} // namespace doris

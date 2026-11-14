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

#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <list>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"

namespace doris {
class RuntimeProfile;

namespace segment_v2 {
class Segment;
class ColumnIterator;
} // namespace segment_v2

class VStatisticsIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    VStatisticsIterator(std::shared_ptr<Segment> segment, const ReadSchema& schema)
            : _segment(std::move(segment)), _schema(schema) {}

    ~VStatisticsIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    const ReadSchema& schema() const override { return _schema; }

private:
    std::shared_ptr<Segment> _segment;
    const ReadSchema& _schema;
    size_t _target_rows = 0;
    size_t _output_rows = 0;
    bool _init = false;
    TPushAggOp::type _push_down_agg_type_opt;
    std::vector<std::unique_ptr<ColumnIterator>> _column_iterators;

    static constexpr size_t MAX_ROW_SIZE_IN_COUNT = 65535;
};

// Create a union iterator for input iterators. Union iterator will read
// input iterators one by one.
//
// Inputs iterators' ownership is taken by created union iterator.
RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       ReadSchemaSPtr read_schema);

RowwiseIterator* new_vstatistics_iterator(std::shared_ptr<Segment> segment,
                                          const ReadSchema& schema);

} // namespace doris

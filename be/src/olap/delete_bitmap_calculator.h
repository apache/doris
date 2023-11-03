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

#include <butil/macros.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "olap/base_tablet.h"
#include "olap/binlog_config.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/version_graph.h"
#include "util/metrics.h"
#include "util/once.h"
#include "util/slice.h"

namespace doris {

class MergeIndexDeleteBitmapCalculatorContext {
public:
    class Comparator {
    public:
        Comparator(size_t sequence_length) : _sequence_length(sequence_length) {}
        bool operator()(MergeIndexDeleteBitmapCalculatorContext* lhs,
                        MergeIndexDeleteBitmapCalculatorContext* rhs) const;
        bool is_key_same(Slice const& lhs, Slice const& rhs) const;

    private:
        size_t _sequence_length;
    };

    MergeIndexDeleteBitmapCalculatorContext(std::unique_ptr<segment_v2::IndexedColumnIterator> iter,
                                            vectorized::DataTypePtr index_type, int32_t segment_id,
                                            size_t num_rows, size_t batch_max_size = 1024)
            : _iter(std::move(iter)),
              _index_type(index_type),
              _num_rows(num_rows),
              _max_batch_size(batch_max_size),
              _segment_id(segment_id) {}
    Status get_current_key(Slice& slice);
    Status advance();
    Status seek_at_or_after(Slice const& key);
    size_t row_id() const { return _cur_row_id; }
    int32_t segment_id() const { return _segment_id; }

private:
    Status _next_batch(size_t row_id);

    std::unique_ptr<segment_v2::IndexedColumnIterator> _iter;
    vectorized::DataTypePtr _index_type;
    vectorized::MutableColumnPtr _index_column;
    size_t _block_size {0};
    size_t _cur_pos {0};
    size_t _cur_row_id {0};
    size_t const _num_rows;
    size_t const _max_batch_size;
    int32_t const _segment_id;
    bool _excat_match {false};
};

class MergeIndexDeleteBitmapCalculator {
public:
    MergeIndexDeleteBitmapCalculator() = default;

    Status init(RowsetId rowset_id, std::vector<SegmentSharedPtr> const& segments,
                size_t seq_col_length = 0, size_t max_batch_size = 1024);

    Status calculate_one(RowLocation& loc);

    Status calculate_all(DeleteBitmapPtr delete_bitmap);

private:
    using Heap = std::priority_queue<MergeIndexDeleteBitmapCalculatorContext*,
                                     std::vector<MergeIndexDeleteBitmapCalculatorContext*>,
                                     MergeIndexDeleteBitmapCalculatorContext::Comparator>;
    std::vector<MergeIndexDeleteBitmapCalculatorContext> _contexts;
    MergeIndexDeleteBitmapCalculatorContext::Comparator _comparator {0};
    RowsetId _rowset_id;
    std::unique_ptr<Heap> _heap;
    std::string _last_key;
    size_t _seq_col_length;
};

} // namespace doris

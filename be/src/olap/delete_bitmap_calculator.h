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
#include "olap/rowset/rowset_tree.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/segment_loader.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "olap/version_graph.h"
#include "util/metrics.h"
#include "util/once.h"
#include "util/slice.h"

namespace doris {

constexpr static size_t kMergedPKIteratorReadBatchSize = 64;

class MergedPKIndexDeleteBitmapCalculatorContext {
public:
    class Comparator {
    public:
        Comparator(size_t sequence_length) : _sequence_length(sequence_length) {}
        bool operator()(MergedPKIndexDeleteBitmapCalculatorContext* lhs,
                        MergedPKIndexDeleteBitmapCalculatorContext* rhs) const;
        [[nodiscard]] int compare_key(Slice const& lhs, Slice const& rhs) const;
        [[nodiscard]] int compare_seq(Slice const& lhs, Slice const& rhs) const;
        [[nodiscard]] bool is_key_same(Slice const& lhs, Slice const& rhs) const;

    private:
        size_t _sequence_length;
    };

    MergedPKIndexDeleteBitmapCalculatorContext(
            std::unique_ptr<segment_v2::IndexedColumnIterator> iter,
            vectorized::DataTypePtr index_type, RowsetId rowset_id, size_t end_version,
            int32_t segment_id, size_t num_rows,
            size_t batch_max_size = kMergedPKIteratorReadBatchSize)
            : _iter(std::move(iter)),
              _index_type(index_type),
              _num_rows(num_rows),
              _max_batch_size(batch_max_size),
              _end_version(end_version),
              _rowset_id(rowset_id),
              _segment_id(segment_id) {}
    Status get_current_key(Slice* slice);
    Status advance();
    Status seek_at_or_after(Slice const& key);
    [[nodiscard]] bool eof() const { return _cur_row_id >= _num_rows; }
    [[nodiscard]] size_t row_id() const { return _cur_row_id; }
    [[nodiscard]] int32_t segment_id() const { return _segment_id; }
    [[nodiscard]] RowsetId rowset_id() const { return _rowset_id; }
    [[nodiscard]] int64_t end_version() const { return _end_version; }

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
    size_t const _end_version;
    RowsetId const _rowset_id;
    int32_t const _segment_id;
    bool _excat_match;
};

class MergedPKIndexDeleteBitmapCalculator {
public:
    MergedPKIndexDeleteBitmapCalculator() = default;

    Status init(std::vector<SegmentSharedPtr> const& segments,
                const std::vector<RowsetSharedPtr>* specified_rowsets = nullptr,
                size_t seq_col_length = 0, size_t max_batch_size = kMergedPKIteratorReadBatchSize);

    Status process(DeleteBitmapPtr delete_bitmap);

private:
    Status init(std::vector<SegmentSharedPtr> const& segments,
                const std::vector<int64_t>* end_versions, size_t seq_col_length,
                size_t max_batch_size);

    using Heap = std::priority_queue<MergedPKIndexDeleteBitmapCalculatorContext*,
                                     std::vector<MergedPKIndexDeleteBitmapCalculatorContext*>,
                                     MergedPKIndexDeleteBitmapCalculatorContext::Comparator>;
    std::vector<MergedPKIndexDeleteBitmapCalculatorContext> _contexts;
    std::unique_ptr<std::vector<std::unique_ptr<SegmentCacheHandle>>> _seg_caches;
    MergedPKIndexDeleteBitmapCalculatorContext::Comparator _comparator {0};
    std::unique_ptr<Heap> _heap;
    std::string _last_key;
    size_t _seq_col_length;
};

} // namespace doris

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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_H

#include <gen_cpp/olap_file.pb.h>

#include <memory>

#include "olap/iterators.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/segment_v2/row_ranges.h"
#include "vec/core/block.h"

namespace doris {

namespace vectorized {
class Block;
}

struct RowSetSplits {
    RowsetReaderSharedPtr rs_reader;

    // if segment_offsets is not empty, means we only scan
    // [pair.first, pair.second) segment in rs_reader, only effective in dup key
    // and pipeline
    std::pair<int, int> segment_offsets;

    // RowRanges of each segment.
    std::vector<RowRanges> segment_row_ranges;

    RowSetSplits(RowsetReaderSharedPtr rs_reader_)
            : rs_reader(rs_reader_), segment_offsets({0, 0}) {}
    RowSetSplits() = default;
};

class RowsetReader {
public:
    virtual ~RowsetReader() = default;

    virtual Status init(RowsetReaderContext* read_context, const RowSetSplits& rs_splits = {}) = 0;

    virtual Status get_segment_iterators(RowsetReaderContext* read_context,
                                         std::vector<RowwiseIteratorUPtr>* out_iters,
                                         bool use_cache = false) = 0;
    virtual void reset_read_options() = 0;

    virtual Status next_block(vectorized::Block* block) = 0;

    virtual Status next_block_view(vectorized::BlockView* block_view) = 0;
    virtual bool support_return_data_by_ref() { return false; }

    virtual bool delete_flag() = 0;

    virtual Version version() = 0;

    virtual RowsetSharedPtr rowset() = 0;

    virtual int64_t filtered_rows() = 0;

    virtual RowsetTypePB type() const = 0;

    virtual int64_t newest_write_timestamp() = 0;
    virtual Status current_block_row_locations(std::vector<RowLocation>* locations) {
        return Status::NotSupported("to be implemented");
    }

    virtual Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) {
        return Status::NotSupported("to be implemented");
    }

    virtual bool update_profile(RuntimeProfile* profile) = 0;

    virtual RowsetReaderSharedPtr clone() = 0;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_H

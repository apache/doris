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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_READER_H

#include "olap/iterators.h"
#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/segment_loader.h"

namespace doris {

class BetaRowsetReader : public RowsetReader {
public:
    BetaRowsetReader(BetaRowsetSharedPtr rowset);

    ~BetaRowsetReader() override { _rowset->release(); }

    OLAPStatus init(RowsetReaderContext* read_context) override;

    // It's ok, because we only get ref here, the block's owner is this reader.
    OLAPStatus next_block(RowBlock** block) override;
    OLAPStatus next_block(vectorized::Block* block) override;

    bool delete_flag() override { return _rowset->delete_flag(); }

    Version version() override { return _rowset->version(); }

    RowsetSharedPtr rowset() override { return std::dynamic_pointer_cast<Rowset>(_rowset); }

    // Return the total number of filtered rows, will be used for validation of schema change
    int64_t filtered_rows() override {
        return _stats->rows_del_filtered + _stats->rows_conditions_filtered;
    }

    RowsetTypePB type() const override { return RowsetTypePB::BETA_ROWSET; }

private:
    std::unique_ptr<Schema> _schema;
    RowsetReaderContext* _context;
    BetaRowsetSharedPtr _rowset;

    OlapReaderStatistics _owned_stats;
    OlapReaderStatistics* _stats;

    std::unique_ptr<RowwiseIterator> _iterator;

    std::unique_ptr<RowBlockV2> _input_block;
    std::unique_ptr<RowBlock> _output_block;
    std::unique_ptr<RowCursor> _row;

    // make sure this handle is initialized and valid before
    // reading data.
    SegmentCacheHandle _segment_cache_handle;
};

} // namespace doris

#endif //DORIS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_READER_H

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

#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/iterators.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_reader.h"

namespace doris {

class BetaRowsetReader : public RowsetReader {
public:
    explicit BetaRowsetReader(BetaRowsetSharedPtr rowset);

    ~BetaRowsetReader() override {
        _rowset->release();
    }

    OLAPStatus init(RowsetReaderContext* read_context) override;

    OLAPStatus next_block(RowBlock** block) override;

    bool delete_flag() override { return _rowset->delete_flag(); }

    Version version() override { return _rowset->version(); }

    VersionHash version_hash() override { return _rowset->version_hash(); }

    RowsetSharedPtr rowset() override { return std::dynamic_pointer_cast<Rowset>(_rowset); }

    int64_t filtered_rows() override {
        return _stats->rows_del_filtered;
    }

private:
    BetaRowsetSharedPtr _rowset;

    RowsetReaderContext* _context;
    OlapReaderStatistics _owned_stats;
    OlapReaderStatistics* _stats;

    std::unique_ptr<RowwiseIterator> _iterator;

    std::unique_ptr<RowBlockV2> _input_block;
    std::unique_ptr<RowBlock> _output_block;
    std::unique_ptr<RowCursor> _row;
};

} // namespace doris

#endif //DORIS_BE_SRC_OLAP_ROWSET_BETA_ROWSET_READER_H

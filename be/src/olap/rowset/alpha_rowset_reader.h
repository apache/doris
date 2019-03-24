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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_READER_H

#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_group.h"
#include "olap/rowset/column_data.h"
#include "olap/rowset/alpha_rowset_meta.h"

#include <vector>

namespace doris {

class AlphaRowsetReader : public RowsetReader {
public:
    AlphaRowsetReader(int num_rows_per_row_block, RowsetMeta* rowset_meta,
                      std::vector<std::shared_ptr<SegmentGroup>> segment_groups,
                      RowsetSharedPtr rowset);
    
    ~AlphaRowsetReader();

    // reader init
    virtual OLAPStatus init(RowsetReaderContext* read_context);

    // read next block data
    virtual OLAPStatus next_block(RowBlock** block);

    virtual bool delete_flag();

    virtual Version version();

    virtual VersionHash version_hash();

    // close reader
    virtual void close();

    virtual RowsetSharedPtr rowset();

    virtual int64_t filtered_rows();

private:

    OLAPStatus _init_column_datas(RowsetReaderContext* read_context);

    OLAPStatus _union_block(RowBlock** block);
    OLAPStatus _merge_block(RowBlock** block);
    OLAPStatus _next_row_for_singleton_rowset(RowCursor** row);
    OLAPStatus _next_block_for_column_data(size_t pos, RowBlock** row_block);

    // Doris will split query predicates to several scan keys
    // This function is used to fetch block when advancing
    // current scan key to next scan key.
    OLAPStatus _fetch_first_block(size_t pos, RowBlock** row_block);

private:
    int _num_key_columns;
    int _num_short_key_columns;
    int _num_rows_per_row_block;
    std::string _rowset_path;
    AlphaRowsetMeta* _alpha_rowset_meta;
    std::vector<std::shared_ptr<SegmentGroup>> _segment_groups;
    std::vector<std::shared_ptr<ColumnData>> _column_datas;

    // For singleton Rowset, there are several SegmentGroups
    // Each of SegmentGroups correponds to a row_block upon scan
    std::vector<RowBlock*> _row_blocks; 
    std::unique_ptr<RowBlock> _read_block;
    OLAPStatus (AlphaRowsetReader::*_next_block)(RowBlock** block) = nullptr;

    // For singleton Rowset, there are several SegmentGroups
    // Each of SegmentGroups correponds to a row_cursor 
    std::vector<RowCursor*> _row_cursors;
    RowCursor* _dst_cursor = nullptr;

    RowsetSharedPtr _rowset;
    int _key_range_size;
    std::vector<int> _key_range_indices;

    // Read data from ColumnData for the first time.
    // ScanKey should be sought in this case.
    std::vector<bool> _first_read_symbols;

    // Singleton Rowset is a rowset which start version
    // and end version of it is equal.
    // In streaming ingestion, row among different segment 
    // groups may overlap, and is necessary to be taken
    // into consideration deliberately.
    bool _is_singleton_rowset;

    RowsetReaderContext* _current_read_context;
    OlapReaderStatistics _owned_stats;
    OlapReaderStatistics* _stats = &_owned_stats;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_READER_H

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

#include <gen_cpp/PaloInternalService_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "olap/collect_iterator.h"
#include "olap/delete_handler.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_reader.h"

namespace doris {

class Tablet;
class RowCursor;

class TupleReader final : public TabletReader {
public:
    // Initialize TupleReader with tablet, data version and fetch range.
    Status init(const ReaderParams& read_params) override;

    Status next_row_with_aggregation(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                     bool* eof) override {
        return (this->*_next_row_func)(row_cursor, mem_pool, agg_pool, eof);
    }

private:
    friend class CollectIterator;
    friend class DeleteHandler;

    // Directly read row from rowset and pass to upper caller. No need to do aggregation.
    // This is usually used for DUPLICATE KEY tables
    Status _direct_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                            bool* eof);
    // Just same as _direct_next_row, but this is only for AGGREGATE KEY tables.
    // And this is an optimization for AGGR tables.
    // When there is only one rowset and is not overlapping, we can read it directly without aggregation.
    Status _direct_agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                    bool* eof);
    // For normal AGGREGATE KEY tables, read data by a merge heap.
    Status _agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                             bool* eof);
    // For UNIQUE KEY tables, read data by a merge heap.
    // The difference from _agg_key_next_row is that it will read the data from high version to low version,
    // to minimize the comparison time in merge heap.
    Status _unique_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                bool* eof);

    Status _init_collect_iter(const ReaderParams& read_params,
                              std::vector<RowsetReaderSharedPtr>* valid_rs_readers);

private:
    const RowCursor* _next_key = nullptr;

    Status (TupleReader::*_next_row_func)(RowCursor* row_cursor, MemPool* mem_pool,
                                          ObjectPool* agg_pool, bool* eof) = nullptr;

    CollectIterator _collect_iter;
};

} // namespace doris

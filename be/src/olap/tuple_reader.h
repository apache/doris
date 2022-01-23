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

#ifndef DORIS_BE_SRC_OLAP_TUPLE_READER_H
#define DORIS_BE_SRC_OLAP_TUPLE_READER_H

#include <gen_cpp/PaloInternalService_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <list>
#include <memory>
#include <queue>
#include <sstream>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "exprs/bloomfilter_predicate.h"
#include "olap/column_predicate.h"
#include "olap/delete_handler.h"
#include "olap/collect_iterator.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet.h"
#include "util/runtime_profile.h"

namespace doris {

class Tablet;
class RowCursor;

class TupleReader final : public TabletReader {
public:
    // Initialize TupleReader with tablet, data version and fetch range.
    OLAPStatus init(const ReaderParams& read_params) override;

    OLAPStatus next_row_with_aggregation(RowCursor* row_cursor, MemPool* mem_pool,
                                         ObjectPool* agg_pool, bool* eof) override {
        return (this->*_next_row_func)(row_cursor, mem_pool, agg_pool, eof);
    }

private:
    friend class CollectIterator;
    friend class DeleteHandler;

    // Direcly read row from rowset and pass to upper caller. No need to do aggregation.
    // This is usually used for DUPLICATE KEY tables
    OLAPStatus _direct_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                bool* eof);
    // Just same as _direct_next_row, but this is only for AGGREGATE KEY tables.
    // And this is an optimization for AGGR tables.
    // When there is only one rowset and is not overlapping, we can read it directly without aggregation.
    OLAPStatus _direct_agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool,
                                        ObjectPool* agg_pool, bool* eof);
    // For normal AGGREGATE KEY tables, read data by a merge heap.
    OLAPStatus _agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                 bool* eof);
    // For UNIQUE KEY tables, read data by a merge heap.
    // The difference from _agg_key_next_row is that it will read the data from high version to low version,
    // to minimize the comparison time in merge heap.
    OLAPStatus _unique_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                    bool* eof);

    OLAPStatus _init_collect_iter(const ReaderParams& read_params, std::vector<RowsetReaderSharedPtr>* valid_rs_readers );

private:
    const RowCursor* _next_key = nullptr;

    OLAPStatus (TupleReader::*_next_row_func)(RowCursor* row_cursor, MemPool* mem_pool,
                                         ObjectPool* agg_pool, bool* eof) = nullptr;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_TUPLE_READER_H

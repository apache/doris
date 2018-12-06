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

#ifndef DORIS_BE_SRC_ROWSET_ROWSET_READER_H
#define DORIS_BE_SRC_ROWSET_ROWSET_READER_H

#include "olap/new_status.h"
#include "olap/schema.h"
#include "olap/column_predicate.h"
#include "olap/row_cursor.h"
#include "olap/row_block.h"

#include <memory>
#include <unordered_map>

namespace doris {

struct ReadContext {
	const Schema* projection;
    std::unordered_map<std::string, ColumnPredicate> predicates; // column name -> column predicate
    const RowCursor* lower_bound_key;
    const RowCursor* exclusive_upper_bound_key;
};

class RowsetReader {
public:
    // reader init
    // check whether this rowset can be filtered
    NewStatus init(ReadContext* read_context) = 0;

    // check whether rowset has more data
    bool has_next() = 0;

	// read next block data
    NewStatus next_block(RowBlock* row_block) = 0;

   // close reader
	void close() = 0;
};

}

#endif // DORIS_BE_SRC_ROWSET_ROWSET_READER_H
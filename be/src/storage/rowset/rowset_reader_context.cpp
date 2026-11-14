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

#include "storage/rowset/rowset_reader_context.h"

#include <cstdint>

#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {

MergeDecision decide_merge(const RowsetReaderContext& context) {
    const ReadSchema& schema = *context.read_schema;
    MergeDecision decision;
    decision.compare_whole_key = context.read_orderby_key_columns == nullptr;
    if (decision.compare_whole_key) {
        for (uint32_t ordinal = 0; ordinal < schema.num_key_columns(); ++ordinal) {
            decision.compare_ordinals.push_back(ordinal);
        }
    } else {
        decision.compare_ordinals = *context.read_orderby_key_columns;
    }
    // A changelog read orders the changes of a key by their TSO, oldest first; a changelog
    // tablet has no sequence column.
    if (const int32_t tso_ordinal = schema.tso_ordinal(); tso_ordinal >= 0) {
        decision.sequence_ordinal = tso_ordinal;
        decision.small_sequence_first = true;
    } else if (context.use_sequence_column_for_merge_order) {
        decision.sequence_ordinal = schema.sequence_ordinal();
    }
    decision.reverse = context.read_orderby_key_reverse;
    decision.older_first = context.use_insert_order_when_same;
    // Only a merge by the whole key may drop the older versions of a key.
    decision.drop_older_versions = context.is_unique && decision.compare_whole_key;
    return decision;
}

} // namespace doris

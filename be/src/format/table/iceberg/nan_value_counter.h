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

#include <cstdint>
#include <map>
#include <utility>
#include <vector>

#include "format/table/iceberg/schema.h"

namespace doris {
class Block;
}

namespace doris::iceberg {

/**
 * Counts NaNs per iceberg field as the rows go past, for one output file.
 *
 * Iceberg excludes NaN from a column's bounds by spec ("NaNs are not permitted as lower or upper
 * bounds"), so nan_value_counts is the ONLY metadata that can prove a file holds no NaN. Without it
 * InclusiveMetricsEvaluator.isNaN must assume NaN may be present and a float range predicate cannot
 * prune the file at all (see the FLOAT/DOUBLE leaves in FE IcebergPredicateConverter). NEITHER parquet
 * nor ORC column statistics carry a NaN count, so unlike every other metric the writers report this one
 * cannot be read back from the footer -- hence this counter, shared by both writers so the semantics
 * below exist once.
 *
 * A field this counter reports is a claim that every one of its values was counted, which is what makes
 * a reported zero trustworthy. Two independent narrowings apply, and a field excluded by either one is
 * simply absent from the result -- "unknown", which iceberg reads conservatively -- rather than wrongly
 * claimed NaN-free:
 *   - policy: {@code requested_field_ids}, the fields whose count FE would keep under the table's
 *     metrics config. Counting one FE drops is a pure waste of a data pass, and iceberg disables metrics
 *     for everything past the first 100 fields by default, so a wide table hits this with no property
 *     set. An empty list (including an older FE that sends nothing) counts nothing at all.
 *   - capability: only top-level columns, because a floating field nested in a struct/list/map is not a
 *     block column of its own.
 */
class NanValueCounter {
public:
    NanValueCounter(const Schema& schema, const std::vector<int32_t>& requested_field_ids);

    // Block column i maps to iceberg column i: both writers build their output schema straight from
    // Schema::columns() in order, and both reject a column-count mismatch before reaching here.
    void count(const Block& block);

    const std::map<int, int64_t>& counts() const { return _counts; }

    bool empty() const { return _counts.empty(); }

private:
    // (block column position, iceberg field id) of the columns counted for this file.
    std::vector<std::pair<size_t, int32_t>> _columns;
    std::map<int, int64_t> _counts;
};

} // namespace doris::iceberg

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

#include <vector>

#include "storage/index/query/spi/memory_budget.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::reader {

// Owns term results beyond the dictionary wave. Keep this object alive and at
// the same address while a prepared lookup state refers to its results.
class BatchLookupResults {
public:
    explicit BatchLookupResults(index_query::MemoryBudget& budget) : budget_(budget) {}
    BatchLookupResults(const BatchLookupResults&) = delete;
    BatchLookupResults& operator=(const BatchLookupResults&) = delete;

    const std::vector<LogicalIndexReader::BatchLookupResult>& results() const { return results_; }
    index_query::MemoryBudget& memory_budget() const { return budget_; }

private:
    friend class LogicalIndexReader;
    Status reserve_slots(size_t count) {
        std::vector<LogicalIndexReader::BatchLookupResult>().swap(results_);
        memory_.reset();
        if (count > budget_.limit_bytes() / sizeof(LogicalIndexReader::BatchLookupResult)) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "index query result slots exceed memory budget");
        }
        return budget_.reserve(count * sizeof(LogicalIndexReader::BatchLookupResult), &memory_);
    }

    index_query::MemoryBudget& budget_;
    index_query::MemoryBudget::Reservation memory_;
    std::vector<LogicalIndexReader::BatchLookupResult> results_;
};

} // namespace doris::snii::reader

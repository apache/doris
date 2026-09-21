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

#include "storage/index/snii/reader/budgeted_frq_prelude.h"

namespace doris::snii::reader {

Status BudgetedFrqPrelude::open(Slice prelude) {
    clear();
    uint64_t retained = 0;
    uint64_t temporary = 0;
    RETURN_IF_ERROR(format::FrqPreludeReader::memory_required(prelude, &retained, &temporary));
    RETURN_IF_ERROR(budget_.reserve(retained + temporary, &memory_));
    Status status = format::FrqPreludeReader::open(prelude, &reader_);
    if (!status.ok()) {
        clear();
        return status;
    }
    return memory_.resize(reader_.memory_usage());
}

void BudgetedFrqPrelude::clear() {
    reader_ = format::FrqPreludeReader {};
    memory_.reset();
}

} // namespace doris::snii::reader

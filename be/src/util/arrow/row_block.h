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

#include <memory>

#include "common/status.h"

// Convert Doris RowBlockV2 to/from Arrow RecordBatch.
// RowBlockV2 is used in Doris storage engine.

namespace arrow {

class Schema;
class MemoryPool;
class RecordBatch;

} // namespace arrow

namespace doris {

class RowBlockV2;
class Schema;

// Convert Doris Schema to Arrow Schema.
Status convert_to_arrow_schema(const Schema& row_desc, std::shared_ptr<arrow::Schema>* result);

// Convert Arrow Schema to Doris Schema.
Status convert_to_doris_schema(const arrow::Schema& schema, std::shared_ptr<Schema>* result);

// Convert a Doris RowBlockV2 to an Arrow RecordBatch. A valid Arrow Schema
// who should match RowBlockV2's schema is given. Memory used by result RecordBatch
// will be allocated from input pool.
Status convert_to_arrow_batch(const RowBlockV2& block, const std::shared_ptr<arrow::Schema>& schema,
                              arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result);

// Convert an Arrow RecordBatch to a Doris RowBlockV2. Schema should match
// with RecordBatch's schema.
Status convert_to_row_block(const arrow::RecordBatch& batch, const Schema& schema,
                            std::shared_ptr<RowBlockV2>* result);

} // namespace doris

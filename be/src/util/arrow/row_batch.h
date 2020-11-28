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

// This file will convert Doris RowBatch to/from Arrow's RecordBatch
// RowBatch is used by Doris query engine to exchange data between
// each execute node.

namespace arrow {

class MemoryPool;
class RecordBatch;
class Schema;

} // namespace arrow

namespace doris {

class MemTracker;
class ObjectPool;
class RowBatch;
class RowDescriptor;

// Convert Doris RowDescriptor to Arrow Schema.
Status convert_to_arrow_schema(const RowDescriptor& row_desc,
                               std::shared_ptr<arrow::Schema>* result);

// Convert an Arrow Schema to a Doris RowDescriptor which will be add to
// input pool.
// Why we should
Status convert_to_row_desc(ObjectPool* pool, const arrow::Schema& schema, RowDescriptor** row_desc);

// Convert a Doris RowBatch to an Arrow RecordBatch. A valid Arrow Schema
// who should match RowBatch's schema is given. Memory used by result RecordBatch
// will be allocated from input pool.
Status convert_to_arrow_batch(const RowBatch& batch, const std::shared_ptr<arrow::Schema>& schema,
                              arrow::MemoryPool* pool, std::shared_ptr<arrow::RecordBatch>* result);

// Convert an Arrow RecordBatch to a Doris RowBatch. A valid RowDescriptor
// whose schema is the same with RecordBatch's should be given. Memory used
// by result RowBatch will be tracked by tracker.
Status convert_to_row_batch(const arrow::RecordBatch& batch, const RowDescriptor& row_desc,
                            const std::shared_ptr<MemTracker>& tracker,
                            std::shared_ptr<RowBatch>* result);

Status serialize_record_batch(const arrow::RecordBatch& record_batch, std::string* result);

} // namespace doris

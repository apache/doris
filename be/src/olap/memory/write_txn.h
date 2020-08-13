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

#include "olap/memory/common.h"
#include "olap/memory/partial_row_batch.h"
#include "olap/memory/schema.h"

namespace doris {
namespace memory {

class PartialRowBatch;

// Class for write transaction
//
// Note: Currently it stores all its operations in memory, to make things simple,
// so we can quickly complete the whole create/read/write pipeline. The data structure may
// change as the project evolves.
//
// TODO: add write to/load from WritexTx files in future.
class WriteTxn {
public:
    explicit WriteTxn(scoped_refptr<Schema>* schema);
    ~WriteTxn();

    const Schema& schema() const { return *_schema.get(); }

    scoped_refptr<Schema> get_schema_ptr() const { return _schema; }

    // Get number of batches
    size_t batch_size() const { return _batches.size(); }

    // Get batch by index
    PartialRowBatch* get_batch(size_t idx) const;

    // Add a new batch to this WriteTx
    PartialRowBatch* new_batch();

private:
    scoped_refptr<Schema> _schema;
    std::vector<std::unique_ptr<PartialRowBatch>> _batches;
};

} // namespace memory
} // namespace doris

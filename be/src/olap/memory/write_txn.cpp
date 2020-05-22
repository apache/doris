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

#include "olap/memory/write_txn.h"

namespace doris {
namespace memory {

WriteTxn::WriteTxn(scoped_refptr<Schema>* schema) : _schema(schema->get()) {}

WriteTxn::~WriteTxn() {}

PartialRowBatch* WriteTxn::new_batch() {
    _batches.emplace_back(new PartialRowBatch(&_schema));
    return _batches.back().get();
}

PartialRowBatch* WriteTxn::get_batch(size_t idx) const {
    return _batches[idx].get();
}

} // namespace memory
} // namespace doris

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

#include "exec/mysql_scan_node.h"
#include "exec/mysql_scanner.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"
namespace doris {

class TextConverter;
class TupleDescriptor;
class RuntimeState;
class Status;

namespace vectorized {

class VMysqlScanNode : public MysqlScanNode {
public:
    VMysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VMysqlScanNode();

    using MysqlScanNode::get_next;
    // Fill the next block by calling next() on the _mysql_scanner,
    // converting text data in MySQL cells to binary data.
    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos);

private:
    Status write_text_column(char* value, int value_length, SlotDescriptor* slot,
                             vectorized::MutableColumnPtr* column_ptr, RuntimeState* state);
};
} // namespace vectorized
} // namespace doris
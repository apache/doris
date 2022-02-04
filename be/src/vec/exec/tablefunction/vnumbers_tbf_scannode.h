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
#include "exec/scan_node.h"
#include "runtime/descriptors.h"

namespace doris {

class TextConverter;
class Tuple;
class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;

namespace vectorized {

class VNumbersTBFScanNode : public ScanNode {
public:
    VNumbersTBFScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VNumbersTBFScanNode();

    // initialize _mysql_scanner, and create _text_converter.
    virtual Status prepare(RuntimeState* state);

    // Start MySQL scan using _mysql_scanner.
    virtual Status open(RuntimeState* state);

    // Fill the next row batch by calling next() on the _mysql_scanner,
    // converting text data in MySQL cells to binary data.
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);

    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos);

    // Close the _mysql_scanner, and report errors.
    virtual Status close(RuntimeState* state);

    // No use
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges);

protected:
    bool _is_init;
    int64_t _total_numbers;   
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Number of returned columns, actually only 1 column
    int _slot_num = 1;

    // Descriptor of tuples generated
    const TupleDescriptor* _tuple_desc;

    int64_t _cur_offset = 0;
};

} // namespace vectorized

} // namespace doris

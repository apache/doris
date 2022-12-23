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
#include "vec/exec/data_gen_functions/vdata_gen_function_inf.h"

namespace doris {

class TextConverter;
class Tuple;
class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;

namespace vectorized {

class VDataGenFunctionScanNode : public ScanNode {
public:
    VDataGenFunctionScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VDataGenFunctionScanNode() override = default;

    // initialize _mysql_scanner, and create _text_converter.
    Status prepare(RuntimeState* state) override;

    // Start MySQL scan using _mysql_scanner.
    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    // Close the _mysql_scanner, and report errors.
    Status close(RuntimeState* state) override;

    // No use
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    std::shared_ptr<VDataGenFunctionInf> _table_func;
    bool _is_init;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Descriptor of tuples generated
    const TupleDescriptor* _tuple_desc;
};

} // namespace vectorized

} // namespace doris

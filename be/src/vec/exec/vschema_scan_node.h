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

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "common/global_types.h"
#include "exec/scan_node.h"
#include "exec/schema_scanner.h"

namespace doris {

class TupleDescriptor;
class RuntimeState;
class Status;
class DescriptorTbl;
class ObjectPool;
class TPlanNode;
class TScanRangeParams;

namespace vectorized {
class Block;

class VSchemaScanNode : public ScanNode {
public:
    VSchemaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VSchemaScanNode() override;
    Status prepare(RuntimeState* state) override;
    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    // Prepare conjuncts, create Schema columns to slots mapping
    // initialize schema_scanner
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    // Start Schema scan using schema_scanner.
    Status open(RuntimeState* state) override;
    // Close the schema_scanner, and report errors.
    Status close(RuntimeState* state) override;

private:
    // this is no use in this class
    Status set_scan_ranges(RuntimeState* state,
                           const std::vector<TScanRangeParams>& scan_ranges) override;

    // Write debug string of this into out.
    void debug_string(int indentation_level, std::stringstream* out) const override;

    bool _is_init;
    const std::string _table_name;
    SchemaScannerParam _scanner_param;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Descriptor of dest tuples
    const TupleDescriptor* _dest_tuple_desc;
    // Tuple index in tuple row.
    int _tuple_idx;
    // slot num need to fill in and return
    int _slot_num;
    // Jni helper for scanning an schema table.
    std::unique_ptr<SchemaScanner> _schema_scanner;
};
} // namespace vectorized
} // namespace doris

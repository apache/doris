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
#include "vec/exec/scan/mysql_scanner.h"
namespace doris {

class TupleDescriptor;
class RuntimeState;
class Status;

namespace vectorized {

class VMysqlScanNode : public ScanNode {
public:
    VMysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VMysqlScanNode() override = default;

    // initialize mysql_scanner, and create text_serde.
    Status prepare(RuntimeState* state) override;

    // Start MySQL scan using mysql_scanner.
    Status open(RuntimeState* state) override;

    // Fill the next block by calling next() on the mysql_scanner,
    // converting text data in MySQL cells to binary data.
    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    // Close the mysql_scanner, and report errors.
    Status close(RuntimeState* state) override;

    // No use
    Status set_scan_ranges(RuntimeState* state,
                           const std::vector<TScanRangeParams>& scan_ranges) override;

private:
    // Write debug string of this into out.
    void debug_string(int indentation_level, std::stringstream* out) const override;

    bool _is_init;
    MysqlScannerParam _my_param;
    // Name of Mysql table
    std::string _table_name;

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // select columns
    std::vector<std::string> _columns;
    // where clause
    std::vector<std::string> _filters;

    // Descriptor of tuples read from MySQL table.
    const TupleDescriptor* _tuple_desc;
    // Tuple index in tuple row.
    int _slot_num;
    // Jni helper for scanning an HBase table.
    std::unique_ptr<MysqlScanner> _mysql_scanner;
    // Helper class for converting text to other types;
    DataTypeSerDeSPtrs _text_serdes;
    DataTypeSerDe::FormatOptions _text_formatOptions;
};
} // namespace vectorized
} // namespace doris

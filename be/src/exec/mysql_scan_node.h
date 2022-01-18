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

#ifndef DORIS_BE_SRC_QUERY_EXEC_MYSQL_SCAN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_MYSQL_SCAN_NODE_H

#include <memory>

#include "exec/mysql_scanner.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"

namespace doris {

class TextConverter;
class Tuple;
class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;

class MysqlScanNode : public ScanNode {
public:
    MysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~MysqlScanNode();

    // initialize _mysql_scanner, and create _text_converter.
    virtual Status prepare(RuntimeState* state);

    // Start MySQL scan using _mysql_scanner.
    virtual Status open(RuntimeState* state);

    // Fill the next row batch by calling next() on the _mysql_scanner,
    // converting text data in MySQL cells to binary data.
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);

    // Close the _mysql_scanner, and report errors.
    virtual Status close(RuntimeState* state);

    // No use
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges);

protected:
    // Write debug string of this into out.
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

private:
    // Writes a slot in _tuple from an MySQL value containing text data.
    // The Mysql value is converted into the appropriate target type.
    Status write_text_slot(char* value, int value_length, SlotDescriptor* slot,
                           RuntimeState* state);
protected:
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
    // Pool for allocating tuple data, including all varying-length slots.
    std::unique_ptr<MemPool> _tuple_pool;
    // Jni helper for scanning an HBase table.
    std::unique_ptr<MysqlScanner> _mysql_scanner;
    // Helper class for converting text to other types;
    std::unique_ptr<TextConverter> _text_converter;
    // Current tuple.
    Tuple* _tuple = nullptr;
};

} // namespace doris

#endif

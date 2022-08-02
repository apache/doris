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

#include "exec/odbc_connector.h"
#include "exec/scan_node.h"
#include "exec/text_converter.hpp"

namespace doris {
namespace vectorized {

class VOdbcScanNode : public ScanNode {
public:
    VOdbcScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs,
                  std::string scan_node_type = "VOdbcScanNode");
    ~VOdbcScanNode() = default;

    // initialize odbc_scanner, and create text_converter.
    virtual Status prepare(RuntimeState* state) override;

    // Start ODBC scan using odbc_scanner.
    virtual Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not Implemented VOdbcScanNode Node::get_next scalar");
    }
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    // Close the odbc_scanner, and report errors.
    virtual Status close(RuntimeState* state) override;
    // No use
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;
    const TupleDescriptor* get_tuple_desc() { return _tuple_desc; }
    TextConverter* get_text_converter() { return _text_converter.get(); }
    ODBCConnector* get_odbc_scanner() { return _odbc_scanner.get(); }
    const std::string& get_scan_node_type() { return _scan_node_type; }

    bool is_init() { return _is_init; }

protected:
    // Write debug string of this into out.
    virtual void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    bool _is_init;

    std::string _scan_node_type;

    // Name of Odbc table
    std::string _table_name;

    std::string _connect_string;

    std::string _query_string;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Descriptor of tuples read from ODBC table.
    const TupleDescriptor* _tuple_desc;
    // Tuple index in tuple row.
    int _slot_num;
    // Pool for allocating tuple data, including all varying-length slots.
    std::unique_ptr<MemPool> _tuple_pool;

    // Scanner of ODBC.
    std::unique_ptr<ODBCConnector> _odbc_scanner;
    ODBCConnectorParam _odbc_param;
    // Helper class for converting text to other types;
    std::unique_ptr<TextConverter> _text_converter;
};
} // namespace vectorized
} // namespace doris

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

#ifndef DORIS_BE_SRC_QUERY_EXEC_CSV_SCAN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_CSV_SCAN_NODE_H

#include <fstream>
#include <sstream>

#include "common/config.h"
#include "exec/csv_scanner.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"

namespace doris {

class TextConverter;
class Tuple;
class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;

// Now, CsvScanNode and CsvScanner are only for unit test
class CsvScanNode : public ScanNode {
public:
    CsvScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~CsvScanNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);

    // initialize _csv_scanner, and create _text_converter.
    virtual Status prepare(RuntimeState* state);

    // Start CSV scan using _csv_scanner.
    virtual Status open(RuntimeState* state);

    // Fill the next row batch by calling next() on the _csv_scanner,
    // converting text data in CSV cells to binary data.
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);

    // Release memory, report 'Counter', and report errors.
    virtual Status close(RuntimeState* state);

    // No use in csv scan process
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges);

    // Write debug string of this into out.
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

private:
    bool check_and_write_text_slot(const std::string& column_name, const TColumnType& column_type,
                                   const char* value, int value_length, const SlotDescriptor* slot,
                                   RuntimeState* state, std::stringstream* error_msg);

    // split one line into fields, check every fields, fill every field into tuple
    bool split_check_fill(const std::string& line, RuntimeState* state);

    void fill_fix_length_string(const char* value, int value_length, MemPool* pool,
                                char** new_value, int new_value_length);
    bool check_decimal_input(const char* value, int value_length, int precision, int scale,
                             std::stringstream* error_msg);

    void hll_hash(const char* src, int len, std::string* result);

    bool check_hll_function(TMiniLoadEtlFunction& function);

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    std::vector<std::string> _file_paths;

    std::string _column_separator;

    std::map<std::string, TColumnType> _column_type_map;
    // mapping function
    std::map<std::string, TMiniLoadEtlFunction> _column_function_map;

    std::vector<std::string> _columns;
    // 'unspecified_columns' is map one-for-one to '_default_values' in the same order
    std::vector<std::string> _unspecified_columns;
    std::vector<std::string> _default_values;

    // Map one-for-one to '_columns' in the same order
    std::vector<SlotDescriptor*> _column_slot_vec;
    std::vector<TColumnType> _column_type_vec;
    // Map one-for-one to '_unspecified_columns' in the same order
    std::vector<SlotDescriptor*> _unspecified_colomn_slot_vec;
    std::vector<TColumnType> _unspecified_colomn_type_vec;

    bool _is_init;

    // Descriptor of tuples read from CSV file.
    const TupleDescriptor* _tuple_desc;
    // Tuple index in tuple row.
    int _slot_num;

    // Pool for allocating tuple data, including all varying-length slots.
    std::unique_ptr<MemPool> _tuple_pool;
    // Util class for doing real file reading
    std::unique_ptr<CsvScanner> _csv_scanner;
    // Helper class for converting text to other types;
    std::unique_ptr<TextConverter> _text_converter;
    // Current tuple.
    Tuple* _tuple;
    // Current RuntimeState
    RuntimeState* _runtime_state;

    int64_t _num_rows_load_total = 0L;
    int64_t _num_rows_load_filtered = 0L;

    RuntimeProfile::Counter* _split_check_timer;
    RuntimeProfile::Counter* _split_line_timer;
    // count hll value num
    int _hll_column_num;
    std::map<std::string, SlotDescriptor*> _column_slot_map;
};

} // end namespace doris

#endif // DORIS_BE_SRC_QUERY_EXEC_CSV_SCAN_NODE_H

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

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "format/generic_reader.h"
#include "format/jni/jni_data_bridge.h"
#include "runtime/runtime_profile.h"
#include "util/jni-util.h"
#include "util/profile_collector.h"
#include "util/string_util.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class Block;
} // namespace doris

namespace doris {

/**
 * JniReader is the base class for all JNI-based readers. It directly manages
 * the JNI lifecycle (open/read/close) for Java scanners that extend
 * org.apache.doris.common.jni.JniScanner.
 *
 * Subclasses only need to:
 * 1. Build scanner_params/column_names in their constructor
 * 2. Pass them to JniReader's constructor
 * 3. Call open() in their init_reader()
 *
 * This class replaces the old JniConnector intermediary.
 */
class JniReader : public GenericReader {
public:
    /**
     * Constructor for scan mode.
     * @param file_slot_descs  Slot descriptors for the output columns
     * @param state            Runtime state
     * @param profile          Runtime profile for metrics
     * @param connector_class  Java scanner class path (e.g. "org/apache/doris/paimon/PaimonJniScanner")
     * @param scanner_params   Configuration map passed to Java scanner constructor
     * @param column_names     Fields to read (also the required_fields in scanner_params)
     * @param self_split_weight  Weight for this split (for profile conditition counter)
     */
    JniReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
              RuntimeProfile* profile, std::string connector_class,
              std::map<std::string, std::string> scanner_params,
              std::vector<std::string> column_names, int64_t self_split_weight = -1);

    /**
     * Constructor for table-schema-only mode (no data reading).
     * @param connector_class  Java scanner class path
     * @param scanner_params   Configuration map passed to Java scanner constructor
     */
    JniReader(std::string connector_class, std::map<std::string, std::string> scanner_params);

    ~JniReader() override = default;

    /**
     * Open the java scanner: set up profile counters, create Java object,
     * get method IDs, and call JniScanner#open.
     */
    Status open(RuntimeState* state, RuntimeProfile* profile);

    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override {
        for (const auto& desc : _file_slot_descs) {
            name_to_type->emplace(desc->col_name(), desc->type());
        }
        return Status::OK();
    }

    /**
     * Read next batch from Java scanner and fill the block.
     */
    virtual Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    /**
     * Get table schema from Java scanner (used by Avro schema discovery).
     */
    Status get_table_schema(std::string& table_schema_str);

    /**
     * Close the scanner and release JNI resources.
     */
    Status close() override;

    /**
     * Set column name to block index map from FileScanner to avoid repeated map creation.
     */
    void set_col_name_to_block_idx(
            const std::unordered_map<std::string, uint32_t>* col_name_to_block_idx) {
        _col_name_to_block_idx = col_name_to_block_idx;
    }

protected:
    void _collect_profile_before_close() override;

    /**
     * Update scanner params and column names after construction.
     * Used by Avro which builds params in init_reader/init_schema_reader
     * rather than in the constructor.
     */
    void _update_scanner_params(std::map<std::string, std::string> params,
                                std::vector<std::string> column_names) {
        _scanner_params = std::move(params);
        _column_names = std::move(column_names);
    }

    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;

private:
    static const std::vector<SlotDescriptor*> _s_empty_slot_descs;

    Status _init_jni_scanner(JNIEnv* env, int batch_size);
    Status _fill_block(Block* block, size_t num_rows);
    Status _get_statistics(JNIEnv* env, std::map<std::string, std::string>* result);

    std::string _connector_name;
    std::string _connector_class;
    std::map<std::string, std::string> _scanner_params;
    std::vector<std::string> _column_names;
    int32_t _self_split_weight = -1;
    bool _is_table_schema = false;

    RuntimeProfile::Counter* _open_scanner_time = nullptr;
    RuntimeProfile::Counter* _java_scan_time = nullptr;
    RuntimeProfile::Counter* _java_append_data_time = nullptr;
    RuntimeProfile::Counter* _java_create_vector_table_time = nullptr;
    RuntimeProfile::Counter* _fill_block_time = nullptr;
    RuntimeProfile::ConditionCounter* _max_time_split_weight_counter = nullptr;

    int64_t _jni_scanner_open_watcher = 0;
    int64_t _java_scan_watcher = 0;
    int64_t _fill_block_watcher = 0;

    size_t _has_read = 0;

    bool _closed = false;
    bool _scanner_opened = false;

    Jni::GlobalClass _jni_scanner_cls;
    Jni::GlobalObject _jni_scanner_obj;
    Jni::MethodId _jni_scanner_open;
    Jni::MethodId _jni_scanner_get_append_data_time;
    Jni::MethodId _jni_scanner_get_create_vector_table_time;
    Jni::MethodId _jni_scanner_get_next_batch;
    Jni::MethodId _jni_scanner_get_table_schema;
    Jni::MethodId _jni_scanner_close;
    Jni::MethodId _jni_scanner_release_column;
    Jni::MethodId _jni_scanner_release_table;
    Jni::MethodId _jni_scanner_get_statistics;

    JniDataBridge::TableMetaAddress _table_meta;

    // Column name to block index map, passed from FileScanner to avoid repeated map creation
    const std::unordered_map<std::string, uint32_t>* _col_name_to_block_idx = nullptr;

    void _set_meta(long meta_addr) { _table_meta.set_meta(meta_addr); }
};

/**
 * The demo usage of JniReader, showing how to read data from java scanner.
 * The java side is also a mock reader that provide values for each type.
 * This class will only be retained during the functional testing phase to verify that
 * the communication and data exchange with the jvm are correct.
 */
class MockJniReader : public JniReader {
public:
    MockJniReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                  RuntimeProfile* profile);

    ~MockJniReader() override = default;

    Status init_reader();
};

} // namespace doris

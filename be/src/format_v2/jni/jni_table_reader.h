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

#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "format/jni/jni_data_bridge.h"
#include "format_v2/table_reader.h"
#include "runtime/runtime_profile.h"
#include "util/jni-util.h"

namespace doris::format {

class JniTableReader : public TableReader {
public:
    struct JniColumn {
        std::string java_name;
        // The index of the column in the output block, which is used to place the data from Java side to the correct position in the output block.
        size_t output_index = 0;
        // The original output type of the column, which is used for type casting after getting the data from Java side. like Bitmap column
        // For columns without special types, the transfer_type and output_type are the same.
        DataTypePtr output_type;
        //Bitmap Type transfer type is String, so the Java scanner will convert the Bitmap column to String before transferring the data to C++, and then C++ side can convert the String back to Bitmap.
        DataTypePtr transfer_type;
        std::string replace_type = "not_replace";
    };

    ~JniTableReader() override = default;

    Status init(TableReadOptions&& options) override;
    Status prepare_split(const SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    Status abort_split() override;
    Status close() override;
    void set_batch_size(size_t batch_size) override;

#ifdef BE_TEST
    void TEST_set_split_state(bool scanner_opened, bool eof) {
        _scanner_opened = scanner_opened;
        _eof = eof;
        if (!scanner_opened) {
            _split_profile_published = false;
        }
    }
    bool TEST_scanner_opened() const { return _scanner_opened; }
    bool TEST_eof() const { return _eof; }
    bool TEST_closed() const { return _closed; }
#endif

protected:
    // Subclasses should implement these methods to specify the Java scanner class
    virtual std::string connector_class() const = 0;
    virtual Status validate_scan_range(const TFileRangeDesc&) const { return Status::OK(); }
    // Subclasses should implement this method to build the scanner params map
    virtual Status build_scanner_params(std::map<std::string, std::string>* params) const = 0;
    // Subclasses can override this method when Java transfer types differ from output types.
    virtual Status build_jni_columns(std::vector<JniColumn>* columns) const;
    virtual Status finalize_jni_block(Block* jni_block, Block* output_block, size_t* rows);
    virtual Status open_jni_scanner_for_split();
    virtual Status close_jni_scanner_for_split();
    void reset_jni_eof() { _eof = false; }
    bool jni_scanner_opened() const { return _scanner_opened; }
    Jni::GlobalObject& jni_scanner_obj() { return _jni_scanner_obj; }
    const Jni::MethodId& jni_scanner_prepare_for_split() const {
        return _jni_scanner_prepare_for_split;
    }
    const Jni::MethodId& jni_scanner_reset_current_split() const {
        return _jni_scanner_reset_current_split;
    }
    // used for profile
    virtual int64_t self_split_weight() const;
    virtual Status _get_next_jni_block(size_t* rows, bool* eof);
    virtual Status _close_jni_scanner();
    virtual Status _set_open_scanner_batch_size(size_t batch_size);
    virtual bool supports_batch_size_update_after_open() const { return true; }
    virtual Status _open_jni_scanner();
    // A derived reader can retain split-local state after the base reader destroys the global
    // Java scanner. Clear that state from this hook rather than relying on a particular close
    // caller to do so.
    virtual void _on_jni_scanner_discarded() {}
    void _publish_jni_scanner_split_timing(JNIEnv* env);
    bool _reserve_split_profile_publication();
    const std::vector<JniColumn>& jni_columns() const { return _jni_columns; }
    RuntimeProfile::Counter* connector_total_timer() const { return _connector_total_time; }
    TFileRangeDesc _current_range;

private:
    // init
    void _init_profile();
    std::string _connector_name() const;
    void _reset_split_state(JNIEnv* env);
    void _prepare_jni_scanner_schema();
    Status _register_jni_class_functions_once(JNIEnv* env);
    Status _create_jni_scanner_object(JNIEnv* env, int batch_size);
    // get_next
    Status _fill_jni_block(JniDataBridge::TableMetaAddress& table_meta, size_t num_rows);
    Status _get_statistics(JNIEnv* env, std::map<std::string, std::string>* result);
    void _collect_jni_scanner_profile(JNIEnv* env);
    void _publish_split_profile(JNIEnv* env);

    std::map<std::string, std::string> _scanner_params;
    std::vector<JniColumn> _jni_columns;
    Block _jni_block_template;

    bool _closed = false;
    bool _scanner_opened = false;
    bool _eof = false;
    bool _split_profile_published = false;

    RuntimeProfile::Counter* _connector_total_time = nullptr;
    RuntimeProfile::Counter* _open_scanner_time = nullptr;
    RuntimeProfile::Counter* _java_scan_time = nullptr;
    RuntimeProfile::Counter* _java_append_data_time = nullptr;
    RuntimeProfile::Counter* _java_create_vector_table_time = nullptr;
    RuntimeProfile::Counter* _fill_block_time = nullptr;
    RuntimeProfile::ConditionCounter* _max_time_split_weight_counter = nullptr;

    int64_t _jni_scanner_open_watcher = 0;
    int64_t _java_scan_watcher = 0;
    int64_t _fill_block_watcher = 0;
    jlong _java_append_data_time_snapshot = 0;
    jlong _java_create_vector_table_time_snapshot = 0;

    Jni::GlobalClass _jni_scanner_cls;
    Jni::GlobalObject _jni_scanner_obj;
    Jni::MethodId _jni_scanner_constructor;
    Jni::MethodId _jni_scanner_open;
    Jni::MethodId _jni_scanner_get_append_data_time;
    Jni::MethodId _jni_scanner_get_create_vector_table_time;
    Jni::MethodId _jni_scanner_get_next_batch;
    Jni::MethodId _jni_scanner_close;
    Jni::MethodId _jni_scanner_release_column;
    Jni::MethodId _jni_scanner_release_table;
    Jni::MethodId _jni_scanner_get_statistics;
    Jni::MethodId _jni_scanner_set_batch_size;
    Jni::MethodId _jni_scanner_prepare_for_split;
    Jni::MethodId _jni_scanner_reset_current_split;
};

} // namespace doris::format

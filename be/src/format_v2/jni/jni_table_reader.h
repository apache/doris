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
    Status close() override;

protected:
    // Subclasses should implement these methods to specify the Java scanner class
    virtual std::string connector_class() const = 0;
    // Subclasses should implement this method to build the scanner params map
    virtual Status build_scanner_params(std::map<std::string, std::string>* params) const = 0;
    // Subclasses can override this method when Java transfer types differ from output types.
    virtual Status build_jni_columns(std::vector<JniColumn>* columns) const;
    virtual Status finalize_jni_block(Block* jni_block, Block* output_block, size_t* rows);
    // used for profile
    virtual int64_t self_split_weight() const { return -1; }
    const std::vector<JniColumn>& jni_columns() const { return _jni_columns; }

private:
    // init
    void _init_profile();
    std::string _connector_name() const;
    // open
    Status _open_jni_scanner();
    void _reset_split_state(JNIEnv* env);
    void _prepare_jni_scanner_schema();
    Status _register_jni_class_functions_once(JNIEnv* env);
    Status _create_jni_scanner_object(JNIEnv* env, int batch_size);
    // get_next
    Status _get_next_jni_block(size_t* rows, bool* eof);
    Status _fill_jni_block(JniDataBridge::TableMetaAddress& table_meta, size_t num_rows);

    Status _close_jni_scanner();

    std::map<std::string, std::string> _scanner_params;
    std::vector<JniColumn> _jni_columns;
    Block _jni_block_template;

    bool _closed = false;
    bool _scanner_opened = false;
    bool _eof = false;
    size_t _batch_size = 0;

    RuntimeProfile::Counter* _open_scanner_time = nullptr;
    RuntimeProfile::Counter* _java_scan_time = nullptr;
    RuntimeProfile::Counter* _java_append_data_time = nullptr;
    RuntimeProfile::Counter* _java_create_vector_table_time = nullptr;
    RuntimeProfile::Counter* _fill_block_time = nullptr;
    RuntimeProfile::ConditionCounter* _max_time_split_weight_counter = nullptr;

    int64_t _jni_scanner_open_watcher = 0;
    int64_t _java_scan_watcher = 0;
    int64_t _fill_block_watcher = 0;

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
};

} // namespace doris::format

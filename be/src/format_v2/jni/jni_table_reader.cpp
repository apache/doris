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

#include "format_v2/jni/jni_table_reader.h"

#include <utility>

#include "common/cast_set.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"

namespace doris::format {

Status JniTableReader::init(TableReadOptions&& options) {
    RETURN_IF_ERROR(TableReader::init(std::move(options)));
    _init_profile();

    // JNI readers do not go through TableReader::open_reader(), where file-local filters are
    // prepared for file readers. They execute table-level conjuncts directly on the JNI block.
    RowDescriptor row_desc;
    for (const auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(conjunct->open(_runtime_state));
    }
    return Status::OK();
}

Status JniTableReader::prepare_split(const SplitReadOptions& options) {
    _current_range = options.current_range;
    RETURN_IF_ERROR(validate_scan_range(options.current_range));
    RETURN_IF_ERROR(TableReader::prepare_split(options));
    DORIS_CHECK(!_closed);
    DORIS_CHECK(!_scanner_opened);
    if (_is_table_level_count_active()) {
        return Status::OK();
    }
    // Subclasses populate split-specific scanner params before calling this method, so the Java
    // scanner can be opened here instead of being lazily opened by the first get_block() call.
    return _open_jni_scanner();
}

Status JniTableReader::get_block(Block* output_block, bool* eos) {
    DORIS_CHECK(output_block != nullptr);
    DORIS_CHECK(eos != nullptr);
    DORIS_CHECK(output_block->columns() == _projected_columns.size());
    output_block->clear_column_data(_projected_columns.size());
    if (_is_table_level_count_active()) {
        return _read_table_level_count(output_block, eos);
    }

    DORIS_CHECK(_scanner_opened);
    if (_eof) {
        *eos = true;
        return Status::OK();
    }

    while (true) {
        size_t current_rows = 0;
        bool current_eof = false;
        // get next block data from Java scanner, and fill the data to _jni_block_template
        RETURN_IF_ERROR(_get_next_jni_block(&current_rows, &current_eof));
        if (current_eof) {
            _eof = true;
            RETURN_IF_ERROR(_close_jni_scanner());
            *eos = true;
            return Status::OK();
        }

        _record_scan_rows(current_rows);
        RETURN_IF_ERROR(finalize_jni_block(&_jni_block_template, output_block, &current_rows));
        if (current_rows == 0) {
            output_block->clear_column_data(_projected_columns.size());
            continue;
        }
        *eos = false;
        return Status::OK();
    }
}

Status JniTableReader::_get_next_jni_block(size_t* rows, bool* eof) {
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    *rows = 0;
    _jni_block_template.clear_column_data(_jni_columns.size());

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    long meta_address = 0;
    {
        SCOPED_RAW_TIMER(&_java_scan_watcher);
        //getNextBatchMeta function, return the meta address
        RETURN_IF_ERROR(_jni_scanner_obj.call_long_method(env, _jni_scanner_get_next_batch)
                                .call(&meta_address));
    }
    RETURN_ERROR_IF_EXC(env);
    if (meta_address == 0) {
        *eof = true;
        return Status::OK();
    }

    JniDataBridge::TableMetaAddress table_meta(meta_address);
    const auto num_rows = table_meta.next_meta_as_long();
    if (num_rows == 0) {
        *eof = true;
        return Status::OK();
    }

    *rows = cast_set<size_t>(num_rows);
    // fill data from Java table meta to C++ block
    RETURN_IF_ERROR(_fill_jni_block(table_meta, *rows));
    // call releaseTable() method in JAVA side to release the Java table Heap free Memory
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_release_table).call());
    RETURN_ERROR_IF_EXC(env);
    *eof = false;
    return Status::OK();
}

// Java table to C++ block
Status JniTableReader::_fill_jni_block(JniDataBridge::TableMetaAddress& table_meta,
                                       size_t num_rows) {
    SCOPED_RAW_TIMER(&_fill_block_watcher);
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    for (size_t i = 0; i < _jni_columns.size(); ++i) {
        const auto& read_column = _jni_columns[i];
        auto& column_with_type_and_name = _jni_block_template.get_by_position(i);
        auto& column_ptr = column_with_type_and_name.column;
        RETURN_IF_ERROR(JniDataBridge::fill_column(table_meta, column_ptr,
                                                   read_column.transfer_type, num_rows));
        // call releaseColumn(int columnIndex) method in JAVA side to release the Java column Heap free Memory
        RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_release_column)
                                .with_arg(cast_set<int>(i))
                                .call());
        RETURN_ERROR_IF_EXC(env);
    }
    return Status::OK();
}

Status JniTableReader::finalize_jni_block(Block* jni_block, Block* output_block, size_t* rows) {
    DORIS_CHECK(jni_block != nullptr);
    DORIS_CHECK(output_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(jni_block->columns() == _jni_columns.size());
    const auto original_rows = *rows;
    for (size_t i = 0; i < _jni_columns.size(); ++i) {
        const auto& column = _jni_columns[i];
        DORIS_CHECK(column.output_index < output_block->columns());
        output_block->get_by_position(column.output_index).type = column.output_type;
        output_block->replace_by_position(column.output_index,
                                          jni_block->get_by_position(i).column);
    }
    DORIS_CHECK(output_block->rows() == original_rows);
    // Apply conjuncts on the output block
    if (!_conjuncts.empty()) {
        RETURN_IF_ERROR(
                VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    }
    *rows = output_block->rows();
    return Status::OK();
}

Status JniTableReader::_get_statistics(JNIEnv* env, std::map<std::string, std::string>* result) {
    DORIS_CHECK(result != nullptr);
    result->clear();
    Jni::LocalObject metrics;
    RETURN_IF_ERROR(
            _jni_scanner_obj.call_object_method(env, _jni_scanner_get_statistics).call(&metrics));
    RETURN_IF_ERROR(Jni::Util::convert_to_cpp_map(env, metrics, result));
    return Status::OK();
}

void JniTableReader::_collect_jni_scanner_profile(JNIEnv* env) {
    if (_scanner_profile == nullptr) {
        return;
    }

    std::map<std::string, std::string> statistics_result;
    Status st = _get_statistics(env, &statistics_result);
    if (!st) {
        LOG(WARNING) << "failed to get_statistics when collect profile: " << st;
        return;
    }

    const auto connector_name = _connector_name();
    for (const auto& metric : statistics_result) {
        std::vector<std::string> type_and_name = split(metric.first, ":");
        if (type_and_name.size() != 2) {
            LOG(WARNING) << "Name of JNI Scanner metric should be pattern like "
                         << "'metricType:metricName'";
            continue;
        }
        long metric_value = std::stol(metric.second);
        RuntimeProfile::Counter* scanner_counter;
        if (type_and_name[0] == "timer") {
            scanner_counter =
                    ADD_CHILD_TIMER(_scanner_profile, type_and_name[1], connector_name.c_str());
        } else if (type_and_name[0] == "counter") {
            scanner_counter = ADD_CHILD_COUNTER(_scanner_profile, type_and_name[1], TUnit::UNIT,
                                                connector_name.c_str());
        } else if (type_and_name[0] == "bytes") {
            scanner_counter = ADD_CHILD_COUNTER(_scanner_profile, type_and_name[1], TUnit::BYTES,
                                                connector_name.c_str());
        } else {
            LOG(WARNING) << "Type of JNI Scanner metric should be timer, counter or bytes";
            continue;
        }
        COUNTER_UPDATE(scanner_counter, metric_value);
    }
}

Status JniTableReader::build_jni_columns(std::vector<JniColumn>* columns) const {
    DORIS_CHECK(columns != nullptr);
    columns->clear();
    columns->reserve(_projected_columns.size());
    for (size_t i = 0; i < _projected_columns.size(); ++i) {
        const auto& table_column = _projected_columns[i];
        columns->push_back({
                .java_name = table_column.name,
                .output_index = i,
                .output_type = table_column.type,
                .transfer_type = table_column.type,
                .replace_type = "not_replace",
        });
    }
    return Status::OK();
}

int64_t JniTableReader::self_split_weight() const {
    return _current_range.__isset.self_split_weight ? _current_range.self_split_weight : -1;
}

Status JniTableReader::close() {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;
    RETURN_IF_ERROR(_close_jni_scanner());
    return TableReader::close();
}

Status JniTableReader::_close_jni_scanner() {
    if (!_scanner_opened) {
        JNIEnv* env = nullptr;
        if (!_jni_scanner_obj.uninitialized()) {
            RETURN_IF_ERROR(Jni::Env::Get(&env));
        }
        _reset_split_state(env);
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    if (_scanner_profile != nullptr) {
        COUNTER_UPDATE(_open_scanner_time, _jni_scanner_open_watcher);
        COUNTER_UPDATE(_fill_block_time, _fill_block_watcher);
    }

    RETURN_ERROR_IF_EXC(env);
    jlong append_data_time = 0;
    RETURN_IF_ERROR(_jni_scanner_obj.call_long_method(env, _jni_scanner_get_append_data_time)
                            .call(&append_data_time));
    jlong create_vector_table_time = 0;
    RETURN_IF_ERROR(
            _jni_scanner_obj.call_long_method(env, _jni_scanner_get_create_vector_table_time)
                    .call(&create_vector_table_time));
    if (_scanner_profile != nullptr) {
        COUNTER_UPDATE(_java_append_data_time, append_data_time);
        COUNTER_UPDATE(_java_create_vector_table_time, create_vector_table_time);
        COUNTER_UPDATE(_java_scan_time,
                       _java_scan_watcher - append_data_time - create_vector_table_time);
        _max_time_split_weight_counter->conditional_update(
                _jni_scanner_open_watcher + _fill_block_watcher + _java_scan_watcher,
                self_split_weight());
    }
    _collect_jni_scanner_profile(env);

    // _fill_jni_block may fail before releasing the current Java table. JniScanner::releaseTable()
    // is idempotent, so closing the split always releases it.
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_release_table).call());
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_close).call());
    _reset_split_state(env);
    return Status::OK();
}

void JniTableReader::_reset_split_state(JNIEnv* env) {
    if (!_jni_scanner_obj.uninitialized()) {
        DORIS_CHECK(env != nullptr);
        _jni_scanner_obj.reset(env);
    }
    _scanner_opened = false;
    _eof = false;
    _scanner_params.clear();
    _jni_columns.clear();
    _jni_block_template.clear();
    _jni_scanner_open_watcher = 0;
    _java_scan_watcher = 0;
    _fill_block_watcher = 0;
}

Status JniTableReader::_open_jni_scanner() {
    // subclasses build map<string,string> _scanner_params to JAVA side
    RETURN_IF_ERROR(build_scanner_params(&_scanner_params));
    // subclasses build _jni_columns info to JAVA side, including column name and column type
    RETURN_IF_ERROR(build_jni_columns(&_jni_columns));
    // _jni_columns info is used to build Java scanner schema params and JNI block template.
    _prepare_jni_scanner_schema();

    if (_runtime_state != nullptr && _batch_size == 0) {
        _batch_size = _runtime_state->batch_size();
    }
    if (_runtime_state != nullptr) {
        _scanner_params["time_zone"] = _runtime_state->timezone();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    SCOPED_RAW_TIMER(&_jni_scanner_open_watcher);
    RETURN_IF_ERROR(_register_jni_class_functions_once(env));
    RETURN_IF_ERROR(_create_jni_scanner_object(env, cast_set<int>(_batch_size)));
    // call open() method in JAVA side.
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_open).call());
    RETURN_ERROR_IF_EXC(env);

    _scanner_opened = true;
    return Status::OK();
}

void JniTableReader::_prepare_jni_scanner_schema() {
    std::vector<std::string> required_fields;
    std::vector<std::string> column_types;
    std::vector<std::string> replace_types;
    required_fields.reserve(_jni_columns.size());
    column_types.reserve(_jni_columns.size());
    replace_types.reserve(_jni_columns.size());
    _jni_block_template.clear();
    _jni_block_template.reserve(_jni_columns.size());

    bool has_replace_type = false;
    for (const auto& column : _jni_columns) {
        DORIS_CHECK(column.transfer_type != nullptr);
        required_fields.push_back(column.java_name);
        column_types.push_back(
                JniDataBridge::get_jni_type_with_different_string(column.transfer_type));
        replace_types.push_back(column.replace_type);
        has_replace_type = has_replace_type || column.replace_type != "not_replace";
        _jni_block_template.insert(
                {column.transfer_type->create_column(), column.transfer_type, column.java_name});
    }
    _scanner_params["required_fields"] = join(required_fields, ",");
    _scanner_params["columns_types"] = join(column_types, "#");
    if (has_replace_type) {
        _scanner_params["replace_string"] = join(replace_types, ",");
    }
}

Status JniTableReader::_register_jni_class_functions_once(JNIEnv* env) {
    if (!_jni_scanner_cls.uninitialized()) {
        return Status::OK();
    }

    RETURN_IF_ERROR(
            Jni::Util::get_jni_scanner_class(env, connector_class().c_str(), &_jni_scanner_cls));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "<init>", "(ILjava/util/Map;)V",
                                                &_jni_scanner_constructor));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "open", "()V", &_jni_scanner_open));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getNextBatchMeta", "()J",
                                                &_jni_scanner_get_next_batch));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getAppendDataTime", "()J",
                                                &_jni_scanner_get_append_data_time));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getCreateVectorTableTime", "()J",
                                                &_jni_scanner_get_create_vector_table_time));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "close", "()V", &_jni_scanner_close));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "releaseColumn", "(I)V",
                                                &_jni_scanner_release_column));
    RETURN_IF_ERROR(
            _jni_scanner_cls.get_method(env, "releaseTable", "()V", &_jni_scanner_release_table));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getStatistics", "()Ljava/util/Map;",
                                                &_jni_scanner_get_statistics));
    RETURN_IF_ERROR(
            _jni_scanner_cls.get_method(env, "setBatchSize", "(I)V", &_jni_scanner_set_batch_size));
    return Status::OK();
}

Status JniTableReader::_create_jni_scanner_object(JNIEnv* env, int batch_size) {
    DORIS_CHECK(!_jni_scanner_cls.uninitialized());
    DORIS_CHECK(!_jni_scanner_constructor.uninitialized());
    DORIS_CHECK(_jni_scanner_obj.uninitialized());
    Jni::LocalObject hashmap_object;
    RETURN_IF_ERROR(Jni::Util::convert_to_java_map(env, _scanner_params, &hashmap_object));
    RETURN_IF_ERROR(_jni_scanner_cls.new_object(env, _jni_scanner_constructor)
                            .with_arg(batch_size)
                            .with_arg(hashmap_object)
                            .call(&_jni_scanner_obj));
    return Status::OK();
}

void JniTableReader::_init_profile() {
    if (_scanner_profile == nullptr) {
        return;
    }
    const auto connector_name = _connector_name();
    ADD_TIMER(_scanner_profile, connector_name);
    _open_scanner_time = ADD_CHILD_TIMER(_scanner_profile, "OpenScannerTime", connector_name);
    _java_scan_time = ADD_CHILD_TIMER(_scanner_profile, "JavaScanTime", connector_name);
    _java_append_data_time =
            ADD_CHILD_TIMER(_scanner_profile, "JavaAppendDataTime", connector_name);
    _java_create_vector_table_time =
            ADD_CHILD_TIMER(_scanner_profile, "JavaCreateVectorTableTime", connector_name);
    _fill_block_time = ADD_CHILD_TIMER(_scanner_profile, "FillBlockTime", connector_name);
    _max_time_split_weight_counter = _scanner_profile->add_conditition_counter(
            "MaxTimeSplitWeight", TUnit::UNIT, [](int64_t _c, int64_t c) { return c > _c; },
            connector_name);
}

std::string JniTableReader::_connector_name() const {
    const auto parts = split(connector_class(), "/");
    return parts.empty() ? connector_class() : parts.back();
}

} // namespace doris::format

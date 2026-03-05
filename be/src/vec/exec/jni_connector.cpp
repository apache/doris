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

#include "jni_connector.h"

#include <glog/logging.h>

#include <sstream>
#include <variant>

#include "jni.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "util/jni-util.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_varbinary.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_varbinary.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeProfile;
} // namespace doris

namespace doris::vectorized {

#define FOR_FIXED_LENGTH_TYPES(M)                                  \
    M(PrimitiveType::TYPE_TINYINT, ColumnInt8, Int8)               \
    M(PrimitiveType::TYPE_BOOLEAN, ColumnUInt8, UInt8)             \
    M(PrimitiveType::TYPE_SMALLINT, ColumnInt16, Int16)            \
    M(PrimitiveType::TYPE_INT, ColumnInt32, Int32)                 \
    M(PrimitiveType::TYPE_BIGINT, ColumnInt64, Int64)              \
    M(PrimitiveType::TYPE_LARGEINT, ColumnInt128, Int128)          \
    M(PrimitiveType::TYPE_FLOAT, ColumnFloat32, Float32)           \
    M(PrimitiveType::TYPE_DOUBLE, ColumnFloat64, Float64)          \
    M(PrimitiveType::TYPE_DECIMALV2, ColumnDecimal128V2, Int128)   \
    M(PrimitiveType::TYPE_DECIMAL128I, ColumnDecimal128V3, Int128) \
    M(PrimitiveType::TYPE_DECIMAL32, ColumnDecimal32, Int32)       \
    M(PrimitiveType::TYPE_DECIMAL64, ColumnDecimal64, Int64)       \
    M(PrimitiveType::TYPE_DATE, ColumnDate, Int64)                 \
    M(PrimitiveType::TYPE_DATEV2, ColumnDateV2, UInt32)            \
    M(PrimitiveType::TYPE_DATETIME, ColumnDateTime, Int64)         \
    M(PrimitiveType::TYPE_DATETIMEV2, ColumnDateTimeV2, UInt64)    \
    M(PrimitiveType::TYPE_TIMESTAMPTZ, ColumnTimeStampTz, UInt64)  \
    M(PrimitiveType::TYPE_IPV4, ColumnIPv4, IPv4)                  \
    M(PrimitiveType::TYPE_IPV6, ColumnIPv6, IPv6)

Status JniConnector::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;
    ADD_TIMER(_profile, _connector_name.c_str());
    _open_scanner_time = ADD_CHILD_TIMER(_profile, "OpenScannerTime", _connector_name.c_str());
    _java_scan_time = ADD_CHILD_TIMER(_profile, "JavaScanTime", _connector_name.c_str());
    _java_append_data_time =
            ADD_CHILD_TIMER(_profile, "JavaAppendDataTime", _connector_name.c_str());
    _java_create_vector_table_time =
            ADD_CHILD_TIMER(_profile, "JavaCreateVectorTableTime", _connector_name.c_str());
    _fill_block_time = ADD_CHILD_TIMER(_profile, "FillBlockTime", _connector_name.c_str());
    _max_time_split_weight_counter = _profile->add_conditition_counter(
            "MaxTimeSplitWeight", TUnit::UNIT, [](int64_t _c, int64_t c) { return c > _c; },
            _connector_name.c_str());
    _java_scan_watcher = 0;
    // cannot put the env into fields, because frames in an env object is limited
    // to avoid limited frames in a thread, we should get local env in a method instead of in whole object.
    JNIEnv* env = nullptr;
    int batch_size = 0;
    if (!_is_table_schema) {
        batch_size = _state->batch_size();
    }
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    SCOPED_RAW_TIMER(&_jni_scanner_open_watcher);
    _scanner_params.emplace("time_zone", _state->timezone());
    RETURN_IF_ERROR(_init_jni_scanner(env, batch_size));
    // Call org.apache.doris.common.jni.JniScanner#open
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_open).call());

    RETURN_ERROR_IF_EXC(env);
    _scanner_opened = true;
    return Status::OK();
}

Status JniConnector::init() {
    return Status::OK();
}

Status JniConnector::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    // Call org.apache.doris.common.jni.JniScanner#getNextBatchMeta
    // return the address of meta information
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    long meta_address = 0;
    {
        SCOPED_RAW_TIMER(&_java_scan_watcher);
        RETURN_IF_ERROR(_jni_scanner_obj.call_long_method(env, _jni_scanner_get_next_batch)
                                .call(&meta_address));
    }
    if (meta_address == 0) {
        // Address == 0 when there's no data in scanner
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    _set_meta(meta_address);
    long num_rows = _table_meta.next_meta_as_long();
    if (num_rows == 0) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_fill_block(block, num_rows));
    *read_rows = num_rows;
    *eof = false;
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_release_table).call());
    _has_read += num_rows;
    return Status::OK();
}

Status JniConnector::get_table_schema(std::string& table_schema_str) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    Jni::LocalString jstr;
    RETURN_IF_ERROR(
            _jni_scanner_obj.call_object_method(env, _jni_scanner_get_table_schema).call(&jstr));
    Jni::LocalStringBufferGuard cstr;
    RETURN_IF_ERROR(jstr.get_string_chars(env, &cstr));
    table_schema_str = std::string {cstr.get()}; // copy to std::string
    return Status::OK();
}

Status JniConnector::get_statistics(JNIEnv* env, std::map<std::string, std::string>* result) {
    result->clear();
    Jni::LocalObject metrics;
    RETURN_IF_ERROR(
            _jni_scanner_obj.call_object_method(env, _jni_scanner_get_statistics).call(&metrics));

    RETURN_IF_ERROR(Jni::Util::convert_to_cpp_map(env, metrics, result));
    return Status::OK();
}

Status JniConnector::close() {
    if (!_closed) {
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(Jni::Env::Get(&env));
        if (_scanner_opened) {
            COUNTER_UPDATE(_open_scanner_time, _jni_scanner_open_watcher);
            COUNTER_UPDATE(_fill_block_time, _fill_block_watcher);

            RETURN_ERROR_IF_EXC(env);
            jlong _append = 0;
            RETURN_IF_ERROR(
                    _jni_scanner_obj.call_long_method(env, _jni_scanner_get_append_data_time)
                            .call(&_append));

            COUNTER_UPDATE(_java_append_data_time, _append);

            jlong _create = 0;
            RETURN_IF_ERROR(
                    _jni_scanner_obj
                            .call_long_method(env, _jni_scanner_get_create_vector_table_time)
                            .call(&_create));

            COUNTER_UPDATE(_java_create_vector_table_time, _create);

            COUNTER_UPDATE(_java_scan_time, _java_scan_watcher - _append - _create);

            _max_time_split_weight_counter->conditional_update(
                    _jni_scanner_open_watcher + _fill_block_watcher + _java_scan_watcher,
                    _self_split_weight);

            // _fill_block may be failed and returned, we should release table in close.
            // org.apache.doris.common.jni.JniScanner#releaseTable is idempotent
            RETURN_IF_ERROR(
                    _jni_scanner_obj.call_void_method(env, _jni_scanner_release_table).call());
            RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_close).call());
        }
    }
    return Status::OK();
}

Status JniConnector::_init_jni_scanner(JNIEnv* env, int batch_size) {
    RETURN_IF_ERROR(
            Jni::Util::get_jni_scanner_class(env, _connector_class.c_str(), &_jni_scanner_cls));

    Jni::MethodId scanner_constructor;
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "<init>", "(ILjava/util/Map;)V",
                                                &scanner_constructor));

    // prepare constructor parameters
    Jni::LocalObject hashmap_object;
    RETURN_IF_ERROR(Jni::Util::convert_to_java_map(env, _scanner_params, &hashmap_object));
    RETURN_IF_ERROR(_jni_scanner_cls.new_object(env, scanner_constructor)
                            .with_arg(batch_size)
                            .with_arg(hashmap_object)
                            .call(&_jni_scanner_obj));

    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "open", "()V", &_jni_scanner_open));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getNextBatchMeta", "()J",
                                                &_jni_scanner_get_next_batch));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getAppendDataTime", "()J",
                                                &_jni_scanner_get_append_data_time));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getCreateVectorTableTime", "()J",
                                                &_jni_scanner_get_create_vector_table_time));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getTableSchema", "()Ljava/lang/String;",
                                                &_jni_scanner_get_table_schema));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "close", "()V", &_jni_scanner_close));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "releaseColumn", "(I)V",
                                                &_jni_scanner_release_column));
    RETURN_IF_ERROR(
            _jni_scanner_cls.get_method(env, "releaseTable", "()V", &_jni_scanner_release_table));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getStatistics", "()Ljava/util/Map;",
                                                &_jni_scanner_get_statistics));
    return Status::OK();
}

Status JniConnector::_fill_block(Block* block, size_t num_rows) {
    SCOPED_RAW_TIMER(&_fill_block_watcher);
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    for (int i = 0; i < _column_names.size(); ++i) {
        auto& column_with_type_and_name =
                block->get_by_position(_col_name_to_block_idx->at(_column_names[i]));
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        RETURN_IF_ERROR(JniDataBridge::fill_column(_table_meta, column_ptr, column_type, num_rows));
        // Column is not released when fill_column failed. It will be released when releasing table.
        RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_release_column)
                                .with_arg(i)
                                .call());
        RETURN_ERROR_IF_EXC(env);
    }
    return Status::OK();
}

void JniConnector::_collect_profile_before_close() {
    if (_scanner_opened && _profile != nullptr) {
        JNIEnv* env = nullptr;
        Status st = Jni::Env::Get(&env);
        if (!st) {
            LOG(WARNING) << "failed to get jni env when collect profile: " << st;
            return;
        }
        // update scanner metrics
        std::map<std::string, std::string> statistics_result;
        st = get_statistics(env, &statistics_result);
        if (!st) {
            LOG(WARNING) << "failed to get_statistics when collect profile: " << st;
            return;
        }

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
                        ADD_CHILD_TIMER(_profile, type_and_name[1], _connector_name.c_str());
            } else if (type_and_name[0] == "counter") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::UNIT,
                                                    _connector_name.c_str());
            } else if (type_and_name[0] == "bytes") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::BYTES,
                                                    _connector_name.c_str());
            } else {
                LOG(WARNING) << "Type of JNI Scanner metric should be timer, counter or bytes";
                continue;
            }
            COUNTER_UPDATE(scanner_counter, metric_value);
        }
    }
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized

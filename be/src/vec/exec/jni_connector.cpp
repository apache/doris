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
#include <stdint.h>

#include <sstream>
#include <variant>

#include "jni.h"
#include "runtime/decimalv2_value.h"
#include "runtime/runtime_state.h"
#include "util/jni-util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris {
class RuntimeProfile;
} // namespace doris

namespace doris::vectorized {

#define FOR_LOGICAL_NUMERIC_TYPES(M) \
    M(TypeIndex::Int8, Int8)         \
    M(TypeIndex::UInt8, UInt8)       \
    M(TypeIndex::Int16, Int16)       \
    M(TypeIndex::UInt16, UInt16)     \
    M(TypeIndex::Int32, Int32)       \
    M(TypeIndex::UInt32, UInt32)     \
    M(TypeIndex::Int64, Int64)       \
    M(TypeIndex::UInt64, UInt64)     \
    M(TypeIndex::Int128, Int128)     \
    M(TypeIndex::Float32, Float32)   \
    M(TypeIndex::Float64, Float64)

JniConnector::~JniConnector() {
    Status st = close();
    if (!st.ok()) {
        // Ensure successful resource release
        LOG(FATAL) << "Failed to release jni resource: " << st.to_string();
    }
}

Status JniConnector::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;
    ADD_TIMER(_profile, _connector_name.c_str());
    _open_scanner_time = ADD_CHILD_TIMER(_profile, "OpenScannerTime", _connector_name.c_str());
    _java_scan_time = ADD_CHILD_TIMER(_profile, "JavaScanTime", _connector_name.c_str());
    _fill_block_time = ADD_CHILD_TIMER(_profile, "FillBlockTime", _connector_name.c_str());
    // cannot put the env into fields, because frames in an env object is limited
    // to avoid limited frames in a thread, we should get local env in a method instead of in whole object.
    JNIEnv* env = nullptr;
    int batch_size = 0;
    if (!_is_table_schema) {
        batch_size = _state->batch_size();
    }
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    if (env == nullptr) {
        return Status::InternalError("Failed to get/create JVM");
    }
    SCOPED_TIMER(_open_scanner_time);
    RETURN_IF_ERROR(_init_jni_scanner(env, batch_size));
    // Call org.apache.doris.common.jni.JniScanner#open
    env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_open);
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status JniConnector::init(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _generate_predicates(colname_to_value_range);
    if (_predicates_length != 0 && _predicates != nullptr) {
        int64_t predicates_address = (int64_t)_predicates.get();
        // We can call org.apache.doris.common.jni.vec.ScanPredicate#parseScanPredicates to parse the
        // serialized predicates in java side.
        _scanner_params.emplace("push_down_predicates", std::to_string(predicates_address));
    }
    return Status::OK();
}

Status JniConnector::get_nex_block(Block* block, size_t* read_rows, bool* eof) {
    // Call org.apache.doris.common.jni.JniScanner#getNextBatchMeta
    // return the address of meta information
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    long meta_address = 0;
    {
        SCOPED_TIMER(_java_scan_time);
        meta_address = env->CallLongMethod(_jni_scanner_obj, _jni_scanner_get_next_batch);
    }
    RETURN_ERROR_IF_EXC(env);
    if (meta_address == 0) {
        // Address == 0 when there's no data in scanner
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    _set_meta(meta_address);
    long num_rows = _next_meta_as_long();
    if (num_rows == 0) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_fill_block(block, num_rows));
    *read_rows = num_rows;
    *eof = false;
    env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_table);
    RETURN_ERROR_IF_EXC(env);
    _has_read += num_rows;
    return Status::OK();
}

Status JniConnector::get_table_schema(std::string& table_schema_str) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    // Call org.apache.doris.jni.JniScanner#getTableSchema
    // return the TableSchema information
    jstring jstr = (jstring)env->CallObjectMethod(_jni_scanner_obj, _jni_scanner_get_table_schema);
    RETURN_ERROR_IF_EXC(env);
    table_schema_str = env->GetStringUTFChars(jstr, nullptr);
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

std::map<std::string, std::string> JniConnector::get_statistics(JNIEnv* env) {
    jobject metrics = env->CallObjectMethod(_jni_scanner_obj, _jni_scanner_get_statistics);
    std::map<std::string, std::string> result = JniUtil::convert_to_cpp_map(env, metrics);
    env->DeleteLocalRef(metrics);
    return result;
}

Status JniConnector::close() {
    if (!_closed) {
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
        if (_scanner_initialized) {
            // update scanner metrics
            for (const auto& metric : get_statistics(env)) {
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

            // _fill_block may be failed and returned, we should release table in close.
            // org.apache.doris.common.jni.JniScanner#releaseTable is idempotent
            env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_table);
            env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_close);
            env->DeleteGlobalRef(_jni_scanner_obj);
        }
        env->DeleteGlobalRef(_jni_scanner_cls);
        _closed = true;
        jthrowable exc = (env)->ExceptionOccurred();
        if (exc != nullptr) {
            LOG(FATAL) << "Failed to release jni resource: "
                       << JniUtil::GetJniExceptionMsg(env).to_string();
        }
    }
    return Status::OK();
}

Status JniConnector::_init_jni_scanner(JNIEnv* env, int batch_size) {
    RETURN_IF_ERROR(
            JniUtil::get_jni_scanner_class(env, _connector_class.c_str(), &_jni_scanner_cls));
    if (_jni_scanner_cls == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Fail to get JniScanner class.");
    }
    RETURN_ERROR_IF_EXC(env);
    jmethodID scanner_constructor =
            env->GetMethodID(_jni_scanner_cls, "<init>", "(ILjava/util/Map;)V");
    RETURN_ERROR_IF_EXC(env);

    // prepare constructor parameters
    jobject hashmap_object = JniUtil::convert_to_java_map(env, _scanner_params);
    jobject jni_scanner_obj =
            env->NewObject(_jni_scanner_cls, scanner_constructor, batch_size, hashmap_object);
    env->DeleteLocalRef(hashmap_object);
    RETURN_ERROR_IF_EXC(env);

    _jni_scanner_open = env->GetMethodID(_jni_scanner_cls, "open", "()V");
    _jni_scanner_get_next_batch = env->GetMethodID(_jni_scanner_cls, "getNextBatchMeta", "()J");
    _jni_scanner_get_table_schema =
            env->GetMethodID(_jni_scanner_cls, "getTableSchema", "()Ljava/lang/String;");
    RETURN_ERROR_IF_EXC(env);
    _jni_scanner_close = env->GetMethodID(_jni_scanner_cls, "close", "()V");
    _jni_scanner_release_column = env->GetMethodID(_jni_scanner_cls, "releaseColumn", "(I)V");
    _jni_scanner_release_table = env->GetMethodID(_jni_scanner_cls, "releaseTable", "()V");
    _jni_scanner_get_statistics =
            env->GetMethodID(_jni_scanner_cls, "getStatistics", "()Ljava/util/Map;");
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, jni_scanner_obj, &_jni_scanner_obj));
    _scanner_initialized = true;
    env->DeleteLocalRef(jni_scanner_obj);
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status JniConnector::_fill_block(Block* block, size_t num_rows) {
    SCOPED_TIMER(_fill_block_time);
    for (int i = 0; i < _column_names.size(); ++i) {
        auto& column_with_type_and_name = block->get_by_name(_column_names[i]);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        RETURN_IF_ERROR(_fill_column(column_ptr, column_type, num_rows));
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
        // Column is not released when _fill_column failed. It will be released when releasing table.
        env->CallVoidMethod(_jni_scanner_obj, _jni_scanner_release_column, i);
        RETURN_ERROR_IF_EXC(env);
    }
    return Status::OK();
}

Status JniConnector::_fill_column(ColumnPtr& doris_column, DataTypePtr& data_type,
                                  size_t num_rows) {
    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    void* null_map_ptr = _next_meta_as_ptr();
    if (null_map_ptr == nullptr) {
        // org.apache.doris.common.jni.vec.ColumnType.Type#UNSUPPORTED will set column address as 0
        return Status::InternalError("Unsupported type {} in java side", getTypeName(logical_type));
    }
    MutableColumnPtr data_column;
    if (doris_column->is_nullable()) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                (*std::move(doris_column)).mutate().get());
        data_column = nullable_column->get_nested_column_ptr();
        NullMap& null_map = nullable_column->get_null_map_data();
        size_t origin_size = null_map.size();
        null_map.resize(origin_size + num_rows);
        memcpy(null_map.data() + origin_size, static_cast<bool*>(null_map_ptr), num_rows);
    } else {
        data_column = doris_column->assume_mutable();
    }
    // Date and DateTime are deprecated and not supported.
    switch (logical_type) {
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE)       \
    case NUMERIC_TYPE:                                 \
        return _fill_numeric_column<CPP_NUMERIC_TYPE>( \
                data_column, reinterpret_cast<CPP_NUMERIC_TYPE*>(_next_meta_as_ptr()), num_rows);
        FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    case TypeIndex::Decimal128:
        [[fallthrough]];
    case TypeIndex::Decimal128I:
        return _fill_decimal_column<Int128>(
                data_column, reinterpret_cast<Int128*>(_next_meta_as_ptr()), num_rows);
    case TypeIndex::Decimal32:
        return _fill_decimal_column<Int32>(data_column,
                                           reinterpret_cast<Int32*>(_next_meta_as_ptr()), num_rows);
    case TypeIndex::Decimal64:
        return _fill_decimal_column<Int64>(data_column,
                                           reinterpret_cast<Int64*>(_next_meta_as_ptr()), num_rows);
    case TypeIndex::DateV2:
        return _decode_time_column<UInt32>(
                data_column, reinterpret_cast<UInt32*>(_next_meta_as_ptr()), num_rows);
    case TypeIndex::DateTimeV2:
        return _decode_time_column<UInt64>(
                data_column, reinterpret_cast<UInt64*>(_next_meta_as_ptr()), num_rows);
    case TypeIndex::String:
        [[fallthrough]];
    case TypeIndex::FixedString:
        return _fill_string_column(data_column, num_rows);
    default:
        return Status::InvalidArgument("Unsupported type {} in jni scanner",
                                       getTypeName(logical_type));
    }
    return Status::OK();
}

Status JniConnector::_fill_string_column(MutableColumnPtr& doris_column, size_t num_rows) {
    int* offsets = reinterpret_cast<int*>(_next_meta_as_ptr());
    char* data = reinterpret_cast<char*>(_next_meta_as_ptr());
    std::vector<StringRef> string_values;
    string_values.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        int start_offset = i == 0 ? 0 : offsets[i - 1];
        int end_offset = offsets[i];
        string_values.emplace_back(data + start_offset, end_offset - start_offset);
    }
    doris_column->insert_many_strings(&string_values[0], num_rows);
    return Status::OK();
}

void JniConnector::_generate_predicates(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    if (colname_to_value_range == nullptr) {
        return;
    }
    for (auto& kv : *colname_to_value_range) {
        const std::string& column_name = kv.first;
        const ColumnValueRangeType& col_val_range = kv.second;
        std::visit([&](auto&& range) { _parse_value_range(range, column_name); }, col_val_range);
    }
}

std::string JniConnector::get_hive_type(const TypeDescriptor& desc) {
    std::ostringstream buffer;
    switch (desc.type) {
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
        return "tinyint";
    case TYPE_SMALLINT:
        return "smallint";
    case TYPE_INT:
        return "int";
    case TYPE_BIGINT:
        return "bigint";
    case TYPE_LARGEINT:
        return "largeint";
    case TYPE_FLOAT:
        return "float";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_VARCHAR: {
        buffer << "varchar(" << desc.len << ")";
        return buffer.str();
    }
    case TYPE_DATE:
        [[fallthrough]];
    case TYPE_DATEV2:
        return "date";
    case TYPE_DATETIME:
        [[fallthrough]];
    case TYPE_DATETIMEV2:
        [[fallthrough]];
    case TYPE_TIME:
        [[fallthrough]];
    case TYPE_TIMEV2:
        return "timestamp";
    case TYPE_BINARY:
        return "binary";
    case TYPE_CHAR: {
        buffer << "char(" << desc.len << ")";
        return buffer.str();
    }
    case TYPE_STRING:
        return "string";
    case TYPE_DECIMALV2: {
        buffer << "decimalv2(" << DecimalV2Value::PRECISION << "," << DecimalV2Value::SCALE << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL32: {
        buffer << "decimal32(" << desc.precision << "," << desc.scale << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL64: {
        buffer << "decimal64(" << desc.precision << "," << desc.scale << ")";
        return buffer.str();
    }
    case TYPE_DECIMAL128I: {
        buffer << "decimal128(" << desc.precision << "," << desc.scale << ")";
        return buffer.str();
    }
    case TYPE_STRUCT: {
        buffer << "struct<";
        for (int i = 0; i < desc.children.size(); ++i) {
            if (i != 0) {
                buffer << ",";
            }
            buffer << desc.field_names[i] << ":" << get_hive_type(desc.children[i]);
        }
        buffer << ">";
        return buffer.str();
    }
    case TYPE_ARRAY: {
        buffer << "array<" << get_hive_type(desc.children[0]) << ">";
        return buffer.str();
    }
    case TYPE_MAP: {
        buffer << "map<" << get_hive_type(desc.children[0]) << ","
               << get_hive_type(desc.children[1]) << ">";
        return buffer.str();
    }
    default:
        return "unsupported";
    }
}

Status JniConnector::generate_meta_info(Block* block, std::unique_ptr<long[]>& meta) {
    std::vector<long> meta_data;
    // insert number of rows
    meta_data.emplace_back(block->rows());
    for (int i = 0; i < block->columns(); ++i) {
        auto& column_with_type_and_name = block->get_by_position(i);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        TypeIndex logical_type = remove_nullable(column_type)->get_type_id();

        // insert null map address
        MutableColumnPtr data_column;
        if (column_ptr->is_nullable()) {
            auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                    column_ptr->assume_mutable().get());
            data_column = nullable_column->get_nested_column_ptr();
            NullMap& null_map = nullable_column->get_null_map_data();
            meta_data.emplace_back((long)null_map.data());
        } else {
            meta_data.emplace_back(0);
            data_column = column_ptr->assume_mutable();
        }

        switch (logical_type) {
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE)                                          \
    case NUMERIC_TYPE: {                                                                  \
        meta_data.emplace_back(_get_numeric_data_address<CPP_NUMERIC_TYPE>(data_column)); \
        break;                                                                            \
    }
            FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        case TypeIndex::Decimal128:
            [[fallthrough]];
        case TypeIndex::Decimal128I: {
            meta_data.emplace_back(_get_decimal_data_address<Int128>(data_column));
            break;
        }
        case TypeIndex::Decimal32: {
            meta_data.emplace_back(_get_decimal_data_address<Int32>(data_column));
            break;
        }
        case TypeIndex::Decimal64: {
            meta_data.emplace_back(_get_decimal_data_address<Int64>(data_column));
            break;
        }
        case TypeIndex::DateV2: {
            meta_data.emplace_back(_get_time_data_address<UInt32>(data_column));
            break;
        }
        case TypeIndex::DateTimeV2: {
            meta_data.emplace_back(_get_time_data_address<UInt64>(data_column));
            break;
        }
        case TypeIndex::String:
            [[fallthrough]];
        case TypeIndex::FixedString: {
            auto& string_column = static_cast<ColumnString&>(*data_column);
            // inert offsets
            meta_data.emplace_back((long)string_column.get_offsets().data());
            meta_data.emplace_back((long)string_column.get_chars().data());
            break;
        }
        case TypeIndex::Array:
            [[fallthrough]];
        case TypeIndex::Struct:
            [[fallthrough]];
        case TypeIndex::Map:
            return Status::IOError("Unhandled type {}", getTypeName(logical_type));
        default:
            return Status::IOError("Unsupported type {}", getTypeName(logical_type));
        }
    }

    meta.reset(new long[meta_data.size()]);
    memcpy(meta.get(), &meta_data[0], meta_data.size() * 8);
    return Status::OK();
}
} // namespace doris::vectorized

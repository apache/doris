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

#include <jni.h>
#include <string.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/olap_common.h"
#include "exec/olap_utils.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "util/profile_collector.h"
#include "util/runtime_profile.h"
#include "util/string_util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
template <typename T>
class ColumnDecimal;
template <typename T>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/**
 * Connector to java jni scanner, which should extend org.apache.doris.common.jni.JniScanner
 */
class JniConnector : public ProfileCollector {
public:
    class TableMetaAddress {
    private:
        long* _meta_ptr;
        int _meta_index;

    public:
        TableMetaAddress() {
            _meta_ptr = nullptr;
            _meta_index = 0;
        }

        TableMetaAddress(long meta_addr) {
            _meta_ptr = static_cast<long*>(reinterpret_cast<void*>(meta_addr));
            _meta_index = 0;
        }

        void set_meta(long meta_addr) {
            _meta_ptr = static_cast<long*>(reinterpret_cast<void*>(meta_addr));
            _meta_index = 0;
        }

        long next_meta_as_long() { return _meta_ptr[_meta_index++]; }

        void* next_meta_as_ptr() { return reinterpret_cast<void*>(_meta_ptr[_meta_index++]); }
    };

    /**
     * The predicates that can be pushed down to java side.
     * Reference to java class org.apache.doris.common.jni.vec.ScanPredicate
     */
    template <typename CppType>
    struct ScanPredicate {
        ScanPredicate() = default;
        ~ScanPredicate() = default;
        const std::string column_name;
        SQLFilterOp op;
        std::vector<const CppType*> values;
        int scale;

        ScanPredicate(const std::string column_name) : column_name(std::move(column_name)) {}

        ScanPredicate(const ScanPredicate& other) {
            column_name = other.column_name;
            op = other.op;
            for (auto v : other.values) {
                values.emplace_back(v);
            }
            scale = other.scale;
        }

        int length() {
            // name_length(4) + column_name + operator(4) + scale(4) + num_values(4)
            int len = 4 + column_name.size() + 4 + 4 + 4;
            if constexpr (std::is_same_v<CppType, StringRef>) {
                for (const StringRef* s : values) {
                    // string_length(4) + string
                    len += 4 + s->size;
                }
            } else {
                int type_len = sizeof(CppType);
                // value_length(4) + value
                len += (4 + type_len) * values.size();
            }
            return len;
        }

        /**
         * The value ranges can be stored as byte array as following format:
         * number_filters(4) | length(4) | column_name | op(4) | scale(4) | num_values(4) | value_length(4) | value | ...
         * The read method is implemented in org.apache.doris.common.jni.vec.ScanPredicate#parseScanPredicates
         */
        int write(std::unique_ptr<char[]>& predicates, int origin_length) {
            int num_filters = 0;
            if (origin_length != 0) {
                num_filters = *reinterpret_cast<int*>(predicates.get());
            } else {
                origin_length = 4;
            }
            num_filters += 1;
            int new_length = origin_length + length();
            char* new_bytes = new char[new_length];
            if (origin_length != 4) {
                memcpy(new_bytes, predicates.get(), origin_length);
            }
            *reinterpret_cast<int*>(new_bytes) = num_filters;

            char* char_ptr = new_bytes + origin_length;
            *reinterpret_cast<int*>(char_ptr) = column_name.size();
            char_ptr += 4;
            memcpy(char_ptr, column_name.data(), column_name.size());
            char_ptr += column_name.size();
            *reinterpret_cast<int*>(char_ptr) = op;
            char_ptr += 4;
            *reinterpret_cast<int*>(char_ptr) = scale;
            char_ptr += 4;
            *reinterpret_cast<int*>(char_ptr) = values.size();
            char_ptr += 4;
            if constexpr (std::is_same_v<CppType, StringRef>) {
                for (const StringRef* s : values) {
                    *reinterpret_cast<int*>(char_ptr) = s->size;
                    char_ptr += 4;
                    memcpy(char_ptr, s->data, s->size);
                    char_ptr += s->size;
                }
            } else {
                // FIXME: it can not handle decimal type correctly.
                // but this logic is deprecated and not used.
                // so may be deleted or fixed later.
                for (const CppType* v : values) {
                    int type_len = sizeof(CppType);
                    *reinterpret_cast<int*>(char_ptr) = type_len;
                    char_ptr += 4;
                    *reinterpret_cast<CppType*>(char_ptr) = *v;
                    char_ptr += type_len;
                }
            }

            predicates.reset(new_bytes);
            return new_length;
        }
    };

    /**
     * Use configuration map to provide scan information. The java side should determine how the parameters
     * are parsed. For example, using "required_fields=col0,col1,...,colN" to provide the scan fields.
     * @param connector_class Java scanner class
     * @param scanner_params Provided configuration map
     * @param column_names Fields to read, also the required_fields in scanner_params
     */
    JniConnector(std::string connector_class, std::map<std::string, std::string> scanner_params,
                 std::vector<std::string> column_names)
            : _connector_class(std::move(connector_class)),
              _scanner_params(std::move(scanner_params)),
              _column_names(std::move(column_names)) {
        // Use java class name as connector name
        _connector_name = split(_connector_class, "/").back();
    }

    /**
     * Just use to get the table schema.
     * @param connector_class Java scanner class
     * @param scanner_params Provided configuration map
     */
    JniConnector(std::string connector_class, std::map<std::string, std::string> scanner_params)
            : _connector_class(std::move(connector_class)),
              _scanner_params(std::move(scanner_params)) {
        _is_table_schema = true;
    }

    ~JniConnector() override = default;

    /**
     * Open java scanner, and get the following scanner methods by jni:
     * 1. getNextBatchMeta: read next batch and return the address of meta information
     * 2. close: close java scanner, and release jni resources
     * 3. releaseColumn: release a single column
     * 4. releaseTable: release current batch, which will also release columns and meta information
     */
    Status open(RuntimeState* state, RuntimeProfile* profile);

    /**
     * Should call before open, parse the pushed down filters. The value ranges can be stored as byte array in heap:
     * number_filters(4) | length(4) | column_name | op(4) | scale(4) | num_values(4) | value_length(4) | value | ...
     * Then, pass the byte array address in configuration map, like "push_down_predicates=${address}"
     */
    Status init(std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

    /**
     * Call java side function JniScanner.getNextBatchMeta. The columns information are stored as long array:
     *                            | number of rows |
     *                            | null indicator start address of fixed length column-A |
     *                            | data column start address of the fixed length column-A  |
     *                            | ... |
     *                            | null indicator start address of variable length column-B |
     *                            | offset column start address of the variable length column-B |
     *                            | data column start address of the variable length column-B |
     *                            | ... |
     */
    Status get_next_block(Block* block, size_t* read_rows, bool* eof);

    /**
     * Get performance metrics from java scanner
     */
    std::map<std::string, std::string> get_statistics(JNIEnv* env);

    /**
     * Call java side function JniScanner.getTableSchema.
     *
     * The schema information are stored as json format
     */
    Status get_table_schema(std::string& table_schema_str);

    /**
     * Close scanner and release jni resources.
     */
    Status close();

    static std::string get_jni_type(const DataTypePtr& data_type);

    /**
     * Map PrimitiveType to hive type.
     */
    static std::string get_jni_type(const TypeDescriptor& desc);

    static Status to_java_table(Block* block, size_t num_rows, const ColumnNumbers& arguments,
                                std::unique_ptr<long[]>& meta);

    static Status to_java_table(Block* block, std::unique_ptr<long[]>& meta);

    static std::pair<std::string, std::string> parse_table_schema(Block* block,
                                                                  const ColumnNumbers& arguments,
                                                                  bool ignore_column_name = true);

    static std::pair<std::string, std::string> parse_table_schema(Block* block);

    static Status fill_block(Block* block, const ColumnNumbers& arguments, long table_address);

protected:
    void _collect_profile_before_close() override;

private:
    std::string _connector_name;
    std::string _connector_class;
    std::map<std::string, std::string> _scanner_params;
    std::vector<std::string> _column_names;
    bool _is_table_schema = false;

    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    RuntimeProfile::Counter* _open_scanner_time = nullptr;
    RuntimeProfile::Counter* _java_scan_time = nullptr;
    RuntimeProfile::Counter* _fill_block_time = nullptr;
    std::map<std::string, RuntimeProfile::Counter*> _scanner_profile;

    size_t _has_read = 0;

    bool _closed = false;
    bool _scanner_opened = false;
    jclass _jni_scanner_cls;
    jobject _jni_scanner_obj;
    jmethodID _jni_scanner_open;
    jmethodID _jni_scanner_get_next_batch;
    jmethodID _jni_scanner_get_table_schema;
    jmethodID _jni_scanner_close;
    jmethodID _jni_scanner_release_column;
    jmethodID _jni_scanner_release_table;
    jmethodID _jni_scanner_get_statistics;

    TableMetaAddress _table_meta;

    int _predicates_length = 0;
    std::unique_ptr<char[]> _predicates;

    /**
     * Set the address of meta information, which is returned by org.apache.doris.common.jni.JniScanner#getNextBatchMeta
     */
    void _set_meta(long meta_addr) { _table_meta.set_meta(meta_addr); }

    Status _init_jni_scanner(JNIEnv* env, int batch_size);

    Status _fill_block(Block* block, size_t num_rows);

    static Status _fill_column(TableMetaAddress& address, ColumnPtr& doris_column,
                               DataTypePtr& data_type, size_t num_rows);

    static Status _fill_string_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                      size_t num_rows);

    static Status _fill_map_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                   DataTypePtr& data_type, size_t num_rows);

    static Status _fill_array_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                     DataTypePtr& data_type, size_t num_rows);

    static Status _fill_struct_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                      DataTypePtr& data_type, size_t num_rows);

    static Status _fill_column_meta(ColumnPtr& doris_column, DataTypePtr& data_type,
                                    std::vector<long>& meta_data);

    template <typename COLUMN_TYPE, typename CPP_TYPE>
    static Status _fill_fixed_length_column(MutableColumnPtr& doris_column, CPP_TYPE* ptr,
                                            size_t num_rows) {
        auto& column_data = static_cast<COLUMN_TYPE&>(*doris_column).get_data();
        size_t origin_size = column_data.size();
        column_data.resize(origin_size + num_rows);
        memcpy(column_data.data() + origin_size, ptr, sizeof(CPP_TYPE) * num_rows);
        return Status::OK();
    }

    template <typename COLUMN_TYPE>
    static long _get_fixed_length_column_address(MutableColumnPtr& doris_column) {
        return (long)static_cast<COLUMN_TYPE&>(*doris_column).get_data().data();
    }

    void _generate_predicates(
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

    template <PrimitiveType primitive_type>
    void _parse_value_range(const ColumnValueRange<primitive_type>& col_val_range,
                            const std::string& column_name) {
        using CppType = typename PrimitiveTypeTraits<primitive_type>::CppType;

        if (col_val_range.is_fixed_value_range()) {
            ScanPredicate<CppType> in_predicate(column_name);
            in_predicate.op = SQLFilterOp::FILTER_IN;
            in_predicate.scale = col_val_range.scale();
            for (const auto& value : col_val_range.get_fixed_value_set()) {
                in_predicate.values.emplace_back(&value);
            }
            if (!in_predicate.values.empty()) {
                _predicates_length = in_predicate.write(_predicates, _predicates_length);
            }
            return;
        }

        const CppType high_value = col_val_range.get_range_max_value();
        const CppType low_value = col_val_range.get_range_min_value();
        const SQLFilterOp high_op = col_val_range.get_range_high_op();
        const SQLFilterOp low_op = col_val_range.get_range_low_op();

        // orc can only push down is_null. When col_value_range._contain_null = true, only indicating that
        // value can be null, not equals null, so ignore _contain_null in col_value_range
        if (col_val_range.is_high_value_maximum() && high_op == SQLFilterOp::FILTER_LESS_OR_EQUAL &&
            col_val_range.is_low_value_mininum() && low_op == SQLFilterOp::FILTER_LARGER_OR_EQUAL) {
            return;
        }

        if (low_value < high_value) {
            if (!col_val_range.is_low_value_mininum() ||
                SQLFilterOp::FILTER_LARGER_OR_EQUAL != low_op) {
                ScanPredicate<CppType> low_predicate(column_name);
                low_predicate.scale = col_val_range.scale();
                low_predicate.op = low_op;
                low_predicate.values.emplace_back(col_val_range.get_range_min_value_ptr());
                _predicates_length = low_predicate.write(_predicates, _predicates_length);
            }
            if (!col_val_range.is_high_value_maximum() ||
                SQLFilterOp::FILTER_LESS_OR_EQUAL != high_op) {
                ScanPredicate<CppType> high_predicate(column_name);
                high_predicate.scale = col_val_range.scale();
                high_predicate.op = high_op;
                high_predicate.values.emplace_back(col_val_range.get_range_max_value_ptr());
                _predicates_length = high_predicate.write(_predicates, _predicates_length);
            }
        }
    }
};

} // namespace doris::vectorized

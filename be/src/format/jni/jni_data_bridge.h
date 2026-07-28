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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/column/column_decimal.h"
#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {

class Block;
template <PrimitiveType T>
class ColumnDecimal;
template <PrimitiveType T>
class ColumnVector;

/**
 * JniDataBridge is a stateless utility class that handles data exchange
 * between C++ Blocks and Java-side shared memory via JNI.
 *
 * It is data-source agnostic — it only cares about data types and Block structure.
 * All methods are static.
 *
 * This class was extracted from JniConnector to separate the data exchange
 * concerns from the JNI scanner lifecycle management.
 */
class JniDataBridge {
public:
    /**
     * Helper class to read metadata from the address returned by Java side.
     * The metadata is stored as a long array in shared memory.
     */
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

    // =========================================================================
    // Read direction: Java shared memory → C++ Block
    // =========================================================================

    /**
     * Fill specified columns in a Block from a Java-side table address.
     * The table_address points to metadata returned by Java JniScanner/JdbcExecutor.
     */
    static Status fill_block(Block* block, const ColumnNumbers& arguments, long table_address);

    /**
     * Fill a single column from a TableMetaAddress. Supports all Doris types
     * including nested types (Array, Map, Struct).
     */
    static Status fill_column(TableMetaAddress& address, ColumnPtr& doris_column,
                              const DataTypePtr& data_type, size_t num_rows);

    // =========================================================================
    // Write direction: C++ Block → Java shared memory
    // =========================================================================

    /**
     * Serialize all columns of a Block into a long[] metadata array
     * that Java side can read via VectorTable.createReadableTable().
     */
    static Status to_java_table(Block* block, std::unique_ptr<long[]>& meta);

    /**
     * Serialize specified columns of a Block into a long[] metadata array.
     */
    static Status to_java_table(Block* block, size_t num_rows, const ColumnNumbers& arguments,
                                std::unique_ptr<long[]>& meta);

    /**
     * Parse Block schema into JNI format strings.
     * Returns (required_fields, columns_types) pair.
     */
    static std::pair<std::string, std::string> parse_table_schema(Block* block);

    static std::pair<std::string, std::string> parse_table_schema(Block* block,
                                                                  const ColumnNumbers& arguments,
                                                                  bool ignore_column_name = true);

    // =========================================================================
    // Type mapping
    // =========================================================================

    /**
     * Convert a Doris DataType to its JNI type string representation.
     * e.g., TYPE_INT -> "int", TYPE_DECIMAL128I -> "decimal128(p,s)"
     */
    static std::string get_jni_type(const DataTypePtr& data_type);

    /**
     * Like get_jni_type but preserves varchar/char length info in the type string.
     * e.g., TYPE_VARCHAR -> "varchar(len)" instead of just "string"
     */
    static std::string get_jni_type_with_different_string(const DataTypePtr& data_type);

private:
    // Column fill helpers for various types
    static Status _fill_string_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                      size_t num_rows);

    static Status _fill_varbinary_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                         size_t num_rows);

    static Status _fill_array_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                     const DataTypePtr& data_type, size_t num_rows);

    static Status _fill_map_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                   const DataTypePtr& data_type, size_t num_rows);

    static Status _fill_struct_column(TableMetaAddress& address, MutableColumnPtr& doris_column,
                                      const DataTypePtr& data_type, size_t num_rows);

    /**
     * Fill column metadata (addresses) for a single column, used by to_java_table.
     */
    static Status _fill_column_meta(const ColumnPtr& doris_column, const DataTypePtr& data_type,
                                    std::vector<long>& meta_data);

    // Fixed-length column fill specializations
    template <typename COLUMN_TYPE, typename CPP_TYPE>
        requires(!std::is_same_v<COLUMN_TYPE, ColumnDecimal128V2> &&
                 !std::is_same_v<COLUMN_TYPE, ColumnDate> &&
                 !std::is_same_v<COLUMN_TYPE, ColumnDateTime> &&
                 !std::is_same_v<COLUMN_TYPE, ColumnDateV2> &&
                 !std::is_same_v<COLUMN_TYPE, ColumnDateTimeV2> &&
                 !std::is_same_v<COLUMN_TYPE, ColumnTimeStampTz>)
    static Status _fill_fixed_length_column(MutableColumnPtr& doris_column, CPP_TYPE* ptr,
                                            size_t num_rows) {
        auto& column_data = assert_cast<COLUMN_TYPE&>(*doris_column).get_data();
        size_t origin_size = column_data.size();
        column_data.resize(origin_size + num_rows);
        memcpy(column_data.data() + origin_size, ptr, sizeof(CPP_TYPE) * num_rows);
        return Status::OK();
    }

    template <typename COLUMN_TYPE, typename CPP_TYPE>
        requires(std::is_same_v<COLUMN_TYPE, ColumnDate> ||
                 std::is_same_v<COLUMN_TYPE, ColumnDateTime>)
    static Status _fill_fixed_length_column(MutableColumnPtr& doris_column, CPP_TYPE* ptr,
                                            size_t num_rows) {
        auto& column_data = assert_cast<COLUMN_TYPE&>(*doris_column).get_data();
        size_t origin_size = column_data.size();
        column_data.resize(origin_size + num_rows);
        memcpy((int64_t*)column_data.data() + origin_size, ptr, sizeof(CPP_TYPE) * num_rows);
        return Status::OK();
    }

    template <typename COLUMN_TYPE, typename CPP_TYPE>
        requires(std::is_same_v<COLUMN_TYPE, ColumnDateV2>)
    static Status _fill_fixed_length_column(MutableColumnPtr& doris_column, CPP_TYPE* ptr,
                                            size_t num_rows) {
        auto& column_data = assert_cast<COLUMN_TYPE&>(*doris_column).get_data();
        size_t origin_size = column_data.size();
        column_data.resize(origin_size + num_rows);
        memcpy((uint32_t*)column_data.data() + origin_size, ptr, sizeof(CPP_TYPE) * num_rows);
        return Status::OK();
    }

    template <typename COLUMN_TYPE, typename CPP_TYPE>
        requires(std::is_same_v<COLUMN_TYPE, ColumnDateTimeV2> ||
                 std::is_same_v<COLUMN_TYPE, ColumnTimeStampTz>)
    static Status _fill_fixed_length_column(MutableColumnPtr& doris_column, CPP_TYPE* ptr,
                                            size_t num_rows) {
        auto& column_data = assert_cast<COLUMN_TYPE&>(*doris_column).get_data();
        size_t origin_size = column_data.size();
        column_data.resize(origin_size + num_rows);
        memcpy((uint64_t*)column_data.data() + origin_size, ptr, sizeof(CPP_TYPE) * num_rows);
        return Status::OK();
    }

    template <typename COLUMN_TYPE, typename CPP_TYPE>
        requires(std::is_same_v<COLUMN_TYPE, ColumnDecimal128V2>)
    static Status _fill_fixed_length_column(MutableColumnPtr& doris_column, CPP_TYPE* ptr,
                                            size_t num_rows) {
        auto& column_data = assert_cast<COLUMN_TYPE&>(*doris_column).get_data();
        size_t origin_size = column_data.size();
        column_data.resize(origin_size + num_rows);
        for (size_t i = 0; i < num_rows; i++) {
            column_data[origin_size + i] = DecimalV2Value(ptr[i]);
        }
        return Status::OK();
    }

    template <typename COLUMN_TYPE>
    static long _get_fixed_length_column_address(const IColumn& doris_column) {
        return (long)assert_cast<const COLUMN_TYPE&>(doris_column).get_data().data();
    }
};

} // namespace doris

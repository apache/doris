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
#include <cstdint>
#include <memory>
#include <orc/OrcFile.hh>
#include <vector>

#include "arrow/status.h"
#include "common/cast_set.h"
#include "common/status.h"
#include "util/mysql_row_buffer.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/field.h"
#include "vec/core/types.h"

namespace arrow {
class ArrayBuilder;
class Array;
} // namespace arrow
namespace cctz {
class time_zone;
} // namespace cctz
namespace orc {
struct ColumnVectorBatch;
} // namespace orc

#define SERIALIZE_COLUMN_TO_JSON()                                            \
    for (auto i = start_idx; i < end_idx; ++i) {                              \
        if (i != start_idx) {                                                 \
            bw.write(options.field_delim.data(), options.field_delim.size()); \
        }                                                                     \
        RETURN_IF_ERROR(serialize_one_cell_to_json(column, i, bw, options));  \
    }                                                                         \
    return Status::OK();

#define DESERIALIZE_COLUMN_FROM_JSON_VECTOR()                                       \
    for (int i = 0; i < slices.size(); ++i) {                                       \
        if (Status st = deserialize_one_cell_from_json(column, slices[i], options); \
            st != Status::OK()) {                                                   \
            return st;                                                              \
        }                                                                           \
        ++*num_deserialized;                                                        \
    }

#define DESERIALIZE_COLUMN_FROM_HIVE_TEXT_VECTOR()                                       \
    for (int i = 0; i < slices.size(); ++i) {                                            \
        if (Status st = deserialize_one_cell_from_hive_text(                             \
                    column, slices[i], options, hive_text_complex_type_delimiter_level); \
            st != Status::OK()) {                                                        \
            return st;                                                                   \
        }                                                                                \
        ++*num_deserialized;                                                             \
    }

namespace doris {
class PValues;
struct JsonbValue;
class JsonbOutStream;
class SlotDescriptor;

template <class OS_TYPE>
class JsonbWriterT;

using JsonbWriter = JsonbWriterT<JsonbOutStream>;

#include "common/compile_check_begin.h"
namespace vectorized {
class IColumn;
class Arena;
class IDataType;
struct CastParameters;

class DataTypeSerDe;
using DataTypeSerDeSPtr = std::shared_ptr<DataTypeSerDe>;
using DataTypeSerDeSPtrs = std::vector<DataTypeSerDeSPtr>;

// Deserialize means read from different file format or memory format,
// for example read from arrow, read from parquet.
// Serialize means write the column cell or the total column into another
// memory format or file format.
// Every data type should implement this interface.
// In the past, there are many switch cases in the code and we do not know
// how many cases or files we has to modify when we add a new type. And also
// it is very difficult to add a new read file format or write file format because
// the developer does not know how many datatypes has to deal.
class DataTypeSerDe {
public:
    // return type name , such as "BOOL", "BIGINT", "ARRAY<DATE>"
    virtual std::string get_name() const = 0;
    // Text serialization/deserialization of data types depend on some settings witch we define
    // in formatOptions.
    struct FormatOptions {
        /**
         * field delimiter is used to separate fields in one row
         */
        std::string field_delim = ",";
        /**
         * collection_delim is used to separate elements in collection, such as array, map
         */
        char collection_delim = ',';
        /**
         * map_key_delim is used to separate key and value in map , eg. key:value
         */
        char map_key_delim = ':';
        /**
         * used in deserialize with text format, if the element is packed in string using "" or '', but not string type, and this switch is open
         *  we can convert the string to the element type, such as int, float, double, date, datetime, timestamp, decimal
         *  by dropping the "" or ''.
         */
        bool converted_from_string = false;

        char quote_char = '"';

        char escape_char = 0;
        /**
         * flags for each byte to indicate if escape is needed.
         */
        bool need_escape[256] = {false};

        /**
         * only used for export data
         */
        bool _output_object_data = true;

        /**
         * The format of null value in nested type, eg:
         *      NULL
         *      null
         */
        const char* null_format = "\\N";
        size_t null_len = 2;

        /**
         * The wrapper char for string type in nested type.
         *  eg, if set to empty, the array<string> will be:
         *       [abc, def, , hig]
         *      if set to '"', the array<string> will be:
         *       ["abc", "def", "", "hig"]
         */
        const char* nested_string_wrapper;
        int wrapper_len = 0;

        /**
         * mysql_collection_delim is used to separate elements in collection, such as array, map, struct
         * It is used to write to mysql.
         */
        std::string mysql_collection_delim = ", ";

        /**
         * is_bool_value_num is used to display bool value in collection, such as array, map, struct
         * eg, if set to true, the array<true> will be:
         *      [1]
         *     if set to false, the array<true> will be:
         *      [true]
         */
        bool is_bool_value_num = true;

        const cctz::time_zone* timezone = nullptr;

        [[nodiscard]] char get_collection_delimiter(
                int hive_text_complex_type_delimiter_level) const {
            CHECK(0 <= hive_text_complex_type_delimiter_level &&
                  hive_text_complex_type_delimiter_level <= 153);

            char ans;
            //https://github.com/apache/hive/blob/master/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L250
            //use only control chars that are very unlikely to be part of the string
            // the following might/likely to be used in text files for strings
            // 9 (horizontal tab, HT, \t, ^I)
            // 10 (line feed, LF, \n, ^J),
            // 12 (form feed, FF, \f, ^L),
            // 13 (carriage return, CR, \r, ^M),
            // 27 (escape, ESC, \e [GCC only], ^[).
            if (hive_text_complex_type_delimiter_level == 0) {
                ans = field_delim[0];
            } else if (hive_text_complex_type_delimiter_level == 1) {
                ans = collection_delim;
            } else if (hive_text_complex_type_delimiter_level == 2) {
                ans = map_key_delim;
            } else if (hive_text_complex_type_delimiter_level <= 7) {
                // [3, 7] -> [4, 8]
                ans = cast_set<char, int, false>(hive_text_complex_type_delimiter_level + 1);
            } else if (hive_text_complex_type_delimiter_level == 8) {
                // [8] -> [11]
                ans = 11;
            } else if (hive_text_complex_type_delimiter_level <= 21) {
                // [9, 21] -> [14, 26]
                ans = cast_set<char, int, false>(hive_text_complex_type_delimiter_level + 5);
            } else if (hive_text_complex_type_delimiter_level <= 25) {
                // [22, 25] -> [28, 31]
                ans = cast_set<char, int, false>(hive_text_complex_type_delimiter_level + 6);
            } else {
                // [26, 153] -> [-128, -1]
                ans = cast_set<char, int, false>(hive_text_complex_type_delimiter_level +
                                                 (-26 - 128));
            }

            return ans;
        }
    };

    // only used for orc file format.
    // Buffer used by date/datetime/datev2/datetimev2/largeint type
    // date/datetime/datev2/datetimev2/largeint type will be converted to string bytes to store in Buffer
    // The minimum value of largeint has 40 bytes after being converted to string(a negative number occupies a byte)
    // The bytes of date/datetime/datev2/datetimev2 after converted to string are smaller than largeint
    // Because a block is 4064 rows by default, here is 4064*40 bytes to BUFFER,
    static constexpr size_t BUFFER_UNIT_SIZE = 4064 * 40;
    // buffer reserves 40 bytes. The reserved space is just to prevent Headp-Buffer-Overflow
    static constexpr size_t BUFFER_RESERVED_SIZE = 40;

    // For the NULL value in the complex type, we use the `null` of the lowercase
    static const std::string NULL_IN_COMPLEX_TYPE;

    // For the NULL value in the ordinary type in csv file format, we use `\N`
    static const std::string NULL_IN_CSV_FOR_ORDINARY_TYPE;

public:
    DataTypeSerDe(int nesting_level = 1) : _nesting_level(nesting_level) {};
    virtual ~DataTypeSerDe();

    Status default_from_string(StringRef& str, IColumn& column) const;

    // All types can override this function
    // When this function is called, column should be of the corresponding type
    // everytime call this, should insert new cell to the end of column
    virtual Status from_string(StringRef& str, IColumn& column,
                               const FormatOptions& options) const {
        return Status::NotSupported("from_string is not supported {}", get_name());
    }

    // Similar to from_string, but in strict mode, we should not handle errors.
    // If conversion from string fails, we should return immediately
    virtual Status from_string_strict_mode(StringRef& str, IColumn& column,
                                           const FormatOptions& options) const {
        return from_string(str, column, options);
    }

    // Override functions with _batch suffix only when performance is critical
    // Only used for basic data types, such as Ip, Date, Number, etc.
    virtual Status from_string_batch(const ColumnString& str, ColumnNullable& column,
                                     const FormatOptions& options) const {
        return Status::NotSupported("from_string is not supported");
    }

    // For strict mode, we should not have nullable columns, as we will directly report errors when string conversion fails instead of handling them
    virtual Status from_string_strict_mode_batch(
            const ColumnString& str, IColumn& column, const FormatOptions& options,
            const NullMap::value_type* null_map = nullptr) const {
        return Status::NotSupported("from_string is not supported");
    }

    // Text serializer and deserializer with formatOptions to handle different text format
    virtual Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                              BufferWritable& bw, FormatOptions& options) const = 0;

    // this function serialize multi-column to one row text to avoid virtual function call in complex type nested loop
    virtual Status serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                            int64_t end_idx, BufferWritable& bw,
                                            FormatOptions& options) const = 0;

    virtual Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                  const FormatOptions& options) const = 0;

    // In some cases, CSV and JSON deserialization behaviors may differ
    // so we provide a default implementation that uses JSON deserialization
    virtual Status deserialize_one_cell_from_csv(IColumn& column, Slice& slice,
                                                 const FormatOptions& options) const {
        return deserialize_one_cell_from_json(column, slice, options);
    }

    // deserialize text vector is to avoid virtual function call in complex type nested loop
    virtual Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                                       uint64_t* num_deserialized,
                                                       const FormatOptions& options) const = 0;
    // deserialize fixed values.Repeatedly insert the value row times into the column.
    virtual Status deserialize_column_from_fixed_json(IColumn& column, Slice& slice, uint64_t rows,
                                                      uint64_t* num_deserialized,
                                                      const FormatOptions& options) const {
        //In this function implementation, we need to consider the case where rows is 0, 1, and other larger integers.
        if (rows < 1) [[unlikely]] {
            return Status::OK();
        }
        Status st = deserialize_one_cell_from_json(column, slice, options);
        if (!st.ok()) {
            *num_deserialized = 0;
            return st;
        }
        if (rows > 1) [[likely]] {
            insert_column_last_value_multiple_times(column, rows - 1);
        }
        *num_deserialized = rows;
        return Status::OK();
    }
    // Insert the last value to the end of this column multiple times.
    virtual void insert_column_last_value_multiple_times(IColumn& column, uint64_t times) const {
        if (times < 1) [[unlikely]] {
            return;
        }
        //If you try to simplify this operation by using `column.insert_many_from(column, column.size() - 1, rows - 1);`
        // you are likely to get incorrect data results.
        MutableColumnPtr dum_col = column.clone_empty();
        dum_col->insert_from(column, column.size() - 1);
        column.insert_many_from(*dum_col.get(), 0, times);
    }

    virtual Status deserialize_one_cell_from_hive_text(
            IColumn& column, Slice& slice, const FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const {
        return deserialize_one_cell_from_json(column, slice, options);
    };
    virtual Status deserialize_column_from_hive_text_vector(
            IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
            const FormatOptions& options, int hive_text_complex_type_delimiter_level = 1) const {
        return deserialize_column_from_json_vector(column, slices, num_deserialized, options);
    };
    virtual Status serialize_one_cell_to_hive_text(
            const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const {
        return serialize_one_cell_to_json(column, row_num, bw, options);
    }

    virtual Status serialize_column_to_jsonb(const IColumn& from_column, int64_t row_num,
                                             JsonbWriter& writer) const {
        return Status::NotSupported("{} does not support serialize_column_to_jsonb", get_name());
    }

    virtual Status serialize_column_to_jsonb_vector(const IColumn& from_column,
                                                    ColumnString& to_column) const;

    virtual Status deserialize_column_from_jsonb(IColumn& column, const JsonbValue* jsonb_value,
                                                 CastParameters& castParms) const {
        return Status::NotSupported("{} does not support serialize_column_to_jsonb_vector",
                                    get_name());
    }

    Status parse_column_from_jsonb_string(IColumn& column, const JsonbValue* jsonb_value,
                                          CastParameters& castParms) const;
    // Protobuf serializer and deserializer
    virtual Status write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                                      int64_t end) const = 0;

    virtual Status read_column_from_pb(IColumn& column, const PValues& arg) const = 0;

    // JSONB serializer and deserializer, should write col_id
    virtual void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                         Arena& mem_pool, int32_t col_id,
                                         int64_t row_num) const = 0;

    virtual void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const = 0;

    // MySQL serializer and deserializer
    virtual Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                         int64_t row_idx, bool col_const,
                                         const FormatOptions& options) const = 0;

    virtual Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                         int64_t row_idx, bool col_const,
                                         const FormatOptions& options) const = 0;
    // Thrift serializer and deserializer

    // JSON serializer and deserializer

    // Arrow serializer and deserializer
    virtual Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                         arrow::ArrayBuilder* array_builder, int64_t start,
                                         int64_t end, const cctz::time_zone& ctz) const = 0;
    virtual Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                          int64_t start, int64_t end,
                                          const cctz::time_zone& ctz) const = 0;

    // ORC serializer
    virtual Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                                       const NullMap* null_map,
                                       orc::ColumnVectorBatch* orc_col_batch, int64_t start,
                                       int64_t end, vectorized::Arena& arena) const = 0;
    // ORC deserializer

    virtual void set_return_object_as_string(bool value) { _return_object_as_string = value; }

    virtual DataTypeSerDeSPtrs get_nested_serdes() const {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Method get_nested_serdes is not supported for this serde");
    }

    virtual void write_one_cell_to_binary(const IColumn& src_column, ColumnString::Chars& chars,
                                          int64_t row_num) const {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "write_one_cell_to_binary");
    }

protected:
    bool _return_object_as_string = false;
    // This parameter indicates what level the serde belongs to and is mainly used for complex types
    // The default level is 1, and each time you nest, the level increases by 1,
    // for example: struct<string>
    // The _nesting_level of StructSerde is 1
    // The _nesting_level of StringSerde is 2
    int _nesting_level = 1;
};

/// Invert values since Arrow interprets 1 as a non-null value, while doris as a null
inline static NullMap revert_null_map(const NullMap* null_bytemap, size_t start, size_t end) {
    NullMap res;
    if (!null_bytemap) {
        return res;
    }

    res.resize(end - start);
    const auto* __restrict src_data = (*null_bytemap).data();
    auto* __restrict res_data = res.data();
    for (size_t i = 0; i < res.size(); ++i) {
        res_data[i] = !src_data[i + start];
    }
    return res;
}

inline Status checkArrowStatus(const arrow::Status& status, const std::string& column,
                               const std::string& format_name) {
    if (!status.ok()) {
        return Status::FatalError("arrow serde with arrow: {} with column : {} with error msg: {}",
                                  format_name, column, status.ToString());
    }
    return Status::OK();
}

DataTypeSerDeSPtrs create_data_type_serdes(
        const std::vector<std::shared_ptr<const IDataType>>& types);
DataTypeSerDeSPtrs create_data_type_serdes(const std::vector<SlotDescriptor*>& slots);
#include "common/compile_check_end.h"
} // namespace vectorized
} // namespace doris

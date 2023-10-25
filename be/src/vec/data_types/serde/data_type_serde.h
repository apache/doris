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

#include <stdint.h>

#include <memory>
#include <orc/OrcFile.hh>
#include <vector>

#include "arrow/status.h"
#include "common/status.h"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/pod_array.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/io/reader_buffer.h"

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

#define SERIALIZE_COLUMN_TO_JSON()                                                          \
    for (size_t i = start_idx; i < end_idx; ++i) {                                          \
        if (i != start_idx) {                                                               \
            bw.write(options.field_delim.data(), options.field_delim.size());               \
        }                                                                                   \
        RETURN_IF_ERROR(serialize_one_cell_to_json(column, i, bw, options, nesting_level)); \
    }                                                                                       \
    return Status::OK();

#define DESERIALIZE_COLUMN_FROM_JSON_VECTOR()                                                      \
    for (int i = 0; i < slices.size(); ++i) {                                                      \
        if (Status st = deserialize_one_cell_from_json(column, slices[i], options, nesting_level); \
            st != Status::OK()) {                                                                  \
            return st;                                                                             \
        }                                                                                          \
        ++*num_deserialized;                                                                       \
    }

#define DESERIALIZE_COLUMN_FROM_HIVE_TEXT_VECTOR()                                      \
    for (int i = 0; i < slices.size(); ++i) {                                           \
        if (Status st = deserialize_one_cell_from_hive_text(column, slices[i], options, \
                                                            nesting_level);             \
            st != Status::OK()) {                                                       \
            return st;                                                                  \
        }                                                                               \
        ++*num_deserialized;                                                            \
    }

#define REALLOC_MEMORY_FOR_ORC_WRITER()                                                  \
    while (bufferRef.size - BUFFER_RESERVED_SIZE < offset + len) {                       \
        char* new_ptr = (char*)malloc(bufferRef.size + BUFFER_UNIT_SIZE);                \
        if (!new_ptr) {                                                                  \
            return Status::InternalError(                                                \
                    "malloc memory error when write largeint column data to orc file."); \
        }                                                                                \
        memcpy(new_ptr, bufferRef.data, bufferRef.size);                                 \
        free(const_cast<char*>(bufferRef.data));                                         \
        bufferRef.data = new_ptr;                                                        \
        bufferRef.size = bufferRef.size + BUFFER_UNIT_SIZE;                              \
    }

namespace doris {
class PValues;
class JsonbValue;
class SlotDescriptor;

namespace vectorized {
class IColumn;
class Arena;
class IDataType;
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
    // Text serialization/deserialization of data types depend on some settings witch we define
    // in formatOptions.
    struct FormatOptions {
        /**
         * if true, we will use olap format which defined in src/olap/types.h, but we do not suggest
         * use this format in olap, because it is more slower, keep this option is for compatibility.
         */
        bool date_olap_format = false;
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

        char escape_char = 0;

        /**
         * only used for export data
         */
        bool _output_object_data = true;

        [[nodiscard]] char get_collection_delimiter(int nesting_level) const {
            CHECK(0 <= nesting_level && nesting_level <= 153);

            char ans = '\002';
            //https://github.com/apache/hive/blob/master/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L250
            //use only control chars that are very unlikely to be part of the string
            // the following might/likely to be used in text files for strings
            // 9 (horizontal tab, HT, \t, ^I)
            // 10 (line feed, LF, \n, ^J),
            // 12 (form feed, FF, \f, ^L),
            // 13 (carriage return, CR, \r, ^M),
            // 27 (escape, ESC, \e [GCC only], ^[).

            if (nesting_level == 1) {
                ans = collection_delim;
            } else if (nesting_level == 2) {
                ans = map_key_delim;
            } else if (nesting_level <= 7) {
                // [3, 7] -> [4, 8]
                ans = nesting_level + 1;
            } else if (nesting_level == 8) {
                // [8] -> [11]
                ans = 11;
            } else if (nesting_level <= 21) {
                // [9, 21] -> [14, 26]
                ans = nesting_level + 5;
            } else if (nesting_level <= 25) {
                // [22, 25] -> [28, 31]
                ans = nesting_level + 6;
            } else if (nesting_level <= 153) {
                // [26, 153] -> [-128, -1]
                ans = nesting_level + (-26 - 128);
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

public:
    DataTypeSerDe(int nesting_level = 1) : _nesting_level(nesting_level) {};
    virtual ~DataTypeSerDe();
    // Text serializer and deserializer with formatOptions to handle different text format
    virtual Status serialize_one_cell_to_json(const IColumn& column, int row_num,
                                              BufferWritable& bw, FormatOptions& options,
                                              int nesting_level = 1) const = 0;

    // this function serialize multi-column to one row text to avoid virtual function call in complex type nested loop
    virtual Status serialize_column_to_json(const IColumn& column, int start_idx, int end_idx,
                                            BufferWritable& bw, FormatOptions& options,
                                            int nesting_level = 1) const = 0;

    virtual Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                  const FormatOptions& options,
                                                  int nesting_level = 1) const = 0;
    // deserialize text vector is to avoid virtual function call in complex type nested loop
    virtual Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                                       int* num_deserialized,
                                                       const FormatOptions& options,
                                                       int nesting_level = 1) const = 0;

    virtual Status deserialize_one_cell_from_hive_text(IColumn& column, Slice& slice,
                                                       const FormatOptions& options,
                                                       int nesting_level = 1) const {
        return deserialize_one_cell_from_json(column, slice, options, nesting_level);
    };
    virtual Status deserialize_column_from_hive_text_vector(IColumn& column,
                                                            std::vector<Slice>& slices,
                                                            int* num_deserialized,
                                                            const FormatOptions& options,
                                                            int nesting_level = 1) const {
        return deserialize_column_from_json_vector(column, slices, num_deserialized, options,
                                                   nesting_level);
    };
    virtual void serialize_one_cell_to_hive_text(const IColumn& column, int row_num,
                                                 BufferWritable& bw, FormatOptions& options,
                                                 int nesting_level = 1) const {
        Status st = serialize_one_cell_to_json(column, row_num, bw, options);
        if (!st.ok()) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "serialize_one_cell_to_json error: {}", st.to_string());
        }
    }

    // Protobuf serializer and deserializer
    virtual Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                                      int end) const = 0;

    virtual Status read_column_from_pb(IColumn& column, const PValues& arg) const = 0;

    // JSONB serializer and deserializer, should write col_id
    virtual void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                         Arena* mem_pool, int32_t col_id, int row_num) const = 0;

    virtual void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const = 0;

    // MySQL serializer and deserializer
    virtual Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                         int row_idx, bool col_const) const = 0;

    virtual Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                         int row_idx, bool col_const) const = 0;
    // Thrift serializer and deserializer

    // JSON serializer and deserializer

    // Arrow serializer and deserializer
    virtual void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                       arrow::ArrayBuilder* array_builder, int start,
                                       int end) const = 0;
    virtual void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                        int end, const cctz::time_zone& ctz) const = 0;

    // ORC serializer
    virtual Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                                       const NullMap* null_map,
                                       orc::ColumnVectorBatch* orc_col_batch, int start, int end,
                                       std::vector<StringRef>& buffer_list) const = 0;
    // ORC deserializer

    virtual void set_return_object_as_string(bool value) { _return_object_as_string = value; }

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

    res.reserve(end - start);
    for (size_t i = start; i < end; ++i) {
        res.emplace_back(!(*null_bytemap)[i]);
    }
    return res;
}

inline void checkArrowStatus(const arrow::Status& status, const std::string& column,
                             const std::string& format_name) {
    if (!status.ok()) {
        LOG(FATAL) << "arrow serde with arrow: " << format_name << " with column : " << column
                   << " with error msg: " << status.ToString();
    }
}

using DataTypeSerDeSPtr = std::shared_ptr<DataTypeSerDe>;
using DataTypeSerDeSPtrs = std::vector<DataTypeSerDeSPtr>;

DataTypeSerDeSPtrs create_data_type_serdes(
        const std::vector<std::shared_ptr<const IDataType>>& types);
DataTypeSerDeSPtrs create_data_type_serdes(const std::vector<SlotDescriptor*>& slots);

} // namespace vectorized
} // namespace doris

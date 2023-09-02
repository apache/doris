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

#define SERIALIZE_COLUMN_TO_TEXT()                                            \
    for (size_t i = start_idx; i < end_idx; ++i) {                            \
        if (i != start_idx) {                                                 \
            bw.write(options.field_delim.data(), options.field_delim.size()); \
        }                                                                     \
        serialize_one_cell_to_text(column, i, bw, options);                   \
    }

#define DESERIALIZE_COLUMN_FROM_TEXT_VECTOR()                                       \
    for (int i = 0; i < slices.size(); ++i) {                                       \
        if (Status st = deserialize_one_cell_from_text(column, slices[i], options); \
            st != Status::OK()) {                                                   \
            return st;                                                              \
        }                                                                           \
        ++*num_deserialized;                                                        \
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
    };

public:
    DataTypeSerDe();
    virtual ~DataTypeSerDe();
    // Text serializer and deserializer with formatOptions to handle different text format
    virtual void serialize_one_cell_to_text(const IColumn& column, int row_num, BufferWritable& bw,
                                            FormatOptions& options) const = 0;

    // this function serialize multi-column to one row text to avoid virtual function call in complex type nested loop
    virtual void serialize_column_to_text(const IColumn& column, int start_idx, int end_idx,
                                          BufferWritable& bw, FormatOptions& options) const = 0;

    virtual Status deserialize_one_cell_from_text(IColumn& column, Slice& slice,
                                                  const FormatOptions& options) const = 0;
    // deserialize text vector is to avoid virtual function call in complex type nested loop
    virtual Status deserialize_column_from_text_vector(IColumn& column, std::vector<Slice>& slices,
                                                       int* num_deserialized,
                                                       const FormatOptions& options) const = 0;

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

    // ORC serializer and deserializer

    // CSV serializer and deserializer

    // JSON serializer and deserializer

    // Arrow serializer and deserializer
    virtual void write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                       arrow::ArrayBuilder* array_builder, int start,
                                       int end) const = 0;
    virtual void read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int start,
                                        int end, const cctz::time_zone& ctz) const = 0;

    virtual void set_return_object_as_string(bool value) { _return_object_as_string = value; }

protected:
    bool _return_object_as_string = false;
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

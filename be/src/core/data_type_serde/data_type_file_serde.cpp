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

#include "core/data_type_serde/data_type_file_serde.h"

#include "core/assert_cast.h"
#include "core/column/column_file.h"

namespace doris {

DataTypeFileSerDe::DataTypeFileSerDe(DataTypeSerDeSPtr physical_serde, int nesting_level)
        : DataTypeSerDe(nesting_level), _physical_serde(std::move(physical_serde)) {}

std::string DataTypeFileSerDe::get_name() const {
    return "File";
}

const IColumn& DataTypeFileSerDe::get_physical_column(const IColumn& column) const {
    return assert_cast<const ColumnFile&>(column).get_jsonb_column();
}

IColumn& DataTypeFileSerDe::get_physical_column(IColumn& column) const {
    return assert_cast<ColumnFile&>(column).get_jsonb_column();
}

Status DataTypeFileSerDe::from_string(StringRef& str, IColumn& column,
                                      const FormatOptions& options) const {
    return _physical_serde->from_string(str, get_physical_column(column), options);
}

Status DataTypeFileSerDe::from_string_strict_mode(StringRef& str, IColumn& column,
                                                  const FormatOptions& options) const {
    return _physical_serde->from_string_strict_mode(str, get_physical_column(column), options);
}

Status DataTypeFileSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                     BufferWritable& bw,
                                                     FormatOptions& options) const {
    return _physical_serde->serialize_one_cell_to_json(get_physical_column(column), row_num, bw,
                                                       options);
}

Status DataTypeFileSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                   int64_t end_idx, BufferWritable& bw,
                                                   FormatOptions& options) const {
    return _physical_serde->serialize_column_to_json(get_physical_column(column), start_idx, end_idx,
                                                     bw, options);
}

Status DataTypeFileSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                         const FormatOptions& options) const {
    return _physical_serde->deserialize_one_cell_from_json(get_physical_column(column), slice,
                                                           options);
}

Status DataTypeFileSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options) const {
    return _physical_serde->deserialize_column_from_json_vector(get_physical_column(column), slices,
                                                                num_deserialized, options);
}

Status DataTypeFileSerDe::deserialize_one_cell_from_hive_text(
        IColumn& column, Slice& slice, const FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    return _physical_serde->deserialize_one_cell_from_hive_text(
            get_physical_column(column), slice, options, hive_text_complex_type_delimiter_level);
}

Status DataTypeFileSerDe::deserialize_column_from_hive_text_vector(
        IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
        const FormatOptions& options, int hive_text_complex_type_delimiter_level) const {
    return _physical_serde->deserialize_column_from_hive_text_vector(
            get_physical_column(column), slices, num_deserialized, options,
            hive_text_complex_type_delimiter_level);
}

Status DataTypeFileSerDe::serialize_one_cell_to_hive_text(
        const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
        int hive_text_complex_type_delimiter_level) const {
    return _physical_serde->serialize_one_cell_to_hive_text(get_physical_column(column), row_num, bw,
                                                            options,
                                                            hive_text_complex_type_delimiter_level);
}

Status DataTypeFileSerDe::serialize_column_to_jsonb(const IColumn& from_column, int64_t row_num,
                                                    JsonbWriter& writer) const {
    return _physical_serde->serialize_column_to_jsonb(get_physical_column(from_column), row_num,
                                                      writer);
}

Status DataTypeFileSerDe::deserialize_column_from_jsonb(IColumn& column,
                                                        const JsonbValue* jsonb_value,
                                                        CastParameters& castParms) const {
    return _physical_serde->deserialize_column_from_jsonb(get_physical_column(column), jsonb_value,
                                                          castParms);
}

Status DataTypeFileSerDe::write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                                             int64_t end) const {
    return _physical_serde->write_column_to_pb(get_physical_column(column), result, start, end);
}

Status DataTypeFileSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    return _physical_serde->read_column_from_pb(get_physical_column(column), arg);
}

void DataTypeFileSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                Arena& mem_pool, int32_t col_id, int64_t row_num,
                                                const FormatOptions& options) const {
    _physical_serde->write_one_cell_to_jsonb(get_physical_column(column), result, mem_pool, col_id,
                                             row_num, options);
}

void DataTypeFileSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    _physical_serde->read_one_cell_from_jsonb(get_physical_column(column), arg);
}

Status DataTypeFileSerDe::write_column_to_mysql_binary(const IColumn& column,
                                                       MysqlRowBinaryBuffer& row_buffer,
                                                       int64_t row_idx, bool col_const,
                                                       const FormatOptions& options) const {
    return _physical_serde->write_column_to_mysql_binary(get_physical_column(column), row_buffer,
                                                         row_idx, col_const, options);
}

Status DataTypeFileSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int64_t start,
                                                int64_t end, const cctz::time_zone& ctz) const {
    return _physical_serde->write_column_to_arrow(get_physical_column(column), null_map, array_builder,
                                                  start, end, ctz);
}

Status DataTypeFileSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int64_t start, int64_t end,
                                                 const cctz::time_zone& ctz) const {
    return _physical_serde->read_column_from_arrow(get_physical_column(column), arrow_array, start, end,
                                                   ctz);
}

Status DataTypeFileSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                              const NullMap* null_map,
                                              orc::ColumnVectorBatch* orc_col_batch, int64_t start,
                                              int64_t end, Arena& arena,
                                              const FormatOptions& options) const {
    return _physical_serde->write_column_to_orc(timezone, get_physical_column(column), null_map,
                                                orc_col_batch, start, end, arena, options);
}

bool DataTypeFileSerDe::write_column_to_presto_text(const IColumn& column, BufferWritable& bw,
                                                    int64_t row_idx,
                                                    const FormatOptions& options) const {
    return _physical_serde->write_column_to_presto_text(get_physical_column(column), bw, row_idx,
                                                        options);
}

bool DataTypeFileSerDe::write_column_to_hive_text(const IColumn& column, BufferWritable& bw,
                                                  int64_t row_idx,
                                                  const FormatOptions& options) const {
    return _physical_serde->write_column_to_hive_text(get_physical_column(column), bw, row_idx,
                                                      options);
}

void DataTypeFileSerDe::to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                                  const FormatOptions& options) const {
    _physical_serde->to_string(get_physical_column(column), row_num, bw, options);
}

void DataTypeFileSerDe::set_return_object_as_string(bool value) {
    DataTypeSerDe::set_return_object_as_string(value);
    _physical_serde->set_return_object_as_string(value);
}

DataTypeSerDeSPtrs DataTypeFileSerDe::get_nested_serdes() const {
    return _physical_serde->get_nested_serdes();
}

} // namespace doris

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

#include "vec/runtime/vcsv_transformer.h"

#include <glog/logging.h>
#include <stdlib.h>
#include <string.h>

#include <exception>
#include <ostream>

#include "gutil/strings/numbers.h"
#include "io/fs/file_writer.h"
#include "runtime/define_primitive_type.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "util/mysql_global.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

VCSVTransformer::VCSVTransformer(doris::io::FileWriter* file_writer,
                                 const VExprContextSPtrs& output_vexpr_ctxs,
                                 bool output_object_data, std::string_view header_type,
                                 std::string_view header, std::string_view column_separator,
                                 std::string_view line_delimiter)
        : VFileFormatTransformer(output_vexpr_ctxs, output_object_data),
          _column_separator(column_separator),
          _line_delimiter(line_delimiter),
          _file_writer(file_writer) {
    if (header.size() > 0) {
        _csv_header = header;
        if (header_type == BeConsts::CSV_WITH_NAMES_AND_TYPES) {
            _csv_header += _gen_csv_header_types();
        }
    } else {
        _csv_header = "";
    }
}

Status VCSVTransformer::open() {
    if (!_csv_header.empty()) {
        return _file_writer->append(Slice(_csv_header.data(), _csv_header.size()));
    }
    return Status::OK();
}

int64_t VCSVTransformer::written_len() {
    return _file_writer->bytes_appended();
}

Status VCSVTransformer::close() {
    return _file_writer->close();
}

Status VCSVTransformer::write(const Block& block) {
    using doris::operator<<;
    for (size_t i = 0; i < block.rows(); i++) {
        for (size_t col_id = 0; col_id < block.columns(); col_id++) {
            auto col = block.get_by_position(col_id);
            if (col.column->is_null_at(i)) {
                _plain_text_outstream << NULL_IN_CSV;
            } else {
                switch (_output_vexpr_ctxs[col_id]->root()->type().type) {
                case TYPE_BOOLEAN:
                case TYPE_TINYINT:
                    _plain_text_outstream << (int)*reinterpret_cast<const int8_t*>(
                            col.column->get_data_at(i).data);
                    break;
                case TYPE_SMALLINT:
                    _plain_text_outstream
                            << *reinterpret_cast<const int16_t*>(col.column->get_data_at(i).data);
                    break;
                case TYPE_INT:
                    _plain_text_outstream
                            << *reinterpret_cast<const int32_t*>(col.column->get_data_at(i).data);
                    break;
                case TYPE_BIGINT:
                    _plain_text_outstream
                            << *reinterpret_cast<const int64_t*>(col.column->get_data_at(i).data);
                    break;
                case TYPE_LARGEINT:
                    _plain_text_outstream
                            << *reinterpret_cast<const __int128*>(col.column->get_data_at(i).data);
                    break;
                case TYPE_FLOAT: {
                    char buffer[MAX_FLOAT_STR_LENGTH + 2];
                    float float_value =
                            *reinterpret_cast<const float*>(col.column->get_data_at(i).data);
                    buffer[0] = '\0';
                    int length = FloatToBuffer(float_value, MAX_FLOAT_STR_LENGTH, buffer);
                    DCHECK(length >= 0) << "gcvt float failed, float value=" << float_value;
                    _plain_text_outstream << buffer;
                    break;
                }
                case TYPE_DOUBLE: {
                    // To prevent loss of precision on float and double types,
                    // they are converted to strings before output.
                    // For example: For a double value 27361919854.929001,
                    // the direct output of using std::stringstream is 2.73619e+10,
                    // and after conversion to a string, it outputs 27361919854.929001
                    char buffer[MAX_DOUBLE_STR_LENGTH + 2];
                    double double_value =
                            *reinterpret_cast<const double*>(col.column->get_data_at(i).data);
                    buffer[0] = '\0';
                    int length = DoubleToBuffer(double_value, MAX_DOUBLE_STR_LENGTH, buffer);
                    DCHECK(length >= 0) << "gcvt double failed, double value=" << double_value;
                    _plain_text_outstream << buffer;
                    break;
                }
                case TYPE_DATEV2: {
                    char buf[64];
                    const DateV2Value<DateV2ValueType>* time_val =
                            (const DateV2Value<DateV2ValueType>*)(col.column->get_data_at(i).data);
                    time_val->to_string(buf);
                    _plain_text_outstream << buf;
                    break;
                }
                case TYPE_DATETIMEV2: {
                    char buf[64];
                    const DateV2Value<DateTimeV2ValueType>* time_val =
                            (const DateV2Value<DateTimeV2ValueType>*)(col.column->get_data_at(i)
                                                                              .data);
                    time_val->to_string(buf, _output_vexpr_ctxs[col_id]->root()->type().scale);
                    _plain_text_outstream << buf;
                    break;
                }
                case TYPE_DATE:
                case TYPE_DATETIME: {
                    char buf[64];
                    const VecDateTimeValue* time_val =
                            (const VecDateTimeValue*)(col.column->get_data_at(i).data);
                    time_val->to_string(buf);
                    _plain_text_outstream << buf;
                    break;
                }
                case TYPE_OBJECT:
                case TYPE_HLL: {
                    if (!_output_object_data) {
                        _plain_text_outstream << NULL_IN_CSV;
                        break;
                    }
                    [[fallthrough]];
                }
                case TYPE_VARCHAR:
                case TYPE_CHAR:
                case TYPE_STRING: {
                    auto value = col.column->get_data_at(i);
                    _plain_text_outstream << value;
                    break;
                }
                case TYPE_DECIMALV2: {
                    const DecimalV2Value decimal_val(
                            reinterpret_cast<const PackedInt128*>(col.column->get_data_at(i).data)
                                    ->value);
                    std::string decimal_str;
                    decimal_str = decimal_val.to_string();
                    _plain_text_outstream << decimal_str;
                    break;
                }
                case TYPE_DECIMAL32: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                case TYPE_DECIMAL64: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                case TYPE_DECIMAL128I: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                case TYPE_ARRAY: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                case TYPE_MAP: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                case TYPE_STRUCT: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                default: {
                    // not supported type, like BITMAP, just export null
                    _plain_text_outstream << NULL_IN_CSV;
                }
                }
            }
            if (col_id < block.columns() - 1) {
                _plain_text_outstream << _column_separator;
            }
        }
        _plain_text_outstream << _line_delimiter;
    }

    return _flush_plain_text_outstream();
}

Status VCSVTransformer::_flush_plain_text_outstream() {
    size_t pos = _plain_text_outstream.tellp();
    if (pos == 0) {
        return Status::OK();
    }

    const std::string& buf = _plain_text_outstream.str();
    RETURN_IF_ERROR(_file_writer->append(buf));

    // clear the stream
    _plain_text_outstream.str("");
    _plain_text_outstream.clear();

    return Status::OK();
}

std::string VCSVTransformer::_gen_csv_header_types() {
    std::string types;
    int num_columns = _output_vexpr_ctxs.size();
    for (int i = 0; i < num_columns; ++i) {
        types += type_to_string(_output_vexpr_ctxs[i]->root()->type().type);
        if (i < num_columns - 1) {
            types += _column_separator;
        }
    }
    types += _line_delimiter;
    return types;
}

const std::string VCSVTransformer::NULL_IN_CSV = "\\N";
} // namespace doris::vectorized

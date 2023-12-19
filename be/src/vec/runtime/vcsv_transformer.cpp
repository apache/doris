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

#include "common/status.h"
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
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/exec/format/csv/csv_reader.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

VCSVTransformer::VCSVTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                                 const VExprContextSPtrs& output_vexpr_ctxs,
                                 bool output_object_data, std::string_view header_type,
                                 std::string_view header, std::string_view column_separator,
                                 std::string_view line_delimiter)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
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
    auto ser_col = ColumnString::create();
    ser_col->reserve(block.columns());
    VectorBufferWriter buffer_writer(*ser_col.get());
    for (size_t i = 0; i < block.rows(); i++) {
        for (size_t col_id = 0; col_id < block.columns(); col_id++) {
            if (col_id != 0) {
                buffer_writer.write(_column_separator.data(), _column_separator.size());
            }
            Status st = _serdes[col_id]->serialize_one_cell_to_json(
                    *(block.get_by_position(col_id).column), i, buffer_writer, _options);
            if (!st.ok()) {
                // VectorBufferWriter must do commit before deconstruct,
                // or it may throw DCHECK failure.
                buffer_writer.commit();
                return st;
            }
        }
        buffer_writer.write(_line_delimiter.data(), _line_delimiter.size());
        buffer_writer.commit();
    }
    return _flush_plain_text_outstream(*ser_col.get());
}

Status VCSVTransformer::_flush_plain_text_outstream(ColumnString& ser_col) {
    if (ser_col.byte_size() == 0) {
        return Status::OK();
    }

    RETURN_IF_ERROR(
            _file_writer->append(Slice(ser_col.get_chars().data(), ser_col.get_chars().size())));

    // clear the stream
    ser_col.clear();
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
} // namespace doris::vectorized

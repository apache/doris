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

#include "vparquet_reader.h"

#include "parquet_thrift_util.h"

namespace doris::vectorized {
doris::vectorized::ParquetReader::ParquetReader(doris::FileReader* file_reader, int64_t batch_size,
                                                int32_t num_of_columns_from_file,
                                                int64_t range_start_offset, int64_t range_size) {
    //        : _batch_size(batch_size), _num_of_columns_from_file(num_of_columns_from_file) {
    _file_reader = file_reader;
    _total_groups = 0;
    //    _current_group = 0;
    //        _statistics = std::make_shared<Statistics>();
}

doris::vectorized::ParquetReader::~ParquetReader() {
    //    _batch.clear();
}

Status ParquetReader::init_reader(const TupleDescriptor* tuple_desc,
                                  const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                  const std::vector<ExprContext*>& conjunct_ctxs,
                                  const std::string& timezone) {
    _file_reader->open();
    RETURN_IF_ERROR(parse_thrift_footer(_file_reader, _file_metadata));
    auto metadata = _file_metadata->to_thrift_metadata();

    _total_groups = metadata.row_groups.size();
    if (_total_groups == 0) {
        return Status::EndOfFile("Empty Parquet File");
    }

    return Status::OK();
}

int64_t ParquetReader::_get_row_group_start_offset(const tparquet::RowGroup& row_group) {
    if (row_group.__isset.file_offset) {
        return row_group.file_offset;
    }
    const tparquet::ColumnMetaData& first_column = row_group.columns[0].meta_data;
    return first_column.data_page_offset;
}

} // namespace doris::vectorized
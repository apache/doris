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

#include "common/status.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "storage/segment/column_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/json/path_in_data.h"

namespace doris {

class OlapBlockDataConvertor;

namespace segment_v2 {

#include "common/compile_check_begin.h"

namespace variant_writer_helpers {

Status convert_and_write_column(OlapBlockDataConvertor* converter, const TabletColumn& column,
                                DataTypePtr data_type, ColumnWriter* writer,
                                const ColumnPtr& src_column, size_t num_rows, int column_id);

void maybe_remove_root_jsonb_with_empty_defaults(MutableColumnPtr* root_column, size_t num_rows,
                                                 bool remove_root_jsonb);

Status prepare_subcolumn_writer_target(
        const ColumnWriterOptions& base_opts, const TabletColumn& parent_column,
        int current_column_id, const PathInData& relative_path, const DataTypePtr& current_type,
        int64_t none_null_value_size, size_t num_rows,
        const TabletSchema::SubColumnInfo* existing_subcolumn_info, bool check_storage_type,
        TabletIndexes* out_subcolumn_indexes, ColumnWriterOptions* out_subcolumn_opts,
        std::unique_ptr<ColumnWriter>* out_writer, TabletColumn* out_tablet_column);

} // namespace variant_writer_helpers

#include "common/compile_check_end.h"

} // namespace segment_v2
} // namespace doris

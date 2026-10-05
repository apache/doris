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

#include "format_v2/wal/wal_table_reader.h"

#include "format_v2/wal/wal_reader.h"
#include "runtime/descriptors.h"

namespace doris::format::wal {

Status WalTableReader::annotate_projected_column(const TFileScanSlotInfo&,
                                                 ProjectedColumnBuildContext* context,
                                                 ColumnDefinition* column) const {
    DORIS_CHECK(context != nullptr);
    DORIS_CHECK(column != nullptr);
    if (context->slot_desc == nullptr || context->slot_desc->col_unique_id() < 0) {
        return Status::InternalError("WAL projected column {} has no valid unique id",
                                     column->name);
    }
    // WAL headers carry stable Doris column unique ids, so name-based matching would return a
    // renamed column from the wrong physical position.
    column->identifier = Field::create_field<TYPE_INT>(context->slot_desc->col_unique_id());
    return Status::OK();
}

Status WalTableReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    DORIS_CHECK(reader != nullptr);
    *reader = std::make_unique<WalReader>(_system_properties, _current_task->data_file, _io_ctx,
                                          _scanner_profile, _projected_columns);
    return Status::OK();
}

} // namespace doris::format::wal

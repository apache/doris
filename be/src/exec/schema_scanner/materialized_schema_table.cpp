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

#include "exec/schema_scanner/materialized_schema_table.h"

namespace doris {
#include "common/compile_check_begin.h"

Status MaterializedSchemaTable::prepare() {
    writer_ = std::make_unique<SpillWriter>(state_->get_query_ctx()->resource_ctx(), profile_,
                                            stream_id_, batch_rows_, data_dir_, spill_dir_);
    _set_write_counters(profile_);

    reader_ = std::make_unique<SpillReader>(state_->get_query_ctx()->resource_ctx(), stream_id_,
                                            writer_->get_file_path());

    DBUG_EXECUTE_IF("fault_inject::spill_stream::prepare_spill", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_stream prepare_spill failed");
    });
    COUNTER_UPDATE(_total_file_count, 1);
    if (_current_file_count) {
        COUNTER_UPDATE(_current_file_count, 1);
    }
    return writer_->open();
}

} // namespace doris

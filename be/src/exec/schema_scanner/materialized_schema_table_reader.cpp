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

#include "exec/schema_scanner/materialized_schema_table_reader.h"

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"

namespace doris {
#include "common/compile_check_begin.h"

Status MaterializedSchemaTableReader::read_next_time_slice_(vectorized::Block* block, bool* eos) {
    if (read_time_slice_index_ >= time_slice_filenames_.size()) {
        *eos = true;
        return Status::OK();
    }
    const auto& file_path = time_slice_filenames_[read_time_slice_index_++];
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path, &file_reader));

    block->clear_column_data();
    size_t file_size = file_reader->size();
    DCHECK(file_size != 0);
    
    std::unique_ptr<char[]> read_buff_;
    Slice result(read_buff_.get(), file_size);
    size_t bytes_read = 0;
    

}

}
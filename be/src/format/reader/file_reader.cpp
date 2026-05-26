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

#include "format/reader/file_reader.h"

#include "io/fs/buffered_reader.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/runtime_state.h"

namespace doris::reader {

Status FileReader::init(RuntimeState* state) {
    _init_profile();
    SCOPED_RAW_TIMER(&_reader_statistics.file_reader_create_time);
    ++_reader_statistics.open_file_num;
    io::FileReaderOptions reader_options =
            FileFactory::get_reader_options(state->query_options(), *_file_description);
    _file_reader = DORIS_TRY(io::DelegateReader::create_file_reader(
            _profile, *_system_properties, *_file_description, reader_options,
            io::DelegateReader::AccessMode::RANDOM, _io_ctx));
    _tracing_file_reader = _io_ctx ? std::make_shared<io::TracingFileReader>(
                                             _file_reader, _io_ctx->file_reader_stats)
                                   : _file_reader;
    _eof = false;
    return Status::OK();
}

} // namespace doris::reader

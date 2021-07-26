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

#include "exec/plain_binary_line_reader.h"

#include "common/status.h"
#include "exec/file_reader.h"

namespace doris {

PlainBinaryLineReader::PlainBinaryLineReader(FileReader* file_reader)
        : _file_reader(file_reader) {
}

PlainBinaryLineReader::~PlainBinaryLineReader() {
    close();
}

void PlainBinaryLineReader::close() {
}

Status PlainBinaryLineReader::read_line(const uint8_t** ptr, size_t* size, bool* eof) {
    std::unique_ptr<uint8_t[]> file_buf;
    int64_t read_size = 0;
    RETURN_IF_ERROR(_file_reader->read_one_message(&file_buf, &read_size));
    *ptr = file_buf.release();
    *size = read_size;
    if (read_size == 0) {
        *eof = true;
    }
    return Status::OK();
}

} // namespace doris

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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_RUN_LENGTH_BYTE_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_RUN_LENGTH_BYTE_READER_H

#include "olap/file_stream.h"
#include "olap/olap_define.h"
#include "olap/rowset/run_length_byte_writer.h"

namespace doris {

class ReadOnlyFileStream;
class PositionProvider;

// A reader that reads a sequence of bytes. A control byte is read before
// each run with positive values 0 to 127 meaning 3 to 130 repetitions. If the
// byte is -1 to -128, 1 to 128 literal byte values follow.
class RunLengthByteReader {
public:
    explicit RunLengthByteReader(ReadOnlyFileStream* input);
    ~RunLengthByteReader() {}
    bool has_next() const;
    // Gets the next piece of data, or if there is no more, returns OLAP_ERR_DATA_EOF
    OLAPStatus next(char* value);
    OLAPStatus seek(PositionProvider* position);
    OLAPStatus skip(uint64_t num_values);

private:
    OLAPStatus _read_values();

    ReadOnlyFileStream* _input;
    char _literals[RunLengthByteWriter::MAX_LITERAL_SIZE];
    int32_t _num_literals;
    int32_t _used;
    bool _repeat;

    DISALLOW_COPY_AND_ASSIGN(RunLengthByteReader);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_RUN_LENGTH_BYTE_READER_H
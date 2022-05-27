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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_BIT_FIELD_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_BIT_FIELD_READER_H

#include "olap/olap_define.h"
#include "olap/stream_index_reader.h"

namespace doris {

class ReadOnlyFileStream;
class RunLengthByteReader;

class BitFieldReader {
public:
    BitFieldReader(ReadOnlyFileStream* input);
    ~BitFieldReader();
    Status init();
    // 获取下一条数据, 如果没有更多的数据了, 返回Status::OLAPInternalError(OLAP_ERR_DATA_EOF)
    // 返回的value只可能是0或1
    Status next(char* value);
    Status seek(PositionProvider* position);
    Status skip(uint64_t num_values);

private:
    Status _read_byte();

    ReadOnlyFileStream* _input;
    RunLengthByteReader* _byte_reader;
    char _current;
    uint32_t _bits_left;

    DISALLOW_COPY_AND_ASSIGN(BitFieldReader);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_BIT_FIELD_READER_H
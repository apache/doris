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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_BIT_FIELD_WRITER_H
#define DORIS_BE_SRC_OLAP_ROWSET_BIT_FIELD_WRITER_H

#include "olap/stream_index_writer.h"
#include "olap/olap_define.h"

namespace doris {

class OutStream;
class RunLengthByteWriter;

class BitFieldWriter {
public:
    explicit BitFieldWriter(OutStream* output);
    ~BitFieldWriter();
    OLAPStatus init();
    // 写入一个bit, bit_value为true表示写入1, false表示写入0
    OLAPStatus write(bool bit_value);
    OLAPStatus flush();
    void get_position(PositionEntryWriter* index_entry) const;
private:
    OLAPStatus _write_byte();

    OutStream* _output;
    RunLengthByteWriter* _byte_writer;
    char _current;
    uint8_t _bits_left;

    DISALLOW_COPY_AND_ASSIGN(BitFieldWriter);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_BIT_FIELD_WRITER_H
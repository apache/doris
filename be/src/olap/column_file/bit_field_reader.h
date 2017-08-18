// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_BIT_FIELD_READER_H
#define BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_BIT_FIELD_READER_H

#include "olap/column_file/stream_index_reader.h"
#include "olap/olap_define.h"

namespace palo {
namespace column_file {

class ReadOnlyFileStream;
class RunLengthByteReader;

class BitFieldReader {
public:
    BitFieldReader(ReadOnlyFileStream* input);
    ~BitFieldReader();
    OLAPStatus init();
    // 获取下一条数据, 如果没有更多的数据了, 返回OLAP_ERR_DATA_EOF
    // 返回的value只可能是0或1
    OLAPStatus next(char* value);
    OLAPStatus seek(PositionProvider* position);
    OLAPStatus skip(uint64_t num_values);

private:
    OLAPStatus _read_byte();

    ReadOnlyFileStream* _input;
    RunLengthByteReader* _byte_reader;
    char _current;
    uint32_t _bits_left;

    DISALLOW_COPY_AND_ASSIGN(BitFieldReader);
};

}  // namespace column_file
}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_BIT_FIELD_READER_H

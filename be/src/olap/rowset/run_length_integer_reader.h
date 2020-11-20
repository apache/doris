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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_RUN_LENGTH_INTEGER_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_RUN_LENGTH_INTEGER_READER_H

#include "olap/file_stream.h"
#include "olap/olap_define.h"
#include "olap/rowset/run_length_integer_writer.h"
#include "olap/stream_index_reader.h"
#include "util/runtime_profile.h"

namespace doris {

class ReadOnlyFileStream;
class PositionProvider;

class RunLengthIntegerReader {
public:
    explicit RunLengthIntegerReader(ReadOnlyFileStream* input, bool is_singed);
    ~RunLengthIntegerReader() {}
    inline bool has_next() const { return _used != _num_literals || !_input->eof(); }
    // 获取下一条数据, 如果没有更多的数据了, 返回OLAP_ERR_DATA_EOF
    inline OLAPStatus next(int64_t* value) {
        OLAPStatus res = OLAP_SUCCESS;

        if (OLAP_UNLIKELY(_used == _num_literals)) {
            _num_literals = 0;
            _used = 0;

            res = _read_values();
            if (OLAP_SUCCESS != res) {
                return res;
            }
        }

        *value = _literals[_used++];
        return res;
    }
    OLAPStatus seek(PositionProvider* position);
    OLAPStatus skip(uint64_t num_values);

private:
    OLAPStatus _read_values();
    OLAPStatus _read_delta_values(uint8_t first_byte);
    OLAPStatus _read_patched_base_values(uint8_t first_byte);
    OLAPStatus _read_direct_values(uint8_t first_byte);
    OLAPStatus _read_short_repeat_values(uint8_t first_byte);

    ReadOnlyFileStream* _input;
    bool _signed;
    int64_t _literals[RunLengthIntegerWriter::MAX_SCOPE];
    int32_t _num_literals;
    int32_t _used;

    DISALLOW_COPY_AND_ASSIGN(RunLengthIntegerReader);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_RUN_LENGTH_INTEGER_READER_H
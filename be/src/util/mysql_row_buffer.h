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

#ifndef DORIS_BE_SRC_QUERY_MYSQL_MYSQL_ROW_BUFFER_H
#define DORIS_BE_SRC_QUERY_MYSQL_MYSQL_ROW_BUFFER_H

#include <stdint.h>

namespace doris {

// helper for construct MySQL send row
// Now only support text protocol
class MysqlRowBuffer {
public:
    MysqlRowBuffer();
    ~MysqlRowBuffer();

    void reset() { _pos = _buf; }

    // TODO(zhaochun): add signed/unsigned support
    int push_tinyint(int8_t data);
    int push_smallint(int16_t data);
    int push_int(int32_t data);
    int push_bigint(int64_t data);
    int push_unsigned_bigint(uint64_t data);
    int push_float(float data);
    int push_double(double data);
    int push_string(const char* str, int length);
    int push_null();

    // this function reserved size, change the pos step size, return old pos
    // Becareful when use the returned pointer.
    char* reserved(int size);

    const char* buf() const { return _buf; }
    const char* pos() const { return _pos; }
    int length() const { return _pos - _buf; }

private:
    int reserve(int size);

    char* _pos;
    char* _buf;
    int _buf_size;
    char _default_buf[4096];
};

} // namespace doris

#endif // DORIS_BE_SRC_QUERY_MYSQL_MYSQL_ROW_BUFFER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 noet: */

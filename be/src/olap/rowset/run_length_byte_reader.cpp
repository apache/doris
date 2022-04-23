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

#include "olap/rowset/run_length_byte_reader.h"

#include "olap/in_stream.h"
#include "olap/rowset/column_reader.h"

namespace doris {

RunLengthByteReader::RunLengthByteReader(ReadOnlyFileStream* input)
        : _input(input), _num_literals(0), _used(0), _repeat(false) {}

Status RunLengthByteReader::_read_values() {
    Status res = Status::OK();
    _used = 0;
    char control_byte = 0;

    res = _input->read(&control_byte);
    if (!res.ok()) {
        LOG(WARNING) << "fail to read control byte.res = " << res;
        return res;
    }

    if (control_byte > -1) {
        _repeat = true;
        _num_literals = control_byte + RunLengthByteWriter::MIN_REPEAT_SIZE;

        res = _input->read(&_literals[0]);
        if (!res.ok()) {
            LOG(WARNING) << "fail to read value byte.res = " << res;
            return res;
        }
    } else {
        _repeat = false;
        _num_literals = -control_byte;
        uint64_t bytes = 0;

        while (bytes < static_cast<uint64_t>(_num_literals)) {
            uint64_t to_read = _num_literals - bytes;

            res = _input->read(&_literals[bytes], &to_read);
            if (!res.ok()) {
                LOG(WARNING) << "fail to read value byte.res = " << res;
                return res;
            }

            bytes += to_read;
        }
    }

    return res;
}

bool RunLengthByteReader::has_next() const {
    return _used != _num_literals || !_input->eof();
}

Status RunLengthByteReader::next(char* value) {
    Status res = Status::OK();

    if (_used == _num_literals) {
        res = _read_values();
        if (!res.ok()) {
            LOG(WARNING) << "fail to read values.res = " << res;
            return res;
        }
    }

    if (_repeat) {
        _used += 1;
        *value = _literals[0];
    } else {
        *value = _literals[_used++];
    }

    return res;
}

Status RunLengthByteReader::seek(PositionProvider* position) {
    Status res = Status::OK();

    res = _input->seek(position);
    if (!res.ok()) {
        VLOG_TRACE << "fail to ReadOnlyFileStream seek. res = " << res;
        return res;
    }

    int64_t consumed = position->get_next();

    if (consumed != 0) {
        // a loop is required for cases where we break the run into two parts
        while (consumed > 0) {
            res = _read_values();
            if (!res.ok()) {
                LOG(WARNING) << "fail to read values.res = " << res;
                break;
            }

            _used = consumed;
            consumed -= _num_literals;
        }
    } else {
        _used = 0;
        _num_literals = 0;
    }

    return res;
}

Status RunLengthByteReader::skip(uint64_t num_values) {
    Status res = Status::OK();

    while (num_values > 0) {
        if (_used == _num_literals) {
            res = _read_values();
            if (!res.ok()) {
                LOG(WARNING) << "fail to read values.res = " << res;
                break;
            }
        }

        uint64_t consume = std::min(num_values, static_cast<uint64_t>(_num_literals - _used));
        _used += consume;
        num_values -= consume;
    }

    return res;
}

} // namespace doris
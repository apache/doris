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

#include "olap/rowset/run_length_byte_writer.h"

#include "olap/out_stream.h"

namespace doris {

const int32_t RunLengthByteWriter::MIN_REPEAT_SIZE;
const int32_t RunLengthByteWriter::MAX_LITERAL_SIZE;
const int32_t RunLengthByteWriter::MAX_REPEAT_SIZE;

RunLengthByteWriter::RunLengthByteWriter(OutStream* output)
        : _output(output), _num_literals(0), _repeat(false), _tail_run_length(0) {}

Status RunLengthByteWriter::_write_values() {
    Status res = Status::OK();

    if (_num_literals != 0) {
        if (_repeat) {
            res = _output->write(_num_literals - MIN_REPEAT_SIZE);
            if (!res.ok()) {
                OLAP_LOG_WARNING("fail to write control byte.");
                return res;
            }

            res = _output->write(_literals[0]);
            if (!res.ok()) {
                OLAP_LOG_WARNING("fail to write repeat byte");
                return res;
            }
        } else {
            res = _output->write(-_num_literals);
            if (!res.ok()) {
                OLAP_LOG_WARNING("fail to write control byte.");
                return res;
            }

            res = _output->write(_literals, _num_literals);
            if (!res.ok()) {
                OLAP_LOG_WARNING("fail to write literals bytes.");
                return res;
            }
        }

        _repeat = false;
        _tail_run_length = 0;
        _num_literals = 0;
    }

    return res;
}

Status RunLengthByteWriter::write(char value) {
    Status res = Status::OK();

    if (0 == _num_literals) {
        _literals[0] = value;
        _num_literals = 1;
        _tail_run_length = 1;
    } else if (_repeat) {
        if (value == _literals[0]) {
            _num_literals++;

            if (_num_literals == MAX_REPEAT_SIZE) {
                res = _write_values();
            }
        } else {
            res = _write_values();

            if (res.ok()) {
                _literals[0] = value;
                _num_literals = 1;
                _tail_run_length = 1;
            }
        }
    } else {
        if (value == _literals[_num_literals - 1]) {
            _tail_run_length++;
        } else {
            _tail_run_length = 1;
        }

        if (_tail_run_length == MIN_REPEAT_SIZE) {
            if (_num_literals + 1 == MIN_REPEAT_SIZE) {
                _repeat = true;
                _num_literals++;
            } else {
                _num_literals -= MIN_REPEAT_SIZE - 1;
                res = _write_values();

                if (res.ok()) {
                    _literals[0] = value;
                    _repeat = true;
                    _num_literals = MIN_REPEAT_SIZE;
                }
            }
        } else {
            _literals[_num_literals] = value;
            _num_literals++;

            if (_num_literals == MAX_LITERAL_SIZE) {
                res = _write_values();
            }
        }
    }

    return res;
}

Status RunLengthByteWriter::flush() {
    Status res;

    res = _write_values();
    if (!res.ok()) {
        return res;
    }

    return _output->flush();
}

void RunLengthByteWriter::get_position(PositionEntryWriter* index_entry) const {
    _output->get_position(index_entry);
    index_entry->add_position(_num_literals);
}

} // namespace doris
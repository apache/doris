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

#include "olap/rowset/bit_field_writer.h"

#include <gen_cpp/column_data_file.pb.h>

#include "olap/rowset/run_length_byte_writer.h"

namespace doris {

BitFieldWriter::BitFieldWriter(OutStream* output)
        : _output(output), _byte_writer(nullptr), _current(0), _bits_left(8) {}

BitFieldWriter::~BitFieldWriter() {
    SAFE_DELETE(_byte_writer);
}

OLAPStatus BitFieldWriter::init() {
    _byte_writer = new (std::nothrow) RunLengthByteWriter(_output);

    if (nullptr == _byte_writer) {
        OLAP_LOG_WARNING("fail to create RunLengthByteWriter");
        return OLAP_ERR_MALLOC_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus BitFieldWriter::_write_byte() {
    OLAPStatus res = OLAP_SUCCESS;

    if (OLAP_SUCCESS != (res = _byte_writer->write(_current))) {
        OLAP_LOG_WARNING("fail to write byte to byte writer");
        return res;
    }

    _current = 0;
    _bits_left = 8;
    return res;
}

OLAPStatus BitFieldWriter::write(bool bit_value) {
    OLAPStatus res = OLAP_SUCCESS;

    _bits_left--;

    if (bit_value) {
        _current |= 1 << _bits_left;
    }

    if (_bits_left == 0) {
        res = _write_byte();
    }

    return res;
}

OLAPStatus BitFieldWriter::flush() {
    OLAPStatus res = OLAP_SUCCESS;

    if (_bits_left != 8) {
        if (OLAP_SUCCESS != (res = _write_byte())) {
            return res;
        }
    }

    return _byte_writer->flush();
}

void BitFieldWriter::get_position(PositionEntryWriter* index_entry) const {
    if (nullptr != _byte_writer) {
        _byte_writer->get_position(index_entry);
    } else {
        // for stream
        index_entry->add_position(0);
        index_entry->add_position(0);
        // for rle byte writer
        index_entry->add_position(0);
    }

    index_entry->add_position(8 - _bits_left);
}

} // namespace doris
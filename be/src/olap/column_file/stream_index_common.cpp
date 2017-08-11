// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "olap/column_file/stream_index_common.h"

#include "olap/column_file/stream_index_common.h"
#include "olap/field.h"

namespace palo {
namespace column_file {

ColumnStatistics::ColumnStatistics() : 
        _minimum(NULL),
        _maximum(NULL),
         _ignored(true),
        _null_supported(false) {
}

ColumnStatistics::~ColumnStatistics() {
    SAFE_DELETE(_minimum);
    SAFE_DELETE(_maximum);
}

OLAPStatus ColumnStatistics::init(const FieldType& type, bool null_supported) {
    SAFE_DELETE(_minimum);
    SAFE_DELETE(_maximum);
    // 当数据类型为 String和varchar或是未知类型时，实际上不会有统计信息。
    _minimum = Field::create_by_type(type);
    _maximum = Field::create_by_type(type);

    _null_supported = null_supported;
    if (NULL == _minimum || NULL == _maximum) {
        _ignored = true;
    } else {
        _ignored = false;
        memset(_buf, 0, MAX_STATISTIC_LENGTH);
        _minimum->attach_field(_buf);
        _maximum->attach_field(_buf + _minimum->field_size());
        reset();
    }

    return OLAP_SUCCESS;
}

void ColumnStatistics::reset() {
    if (!_ignored) {
        _minimum->set_to_max();
        _maximum->set_to_min();
        if (true == _null_supported) {
            _maximum->set_null();
        }
    }
}

void ColumnStatistics::add(const Field* field) {
    if (_ignored) {
        return;
    }

    if (field->cmp(_maximum) > 0) {
        _maximum->copy(field);
    }

    if (field->cmp(_minimum) < 0) {
        _minimum->copy(field);
    }
}

void ColumnStatistics::merge(ColumnStatistics* other) {
    if (_ignored || other->ignored()) {
        return;
    }

    if (other->maximum()->cmp(_maximum) > 0) {
        _maximum->copy(other->maximum());
    }

    if (_minimum->cmp(other->minimum()) > 0) {
        _minimum->copy(other->minimum());
    }
}

size_t ColumnStatistics::size() const {
    if (_ignored) {
        return 0;
    }

    if (false == _null_supported) {
        return _minimum->size() + _maximum->size();
    } else {
        return _minimum->field_size() + _maximum->field_size();
    }
}

void ColumnStatistics::attach(char* buffer) {
    if (_ignored) {
        return;
    }

    if (false == _null_supported) {
        _minimum->attach_buf(buffer);
        _maximum->attach_buf(buffer + _minimum->size());
    } else {
        _minimum->attach_field(buffer);
        _maximum->attach_field(buffer + _minimum->field_size());
    }
}

OLAPStatus ColumnStatistics::write_to_buffer(char* buffer, size_t size) {
    if (_ignored) {
        return OLAP_SUCCESS;
    }

    if (size < this->size()) {
        return OLAP_ERR_BUFFER_OVERFLOW;
    }

    memcpy(buffer, _buf, this->size());
    return OLAP_SUCCESS;
}

}  // namespace column_file
}  // namespace palo

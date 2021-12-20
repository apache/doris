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

#include "olap/stream_index_common.h"

#include "olap/field.h"
#include "olap/wrapper_field.h"

namespace doris {

ColumnStatistics::ColumnStatistics()
        : _minimum(nullptr), _maximum(nullptr), _ignored(true), _null_supported(false) {}

ColumnStatistics::~ColumnStatistics() {
    SAFE_DELETE(_minimum);
    SAFE_DELETE(_maximum);
}

OLAPStatus ColumnStatistics::init(const FieldType& type, bool null_supported) {
    SAFE_DELETE(_minimum);
    SAFE_DELETE(_maximum);

    _null_supported = null_supported;
    if (type == OLAP_FIELD_TYPE_CHAR || type == OLAP_FIELD_TYPE_VARCHAR ||
        type == OLAP_FIELD_TYPE_HLL || type == OLAP_FIELD_TYPE_OBJECT ||
        type == OLAP_FIELD_TYPE_STRING) {
        _ignored = true;
    } else {
        // 当数据类型为 String和varchar或是未知类型时，实际上不会有统计信息。
        _minimum = WrapperField::create_by_type(type);
        _maximum = WrapperField::create_by_type(type);
        _ignored = false;
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

    // TODO(zc): too ugly
    if (_null_supported) {
        size_t copy_size = _minimum->field_size();
        memcpy(buffer, _minimum->nullable_cell_ptr(), copy_size);
        memcpy(buffer + copy_size, _maximum->nullable_cell_ptr(), copy_size);
    } else {
        size_t copy_size = _minimum->size();
        memcpy(buffer, _minimum->ptr(), copy_size);
        memcpy(buffer + copy_size, _maximum->ptr(), copy_size);
    }

    return OLAP_SUCCESS;
}

} // namespace doris

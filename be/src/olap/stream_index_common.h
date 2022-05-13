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

#pragma once

#include <functional>

#include "olap/field.h"
#include "olap/olap_define.h"
#include "olap/wrapper_field.h"

namespace doris {

// Describe the format of streamindex
struct StreamIndexHeader {
    uint64_t block_count;      // The number of blocks in this index
    uint32_t position_format;  // The number of positions, each length is sizeof(uint32_t)
    uint32_t statistic_format; // The statistical information format is actually OLAP_FIELD_TYPE_XXX
    // When it is OLAP_FIELD_TYPE_NONE, it means no index
    StreamIndexHeader()
            : block_count(0), position_format(0), statistic_format(OLAP_FIELD_TYPE_NONE) {}
} __attribute__((packed));

// TODO: string type(char, varchar) has no columnar statistics at present.
// when you want to add columnar statistics for string type,
// don't forget to convert storage layout between disk and memory.
// Processing column statistics, read and write in one, can also be separated.
class ColumnStatistics {
public:
    ColumnStatistics();
    ~ColumnStatistics();

    // Initialization, FieldType needs to be used to initialize the maximum and minimum values
    // It must be initialized before use, otherwise it will be invalid
    Status init(const FieldType& type, bool null_supported);
    // Just reset the maximum and minimum values, set the minimum value to MAX, and the maximum value to MIN.
    void reset();

    template <typename CellType>
    void add(const CellType& cell) {
        if (_ignored) {
            return;
        }
        if (_maximum->field()->compare_cell(*_maximum, cell) < 0) {
            _maximum->field()->direct_copy(_maximum, cell);
        }
        if (_minimum->field()->compare_cell(*_minimum, cell) > 0) {
            _minimum->field()->direct_copy(_minimum, cell);
        }
    }

    // Combine, merge another statistic information into the current statistic
    void merge(ColumnStatistics* other);
    // It returns the memory occupied by the maximum and minimum values "when outputting", and "isn't it?"
    // ?? The size of the memory occupied by the current structure
    size_t size() const;
    // Attach the maximum and minimum values to the given buffer
    void attach(char* buffer);
    // Output the maximum and minimum values to the buffer
    Status write_to_buffer(char* buffer, size_t size);

    // Attributes
    const WrapperField* minimum() const { return _minimum; }
    const WrapperField* maximum() const { return _maximum; }
    std::pair<WrapperField*, WrapperField*> pair() const {
        return std::make_pair(_minimum, _maximum);
    }
    bool ignored() const { return _ignored; }

protected:
    WrapperField* _minimum;
    WrapperField* _maximum;
    // As the statistical information of string is not supported for the time being,
    // the length is directly defined for convenience
    // Can also be assigned every time
    bool _ignored;
    bool _null_supported;
};

} // namespace doris

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

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>

#include "runtime/string_value.h"
#include "vec/columns/column.h"
#include "vec/exprs/table_function/table_function.h"

namespace doris::vectorized {

enum ExplodeJsonArrayType { INT = 0, DOUBLE, STRING };

struct ParsedData {
    static std::string true_value;
    static std::string false_value;

    // The number parsed from json array
    // the `_backup` saved the real number entity.
    std::vector<void*> _data;
    std::vector<StringValue> _data_string;
    std::vector<int64_t> _backup_int;
    std::vector<double> _backup_double;
    std::vector<std::string> _backup_string;
    std::vector<bool> _string_nulls;
    char tmp_buf[128] = {0};

    void reset(ExplodeJsonArrayType type) {
        switch (type) {
        case ExplodeJsonArrayType::INT:
            _data.clear();
            _backup_int.clear();
            break;
        case ExplodeJsonArrayType::DOUBLE:
            _data.clear();
            _backup_double.clear();
            break;
        case ExplodeJsonArrayType::STRING:
            _data_string.clear();
            _backup_string.clear();
            _string_nulls.clear();
            break;
        default:
            CHECK(false) << type;
            break;
        }
    }

    void get_value(ExplodeJsonArrayType type, int64_t offset, void** output, bool real = false) {
        switch (type) {
        case ExplodeJsonArrayType::INT:
        case ExplodeJsonArrayType::DOUBLE:
            *output = _data[offset];
            break;
        case ExplodeJsonArrayType::STRING:
            *output = _string_nulls[offset] ? nullptr
                      : real                ? reinterpret_cast<void*>(_backup_string[offset].data())
                                            : &_data_string[offset];
            break;
        default:
            CHECK(false) << type;
        }
    }

    void get_value_length(ExplodeJsonArrayType type, int64_t offset, int64_t* length) {
        switch (type) {
        case ExplodeJsonArrayType::INT:
        case ExplodeJsonArrayType::DOUBLE:
            break;
        case ExplodeJsonArrayType::STRING:
            *length = _string_nulls[offset] ? -1 : _backup_string[offset].size();
            break;
        default:
            CHECK(false) << type;
        }
    }

    int set_output(ExplodeJsonArrayType type, rapidjson::Document& document);
};

class VExplodeJsonArrayTableFunction final : public TableFunction {
public:
    VExplodeJsonArrayTableFunction(ExplodeJsonArrayType type);
    ~VExplodeJsonArrayTableFunction() override = default;

    Status process_init(vectorized::Block* block) override;
    Status process_row(size_t row_idx) override;
    Status process_close() override;
    Status get_value(void** output) override;
    Status get_value_length(int64_t* length) override;

    Status reset() override;

private:
    ParsedData _parsed_data;
    ExplodeJsonArrayType _type;

    ColumnPtr _text_column;
};

} // namespace doris::vectorized
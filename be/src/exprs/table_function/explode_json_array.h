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

#include "exprs/table_function/table_function.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include "gutil/strings/stringpiece.h"
#include "runtime/string_value.h"

namespace doris {

enum ExplodeJsonArrayType {
    INT = 0,
    DOUBLE,
    STRING
};

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

    void set_null_output(ExplodeJsonArrayType type) {
        switch (type) {
            case ExplodeJsonArrayType::INT:
            case ExplodeJsonArrayType::DOUBLE:
                _data.resize(1);
                _data[0] = nullptr;
                break;
            case ExplodeJsonArrayType::STRING:
                _string_nulls.resize(1);
                _string_nulls[0] = true;
                break;
            default:
                CHECK(false) << type;
                break;
        }
    }

    void get_value(ExplodeJsonArrayType type, int64_t offset, void** output) {
        switch(type) {
            case ExplodeJsonArrayType::INT:
            case ExplodeJsonArrayType::DOUBLE:
                *output = _data[offset];
                break;
            case ExplodeJsonArrayType::STRING:
                *output = _string_nulls[offset] ? nullptr : &_data_string[offset];
                break;
            default:
                CHECK(false) << type;
        }
    }

    int set_output(ExplodeJsonArrayType type, rapidjson::Document& document);
};

// Input:  json array: [1,2,3,4,5];
// Output: rows: 1,2,3,4,5
// If json array contains non-numeric type, or is not a json array, will return null
class ExplodeJsonArrayTableFunction : public TableFunction {
public:
    ExplodeJsonArrayTableFunction(ExplodeJsonArrayType type);
    virtual ~ExplodeJsonArrayTableFunction();

    virtual Status process(TupleRow* tuple_row) override;
    virtual Status reset() override;
    virtual Status get_value(void** output) override;

private:
    void _set_null_output();

private:
    ParsedData _parsed_data;

    ExplodeJsonArrayType _type;
};

} // namespace doris

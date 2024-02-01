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

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <stddef.h>
#include <stdint.h>

#include <ostream>
#include <string>
#include <vector>

#include "common/status.h"
#include "gutil/integral_types.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/table_function/table_function.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

enum ExplodeJsonArrayType { INT = 0, DOUBLE, STRING, JSON };

struct ParsedData {
    static std::string true_value;
    static std::string false_value;

    // The number parsed from json array
    // the `_backup` saved the real number entity.
    std::vector<void*> _data;
    std::vector<StringRef> _data_string;
    std::vector<int64_t> _backup_int;
    std::vector<double> _backup_double;
    std::vector<std::string> _backup_string;
    std::vector<bool> _string_nulls;
    char tmp_buf[128] = {0};

    void* get_value(ExplodeJsonArrayType type, int64_t offset, bool real = false) {
        switch (type) {
        case ExplodeJsonArrayType::INT:
        case ExplodeJsonArrayType::DOUBLE:
            return _data[offset];
        case ExplodeJsonArrayType::JSON:
        case ExplodeJsonArrayType::STRING:
            return _string_nulls[offset] ? nullptr
                   : real                ? reinterpret_cast<void*>(_backup_string[offset].data())
                                         : &_data_string[offset];
        default:
            return nullptr;
        }
    }

    int64 get_value_length(ExplodeJsonArrayType type, int64_t offset) {
        if ((type == ExplodeJsonArrayType::STRING || type == ExplodeJsonArrayType::JSON) &&
            !_string_nulls[offset]) {
            return _backup_string[offset].size();
        }
        return 0;
    }

    int set_output(ExplodeJsonArrayType type, rapidjson::Document& document);
};

class VExplodeJsonArrayTableFunction final : public TableFunction {
    ENABLE_FACTORY_CREATOR(VExplodeJsonArrayTableFunction);

public:
    VExplodeJsonArrayTableFunction(ExplodeJsonArrayType type);
    ~VExplodeJsonArrayTableFunction() override = default;

    Status process_init(Block* block, RuntimeState* state) override;
    void process_row(size_t row_idx) override;
    void process_close() override;
    void get_value(MutableColumnPtr& column) override;

private:
    ParsedData _parsed_data;
    ExplodeJsonArrayType _type;

    ColumnPtr _text_column;
};

} // namespace doris::vectorized
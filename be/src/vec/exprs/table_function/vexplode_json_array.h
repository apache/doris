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
#include "vec/core/types.h"
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

    std::vector<int64_t> _backup_int;
    std::vector<double> _backup_double;
    std::vector<StringRef> _backup_string;
    std::vector<UInt8> _values_null_flag;
    ExplodeJsonArrayType _data_type;
    char tmp_buf[128] = {0};

    Status set_type(ExplodeJsonArrayType type);
    int set_output(rapidjson::Document& document);
    Status insert_result_from_parsed_data(MutableColumnPtr& column, int max_step,
                                          int64_t cur_offset);
    const char* get_null_flag_address(int cur_offset) {
        return reinterpret_cast<const char*>(_values_null_flag.data() + cur_offset);
    }
    void reset() {
        _backup_int.clear();
        _backup_double.clear();
        _backup_string.clear();
        _values_null_flag.clear();
    }
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
    int get_value(MutableColumnPtr& column, int max_step) override;

private:
    ParsedData _parsed_data;
    ExplodeJsonArrayType _type;

    ColumnPtr _text_column;
};

} // namespace doris::vectorized
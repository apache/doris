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

#include <cctype>

#include "common/status.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class FunctionSoundex : public IFunction {
public:
    static constexpr auto name = "soundex";

    static FunctionPtr create() { return std::make_shared<FunctionSoundex>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr col_ptr = block.get_by_position(arguments[0]).column;

        auto res_column = ColumnString::create();
        res_column->reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            StringRef ref = col_ptr->get_data_at(i);
            RETURN_IF_ERROR(calculate_soundex_and_insert(ref, res_column.get(), i));
        }

        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

private:
    Status calculate_soundex_and_insert(const StringRef& ref, ColumnString* res_column,
                                        const size_t row) const {
        if (ref.size == 0) {
            res_column->insert_data("", 0);
            return Status::OK();
        }

        std::string result;
        result.reserve(4);
        char pre_code = '\0';
        for (size_t i = 0; i < ref.size; ++i) {
            auto c = static_cast<unsigned char>(ref.data[i]);

            if (c > 0x7f) {
                return Status::InvalidArgument("soundex only supports ASCII, but got: {}",
                                               ref.data[i]);
            }
            if (!std::isalpha(c)) {
                continue;
            }

            c = static_cast<char>(std::toupper(c));
            if (result.empty()) {
                result += c;
                pre_code = (SOUNDEX_TABLE[c - 'A'] == 'N') ? '\0' : SOUNDEX_TABLE[c - 'A'];
            } else if (char code = SOUNDEX_TABLE[c - 'A']; code != 'N') {
                if (code != 'V' && code != pre_code) {
                    result += code;
                    if (result.size() == 4) {
                        res_column->insert_data(result.c_str(), result.length());
                        return Status::OK();
                    }
                }

                pre_code = code;
            }
        }

        while (!result.empty() && result.size() < 4) {
            result += '0';
        }

        res_column->insert_data(result.c_str(), result.length());
        return Status::OK();
    }

    /** 1. If a vowel (A, E, I, O, U) separates two consonants that have the same soundex code
     *  the consonant to the right of the vowel is coded. Here we use 'V' to represent vowels.
     *  eg : **Tymczak** is coded as T-522 (T, 5 for the M, 2 for the C, Z ignored , 2 for the K). 
     *  Since the vowel "A" separates the Z and K, the K is coded.
     *
     *  2. If "H" or "W" separate two consonants that have the same soundex code, the consonant to the right of the vowel is NOT coded.
     *  Here we use 'N' to represent these two characters.
     *  eg : **Ashcraft** is coded A-261 (A, 2 for the S, C ignored, 6 for the R, 1 for the F). It is not coded A-226.
     */
    static constexpr char SOUNDEX_TABLE[26] = {'V', '1', '2', '3', 'V', '1', '2', 'N', 'V',
                                               '2', '2', '4', '5', '5', 'V', '1', '2', '6',
                                               '2', '3', 'V', '1', 'N', '2', 'V', '2'};
};

void register_function_soundex(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSoundex>();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
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

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "cast_to_decimal.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/string_parser.hpp"
#include "vec/columns/column_vector.h"
#include "vec/columns/common_column_test.h"
#include "vec/core/field.h"

namespace doris::vectorized {
struct FunctionCastToDecimalPerfTest : public FunctionCastTest {
    template <PrimitiveType FromPT, PrimitiveType ToPT>
    void perf_test_func(const std::string& test_data_file, size_t orig_line_count,
                        int from_precision, int from_scale, int to_precision, int to_scale,
                        bool nullable) {
        DataTypePtr dt_from = DataTypeFactory::instance().create_data_type(
                FromPT, false, from_precision, from_scale);
        auto column_orig = dt_from->create_column();
        {
            MutableColumns columns;
            columns.push_back(column_orig->get_ptr());
            DataTypeSerDeSPtrs serde = {dt_from->get_serde()};
            load_columns_data_from_file(columns, serde, ';', {0}, test_data_file);
            EXPECT_EQ(column_orig->size(), orig_line_count);
        }
        auto column_from = dt_from->create_column();
        for (int i = 0; i < 10; ++i) {
            column_from->insert_range_from(*column_orig, 0, column_orig->size());
        }
        EXPECT_EQ(column_from->size(), orig_line_count * 10);
        std::cout << "column_from size: " << column_from->size() << std::endl;

        DataTypePtr dt_to =
                DataTypeFactory::instance().create_data_type(ToPT, true, to_precision, to_scale);

        bool is_strict_mode = false;
        auto ctx = create_context(is_strict_mode);
        auto fn = get_cast_wrapper(ctx.get(), dt_from, dt_to);
        ASSERT_TRUE(fn != nullptr);
        Block block = {
                {std::move(column_from), dt_from, "from"},
                {nullptr, dt_to, "to"},
        };

        int64_t duration_ns = 0;
        {
            SCOPED_RAW_TIMER(&duration_ns);
            EXPECT_TRUE(fn(ctx.get(), block, {0}, 1, block.rows(), nullptr));
        }
        auto result = block.get_by_position(1).column;
        EXPECT_EQ(result->size(), orig_line_count * 10);

        std::cout << block.dump_data(0, 10) << "\n";
        std::cout << fmt::format("cast {} to {} time {}\n", type_to_string(FromPT),
                                 type_to_string(ToPT),
                                 PrettyPrinter::print(duration_ns, TUnit::TIME_NS));
    }
};

// Optimized version with reserve for better performance
void read_file_to_vector_optimized(const std::string& filename, std::vector<std::string>& lines) {
    std::ifstream file(filename);

    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << filename << std::endl;
        return;
    }

    // Get file size to estimate number of lines for better memory allocation
    file.seekg(0, std::ios::end);
    std::streamsize file_size = file.tellg();
    file.seekg(0, std::ios::beg);

    // Rough estimate: average line length of 50 characters
    lines.reserve(file_size / 50);

    std::string line;
    while (std::getline(file, line)) {
        lines.push_back(std::move(line)); // Move instead of copy
    }
}

/*
TEST_F(FunctionCastToDecimalPerfTest, test_to_decimal128v3_from_string_perf) {
    std::vector<std::string> file_contents;
    read_file_to_vector_optimized("/mnt/disk2/tengjianping/cast_perf/decimal_test_data_38_19.txt",
                                  file_contents);

    size_t line_count = file_contents.size();
    std::cout << "Read " << line_count << " lines" << std::endl;

    // Show first few lines
    for (size_t i = 0; i < std::min(size_t(5), file_contents.size()); ++i) {
        std::cout << "Line " << i + 1 << ": " << file_contents[i] << std::endl;
    }
    int64_t duration_ns = 0;
    StringParser::ParseResult result;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        for (size_t i = 0; i != line_count; ++i) {
            auto decimal_value = StringParser::string_to_decimal<PrimitiveType::TYPE_DECIMAL128I>(
                    file_contents[i].c_str(), file_contents[i].size(), 38, 19, &result);
            (void)decimal_value; // Use the value to avoid unused variable warning
        }
    }

    std::cout << "parse time: " << PrettyPrinter::print(duration_ns, TUnit::TIME_NS) << "\n";
}

TEST_F(FunctionCastToDecimalPerfTest, test_to_decimal128v3_from_decimal128v3_perf) {
    std::string test_data_file = "/mnt/disk2/tengjianping/cast_perf/decimal_test_data_38_19.txt";
    size_t orig_line_count = 10000000; // 10 million lines
    perf_test_func<PrimitiveType::TYPE_DECIMAL128I, PrimitiveType::TYPE_DECIMAL128I>(
            test_data_file, orig_line_count, 38, 19, 38, 18, false);
}
TEST_F(FunctionCastToDecimalPerfTest, test_to_decimal128v3_from_decimalv2_perf) {
    std::string test_data_file = "/mnt/disk2/tengjianping/cast_perf/decimalv2_test_data_27_9.txt";
    size_t orig_line_count = 10000000; // 10 million lines
    perf_test_func<PrimitiveType::TYPE_DECIMAL, PrimitiveType::TYPE_DECIMAL128I>(
            test_data_file, orig_line_count, 27, 9, 38, 18, false);
}
TEST_F(FunctionCastToDecimalPerfTest, test_to_decimal128v3_from_int_perf) {
    std::string test_data_file = "/mnt/disk2/tengjianping/cast_perf/decimal_test_data_38_19.txt";
    size_t orig_line_count = 10000000; // 10 million lines
    perf_test_func<PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_DECIMAL128I>(
            test_data_file, orig_line_count, 0, 0, 38, 19, false);
}
TEST_F(FunctionCastToDecimalPerfTest, test_to_decimal128v3_from_float_perf) {
    std::string test_data_file = "/mnt/disk2/tengjianping/cast_perf/decimal_test_data_38_19.txt";
    size_t orig_line_count = 10000000; // 10 million lines
    perf_test_func<PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_DECIMAL128I>(
            test_data_file, orig_line_count, 0, 0, 38, 18, true);
}

TEST_F(FunctionCastToDecimalPerfTest, test_to_largeint_from_decimal128v3_perf) {
    std::string test_data_file = "/mnt/disk2/tengjianping/cast_perf/decimal_test_data_38_19.txt";
    size_t orig_line_count = 10000000; // 10 million lines
    perf_test_func<PrimitiveType::TYPE_DECIMAL128I, PrimitiveType::TYPE_LARGEINT>(
            test_data_file, orig_line_count, 38, 19, 0, 0, true);
}
*/
} // namespace doris::vectorized
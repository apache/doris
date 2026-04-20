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

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "testutil/any_type.h"

namespace doris {

// Fast path: const format string + String column(s)
// The Consted{} wrapper causes check_function to create a ColumnConst for that argument,
// which triggers the pre-parse memcpy fast path in FunctionFormat::execute_inner.

TEST(FunctionFormatTest, ConstPattern2ArgStringFastPath) {
    const std::string func_name = "format";
    // First arg (format) is const: DataSet with Consted can only have one row per call.
    InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR}, PrimitiveType::TYPE_VARCHAR};

    using Row = std::pair<std::vector<AnyType>, AnyType>;
    std::vector<Row> cases = {
            {{std::string("Hello {}!"), std::string("world")}, std::string("Hello world!")},
            {{std::string("Hello {}!"), std::string("Doris")}, std::string("Hello Doris!")},
            {{std::string("Hello {}!"), std::string("")}, std::string("Hello !")},
            {{std::string("Hello {}!"), std::string("123")}, std::string("Hello 123!")},
    };
    for (const auto& row : cases) {
        DataSet ds = {row};
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, ds));
    }
}

TEST(FunctionFormatTest, ConstPattern3ArgStringFastPath) {
    // Multi-arg fast path: const format + 3 string columns, one row per call.
    const std::string func_name = "format";
    InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR}, PrimitiveType::TYPE_VARCHAR,
                                PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

    using Row = std::pair<std::vector<AnyType>, AnyType>;
    std::vector<Row> cases = {
            {{std::string("{} + {} = {}"), std::string("1"), std::string("2"), std::string("3")},
             std::string("1 + 2 = 3")},
            {{std::string("{} + {} = {}"), std::string("a"), std::string("b"), std::string("c")},
             std::string("a + b = c")},
    };
    for (const auto& row : cases) {
        DataSet ds = {row};
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, ds));
    }
}

TEST(FunctionFormatTest, ExplicitIndexFastPath) {
    // Explicit {0} {1} indexing
    const std::string func_name = "format";
    InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR}, PrimitiveType::TYPE_VARCHAR,
                                PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {{{std::string("{0} and {1}"), std::string("foo"), std::string("bar")},
                         std::string("foo and bar")}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionFormatTest, EscapedBracesFastPath) {
    // {{ and }} in pattern → literal { and }
    const std::string func_name = "format";
    InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR}, PrimitiveType::TYPE_VARCHAR,
                                PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            {{std::string("{{{}}} = {}"), std::string("key"), std::string("val")},
             std::string("{key} = val")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionFormatTest, NoPlaceholderFastPath) {
    // Pattern has no placeholders — arg is ignored (fmt silently ignores extra args)
    const std::string func_name = "format";
    InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR}, PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            {{std::string("static_text"), std::string("ignored")}, std::string("static_text")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

// Slow path: format spec falls back to fmt::format

TEST(FunctionFormatTest, FormatSpecSlowPath) {
    // {:f} format spec triggers slow path (returns nullopt from fast-path parser)
    const std::string func_name = "format";
    InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR}, PrimitiveType::TYPE_DOUBLE};

    // Each row tested individually: with Consted, ColumnConst always returns row-0's value.
    {
        DataSet ds = {{{std::string("{:.2f}"), 3.14159}, std::string("3.14")}};
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, ds));
    }
    {
        DataSet ds = {{{std::string("{:.0f}"), 2.71828}, std::string("3")}};
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, ds));
    }
}

TEST(FunctionFormatTest, NumericArgSlowPath) {
    // Numeric arg — always uses slow path (non-String T, no fast path for numerics).
    const std::string func_name = "format";
    InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR}, PrimitiveType::TYPE_INT};

    using Row = std::pair<std::vector<AnyType>, AnyType>;
    std::vector<Row> cases = {
            {{std::string("value={}"), (int32_t)42}, std::string("value=42")},
            {{std::string("value={}"), (int32_t)-1}, std::string("value=-1")},
            {{std::string("value={}"), (int32_t)0}, std::string("value=0")},
    };
    for (const auto& row : cases) {
        DataSet ds = {row};
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, ds));
    }
}

} // namespace doris

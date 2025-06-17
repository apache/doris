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

#include "cast_test.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
using namespace ut_type;

TEST_F(FunctionCastTest, test_from_int_to_bool) {
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT};
        DataSet data_set = {
                {{Int32 {1}}, UInt8(1)},
                {{Int32 {0}}, UInt8(0)},
                {{Int32 {-1}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
        DataSet data_set = {
                {{Int64 {1}}, UInt8(1)},
                {{Int64 {0}}, UInt8(0)},
                {{Int64 {-1}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_LARGEINT};
        DataSet data_set = {
                {{Int128 {1}}, UInt8(1)},
                {{Int128 {0}}, UInt8(0)},
                {{Int128 {-1}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_int_to_bool_strict_mode) {
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_INT};
        DataSet data_set = {
                {{Int32 {1}}, UInt8(1)},
                {{Int32 {0}}, UInt8(0)},
                {{Int32 {-1}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
        DataSet data_set = {
                {{Int64 {1}}, UInt8(1)},
                {{Int64 {0}}, UInt8(0)},
                {{Int64 {-1}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_LARGEINT};
        DataSet data_set = {
                {{Int128 {1}}, UInt8(1)},
                {{Int128 {0}}, UInt8(0)},
                {{Int128 {-1}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_string_to_bool) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("null")}, Null()},
            {{std::string("true")}, uint8_t(1)},
            {{std::string("false")}, uint8_t(0)},
            {{std::string("1")}, uint8_t(1)},
            {{std::string("0")}, uint8_t(0)},
            {{std::string("abc")}, Null()},
            {{std::string("")}, Null()},
            {{std::string(" ")}, Null()},
            {{std::string(" 1")}, uint8_t(1)},
            {{std::string(" 1.1111")}, Null()},
            {{std::string(" 0")}, uint8_t(0)},
            {{std::string("  true")}, uint8_t(1)},
            {{std::string("  false")}, uint8_t(0)},
            {{std::string("  null")}, Null()},
            {{std::string("  abc")}, Null()},
            {{std::string("  ")}, Null()},
            {{std::string("  TrUE  ")}, uint8_t(1)},
            {{std::string("  fAlsE  ")}, uint8_t(0)},

            {{std::string("t")}, uint8_t(1)},
            {{std::string("T")}, uint8_t(1)},
            {{std::string("f")}, uint8_t(0)},
            {{std::string("F")}, uint8_t(0)},

            {{std::string("on")}, uint8_t(1)},
            {{std::string("ON")}, uint8_t(1)},
            {{std::string("On")}, uint8_t(1)},
            {{std::string("oN")}, uint8_t(1)},
            {{std::string("no")}, uint8_t(0)},
            {{std::string("NO")}, uint8_t(0)},
            {{std::string("No")}, uint8_t(0)},
            {{std::string("nO")}, uint8_t(0)},

            {{std::string("yes")}, uint8_t(1)},
            {{std::string("YES")}, uint8_t(1)},
            {{std::string("Yes")}, uint8_t(1)},
            {{std::string("yEs")}, uint8_t(1)},
            {{std::string("off")}, uint8_t(0)},
            {{std::string("OFF")}, uint8_t(0)},
            {{std::string("Off")}, uint8_t(0)},
            {{std::string("oFf")}, uint8_t(0)},

            {{std::string(" t ")}, uint8_t(1)},
            {{std::string(" on ")}, uint8_t(1)},
            {{std::string(" yes ")}, uint8_t(1)},
            {{std::string(" TRUE ")}, uint8_t(1)},

            {{std::string("x")}, Null()},
            {{std::string("tr")}, Null()},
            {{std::string("tru")}, Null()},
            {{std::string("fals")}, Null()},
            {{std::string("truth")}, Null()},
    };
    check_function_for_cast<DataTypeBool>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_from_string_to_bool_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    {
        DataSet data_set = {
                {{std::string("true")}, uint8_t(1)},     {{std::string("false")}, uint8_t(0)},
                {{std::string("1")}, uint8_t(1)},        {{std::string("0")}, uint8_t(0)},
                {{std::string(" 1")}, uint8_t(1)},       {{std::string(" 0")}, uint8_t(0)},
                {{std::string("  true")}, uint8_t(1)},   {{std::string("  false")}, uint8_t(0)},
                {{std::string("  TrUE  ")}, uint8_t(1)}, {{std::string("  fAlsE  ")}, uint8_t(0)},

                {{std::string("t")}, uint8_t(1)},        {{std::string("T")}, uint8_t(1)},
                {{std::string("f")}, uint8_t(0)},        {{std::string("F")}, uint8_t(0)},
                {{std::string(" t ")}, uint8_t(1)},      {{std::string(" f ")}, uint8_t(0)},

                {{std::string("on")}, uint8_t(1)},       {{std::string("ON")}, uint8_t(1)},
                {{std::string("On")}, uint8_t(1)},       {{std::string("oN")}, uint8_t(1)},
                {{std::string("no")}, uint8_t(0)},       {{std::string("NO")}, uint8_t(0)},
                {{std::string("No")}, uint8_t(0)},       {{std::string("nO")}, uint8_t(0)},
                {{std::string(" on ")}, uint8_t(1)},     {{std::string(" no ")}, uint8_t(0)},

                {{std::string("yes")}, uint8_t(1)},      {{std::string("YES")}, uint8_t(1)},
                {{std::string("Yes")}, uint8_t(1)},      {{std::string("yEs")}, uint8_t(1)},
                {{std::string("off")}, uint8_t(0)},      {{std::string("OFF")}, uint8_t(0)},
                {{std::string("Off")}, uint8_t(0)},      {{std::string("oFf")}, uint8_t(0)},
                {{std::string(" yes ")}, uint8_t(1)},    {{std::string(" off ")}, uint8_t(0)},

                {{std::string(" TRUE ")}, uint8_t(1)},   {{std::string(" FALSE ")}, uint8_t(0)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }

    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string(" 1.111")}, Null()}}, "parse number fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string("null")}, Null()}}, "parse number fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string("abc")}, Null()}}, "parse number fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string("")}, Null()}}, "parse number fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string(" ")}, Null()}}, "parse number fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string("x")}, Null()}}, "parse number fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string("tr")}, Null()}}, "parse number fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string("tru")}, Null()}}, "parse number fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string("fals")}, Null()}}, "parse number fail");
    }
    {
        check_function_for_cast_strict_mode<DataTypeBool>(
                input_types, {{{std::string("truth")}, Null()}}, "parse number fail");
    }
}

TEST_F(FunctionCastTest, test_from_bool_to_string) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
    DataSet data_set = {
            {{UInt8 {1}}, std::string("1")},
            {{UInt8 {0}}, std::string("0")},
    };
    check_function_for_cast<DataTypeString>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_from_bool_to_string_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
    DataSet data_set = {
            {{UInt8 {1}}, std::string("1")},
            {{UInt8 {0}}, std::string("0")},
    };
    check_function_for_cast_strict_mode<DataTypeString>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_from_bool_to_int) {
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
        DataSet data_set = {
                {{UInt8 {1}}, Int32(1)},
                {{UInt8 {0}}, Int32(0)},
        };
        check_function_for_cast<DataTypeInt32>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
        DataSet data_set = {
                {{UInt8 {1}}, Int64(1)},
                {{UInt8 {0}}, Int64(0)},
        };
        check_function_for_cast<DataTypeInt64>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_bool_to_int_strict_mode) {
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
        DataSet data_set = {
                {{UInt8 {1}}, Int32(1)},
                {{UInt8 {0}}, Int32(0)},
        };
        check_function_for_cast_strict_mode<DataTypeInt32>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};
        DataSet data_set = {
                {{UInt8 {1}}, Int64(1)},
                {{UInt8 {0}}, Int64(0)},
        };
        check_function_for_cast_strict_mode<DataTypeInt64>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_float_to_bool) {
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_FLOAT};
        DataSet data_set = {
                {{Float32 {1.0}}, UInt8(1)},  {{Float32 {0.0}}, UInt8(0)},
                {{Float32 {+0.0}}, UInt8(0)}, {{Float32 {-0.0}}, UInt8(0)},
                {{Float32 {-1.0}}, UInt8(1)}, {{Float32 {0.5}}, UInt8(1)},
                {{Float32 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
        DataSet data_set = {
                {{Float64 {1.0}}, UInt8(1)},  {{Float64 {0.0}}, UInt8(0)},
                {{Float64 {+0.0}}, UInt8(0)}, {{Float64 {-0.0}}, UInt8(0)},
                {{Float64 {-1.0}}, UInt8(1)}, {{Float64 {0.5}}, UInt8(1)},
                {{Float64 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_float_to_bool_strict_mode) {
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_FLOAT};
        DataSet data_set = {
                {{Float32 {1.0}}, UInt8(1)},  {{Float32 {0.0}}, UInt8(0)},
                {{Float32 {+0.0}}, UInt8(0)}, {{Float32 {-0.0}}, UInt8(0)},
                {{Float32 {-1.0}}, UInt8(1)}, {{Float32 {0.5}}, UInt8(1)},
                {{Float32 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
        DataSet data_set = {
                {{Float64 {1.0}}, UInt8(1)},  {{Float64 {0.0}}, UInt8(0)},
                {{Float64 {+0.0}}, UInt8(0)}, {{Float64 {-0.0}}, UInt8(0)},
                {{Float64 {-1.0}}, UInt8(1)}, {{Float64 {0.5}}, UInt8(1)},
                {{Float64 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_decimal_to_bool) {
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMAL32};
        DataSet data_set = {
                {{Decimal32 {1}}, UInt8(1)},    {{Decimal32 {0}}, UInt8(0)},
                {{Decimal32 {+0}}, UInt8(0)},   {{Decimal32 {-0}}, UInt8(0)},
                {{Decimal32 {-1}}, UInt8(1)},   {{Decimal32 {0.5}}, UInt8(1)},
                {{Decimal32 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMAL64};
        DataSet data_set = {
                {{Decimal64 {1}}, UInt8(1)},    {{Decimal64 {0}}, UInt8(0)},
                {{Decimal64 {+0}}, UInt8(0)},   {{Decimal64 {-0}}, UInt8(0)},
                {{Decimal64 {-1}}, UInt8(1)},   {{Decimal64 {0.5}}, UInt8(1)},
                {{Decimal64 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMALV2};
        DataSet data_set = {
                {{Decimal128V2 {1}}, UInt8(1)},    {{Decimal128V2 {0}}, UInt8(0)},
                {{Decimal128V2 {+0}}, UInt8(0)},   {{Decimal128V2 {-0}}, UInt8(0)},
                {{Decimal128V2 {-1}}, UInt8(1)},   {{Decimal128V2 {0.5}}, UInt8(1)},
                {{Decimal128V2 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMAL128I};
        DataSet data_set = {
                {{Decimal128V3 {1}}, UInt8(1)},    {{Decimal128V3 {0}}, UInt8(0)},
                {{Decimal128V3 {+0}}, UInt8(0)},   {{Decimal128V3 {-0}}, UInt8(0)},
                {{Decimal128V3 {-1}}, UInt8(1)},   {{Decimal128V3 {0.5}}, UInt8(1)},
                {{Decimal128V3 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMAL256};
        DataSet data_set = {
                {{Decimal256 {1}}, UInt8(1)},    {{Decimal256 {0}}, UInt8(0)},
                {{Decimal256 {+0}}, UInt8(0)},   {{Decimal256 {-0}}, UInt8(0)},
                {{Decimal256 {-1}}, UInt8(1)},   {{Decimal256 {0.5}}, UInt8(1)},
                {{Decimal256 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_decimal_to_bool_strict_mode) {
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMAL32};
        DataSet data_set = {
                {{Decimal32 {1}}, UInt8(1)},    {{Decimal32 {0}}, UInt8(0)},
                {{Decimal32 {+0}}, UInt8(0)},   {{Decimal32 {-0}}, UInt8(0)},
                {{Decimal32 {-1}}, UInt8(1)},   {{Decimal32 {0.5}}, UInt8(1)},
                {{Decimal32 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMAL64};
        DataSet data_set = {
                {{Decimal64 {1}}, UInt8(1)},    {{Decimal64 {0}}, UInt8(0)},
                {{Decimal64 {+0}}, UInt8(0)},   {{Decimal64 {-0}}, UInt8(0)},
                {{Decimal64 {-1}}, UInt8(1)},   {{Decimal64 {0.5}}, UInt8(1)},
                {{Decimal64 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMALV2};
        DataSet data_set = {
                {{Decimal128V2 {1}}, UInt8(1)},    {{Decimal128V2 {0}}, UInt8(0)},
                {{Decimal128V2 {+0}}, UInt8(0)},   {{Decimal128V2 {-0}}, UInt8(0)},
                {{Decimal128V2 {-1}}, UInt8(1)},   {{Decimal128V2 {0.5}}, UInt8(1)},
                {{Decimal128V2 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMAL128I};
        DataSet data_set = {
                {{Decimal128V3 {1}}, UInt8(1)},    {{Decimal128V3 {0}}, UInt8(0)},
                {{Decimal128V3 {+0}}, UInt8(0)},   {{Decimal128V3 {-0}}, UInt8(0)},
                {{Decimal128V3 {-1}}, UInt8(1)},   {{Decimal128V3 {0.5}}, UInt8(1)},
                {{Decimal128V3 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DECIMAL256};
        DataSet data_set = {
                {{Decimal256 {1}}, UInt8(1)},    {{Decimal256 {0}}, UInt8(0)},
                {{Decimal256 {+0}}, UInt8(0)},   {{Decimal256 {-0}}, UInt8(0)},
                {{Decimal256 {-1}}, UInt8(1)},   {{Decimal256 {0.5}}, UInt8(1)},
                {{Decimal256 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast_strict_mode<DataTypeBool>(input_types, data_set);
    }
}

} // namespace doris::vectorized

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
        InputTypeSet input_types = {TypeIndex::Int32};
        DataSet data_set = {
                {{Int32 {1}}, UInt8(1)},
                {{Int32 {0}}, UInt8(0)},
                {{Int32 {-1}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Int64};
        DataSet data_set = {
                {{Int64 {1}}, UInt8(1)},
                {{Int64 {0}}, UInt8(0)},
                {{Int64 {-1}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Int128};
        DataSet data_set = {
                {{Int128 {1}}, UInt8(1)},
                {{Int128 {0}}, UInt8(0)},
                {{Int128 {-1}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_string_to_bool) {
    InputTypeSet input_types = {TypeIndex::String};
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
            {{std::string(" 1.1111")}, uint8_t(1)},
            {{std::string(" 0")}, uint8_t(0)},
            {{std::string("  true")}, uint8_t(1)},
            {{std::string("  false")}, uint8_t(0)},
            {{std::string("  null")}, Null()},
            {{std::string("  abc")}, Null()},
            {{std::string("  ")}, Null()},
            {{std::string("  TrUE  ")}, uint8_t(1)},
            {{std::string("  fAlsE  ")}, uint8_t(0)},

    };
    check_function_for_cast<DataTypeBool>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_from_bool_to_string) {
    InputTypeSet input_types = {TypeIndex::UInt8};
    DataSet data_set = {
            {{UInt8 {1}}, std::string("1")},
            {{UInt8 {0}}, std::string("0")},
    };
    check_function_for_cast<DataTypeString>(input_types, data_set);
}

TEST_F(FunctionCastTest, test_from_bool_to_int) {
    {
        InputTypeSet input_types = {TypeIndex::UInt8};
        DataSet data_set = {
                {{UInt8 {1}}, Int32(1)},
                {{UInt8 {0}}, Int32(0)},
        };
        check_function_for_cast<DataTypeInt32>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::UInt8};
        DataSet data_set = {
                {{UInt8 {1}}, Int64(1)},
                {{UInt8 {0}}, Int64(0)},
        };
        check_function_for_cast<DataTypeInt64>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_float_to_bool) {
    {
        InputTypeSet input_types = {TypeIndex::Float32};
        DataSet data_set = {
                {{Float32 {1.0}}, UInt8(1)},  {{Float32 {0.0}}, UInt8(0)},
                {{Float32 {+0.0}}, UInt8(0)}, {{Float32 {-0.0}}, UInt8(0)},
                {{Float32 {-1.0}}, UInt8(1)}, {{Float32 {0.5}}, UInt8(1)},
                {{Float32 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Float64};
        DataSet data_set = {
                {{Float64 {1.0}}, UInt8(1)},  {{Float64 {0.0}}, UInt8(0)},
                {{Float64 {+0.0}}, UInt8(0)}, {{Float64 {-0.0}}, UInt8(0)},
                {{Float64 {-1.0}}, UInt8(1)}, {{Float64 {0.5}}, UInt8(1)},
                {{Float64 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, test_from_decimal_to_bool) {
    {
        InputTypeSet input_types = {TypeIndex::Decimal32};
        DataSet data_set = {
                {{Decimal32 {1}}, UInt8(1)},    {{Decimal32 {0}}, UInt8(0)},
                {{Decimal32 {+0}}, UInt8(0)},   {{Decimal32 {-0}}, UInt8(0)},
                {{Decimal32 {-1}}, UInt8(1)},   {{Decimal32 {0.5}}, UInt8(1)},
                {{Decimal32 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
    {
        InputTypeSet input_types = {TypeIndex::Decimal64};
        DataSet data_set = {
                {{Decimal64 {1}}, UInt8(1)},    {{Decimal64 {0}}, UInt8(0)},
                {{Decimal64 {+0}}, UInt8(0)},   {{Decimal64 {-0}}, UInt8(0)},
                {{Decimal64 {-1}}, UInt8(1)},   {{Decimal64 {0.5}}, UInt8(1)},
                {{Decimal64 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }

    {
        InputTypeSet input_types = {TypeIndex::Decimal128V2};
        DataSet data_set = {
                {{Decimal128V2 {1}}, UInt8(1)},    {{Decimal128V2 {0}}, UInt8(0)},
                {{Decimal128V2 {+0}}, UInt8(0)},   {{Decimal128V2 {-0}}, UInt8(0)},
                {{Decimal128V2 {-1}}, UInt8(1)},   {{Decimal128V2 {0.5}}, UInt8(1)},
                {{Decimal128V2 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }

    {
        InputTypeSet input_types = {TypeIndex::Decimal128V3};
        DataSet data_set = {
                {{Decimal128V3 {1}}, UInt8(1)},    {{Decimal128V3 {0}}, UInt8(0)},
                {{Decimal128V3 {+0}}, UInt8(0)},   {{Decimal128V3 {-0}}, UInt8(0)},
                {{Decimal128V3 {-1}}, UInt8(1)},   {{Decimal128V3 {0.5}}, UInt8(1)},
                {{Decimal128V3 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }

    {
        InputTypeSet input_types = {TypeIndex::Decimal256};
        DataSet data_set = {
                {{Decimal256 {1}}, UInt8(1)},    {{Decimal256 {0}}, UInt8(0)},
                {{Decimal256 {+0}}, UInt8(0)},   {{Decimal256 {-0}}, UInt8(0)},
                {{Decimal256 {-1}}, UInt8(1)},   {{Decimal256 {0.5}}, UInt8(1)},
                {{Decimal256 {-0.5}}, UInt8(1)},
        };
        check_function_for_cast<DataTypeBool>(input_types, data_set);
    }
}

} // namespace doris::vectorized
